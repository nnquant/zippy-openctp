use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{unbounded, Receiver, Sender};
use tracing::{debug, error, info, warn};
use zippy_core::{
    Result as CoreResult, SchemaRef, SegmentTableView, Source, SourceEvent, SourceHandle,
    SourceMode, SourceSink, StreamHello, ZippyError,
};
use zippy_segment_store::{ActiveSegmentReader, ZippySegmentStoreError};

use crate::driver_ctp::Ctp2rsMdDriver;
use crate::metrics::OpenCtpSourceMetrics;
use crate::normalize::{normalize_tick, NormalizeError, NormalizedTickRow, RawTickSnapshot};
use crate::schema::tick_data_schema;
use crate::segment_ingress::{OpenCtpSegmentDebugMetrics, OpenCtpSegmentIngress};

const MAX_TICKS_PER_EMIT: usize = 32768;

fn openctp_debug_enabled() -> bool {
    match std::env::var("OPENCTP_DEBUG") {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => false,
    }
}

fn openctp_debug_log(message: &str) {
    if openctp_debug_enabled() {
        debug!(
            component = "openctp_source",
            event = "debug",
            message = message,
            "{message}"
        );
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OpenCtpMarketDataSourceConfig {
    pub front: String,
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
    pub instruments: Vec<String>,
    pub flow_path: String,
    pub reconnect: bool,
    pub login_timeout_sec: u64,
}

impl Debug for OpenCtpMarketDataSourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenCtpMarketDataSourceConfig")
            .field("front", &self.front)
            .field("broker_id", &self.broker_id)
            .field("user_id", &self.user_id)
            .field("password", &"***redacted***")
            .field("instruments", &self.instruments)
            .field("flow_path", &self.flow_path)
            .field("reconnect", &self.reconnect)
            .field("login_timeout_sec", &self.login_timeout_sec)
            .finish()
    }
}

impl OpenCtpMarketDataSourceConfig {
    /// Build the default low-latency source preset used by the initial plugin path.
    pub fn new(
        front: String,
        broker_id: String,
        user_id: String,
        password: String,
        instruments: Vec<String>,
        flow_path: String,
    ) -> Self {
        Self::low_latency(front, broker_id, user_id, password, instruments, flow_path)
    }

    /// Explicit low-latency preset alias for future callers that want a named default.
    pub fn low_latency(
        front: String,
        broker_id: String,
        user_id: String,
        password: String,
        instruments: Vec<String>,
        flow_path: String,
    ) -> Self {
        Self {
            front,
            broker_id,
            user_id,
            password,
            instruments,
            flow_path,
            reconnect: true,
            login_timeout_sec: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenCtpSourceStatus {
    Created,
    Connecting,
    Running,
    Degraded,
    Stopped,
    Failed,
}

impl OpenCtpSourceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Connecting => "connecting",
            Self::Running => "running",
            Self::Degraded => "degraded",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug)]
pub enum SourceError {
    Normalize(NormalizeError),
    Arrow(arrow::error::ArrowError),
    Clock(String),
    Segment(&'static str),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normalize(error) => write!(f, "normalize tick failed: {error}"),
            Self::Arrow(error) => write!(f, "build record batch failed: {error}"),
            Self::Clock(reason) => write!(f, "sample localtime_ns failed: {reason}"),
            Self::Segment(reason) => write!(f, "segment ingress failed: {reason}"),
        }
    }
}

impl std::error::Error for SourceError {}

impl From<NormalizeError> for SourceError {
    fn from(value: NormalizeError) -> Self {
        Self::Normalize(value)
    }
}

impl From<arrow::error::ArrowError> for SourceError {
    fn from(value: arrow::error::ArrowError) -> Self {
        Self::Arrow(value)
    }
}

pub enum MdDriverEvent {
    Tick(RawTickSnapshot),
    Ticks(Vec<RawTickSnapshot>),
    Rows(Vec<NormalizedTickRow>),
    SubscriptionOutcome(SubscriptionOutcome),
    ReconnectUpdate(ReconnectUpdate),
    Flush,
    Stop,
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectUpdate {
    pub reconnects_total: u64,
    pub status: OpenCtpSourceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOutcome {
    pub succeeded_instruments: Vec<String>,
    pub failed_instruments: Vec<String>,
    pub subscribe_failures_total: u64,
    pub status: OpenCtpSourceStatus,
}

pub fn evaluate_subscription_results(
    requested: &[String],
    succeeded: &[String],
) -> SubscriptionOutcome {
    let mut succeeded_instruments = Vec::new();
    let mut failed_instruments = Vec::new();

    for instrument in requested {
        if succeeded.iter().any(|item| item == instrument) {
            succeeded_instruments.push(instrument.clone());
        } else {
            failed_instruments.push(instrument.clone());
        }
    }

    let subscribe_failures_total = failed_instruments.len() as u64;
    let status = if subscribe_failures_total == 0 {
        OpenCtpSourceStatus::Running
    } else {
        OpenCtpSourceStatus::Degraded
    };

    SubscriptionOutcome {
        succeeded_instruments,
        failed_instruments,
        subscribe_failures_total,
        status,
    }
}

#[derive(Debug, Clone)]
pub struct ReconnectState {
    reconnect_interval: Duration,
    reconnects_total: u64,
    status: OpenCtpSourceStatus,
    disconnected_at: Option<Instant>,
}

impl ReconnectState {
    pub fn new(reconnect_interval: Duration) -> Self {
        Self {
            reconnect_interval,
            reconnects_total: 0,
            status: OpenCtpSourceStatus::Running,
            disconnected_at: None,
        }
    }

    pub fn status(&self) -> OpenCtpSourceStatus {
        self.status
    }

    pub fn reconnect_interval(&self) -> Duration {
        self.reconnect_interval
    }

    pub fn reconnects_total(&self) -> u64 {
        self.reconnects_total
    }

    pub fn mark_disconnected_at(&mut self, now: Instant) {
        self.status = OpenCtpSourceStatus::Degraded;
        self.disconnected_at = Some(now);
    }

    pub fn ready_to_reconnect_at(&self, now: Instant) -> bool {
        match self.disconnected_at {
            Some(disconnected_at) => now.duration_since(disconnected_at) >= self.reconnect_interval,
            None => true,
        }
    }

    pub fn remaining_until_reconnect_at(&self, now: Instant) -> Duration {
        match self.disconnected_at {
            Some(disconnected_at) => {
                let elapsed = now.saturating_duration_since(disconnected_at);
                self.reconnect_interval.saturating_sub(elapsed)
            }
            None => Duration::ZERO,
        }
    }

    pub fn mark_reconnected(&mut self) {
        self.reconnects_total += 1;
        self.status = OpenCtpSourceStatus::Running;
        self.disconnected_at = None;
    }

    pub fn snapshot(&self) -> ReconnectUpdate {
        ReconnectUpdate {
            reconnects_total: self.reconnects_total,
            status: self.status,
        }
    }
}

type MdDriverStopFn = Box<dyn FnMut() -> CoreResult<()> + Send>;
type SharedMdDriverStopFn = Arc<Mutex<Option<MdDriverStopFn>>>;

pub struct MdDriverHandle {
    join_handle: JoinHandle<CoreResult<()>>,
    stop_fn: Option<MdDriverStopFn>,
}

impl MdDriverHandle {
    pub fn new(join_handle: JoinHandle<CoreResult<()>>) -> Self {
        Self {
            join_handle,
            stop_fn: None,
        }
    }

    pub fn new_with_stop(join_handle: JoinHandle<CoreResult<()>>, stop_fn: MdDriverStopFn) -> Self {
        Self {
            join_handle,
            stop_fn: Some(stop_fn),
        }
    }

    fn into_parts(self) -> (JoinHandle<CoreResult<()>>, Option<MdDriverStopFn>) {
        (self.join_handle, self.stop_fn)
    }
}

pub trait MdDriver: Send + 'static {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle>;
}

pub struct FakeMdDriver {
    rx: Receiver<MdDriverEvent>,
}

#[derive(Clone)]
pub struct FakeMdDriverHandle {
    tx: Sender<MdDriverEvent>,
}

impl FakeMdDriver {
    pub fn pair() -> (Self, FakeMdDriverHandle) {
        let (tx, rx) = unbounded();
        (Self { rx }, FakeMdDriverHandle { tx })
    }
}

impl MdDriver for FakeMdDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            while let Ok(event) = self.rx.recv() {
                let should_stop = matches!(event, MdDriverEvent::Stop);
                tx.send(event).map_err(|_| ZippyError::ChannelSend)?;
                if should_stop {
                    return Ok(());
                }
            }

            Err(ZippyError::ChannelReceive)
        });

        Ok(MdDriverHandle::new(join_handle))
    }
}

impl FakeMdDriverHandle {
    pub fn emit_trade_tick(&self, instrument_id: &str, last_price: f64) -> CoreResult<()> {
        self.tx
            .send(MdDriverEvent::Tick(RawTickSnapshot::for_test(
                instrument_id,
                last_price,
            )))
            .map_err(|_| ZippyError::ChannelSend)
    }

    pub fn emit_stop(&self) -> CoreResult<()> {
        self.tx
            .send(MdDriverEvent::Stop)
            .map_err(|_| ZippyError::ChannelSend)
    }

    pub fn emit_sample_sequence(&self) -> CoreResult<()> {
        self.tx
            .send(MdDriverEvent::Tick(sample_tick_snapshot()))
            .map_err(|_| ZippyError::ChannelSend)?;
        self.tx
            .send(MdDriverEvent::Stop)
            .map_err(|_| ZippyError::ChannelSend)
    }
}

pub trait OpenCtpSegmentDescriptorPublisher: Send + Sync + 'static {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> CoreResult<()>;
}

pub struct OpenCtpMarketDataSource {
    config: OpenCtpMarketDataSourceConfig,
    schema: SchemaRef,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    segment_debug_metrics: Arc<Mutex<Option<OpenCtpSegmentDebugMetrics>>>,
    segment_descriptor_publisher: Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
    status: Arc<Mutex<OpenCtpSourceStatus>>,
    driver: Box<dyn MdDriver>,
}

impl OpenCtpMarketDataSource {
    pub fn new(config: OpenCtpMarketDataSourceConfig) -> Self {
        Self::from_driver(config.clone(), Box::new(Ctp2rsMdDriver::new(config)))
    }

    pub fn from_driver(config: OpenCtpMarketDataSourceConfig, driver: Box<dyn MdDriver>) -> Self {
        Self {
            config,
            schema: tick_data_schema(),
            metrics: Arc::new(Mutex::new(OpenCtpSourceMetrics::default())),
            segment_debug_metrics: Arc::new(Mutex::new(None)),
            segment_descriptor_publisher: None,
            status: Arc::new(Mutex::new(OpenCtpSourceStatus::Created)),
            driver,
        }
    }

    pub fn metrics(&self) -> OpenCtpSourceMetrics {
        self.metrics.lock().unwrap().clone()
    }

    pub fn metrics_handle(&self) -> Arc<Mutex<OpenCtpSourceMetrics>> {
        self.metrics.clone()
    }

    pub fn status(&self) -> OpenCtpSourceStatus {
        *self.status.lock().unwrap()
    }

    pub fn segment_debug_metrics(&self) -> Option<OpenCtpSegmentDebugMetrics> {
        self.segment_debug_metrics.lock().unwrap().clone()
    }

    pub fn segment_debug_metrics_handle(&self) -> Arc<Mutex<Option<OpenCtpSegmentDebugMetrics>>> {
        self.segment_debug_metrics.clone()
    }

    pub fn with_segment_descriptor_publisher(
        mut self,
        publisher: Arc<dyn OpenCtpSegmentDescriptorPublisher>,
    ) -> Self {
        self.segment_descriptor_publisher = Some(publisher);
        self
    }

    pub fn status_handle(&self) -> Arc<Mutex<OpenCtpSourceStatus>> {
        self.status.clone()
    }
}

impl Source for OpenCtpMarketDataSource {
    fn name(&self) -> &str {
        "openctp-market-data-source"
    }

    fn output_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn mode(&self) -> SourceMode {
        SourceMode::Pipeline
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> CoreResult<SourceHandle> {
        let Self {
            config,
            schema,
            metrics,
            segment_debug_metrics,
            segment_descriptor_publisher,
            status,
            driver,
        } = *self;

        let (tx, rx) = unbounded();
        set_status(&status, OpenCtpSourceStatus::Connecting);
        let driver_handle = match driver.start(tx) {
            Ok(handle) => handle,
            Err(error) => {
                set_status(&status, OpenCtpSourceStatus::Failed);
                return Err(error);
            }
        };
        let (driver_join_handle, driver_stop_fn) = driver_handle.into_parts();
        let shared_driver_stop_fn =
            driver_stop_fn.map(|stop_fn| Arc::new(Mutex::new(Some(stop_fn))));
        let source_driver_stop_fn = shared_driver_stop_fn.clone();
        let source_stop_requested = Arc::new(AtomicBool::new(false));
        let source_thread_stop_requested = Arc::clone(&source_stop_requested);
        let source_metrics = metrics.clone();
        let source_segment_debug_metrics = segment_debug_metrics.clone();
        let source_status = status.clone();
        let source_front = config.front.clone();
        let source_instrument_count = config.instruments.len();

        let join_handle = thread::spawn(move || -> CoreResult<()> {
            openctp_debug_log("source thread started");
            let runtime_result = (|| -> CoreResult<()> {
                set_status(&source_status, OpenCtpSourceStatus::Running);
                info!(
                    component = "openctp_source",
                    event = "source_start",
                    front = %source_front,
                    instrument_count = source_instrument_count,
                    message = "market data source thread started"
                );
                sink.emit(SourceEvent::Hello(StreamHello::new(
                    "openctp.tick",
                    schema.clone(),
                    1,
                )?))?;

                let mut segment_ingress =
                    OpenCtpSegmentIngress::new_for_source().map_err(|reason| ZippyError::Io {
                        reason: reason.to_string(),
                    })?;
                publish_segment_descriptor_if_needed(
                    &segment_ingress,
                    &segment_descriptor_publisher,
                )?;
                let mut segment_reader = segment_ingress
                    .active_reader()
                    .map_err(segment_zippy_error)?;

                run_driver_event_loop(
                    rx,
                    DriverEventLoopContext {
                        sink,
                        segment_ingress: &mut segment_ingress,
                        segment_reader: &mut segment_reader,
                        metrics: &source_metrics,
                        segment_debug_metrics: &source_segment_debug_metrics,
                        segment_descriptor_publisher: &segment_descriptor_publisher,
                        status: &source_status,
                        stop_requested: &source_thread_stop_requested,
                    },
                )
            })();
            openctp_debug_log(&format!(
                "source thread runtime_result=[{}]",
                runtime_result.is_ok()
            ));
            if let Err(error) = &runtime_result {
                if let Err(stop_error) = request_optional_driver_stop(&source_driver_stop_fn) {
                    warn!(
                        component = "openctp_source",
                        event = "driver_stop_failure",
                        error = %stop_error,
                        source_error = %error,
                        message = "market data source failed before driver stopped cleanly"
                    );
                }
            }
            let driver_result = join_driver_handle(driver_join_handle);
            openctp_debug_log(&format!(
                "source thread driver_result=[{}]",
                driver_result.is_ok()
            ));

            match (runtime_result, driver_result) {
                (Err(err), _) => {
                    set_status(&source_status, OpenCtpSourceStatus::Failed);
                    error!(
                        component = "openctp_source",
                        event = "source_failure",
                        error = %err,
                        message = "market data source thread failed"
                    );
                    Err(err)
                }
                (Ok(()), Err(err)) => {
                    set_status(&source_status, OpenCtpSourceStatus::Failed);
                    error!(
                        component = "openctp_source",
                        event = "driver_join_failure",
                        error = %err,
                        message = "market data driver join failed"
                    );
                    Err(err)
                }
                (Ok(()), Ok(())) => {
                    info!(
                        component = "openctp_source",
                        event = "source_stop",
                        status = OpenCtpSourceStatus::Stopped.as_str(),
                        message = "market data source thread stopped"
                    );
                    Ok(())
                }
            }
        });

        match shared_driver_stop_fn {
            Some(stop_fn) => {
                let stop_requested = Arc::clone(&source_stop_requested);
                Ok(SourceHandle::new_with_stop(
                    join_handle,
                    Box::new(move || {
                        stop_requested.store(true, Ordering::SeqCst);
                        request_shared_driver_stop(&stop_fn)
                    }),
                ))
            }
            None => Ok(SourceHandle::new(join_handle)),
        }
    }
}

struct DriverEventLoopContext<'a> {
    sink: Arc<dyn SourceSink>,
    segment_ingress: &'a mut OpenCtpSegmentIngress,
    segment_reader: &'a mut ActiveSegmentReader,
    metrics: &'a Arc<Mutex<OpenCtpSourceMetrics>>,
    segment_debug_metrics: &'a Arc<Mutex<Option<OpenCtpSegmentDebugMetrics>>>,
    segment_descriptor_publisher: &'a Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
    status: &'a Arc<Mutex<OpenCtpSourceStatus>>,
    stop_requested: &'a Arc<AtomicBool>,
}

fn run_driver_event_loop(
    rx: Receiver<MdDriverEvent>,
    context: DriverEventLoopContext<'_>,
) -> CoreResult<()> {
    let mut pending_event = None;
    loop {
        if context.stop_requested.load(Ordering::SeqCst) {
            openctp_debug_log("source loop stop requested");
            set_status(context.status, OpenCtpSourceStatus::Stopped);
            context.sink.emit(SourceEvent::Stop)?;
            return Ok(());
        }
        let event = match pending_event.take() {
            Some(event) => event,
            None => match rx.recv() {
                Ok(event) => event,
                Err(_) => break,
            },
        };
        match event {
            MdDriverEvent::Tick(raw) => {
                let raw_ticks = collect_tick_batch(raw, &rx, &mut pending_event);
                let tick_count = raw_ticks.len();
                handle_tick_batch(
                    raw_ticks,
                    &context.sink,
                    context.segment_ingress,
                    context.segment_reader,
                    context.metrics,
                    context.segment_descriptor_publisher,
                )?;

                record_ticks_received(context.metrics, tick_count);
                refresh_segment_debug_metrics(
                    context.segment_ingress,
                    context.segment_debug_metrics,
                )?;
                emit_segment_available(context.segment_reader, &context.sink, context.metrics)?;
            }
            MdDriverEvent::Ticks(raw_ticks) => {
                let raw_ticks = collect_tick_batches(raw_ticks, &rx, &mut pending_event);
                let tick_count = raw_ticks.len();
                handle_tick_batch(
                    raw_ticks,
                    &context.sink,
                    context.segment_ingress,
                    context.segment_reader,
                    context.metrics,
                    context.segment_descriptor_publisher,
                )?;

                record_ticks_received(context.metrics, tick_count);
                refresh_segment_debug_metrics(
                    context.segment_ingress,
                    context.segment_debug_metrics,
                )?;
                emit_segment_available(context.segment_reader, &context.sink, context.metrics)?;
            }
            MdDriverEvent::Rows(rows) => {
                let rows = collect_normalized_row_batches(rows, &rx, &mut pending_event);
                let tick_count = rows.len();
                handle_normalized_row_batch(
                    rows,
                    &context.sink,
                    context.segment_ingress,
                    context.segment_reader,
                    context.metrics,
                    context.segment_descriptor_publisher,
                )?;

                record_ticks_received(context.metrics, tick_count);
                refresh_segment_debug_metrics(
                    context.segment_ingress,
                    context.segment_debug_metrics,
                )?;
                emit_segment_available(context.segment_reader, &context.sink, context.metrics)?;
            }
            MdDriverEvent::SubscriptionOutcome(outcome) => {
                openctp_debug_log(&format!(
                    "source loop SubscriptionOutcome status=[{}] failures=[{}]",
                    outcome.status.as_str(),
                    outcome.subscribe_failures_total
                ));
                if outcome.subscribe_failures_total == 0 {
                    info!(
                        component = "openctp_source",
                        event = "subscribe_success",
                        succeeded_instruments = ?outcome.succeeded_instruments,
                        message = "market data subscribe completed successfully"
                    );
                } else {
                    warn!(
                        component = "openctp_source",
                        event = "subscribe_failure",
                        succeeded_instruments = ?outcome.succeeded_instruments,
                        failed_instruments = ?outcome.failed_instruments,
                        subscribe_failures_total = outcome.subscribe_failures_total,
                        message = "market data subscribe completed with failures"
                    );
                }
                context.metrics.lock().unwrap().subscribe_failures_total +=
                    outcome.subscribe_failures_total;
                set_status(context.status, outcome.status);
            }
            MdDriverEvent::ReconnectUpdate(update) => {
                openctp_debug_log(&format!(
                    "source loop ReconnectUpdate status=[{}] reconnects_total=[{}]",
                    update.status.as_str(),
                    update.reconnects_total
                ));
                match update.status {
                    OpenCtpSourceStatus::Degraded => warn!(
                        component = "openctp_source",
                        event = "reconnect",
                        reconnects_total = update.reconnects_total,
                        status = update.status.as_str(),
                        message = "market data source is reconnecting"
                    ),
                    OpenCtpSourceStatus::Running => info!(
                        component = "openctp_source",
                        event = "reconnect_success",
                        reconnects_total = update.reconnects_total,
                        status = update.status.as_str(),
                        message = "market data source reconnected"
                    ),
                    _ => {}
                }
                context.metrics.lock().unwrap().reconnects_total = update.reconnects_total;
                set_status(context.status, update.status);
            }
            MdDriverEvent::Flush => {
                openctp_debug_log("source loop Flush");
                debug!(
                    component = "openctp_source",
                    event = "flush",
                    message = "market data source flush received"
                );
                context.sink.emit(SourceEvent::Flush)?;
            }
            MdDriverEvent::Stop => {
                openctp_debug_log("source loop Stop");
                set_status(context.status, OpenCtpSourceStatus::Stopped);
                info!(
                    component = "openctp_source",
                    event = "stop",
                    status = OpenCtpSourceStatus::Stopped.as_str(),
                    message = "market data source stop received"
                );
                context.sink.emit(SourceEvent::Stop)?;
                return Ok(());
            }
            MdDriverEvent::Error(reason) => {
                openctp_debug_log(&format!("source loop Error reason=[{reason}]"));
                set_status(context.status, OpenCtpSourceStatus::Failed);
                error!(
                    component = "openctp_source",
                    event = "source_error",
                    error = %reason,
                    message = "market data source received driver error"
                );
                context.sink.emit(SourceEvent::Error(reason.clone()))?;
                return Err(ZippyError::Io { reason });
            }
        }
    }

    openctp_debug_log("source loop channel closed");
    warn!(
        component = "openctp_source",
        event = "channel_closed",
        message = "market data source event channel closed"
    );
    Ok(())
}

fn collect_tick_batch(
    raw: RawTickSnapshot,
    rx: &Receiver<MdDriverEvent>,
    pending_event: &mut Option<MdDriverEvent>,
) -> Vec<RawTickSnapshot> {
    collect_tick_batches(vec![raw], rx, pending_event)
}

fn collect_tick_batches(
    mut raw_ticks: Vec<RawTickSnapshot>,
    rx: &Receiver<MdDriverEvent>,
    pending_event: &mut Option<MdDriverEvent>,
) -> Vec<RawTickSnapshot> {
    if raw_ticks.len() > MAX_TICKS_PER_EMIT {
        let overflow = raw_ticks.split_off(MAX_TICKS_PER_EMIT);
        *pending_event = Some(MdDriverEvent::Ticks(overflow));
        return raw_ticks;
    }

    while raw_ticks.len() < MAX_TICKS_PER_EMIT {
        match rx.try_recv() {
            Ok(MdDriverEvent::Tick(raw)) => raw_ticks.push(raw),
            Ok(MdDriverEvent::Ticks(mut next_ticks)) => {
                let remaining = MAX_TICKS_PER_EMIT - raw_ticks.len();
                if next_ticks.len() <= remaining {
                    raw_ticks.append(&mut next_ticks);
                } else {
                    let overflow = next_ticks.split_off(remaining);
                    raw_ticks.append(&mut next_ticks);
                    *pending_event = Some(MdDriverEvent::Ticks(overflow));
                    break;
                }
            }
            Ok(event) => {
                *pending_event = Some(event);
                break;
            }
            Err(crossbeam_channel::TryRecvError::Empty) => break,
            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
        }
    }

    raw_ticks
}

fn collect_normalized_row_batches(
    mut rows: Vec<NormalizedTickRow>,
    rx: &Receiver<MdDriverEvent>,
    pending_event: &mut Option<MdDriverEvent>,
) -> Vec<NormalizedTickRow> {
    if rows.len() > MAX_TICKS_PER_EMIT {
        let overflow = rows.split_off(MAX_TICKS_PER_EMIT);
        *pending_event = Some(MdDriverEvent::Rows(overflow));
        return rows;
    }

    while rows.len() < MAX_TICKS_PER_EMIT {
        match rx.try_recv() {
            Ok(MdDriverEvent::Rows(mut next_rows)) => {
                let remaining = MAX_TICKS_PER_EMIT - rows.len();
                if next_rows.len() <= remaining {
                    rows.append(&mut next_rows);
                } else {
                    let overflow = next_rows.split_off(remaining);
                    rows.append(&mut next_rows);
                    *pending_event = Some(MdDriverEvent::Rows(overflow));
                    break;
                }
            }
            Ok(event) => {
                *pending_event = Some(event);
                break;
            }
            Err(crossbeam_channel::TryRecvError::Empty) => break,
            Err(crossbeam_channel::TryRecvError::Disconnected) => break,
        }
    }

    rows
}

fn handle_tick_batch(
    raw_ticks: Vec<RawTickSnapshot>,
    sink: &Arc<dyn SourceSink>,
    ingress: &mut OpenCtpSegmentIngress,
    reader: &mut ActiveSegmentReader,
    metrics: &Arc<Mutex<OpenCtpSourceMetrics>>,
    segment_descriptor_publisher: &Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
) -> CoreResult<()> {
    if raw_ticks.is_empty() {
        return Ok(());
    }

    let now_ns = sample_localtime_ns().map_err(map_source_error)?;
    let mut rows = Vec::with_capacity(raw_ticks.len());
    let mut skipped_ticks = 0_u64;
    for raw in raw_ticks {
        match normalize_tick(&raw) {
            Ok(mut row) => {
                row.localtime_ns = now_ns;
                row.source_emit_ns = now_ns;
                rows.push(row);
            }
            Err(error) => {
                skipped_ticks += 1;
                record_normalize_failure(metrics);
                if skipped_ticks <= 5 {
                    warn!(
                        component = "openctp_source",
                        event = "normalize_tick_skipped",
                        instrument_id = raw.instrument_id.as_str(),
                        trading_day = raw.trading_day.as_str(),
                        action_day = raw.action_day.as_str(),
                        update_time = raw.update_time.as_str(),
                        update_millisec = raw.update_millisec,
                        error = %error,
                        message = "skipped malformed market data tick"
                    );
                }
            }
        }
    }

    if rows.is_empty() {
        return Ok(());
    }

    handle_normalized_row_batch(
        rows,
        sink,
        ingress,
        reader,
        metrics,
        segment_descriptor_publisher,
    )
}

pub(crate) fn handle_normalized_row_batch(
    mut rows: Vec<NormalizedTickRow>,
    sink: &Arc<dyn SourceSink>,
    ingress: &mut OpenCtpSegmentIngress,
    reader: &mut ActiveSegmentReader,
    metrics: &Arc<Mutex<OpenCtpSourceMetrics>>,
    segment_descriptor_publisher: &Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
) -> CoreResult<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let now_ns = sample_localtime_ns().map_err(map_source_error)?;
    for row in &mut rows {
        row.localtime_ns = now_ns;
        row.source_emit_ns = now_ns;
    }

    let descriptor_changed = write_segment_rows(ingress, &rows, segment_descriptor_publisher)?;
    if descriptor_changed {
        emit_segment_available(reader, sink, metrics)?;
        ingress
            .update_active_reader(reader)
            .map_err(segment_zippy_error)?;
        release_internal_segments_if_unpublished(ingress, segment_descriptor_publisher);
    }
    Ok(())
}

fn release_internal_segments_if_unpublished(
    ingress: &mut OpenCtpSegmentIngress,
    segment_descriptor_publisher: &Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
) {
    if segment_descriptor_publisher.is_none() {
        ingress.release_retired_segments();
    }
}

fn write_segment_rows(
    ingress: &mut OpenCtpSegmentIngress,
    rows: &[NormalizedTickRow],
    segment_descriptor_publisher: &Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
) -> CoreResult<bool> {
    let active_identity_before = ingress.active_segment_identity();
    let descriptor_changed = ingress
        .write_rows(rows)
        .map_err(|reason| map_source_error(SourceError::Segment(reason)))?
        || ingress.active_segment_identity() != active_identity_before;
    if descriptor_changed {
        publish_segment_descriptor_if_needed(ingress, segment_descriptor_publisher)?;
    }
    Ok(descriptor_changed)
}

fn refresh_segment_debug_metrics(
    ingress: &OpenCtpSegmentIngress,
    segment_debug_metrics: &Arc<Mutex<Option<OpenCtpSegmentDebugMetrics>>>,
) -> CoreResult<()> {
    *segment_debug_metrics.lock().unwrap() = Some(
        ingress
            .debug_metrics()
            .map_err(|reason| map_source_error(SourceError::Segment(reason)))?,
    );
    Ok(())
}

fn emit_segment_available(
    reader: &mut ActiveSegmentReader,
    sink: &Arc<dyn SourceSink>,
    metrics: &Arc<Mutex<OpenCtpSourceMetrics>>,
) -> CoreResult<()> {
    let Some(span) = reader.read_available().map_err(segment_zippy_error)? else {
        return Ok(());
    };
    let rows = span.row_count();
    sink.emit(SourceEvent::Data(SegmentTableView::from_row_span(span)))?;
    record_data_emission(metrics, rows);
    Ok(())
}

fn publish_segment_descriptor_if_needed(
    ingress: &OpenCtpSegmentIngress,
    publisher: &Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
) -> CoreResult<()> {
    let Some(publisher) = publisher else {
        return Ok(());
    };
    let envelope = ingress
        .active_descriptor_envelope_bytes()
        .map_err(|reason| map_source_error(SourceError::Segment(reason)))?;
    publisher.publish(envelope)
}

fn sample_localtime_ns() -> Result<i64, SourceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| SourceError::Clock(error.to_string()))?;

    i64::try_from(duration.as_nanos()).map_err(|error| SourceError::Clock(error.to_string()))
}

fn record_ticks_received(metrics: &Arc<Mutex<OpenCtpSourceMetrics>>, rows: usize) {
    metrics.lock().unwrap().ticks_received_total += rows as u64;
}

fn record_normalize_failure(metrics: &Arc<Mutex<OpenCtpSourceMetrics>>) {
    metrics.lock().unwrap().normalize_failures_total += 1;
}

fn record_data_emission(metrics: &Arc<Mutex<OpenCtpSourceMetrics>>, rows: usize) {
    let mut metrics = metrics.lock().unwrap();
    metrics.ticks_emitted_total += rows as u64;
    metrics.batches_emitted_total += 1;
}

fn map_source_error(error: SourceError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn segment_zippy_error(error: ZippySegmentStoreError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn join_driver_handle(join_handle: JoinHandle<CoreResult<()>>) -> CoreResult<()> {
    join_handle.join().map_err(|_| ZippyError::Io {
        reason: "md driver thread panicked".to_string(),
    })?
}

fn request_optional_driver_stop(stop_fn: &Option<SharedMdDriverStopFn>) -> CoreResult<()> {
    let Some(stop_fn) = stop_fn else {
        return Ok(());
    };
    request_shared_driver_stop(stop_fn)
}

fn request_shared_driver_stop(stop_fn: &SharedMdDriverStopFn) -> CoreResult<()> {
    let mut guard = stop_fn.lock().unwrap();
    let Some(stop_fn) = guard.as_mut() else {
        return Ok(());
    };
    stop_fn()?;
    *guard = None;
    Ok(())
}

fn set_status(status: &Arc<Mutex<OpenCtpSourceStatus>>, next: OpenCtpSourceStatus) {
    *status.lock().unwrap() = next;
}

fn sample_tick_snapshot() -> RawTickSnapshot {
    RawTickSnapshot {
        instrument_id: "IF2506".to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "20260408".to_string(),
        action_day: "20260408".to_string(),
        update_time: "09:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume: 1,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    }
}
