use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use std::time::Instant;

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{unbounded, Receiver, Sender};
use zippy_core::{
    Result as CoreResult, SchemaRef, Source, SourceEvent, SourceHandle, SourceMode, SourceSink,
    StreamHello, ZippyError,
};

use crate::driver_ctp::Ctp2rsMdDriver;
use crate::metrics::OpenCtpSourceMetrics;
use crate::normalize::{normalize_tick, NormalizedTickRow, NormalizeError, RawTickSnapshot};
use crate::schema::tick_data_schema;

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
    pub rows_per_batch: usize,
    pub flush_interval_ms: u64,
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
            .field("rows_per_batch", &self.rows_per_batch)
            .field("flush_interval_ms", &self.flush_interval_ms)
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
            rows_per_batch: 1,
            flush_interval_ms: 0,
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
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normalize(error) => write!(f, "normalize tick failed: {error}"),
            Self::Arrow(error) => write!(f, "build record batch failed: {error}"),
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

/// Minimal batching state machine used by Task 4 runtime tests.
pub struct BufferedTickEmitter {
    schema: Arc<arrow::datatypes::Schema>,
    rows_per_batch: usize,
    flush_interval_ms: u64,
    buffer: Vec<NormalizedTickRow>,
    last_flush_at: Option<Instant>,
}

impl BufferedTickEmitter {
    pub fn new(
        schema: Arc<arrow::datatypes::Schema>,
        rows_per_batch: usize,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            schema,
            rows_per_batch,
            flush_interval_ms,
            buffer: Vec::new(),
            last_flush_at: None,
        }
    }

    pub fn push_tick(&mut self, row: NormalizedTickRow) -> Result<Option<RecordBatch>, SourceError> {
        self.push_tick_at(row, Instant::now())
    }

    pub fn push_tick_at(
        &mut self,
        row: NormalizedTickRow,
        now: Instant,
    ) -> Result<Option<RecordBatch>, SourceError> {
        if self.buffer.is_empty() {
            self.last_flush_at = Some(now);
        }

        self.buffer.push(row);
        if self.buffer.len() >= self.rows_per_batch.max(1) {
            return self.flush_at(now);
        }

        Ok(None)
    }

    pub fn flush_if_due(&mut self, now: Instant) -> Result<Option<RecordBatch>, SourceError> {
        if self.flush_interval_ms == 0 || self.buffer.is_empty() {
            return Ok(None);
        }

        let Some(last_flush_at) = self.last_flush_at else {
            return Ok(None);
        };

        if now.duration_since(last_flush_at).as_millis() as u64 >= self.flush_interval_ms {
            return self.flush_at(now);
        }

        Ok(None)
    }

    pub fn flush(&mut self) -> Result<Option<RecordBatch>, SourceError> {
        self.flush_at(Instant::now())
    }

    fn flush_at(&mut self, now: Instant) -> Result<Option<RecordBatch>, SourceError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let rows = self.buffer.drain(..).collect::<Vec<_>>();
        let batch = normalized_rows_to_record_batch(self.schema.clone(), rows)?;
        self.last_flush_at = Some(now);
        Ok(Some(batch))
    }
}

/// Minimal fake runtime used to drive batching tests without any external dependency.
pub struct FakeOpenCtpSourceRuntime {
    emitter: BufferedTickEmitter,
}

impl FakeOpenCtpSourceRuntime {
    pub fn new(config: OpenCtpMarketDataSourceConfig) -> Self {
        Self {
            emitter: BufferedTickEmitter::new(
                tick_data_schema(),
                config.rows_per_batch,
                config.flush_interval_ms,
            ),
        }
    }

    pub fn push_tick(&mut self, raw: RawTickSnapshot) -> Result<Option<RecordBatch>, SourceError> {
        let row = normalize_tick(&raw)?;
        self.emitter.push_tick(row)
    }

    pub fn push_tick_at(
        &mut self,
        raw: RawTickSnapshot,
        now: Instant,
    ) -> Result<Option<RecordBatch>, SourceError> {
        let row = normalize_tick(&raw)?;
        self.emitter.push_tick_at(row, now)
    }

    pub fn flush_if_due(&mut self, now: Instant) -> Result<Option<RecordBatch>, SourceError> {
        self.emitter.flush_if_due(now)
    }
}

pub enum MdDriverEvent {
    Tick(RawTickSnapshot),
    Flush,
    Stop,
    Error(String),
}

type MdDriverStopFn = Box<dyn FnMut() -> CoreResult<()> + Send>;

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
    pub fn emit_sample_sequence(&self) -> CoreResult<()> {
        self.tx
            .send(MdDriverEvent::Tick(sample_tick_snapshot()))
            .map_err(|_| ZippyError::ChannelSend)?;
        self.tx
            .send(MdDriverEvent::Stop)
            .map_err(|_| ZippyError::ChannelSend)
    }
}

pub struct OpenCtpMarketDataSource {
    config: OpenCtpMarketDataSourceConfig,
    schema: SchemaRef,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
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
            status: Arc::new(Mutex::new(OpenCtpSourceStatus::Created)),
            driver,
        }
    }

    pub fn metrics(&self) -> OpenCtpSourceMetrics {
        self.metrics.lock().unwrap().clone()
    }

    pub fn status(&self) -> OpenCtpSourceStatus {
        *self.status.lock().unwrap()
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
        let source_metrics = metrics.clone();
        let source_status = status.clone();

        let join_handle = thread::spawn(move || -> CoreResult<()> {
            set_status(&source_status, OpenCtpSourceStatus::Running);
            sink.emit(SourceEvent::Hello(StreamHello::new(
                "openctp.tick",
                schema.clone(),
                1,
            )?))?;

            let mut emitter = BufferedTickEmitter::new(
                schema,
                config.rows_per_batch,
                config.flush_interval_ms,
            );

            let runtime_result =
                run_driver_event_loop(rx, sink, &mut emitter, &source_metrics, &source_status);
            let driver_result = join_driver_handle(driver_join_handle);

            match (runtime_result, driver_result) {
                (Err(err), _) => {
                    set_status(&source_status, OpenCtpSourceStatus::Failed);
                    Err(err)
                }
                (Ok(()), Err(err)) => {
                    set_status(&source_status, OpenCtpSourceStatus::Failed);
                    Err(err)
                }
                (Ok(()), Ok(())) => Ok(()),
            }
        });

        match driver_stop_fn {
            Some(stop_fn) => Ok(SourceHandle::new_with_stop(join_handle, stop_fn)),
            None => Ok(SourceHandle::new(join_handle)),
        }
    }
}

fn normalized_rows_to_record_batch(
    schema: Arc<arrow::datatypes::Schema>,
    rows: Vec<NormalizedTickRow>,
) -> Result<RecordBatch, SourceError> {
    let instrument_ids = StringArray::from(
        rows.iter()
            .map(|row| row.instrument_id.as_str())
            .collect::<Vec<_>>(),
    );
    let exchange_ids = StringArray::from(
        rows.iter()
            .map(|row| row.exchange_id.as_deref())
            .collect::<Vec<_>>(),
    );
    let trading_days = StringArray::from(
        rows.iter()
            .map(|row| row.trading_day.as_deref())
            .collect::<Vec<_>>(),
    );
    let action_days = StringArray::from(
        rows.iter()
            .map(|row| row.action_day.as_deref())
            .collect::<Vec<_>>(),
    );
    let dts = TimestampNanosecondArray::from(rows.iter().map(|row| row.dt_ns).collect::<Vec<_>>())
        .with_timezone("UTC");
    let last_prices = Float64Array::from(rows.iter().map(|row| row.last_price).collect::<Vec<_>>());
    let volumes = Int64Array::from(rows.iter().map(|row| row.volume).collect::<Vec<_>>());
    let turnovers = Float64Array::from(rows.iter().map(|row| row.turnover).collect::<Vec<_>>());
    let open_interests =
        Float64Array::from(rows.iter().map(|row| row.open_interest).collect::<Vec<_>>());
    let bid_prices = Float64Array::from(rows.iter().map(|row| row.bid_price_1).collect::<Vec<_>>());
    let bid_volumes =
        Int64Array::from(rows.iter().map(|row| row.bid_volume_1).collect::<Vec<_>>());
    let ask_prices = Float64Array::from(rows.iter().map(|row| row.ask_price_1).collect::<Vec<_>>());
    let ask_volumes =
        Int64Array::from(rows.iter().map(|row| row.ask_volume_1).collect::<Vec<_>>());

    let columns: Vec<ArrayRef> = vec![
        Arc::new(instrument_ids),
        Arc::new(exchange_ids),
        Arc::new(trading_days),
        Arc::new(action_days),
        Arc::new(dts),
        Arc::new(last_prices),
        Arc::new(volumes),
        Arc::new(turnovers),
        Arc::new(open_interests),
        Arc::new(bid_prices),
        Arc::new(bid_volumes),
        Arc::new(ask_prices),
        Arc::new(ask_volumes),
    ];

    RecordBatch::try_new(schema, columns).map_err(SourceError::from)
}

fn run_driver_event_loop(
    rx: Receiver<MdDriverEvent>,
    sink: Arc<dyn SourceSink>,
    emitter: &mut BufferedTickEmitter,
    metrics: &Arc<Mutex<OpenCtpSourceMetrics>>,
    status: &Arc<Mutex<OpenCtpSourceStatus>>,
) -> CoreResult<()> {
    while let Ok(event) = rx.recv() {
        match event {
            MdDriverEvent::Tick(raw) => {
                metrics.lock().unwrap().ticks_received_total += 1;
                let row = normalize_tick(&raw).map_err(|error| map_source_error(error.into()))?;
                if let Some(batch) = emitter.push_tick(row).map_err(map_source_error)? {
                    record_batch_emission(metrics, &batch);
                    sink.emit(SourceEvent::Data(batch))?;
                }
            }
            MdDriverEvent::Flush => {
                if let Some(batch) = emitter.flush().map_err(map_source_error)? {
                    record_batch_emission(metrics, &batch);
                    sink.emit(SourceEvent::Data(batch))?;
                }
                sink.emit(SourceEvent::Flush)?;
            }
            MdDriverEvent::Stop => {
                if let Some(batch) = emitter.flush().map_err(map_source_error)? {
                    record_batch_emission(metrics, &batch);
                    sink.emit(SourceEvent::Data(batch))?;
                }
                set_status(status, OpenCtpSourceStatus::Stopped);
                sink.emit(SourceEvent::Stop)?;
                return Ok(());
            }
            MdDriverEvent::Error(reason) => {
                set_status(status, OpenCtpSourceStatus::Failed);
                sink.emit(SourceEvent::Error(reason.clone()))?;
                return Err(ZippyError::Io { reason });
            }
        }
    }

    Ok(())
}

fn record_batch_emission(metrics: &Arc<Mutex<OpenCtpSourceMetrics>>, batch: &RecordBatch) {
    let mut metrics = metrics.lock().unwrap();
    metrics.ticks_emitted_total += batch.num_rows() as u64;
    metrics.batches_emitted_total += 1;
}

fn map_source_error(error: SourceError) -> ZippyError {
    ZippyError::Io {
        reason: error.to_string(),
    }
}

fn join_driver_handle(join_handle: JoinHandle<CoreResult<()>>) -> CoreResult<()> {
    join_handle.join().map_err(|_| ZippyError::Io {
        reason: "md driver thread panicked".to_string(),
    })?
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
