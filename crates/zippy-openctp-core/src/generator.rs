use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender};
use zippy_core::{
    Result as CoreResult, SchemaRef, Source, SourceHandle, SourceMode, SourceSink, ZippyError,
};

use crate::metrics::OpenCtpSourceMetrics;
use crate::normalize::{compose_exchange_timestamp_ns, NormalizedTickRow, RawTickSnapshot};
use crate::schema::tick_data_schema;
use crate::segment_ingress::{OpenCtpColumnarTickBatch, OpenCtpSegmentIngress};
use crate::source::{
    MdDriver, MdDriverEvent, MdDriverHandle, OpenCtpMarketDataSource,
    OpenCtpMarketDataSourceConfig, OpenCtpSegmentDescriptorPublisher, OpenCtpSourceStatus,
};

use zippy_core::{SourceEvent, StreamHello};
use zippy_segment_store::ActiveSegmentReader;

const DEFAULT_EXCHANGE_ID: &str = "CFFEX";
const DEFAULT_BASE_PRICE: f64 = 4000.0;
const DEFAULT_PRICE_STEP: f64 = 0.2;
const DEFAULT_SESSION_START_MS: u64 = 9 * 3_600_000 + 30 * 60_000;
const COLUMNAR_TARGET_BATCH_ROWS: usize = 32_768;

#[derive(Debug, Clone, PartialEq)]
pub struct OpenCtpMarketGeneratorConfig {
    pub instruments: Vec<String>,
    pub interval_ms: u64,
    pub exchange_id: String,
    pub trading_day: String,
    pub action_day: String,
    pub seed: u64,
    pub base_price: f64,
    pub price_step: f64,
    pub max_ticks: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenCtpMarketGeneratorConfigError {
    EmptyInstruments,
    ZeroIntervalMs,
    InvalidBasePrice,
    InvalidPriceStep,
}

impl Display for OpenCtpMarketGeneratorConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyInstruments => write!(f, "instruments must not be empty"),
            Self::ZeroIntervalMs => write!(f, "interval_ms must be greater than zero"),
            Self::InvalidBasePrice => write!(f, "base_price must be positive and finite"),
            Self::InvalidPriceStep => write!(f, "price_step must be positive and finite"),
        }
    }
}

impl Error for OpenCtpMarketGeneratorConfigError {}

impl OpenCtpMarketGeneratorConfig {
    pub fn new(
        instruments: Vec<String>,
        interval_ms: u64,
    ) -> Result<Self, OpenCtpMarketGeneratorConfigError> {
        let instruments = normalize_instruments(instruments);
        if instruments.is_empty() {
            return Err(OpenCtpMarketGeneratorConfigError::EmptyInstruments);
        }
        if interval_ms == 0 {
            return Err(OpenCtpMarketGeneratorConfigError::ZeroIntervalMs);
        }

        let date = current_exchange_date_yyyymmdd();
        Ok(Self {
            instruments,
            interval_ms,
            exchange_id: DEFAULT_EXCHANGE_ID.to_string(),
            trading_day: date.clone(),
            action_day: date,
            seed: default_seed(),
            base_price: DEFAULT_BASE_PRICE,
            price_step: DEFAULT_PRICE_STEP,
            max_ticks: None,
        })
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    pub fn with_max_ticks(mut self, max_ticks: Option<u64>) -> Self {
        self.max_ticks = max_ticks;
        self
    }

    pub fn set_price_model(
        &mut self,
        base_price: f64,
        price_step: f64,
    ) -> Result<(), OpenCtpMarketGeneratorConfigError> {
        if !base_price.is_finite() || base_price <= 0.0 {
            return Err(OpenCtpMarketGeneratorConfigError::InvalidBasePrice);
        }
        if !price_step.is_finite() || price_step <= 0.0 {
            return Err(OpenCtpMarketGeneratorConfigError::InvalidPriceStep);
        }
        self.base_price = base_price;
        self.price_step = price_step;
        Ok(())
    }
}

pub struct OpenCtpMarketGeneratorDriver {
    config: OpenCtpMarketGeneratorConfig,
}

impl OpenCtpMarketGeneratorDriver {
    pub fn new(config: OpenCtpMarketGeneratorConfig) -> Self {
        Self { config }
    }
}

pub struct OpenCtpNormalizedGeneratorDriver {
    config: OpenCtpMarketGeneratorConfig,
}

impl OpenCtpNormalizedGeneratorDriver {
    pub fn new(config: OpenCtpMarketGeneratorConfig) -> Self {
        Self { config }
    }
}

impl MdDriver for OpenCtpMarketGeneratorDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let (stop_tx, stop_rx) = bounded::<()>(1);
        let config = self.config;
        let join_handle = thread::spawn(move || run_generator(config, tx, stop_rx));
        let stop_fn = Box::new(move || {
            let _ = stop_tx.try_send(());
            Ok(())
        });
        Ok(MdDriverHandle::new_with_stop(join_handle, stop_fn))
    }
}

impl MdDriver for OpenCtpNormalizedGeneratorDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let (stop_tx, stop_rx) = bounded::<()>(1);
        let config = self.config;
        let join_handle = thread::spawn(move || run_normalized_generator(config, tx, stop_rx));
        let stop_fn = Box::new(move || {
            let _ = stop_tx.try_send(());
            Ok(())
        });
        Ok(MdDriverHandle::new_with_stop(join_handle, stop_fn))
    }
}

pub struct OpenCtpMarketGeneratorSource {
    inner: OpenCtpMarketDataSource,
}

pub struct OpenCtpNormalizedGeneratorSource {
    inner: OpenCtpMarketDataSource,
}

pub struct OpenCtpColumnarGeneratorSource {
    config: OpenCtpMarketGeneratorConfig,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    status: Arc<Mutex<OpenCtpSourceStatus>>,
    segment_descriptor_publisher: Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
}

impl OpenCtpMarketGeneratorSource {
    pub fn new(config: OpenCtpMarketGeneratorConfig) -> Self {
        let source_config = OpenCtpMarketDataSourceConfig::low_latency(
            "generator://openctp".to_string(),
            "generator".to_string(),
            "generator".to_string(),
            String::new(),
            config.instruments.clone(),
            ".cache/openctp/generator".to_string(),
        );
        let driver = OpenCtpMarketGeneratorDriver::new(config);
        Self {
            inner: OpenCtpMarketDataSource::from_driver(source_config, Box::new(driver)),
        }
    }

    pub fn metrics(&self) -> OpenCtpSourceMetrics {
        self.inner.metrics()
    }

    pub fn metrics_handle(&self) -> Arc<Mutex<OpenCtpSourceMetrics>> {
        self.inner.metrics_handle()
    }

    pub fn status(&self) -> OpenCtpSourceStatus {
        self.inner.status()
    }

    pub fn status_handle(&self) -> Arc<Mutex<OpenCtpSourceStatus>> {
        self.inner.status_handle()
    }

    pub fn with_segment_descriptor_publisher(
        mut self,
        publisher: Arc<dyn OpenCtpSegmentDescriptorPublisher>,
    ) -> Self {
        self.inner = self.inner.with_segment_descriptor_publisher(publisher);
        self
    }
}

impl OpenCtpNormalizedGeneratorSource {
    pub fn new(config: OpenCtpMarketGeneratorConfig) -> Self {
        let source_config = generator_source_config(&config);
        let driver = OpenCtpNormalizedGeneratorDriver::new(config);
        Self {
            inner: OpenCtpMarketDataSource::from_driver(source_config, Box::new(driver)),
        }
    }

    pub fn metrics(&self) -> OpenCtpSourceMetrics {
        self.inner.metrics()
    }

    pub fn metrics_handle(&self) -> Arc<Mutex<OpenCtpSourceMetrics>> {
        self.inner.metrics_handle()
    }

    pub fn status(&self) -> OpenCtpSourceStatus {
        self.inner.status()
    }

    pub fn status_handle(&self) -> Arc<Mutex<OpenCtpSourceStatus>> {
        self.inner.status_handle()
    }

    pub fn with_segment_descriptor_publisher(
        mut self,
        publisher: Arc<dyn OpenCtpSegmentDescriptorPublisher>,
    ) -> Self {
        self.inner = self.inner.with_segment_descriptor_publisher(publisher);
        self
    }
}

impl OpenCtpColumnarGeneratorSource {
    pub fn new(config: OpenCtpMarketGeneratorConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(Mutex::new(OpenCtpSourceMetrics::default())),
            status: Arc::new(Mutex::new(OpenCtpSourceStatus::Created)),
            segment_descriptor_publisher: None,
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

    pub fn status_handle(&self) -> Arc<Mutex<OpenCtpSourceStatus>> {
        self.status.clone()
    }

    pub fn with_segment_descriptor_publisher(
        mut self,
        publisher: Arc<dyn OpenCtpSegmentDescriptorPublisher>,
    ) -> Self {
        self.segment_descriptor_publisher = Some(publisher);
        self
    }
}

impl Source for OpenCtpMarketGeneratorSource {
    fn name(&self) -> &str {
        "openctp-market-generator-source"
    }

    fn output_schema(&self) -> SchemaRef {
        tick_data_schema()
    }

    fn mode(&self) -> SourceMode {
        SourceMode::Pipeline
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> CoreResult<SourceHandle> {
        Box::new(self.inner).start(sink)
    }
}

impl Source for OpenCtpNormalizedGeneratorSource {
    fn name(&self) -> &str {
        "openctp-normalized-generator-source"
    }

    fn output_schema(&self) -> SchemaRef {
        tick_data_schema()
    }

    fn mode(&self) -> SourceMode {
        SourceMode::Pipeline
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> CoreResult<SourceHandle> {
        Box::new(self.inner).start(sink)
    }
}

impl Source for OpenCtpColumnarGeneratorSource {
    fn name(&self) -> &str {
        "openctp-columnar-generator-source"
    }

    fn output_schema(&self) -> SchemaRef {
        tick_data_schema()
    }

    fn mode(&self) -> SourceMode {
        SourceMode::Pipeline
    }

    fn start(self: Box<Self>, sink: Arc<dyn SourceSink>) -> CoreResult<SourceHandle> {
        let Self {
            config,
            metrics,
            status,
            segment_descriptor_publisher,
        } = *self;
        let stop_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source_stop_requested = Arc::clone(&stop_requested);
        let join_handle = thread::spawn(move || {
            run_columnar_generator_source(
                config,
                sink,
                metrics,
                status,
                segment_descriptor_publisher,
                source_stop_requested,
            )
        });
        Ok(SourceHandle::new_with_stop(
            join_handle,
            Box::new(move || {
                stop_requested.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }),
        ))
    }
}

#[derive(Debug, Clone)]
struct InstrumentState {
    instrument_id: String,
    last_price: f64,
    volume: i64,
    turnover: f64,
    open_interest: f64,
}

#[derive(Debug, Clone)]
struct NormalizedInstrumentState {
    instrument_id: String,
    exchange_id: String,
    trading_day: String,
    action_day: String,
    last_price: f64,
    volume: i64,
    turnover: f64,
    open_interest: f64,
}

#[derive(Debug, Clone)]
struct ColumnarInstrumentState {
    last_price: f64,
    volume: i64,
    turnover: f64,
    open_interest: f64,
}

impl InstrumentState {
    fn new(instrument_id: String, index: usize, config: &OpenCtpMarketGeneratorConfig) -> Self {
        let last_price = config.base_price + index as f64 * 10.0;
        Self {
            instrument_id,
            last_price,
            volume: 0,
            turnover: 0.0,
            open_interest: 10_000.0 + index as f64 * 100.0,
        }
    }

    fn next_tick(
        &mut self,
        config: &OpenCtpMarketGeneratorConfig,
        round_index: u64,
        rng: &mut DeterministicRng,
    ) -> RawTickSnapshot {
        let delta = (rng.next_unit_f64() - 0.5) * 2.0 * config.price_step;
        self.last_price = (self.last_price + delta).max(config.price_step);
        let volume_delta = 1 + (rng.next_u64() % 5) as i64;
        self.volume += volume_delta;
        self.turnover += self.last_price * volume_delta as f64 * 10.0;
        let oi_delta = (rng.next_unit_f64() - 0.5) * 2.0;
        self.open_interest = (self.open_interest + oi_delta).max(1.0);

        let (update_time, update_millisec) = ctp_time_from_round(round_index, config.interval_ms);
        RawTickSnapshot {
            instrument_id: self.instrument_id.clone(),
            exchange_id: config.exchange_id.clone(),
            trading_day: config.trading_day.clone(),
            action_day: config.action_day.clone(),
            update_time,
            update_millisec,
            last_price: self.last_price,
            volume: self.volume,
            turnover: self.turnover,
            open_interest: self.open_interest,
            bid_price_1: self.last_price - config.price_step,
            bid_volume_1: 10 + (rng.next_u64() % 50) as i64,
            ask_price_1: self.last_price + config.price_step,
            ask_volume_1: 10 + (rng.next_u64() % 50) as i64,
        }
    }
}

impl NormalizedInstrumentState {
    fn new(instrument_id: String, index: usize, config: &OpenCtpMarketGeneratorConfig) -> Self {
        let last_price = config.base_price + index as f64 * 10.0;
        Self {
            instrument_id,
            exchange_id: config.exchange_id.clone(),
            trading_day: config.trading_day.clone(),
            action_day: config.action_day.clone(),
            last_price,
            volume: 0,
            turnover: 0.0,
            open_interest: 10_000.0 + index as f64 * 100.0,
        }
    }

    fn next_row(
        &mut self,
        config: &OpenCtpMarketGeneratorConfig,
        dt_ns: i64,
        rng: &mut DeterministicRng,
    ) -> NormalizedTickRow {
        let delta = (rng.next_unit_f64() - 0.5) * 2.0 * config.price_step;
        self.last_price = (self.last_price + delta).max(config.price_step);
        let volume_delta = 1 + (rng.next_u64() % 5) as i64;
        self.volume += volume_delta;
        self.turnover += self.last_price * volume_delta as f64 * 10.0;
        let oi_delta = (rng.next_unit_f64() - 0.5) * 2.0;
        self.open_interest = (self.open_interest + oi_delta).max(1.0);

        NormalizedTickRow {
            instrument_id: self.instrument_id.clone(),
            exchange_id: Some(self.exchange_id.clone()),
            trading_day: Some(self.trading_day.clone()),
            action_day: Some(self.action_day.clone()),
            dt_ns,
            localtime_ns: 0,
            source_emit_ns: 0,
            last_price: Some(self.last_price),
            volume: Some(self.volume),
            turnover: Some(self.turnover),
            open_interest: Some(self.open_interest),
            bid_price_1: Some(self.last_price - config.price_step),
            bid_volume_1: Some(10 + (rng.next_u64() % 50) as i64),
            ask_price_1: Some(self.last_price + config.price_step),
            ask_volume_1: Some(10 + (rng.next_u64() % 50) as i64),
        }
    }
}

impl ColumnarInstrumentState {
    fn new(index: usize, config: &OpenCtpMarketGeneratorConfig) -> Self {
        Self {
            last_price: config.base_price + index as f64 * 10.0,
            volume: 0,
            turnover: 0.0,
            open_interest: 10_000.0 + index as f64 * 100.0,
        }
    }

    fn push_row<'a>(
        &mut self,
        instrument_id: &'a str,
        config: &'a OpenCtpMarketGeneratorConfig,
        dt_ns: i64,
        now_ns: i64,
        rng: &mut DeterministicRng,
        batch: &mut OpenCtpColumnarTickBatch<'a>,
    ) {
        let delta = (rng.next_unit_f64() - 0.5) * 2.0 * config.price_step;
        self.last_price = (self.last_price + delta).max(config.price_step);
        let volume_delta = 1 + (rng.next_u64() % 5) as i64;
        self.volume += volume_delta;
        self.turnover += self.last_price * volume_delta as f64 * 10.0;
        let oi_delta = (rng.next_unit_f64() - 0.5) * 2.0;
        self.open_interest = (self.open_interest + oi_delta).max(1.0);

        batch.instrument_ids.push(instrument_id);
        batch.dt_ns.push(dt_ns);
        batch.localtime_ns.push(now_ns);
        batch.source_emit_ns.push(now_ns);
        batch.last_price.push(self.last_price);
        batch.volume.push(self.volume);
        batch.turnover.push(self.turnover);
        batch.open_interest.push(self.open_interest);
        batch.bid_price_1.push(self.last_price - config.price_step);
        batch.bid_volume_1.push(10 + (rng.next_u64() % 50) as i64);
        batch.ask_price_1.push(self.last_price + config.price_step);
        batch.ask_volume_1.push(10 + (rng.next_u64() % 50) as i64);
    }
}

fn generator_source_config(config: &OpenCtpMarketGeneratorConfig) -> OpenCtpMarketDataSourceConfig {
    OpenCtpMarketDataSourceConfig::low_latency(
        "generator://openctp".to_string(),
        "generator".to_string(),
        "generator".to_string(),
        String::new(),
        config.instruments.clone(),
        ".cache/openctp/generator".to_string(),
    )
}

fn run_generator(
    config: OpenCtpMarketGeneratorConfig,
    tx: Sender<MdDriverEvent>,
    stop_rx: Receiver<()>,
) -> CoreResult<()> {
    let mut rng = DeterministicRng::new(config.seed);
    let mut instruments = config
        .instruments
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, instrument_id)| InstrumentState::new(instrument_id, index, &config))
        .collect::<Vec<_>>();
    let mut emitted = 0u64;
    let mut round_index = 0u64;

    loop {
        let mut round_ticks = Vec::with_capacity(instruments.len());
        for instrument in &mut instruments {
            if should_stop(&stop_rx) || reached_max_ticks(emitted, config.max_ticks) {
                if !round_ticks.is_empty() {
                    tx.send(MdDriverEvent::Ticks(round_ticks))
                        .map_err(|_| ZippyError::ChannelSend)?;
                }
                return send_stop(&tx);
            }
            round_ticks.push(instrument.next_tick(&config, round_index, &mut rng));
            emitted += 1;
        }
        if !round_ticks.is_empty() {
            tx.send(MdDriverEvent::Ticks(round_ticks))
                .map_err(|_| ZippyError::ChannelSend)?;
        }
        round_index += 1;

        if reached_max_ticks(emitted, config.max_ticks) {
            return send_stop(&tx);
        }
        match stop_rx.try_recv() {
            Ok(()) | Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return send_stop(&tx);
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {}
        }
    }
}

fn run_normalized_generator(
    config: OpenCtpMarketGeneratorConfig,
    tx: Sender<MdDriverEvent>,
    stop_rx: Receiver<()>,
) -> CoreResult<()> {
    let mut rng = DeterministicRng::new(config.seed);
    let mut instruments = normalized_instrument_states(&config);
    let mut emitted = 0u64;
    let mut round_index = 0u64;

    loop {
        let dt_ns = generator_dt_ns(&config, round_index)?;
        let mut round_rows = Vec::with_capacity(instruments.len());
        for instrument in &mut instruments {
            if should_stop(&stop_rx) || reached_max_ticks(emitted, config.max_ticks) {
                if !round_rows.is_empty() {
                    tx.send(MdDriverEvent::Rows(round_rows))
                        .map_err(|_| ZippyError::ChannelSend)?;
                }
                return send_stop(&tx);
            }
            round_rows.push(instrument.next_row(&config, dt_ns, &mut rng));
            emitted += 1;
        }
        if !round_rows.is_empty() {
            tx.send(MdDriverEvent::Rows(round_rows))
                .map_err(|_| ZippyError::ChannelSend)?;
        }
        round_index += 1;

        if reached_max_ticks(emitted, config.max_ticks) {
            return send_stop(&tx);
        }
        match stop_rx.try_recv() {
            Ok(()) | Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return send_stop(&tx);
            }
            Err(crossbeam_channel::TryRecvError::Empty) => {}
        }
    }
}

fn run_columnar_generator_source(
    config: OpenCtpMarketGeneratorConfig,
    sink: Arc<dyn SourceSink>,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    status: Arc<Mutex<OpenCtpSourceStatus>>,
    segment_descriptor_publisher: Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
    stop_requested: Arc<std::sync::atomic::AtomicBool>,
) -> CoreResult<()> {
    *status.lock().unwrap() = OpenCtpSourceStatus::Running;
    sink.emit(SourceEvent::Hello(StreamHello::new(
        "openctp.tick",
        tick_data_schema(),
        1,
    )?))?;

    let mut segment_ingress =
        OpenCtpSegmentIngress::new_for_source().map_err(|reason| ZippyError::Io {
            reason: reason.to_string(),
        })?;
    if let Some(publisher) = &segment_descriptor_publisher {
        publisher.publish(segment_ingress.active_descriptor_envelope_bytes().map_err(
            |reason| ZippyError::Io {
                reason: reason.to_string(),
            },
        )?)?;
    }
    let mut segment_reader = segment_ingress
        .active_reader()
        .map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?;
    let mut rng = DeterministicRng::new(config.seed);
    let mut states = columnar_instrument_states(&config);
    let mut emitted = 0u64;
    let mut round_index = 0u64;

    loop {
        if stop_requested.load(std::sync::atomic::Ordering::SeqCst)
            || reached_max_ticks(emitted, config.max_ticks)
        {
            *status.lock().unwrap() = OpenCtpSourceStatus::Stopped;
            sink.emit(SourceEvent::Stop)?;
            return Ok(());
        }

        let now_ns = current_unix_time_ns()?;
        let capacity = columnar_batch_capacity(states.len(), emitted, config.max_ticks);
        let mut batch = new_columnar_batch(&config, capacity);
        while batch.len() < capacity {
            let dt_ns = generator_dt_ns(&config, round_index)?;
            for (index, state) in states.iter_mut().enumerate() {
                if batch.len() >= capacity || reached_max_ticks(emitted, config.max_ticks) {
                    break;
                }
                state.push_row(
                    config.instruments[index].as_str(),
                    &config,
                    dt_ns,
                    now_ns,
                    &mut rng,
                    &mut batch,
                );
                emitted += 1;
            }
            round_index += 1;
            if reached_max_ticks(emitted, config.max_ticks) {
                break;
            }
        }
        let tick_count = batch.len();
        if tick_count > 0 {
            write_columnar_batch_to_segment(
                &batch,
                &sink,
                &mut segment_ingress,
                &mut segment_reader,
                &metrics,
                &segment_descriptor_publisher,
            )?;
            metrics.lock().unwrap().ticks_received_total += tick_count as u64;
            if let Some(span) = segment_reader
                .read_available()
                .map_err(|error| ZippyError::Io {
                    reason: error.to_string(),
                })?
            {
                let rows = span.row_count();
                sink.emit(SourceEvent::Data(
                    zippy_core::SegmentTableView::from_row_span(span),
                ))?;
                let mut metrics = metrics.lock().unwrap();
                metrics.ticks_emitted_total += rows as u64;
                metrics.batches_emitted_total += 1;
            }
        }
    }
}

fn normalized_instrument_states(
    config: &OpenCtpMarketGeneratorConfig,
) -> Vec<NormalizedInstrumentState> {
    config
        .instruments
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, instrument_id)| NormalizedInstrumentState::new(instrument_id, index, config))
        .collect()
}

fn columnar_instrument_states(
    config: &OpenCtpMarketGeneratorConfig,
) -> Vec<ColumnarInstrumentState> {
    config
        .instruments
        .iter()
        .enumerate()
        .map(|(index, _)| ColumnarInstrumentState::new(index, config))
        .collect()
}

fn new_columnar_batch<'a>(
    config: &'a OpenCtpMarketGeneratorConfig,
    capacity: usize,
) -> OpenCtpColumnarTickBatch<'a> {
    OpenCtpColumnarTickBatch {
        exchange_id: config.exchange_id.as_str(),
        trading_day: config.trading_day.as_str(),
        action_day: config.action_day.as_str(),
        instrument_ids: Vec::with_capacity(capacity),
        dt_ns: Vec::with_capacity(capacity),
        localtime_ns: Vec::with_capacity(capacity),
        source_emit_ns: Vec::with_capacity(capacity),
        last_price: Vec::with_capacity(capacity),
        volume: Vec::with_capacity(capacity),
        turnover: Vec::with_capacity(capacity),
        open_interest: Vec::with_capacity(capacity),
        bid_price_1: Vec::with_capacity(capacity),
        bid_volume_1: Vec::with_capacity(capacity),
        ask_price_1: Vec::with_capacity(capacity),
        ask_volume_1: Vec::with_capacity(capacity),
    }
}

fn columnar_batch_capacity(instrument_count: usize, emitted: u64, max_ticks: Option<u64>) -> usize {
    let target = COLUMNAR_TARGET_BATCH_ROWS.max(instrument_count.max(1));
    match max_ticks.and_then(|limit| limit.checked_sub(emitted)) {
        Some(remaining) => target.min(remaining as usize),
        None => target,
    }
}

fn write_columnar_batch_to_segment(
    batch: &OpenCtpColumnarTickBatch<'_>,
    sink: &Arc<dyn SourceSink>,
    ingress: &mut OpenCtpSegmentIngress,
    reader: &mut ActiveSegmentReader,
    metrics: &Arc<Mutex<OpenCtpSourceMetrics>>,
    segment_descriptor_publisher: &Option<Arc<dyn OpenCtpSegmentDescriptorPublisher>>,
) -> CoreResult<()> {
    let active_identity_before = ingress.active_segment_identity();
    let descriptor_changed =
        ingress
            .write_columnar_batch(batch)
            .map_err(|reason| ZippyError::Io {
                reason: reason.to_string(),
            })?
            || ingress.active_segment_identity() != active_identity_before;
    if descriptor_changed {
        if let Some(publisher) = segment_descriptor_publisher {
            publisher.publish(
                ingress
                    .active_descriptor_envelope_bytes()
                    .map_err(|reason| ZippyError::Io {
                        reason: reason.to_string(),
                    })?,
            )?;
        }
        if let Some(span) = reader.read_available().map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })? {
            let rows = span.row_count();
            sink.emit(SourceEvent::Data(
                zippy_core::SegmentTableView::from_row_span(span),
            ))?;
            let mut metrics = metrics.lock().unwrap();
            metrics.ticks_emitted_total += rows as u64;
            metrics.batches_emitted_total += 1;
        }
        ingress
            .update_active_reader(reader)
            .map_err(|error| ZippyError::Io {
                reason: error.to_string(),
            })?;
        if segment_descriptor_publisher.is_none() {
            ingress.release_retired_segments();
        }
    }
    Ok(())
}

fn generator_dt_ns(config: &OpenCtpMarketGeneratorConfig, round_index: u64) -> CoreResult<i64> {
    let elapsed_ns = round_index
        .checked_mul(config.interval_ms)
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or_else(|| ZippyError::Io {
            reason: "generator timestamp overflow".to_string(),
        })?;
    let base_ns =
        compose_exchange_timestamp_ns(&config.action_day, "09:30:00", 0).map_err(|error| {
            ZippyError::Io {
                reason: error.to_string(),
            }
        })?;
    base_ns
        .checked_add(elapsed_ns as i64)
        .ok_or_else(|| ZippyError::Io {
            reason: "generator timestamp overflow".to_string(),
        })
}

fn current_unix_time_ns() -> CoreResult<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ZippyError::Io {
            reason: error.to_string(),
        })?;
    i64::try_from(duration.as_nanos()).map_err(|error| ZippyError::Io {
        reason: error.to_string(),
    })
}

fn reached_max_ticks(emitted: u64, max_ticks: Option<u64>) -> bool {
    max_ticks.map(|limit| emitted >= limit).unwrap_or(false)
}

fn should_stop(stop_rx: &Receiver<()>) -> bool {
    stop_rx.try_recv().is_ok()
}

fn send_stop(tx: &Sender<MdDriverEvent>) -> CoreResult<()> {
    tx.send(MdDriverEvent::Stop)
        .map_err(|_| ZippyError::ChannelSend)
}

#[derive(Debug, Clone)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    fn next_unit_f64(&mut self) -> f64 {
        const SCALE: f64 = (1u64 << 53) as f64;
        ((self.next_u64() >> 11) as f64) / SCALE
    }
}

fn ctp_time_from_round(round_index: u64, interval_ms: u64) -> (String, i32) {
    let elapsed_ms = round_index.saturating_mul(interval_ms);
    let day_ms = 86_400_000;
    let total_ms = (DEFAULT_SESSION_START_MS + elapsed_ms) % day_ms;
    let hour = total_ms / 3_600_000;
    let minute = (total_ms % 3_600_000) / 60_000;
    let second = (total_ms % 60_000) / 1_000;
    let millisec = (total_ms % 1_000) as i32;
    (format!("{hour:02}:{minute:02}:{second:02}"), millisec)
}

fn normalize_instruments(instruments: Vec<String>) -> Vec<String> {
    instruments
        .into_iter()
        .map(|instrument| instrument.trim().to_string())
        .filter(|instrument| !instrument.is_empty())
        .collect()
}

fn default_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(1)
        .max(1)
}

fn current_exchange_date_yyyymmdd() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
        + 8 * 3_600;
    let days = seconds.div_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}{month:02}{day:02}")
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}
