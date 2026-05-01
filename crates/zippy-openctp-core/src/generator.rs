use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender};
use zippy_core::{
    Result as CoreResult, SchemaRef, Source, SourceHandle, SourceMode, SourceSink, ZippyError,
};

use crate::metrics::OpenCtpSourceMetrics;
use crate::normalize::RawTickSnapshot;
use crate::schema::tick_data_schema;
use crate::source::{
    MdDriver, MdDriverEvent, MdDriverHandle, OpenCtpMarketDataSource,
    OpenCtpMarketDataSourceConfig, OpenCtpSegmentDescriptorPublisher, OpenCtpSourceStatus,
};

const DEFAULT_EXCHANGE_ID: &str = "CFFEX";
const DEFAULT_BASE_PRICE: f64 = 4000.0;
const DEFAULT_PRICE_STEP: f64 = 0.2;
const DEFAULT_SESSION_START_MS: u64 = 9 * 3_600_000 + 30 * 60_000;

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

pub struct OpenCtpMarketGeneratorSource {
    inner: OpenCtpMarketDataSource,
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

#[derive(Debug, Clone)]
struct InstrumentState {
    instrument_id: String,
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
    let interval = Duration::from_millis(config.interval_ms);
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
        match stop_rx.recv_timeout(interval) {
            Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                return send_stop(&tx);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
        }
    }
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
