use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::record_batch::RecordBatch;

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
