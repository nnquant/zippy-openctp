#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCtpMarketDataSourceConfig {
    pub front: String,
    pub broker_id: String,
    pub user_id: String,
    pub instruments: Vec<String>,
    pub flow_path: String,
    pub rows_per_batch: usize,
    pub flush_interval_ms: u64,
}

impl OpenCtpMarketDataSourceConfig {
    pub fn new(
        front: String,
        broker_id: String,
        user_id: String,
        instruments: Vec<String>,
        flow_path: String,
        rows_per_batch: usize,
        flush_interval_ms: u64,
    ) -> Self {
        Self {
            front,
            broker_id,
            user_id,
            instruments,
            flow_path,
            rows_per_batch,
            flush_interval_ms,
        }
    }
}
