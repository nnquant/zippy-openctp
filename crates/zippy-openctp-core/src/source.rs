use std::fmt::{Debug, Formatter};

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
