#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenCtpSourceMetrics {
    pub ticks_received_total: u64,
    pub ticks_emitted_total: u64,
    pub batches_emitted_total: u64,
    pub normalize_failures_total: u64,
    pub reconnects_total: u64,
    pub login_failures_total: u64,
    pub subscribe_failures_total: u64,
}
