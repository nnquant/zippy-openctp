#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedTickRow {
    pub instrument_id: String,
    pub dt_ns: i64,
    pub last_price: f64,
    pub volume: i64,
}
