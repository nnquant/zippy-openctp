use std::error::Error;
use std::fmt::{Display, Formatter};

const EXCHANGE_UTC_OFFSET_SECONDS: i64 = 8 * 60 * 60;

/// Raw OpenCTP tick snapshot used by the thin normalization layer.
///
/// :param instrument_id: Instrument identifier from the upstream market data callback.
/// :type instrument_id: String
/// :param exchange_id: Exchange identifier in raw upstream form.
/// :type exchange_id: String
/// :param trading_day: Trading day string in `YYYYMMDD` format.
/// :type trading_day: String
/// :param action_day: Action day string in `YYYYMMDD` format.
/// :type action_day: String
/// :param update_time: Update time string in `HH:MM:SS` format.
/// :type update_time: String
/// :param update_millisec: Millisecond component paired with `update_time`.
/// :type update_millisec: i32
#[derive(Debug, Clone, PartialEq)]
pub struct RawTickSnapshot {
    pub instrument_id: String,
    pub exchange_id: String,
    pub trading_day: String,
    pub action_day: String,
    pub update_time: String,
    pub update_millisec: i32,
    pub last_price: f64,
    pub volume: i64,
    pub turnover: f64,
    pub open_interest: f64,
    pub bid_price_1: f64,
    pub bid_volume_1: i64,
    pub ask_price_1: f64,
    pub ask_volume_1: i64,
}

/// Stable normalized tick row aligned with the fixed tick schema contract.
///
/// :param instrument_id: Stable instrument identifier column value.
/// :type instrument_id: String
/// :param exchange_id: Optional normalized exchange identifier.
/// :type exchange_id: Option[String]
/// :param trading_day: Optional normalized trading day.
/// :type trading_day: Option[String]
/// :param action_day: Optional normalized action day. The thin normalizer currently
///     requires this upstream field to be present in order to compose `dt_ns`, so
///     successful rows always carry `Some(action_day)`.
/// :type action_day: Option[String]
/// :param dt_ns: Event timestamp in UTC nanoseconds since Unix epoch.
/// :type dt_ns: i64
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedTickRow {
    pub instrument_id: String,
    pub exchange_id: Option<String>,
    pub trading_day: Option<String>,
    pub action_day: Option<String>,
    pub dt_ns: i64,
    pub last_price: Option<f64>,
    pub volume: Option<i64>,
    pub turnover: Option<f64>,
    pub open_interest: Option<f64>,
    pub bid_price_1: Option<f64>,
    pub bid_volume_1: Option<i64>,
    pub ask_price_1: Option<f64>,
    pub ask_volume_1: Option<i64>,
}

/// Error returned when raw tick fields cannot be normalized into a schema row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    InvalidDate,
    InvalidTime,
    InvalidMillisec,
}

impl Display for NormalizeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDate => write!(f, "invalid action_day date"),
            Self::InvalidTime => write!(f, "invalid update_time value"),
            Self::InvalidMillisec => write!(f, "invalid update_millisec value"),
        }
    }
}

impl Error for NormalizeError {}

/// Normalize a raw tick snapshot into the fixed thin-schema representation.
///
/// Time composition uses `action_day + update_time + update_millisec`, interprets the
/// result in the exchange local time zone (UTC+8), then converts it into UTC
/// nanoseconds since the Unix epoch.
///
/// :param raw: Raw tick snapshot received from the upstream callback.
/// :type raw: RawTickSnapshot
/// :returns: Normalized row aligned with the fixed tick schema.
/// :rtype: Result[NormalizedTickRow, NormalizeError]
pub fn normalize_tick(raw: &RawTickSnapshot) -> Result<NormalizedTickRow, NormalizeError> {
    let action_day = normalize_string(raw.action_day.as_str()).ok_or(NormalizeError::InvalidDate)?;
    let dt_ns = compose_exchange_timestamp_ns(
        action_day.as_str(),
        raw.update_time.as_str(),
        raw.update_millisec,
    )?;

    Ok(NormalizedTickRow {
        instrument_id: raw.instrument_id.clone(),
        exchange_id: normalize_string(raw.exchange_id.as_str()),
        trading_day: normalize_string(raw.trading_day.as_str()),
        action_day: Some(action_day),
        dt_ns,
        last_price: Some(raw.last_price),
        volume: Some(raw.volume),
        turnover: Some(raw.turnover),
        open_interest: Some(raw.open_interest),
        bid_price_1: Some(raw.bid_price_1),
        bid_volume_1: Some(raw.bid_volume_1),
        ask_price_1: Some(raw.ask_price_1),
        ask_volume_1: Some(raw.ask_volume_1),
    })
}

fn normalize_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(trimmed.to_string())
}

fn compose_exchange_timestamp_ns(
    action_day: &str,
    update_time: &str,
    update_millisec: i32,
) -> Result<i64, NormalizeError> {
    if !(0..=999).contains(&update_millisec) {
        return Err(NormalizeError::InvalidMillisec);
    }

    let (year, month, day) = parse_ymd(action_day)?;
    let (hour, minute, second) = parse_hms(update_time)?;
    let days = days_from_civil(year, month, day);

    let seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add(i64::from(hour) * 3_600))
        .and_then(|value| value.checked_add(i64::from(minute) * 60))
        .and_then(|value| value.checked_add(i64::from(second)))
        .ok_or(NormalizeError::InvalidTime)?;

    seconds
        .checked_sub(EXCHANGE_UTC_OFFSET_SECONDS)
        .and_then(|value| value.checked_mul(1_000_000_000))
        .and_then(|value| value.checked_add(i64::from(update_millisec) * 1_000_000))
        .ok_or(NormalizeError::InvalidTime)
}

fn parse_ymd(value: &str) -> Result<(i32, u32, u32), NormalizeError> {
    if value.len() != 8 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(NormalizeError::InvalidDate);
    }

    let year = value[0..4]
        .parse::<i32>()
        .map_err(|_| NormalizeError::InvalidDate)?;
    let month = value[4..6]
        .parse::<u32>()
        .map_err(|_| NormalizeError::InvalidDate)?;
    let day = value[6..8]
        .parse::<u32>()
        .map_err(|_| NormalizeError::InvalidDate)?;

    if !(1..=12).contains(&month) {
        return Err(NormalizeError::InvalidDate);
    }

    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return Err(NormalizeError::InvalidDate),
    };

    if day == 0 || day > max_day {
        return Err(NormalizeError::InvalidDate);
    }

    Ok((year, month, day))
}

fn parse_hms(value: &str) -> Result<(u32, u32, u32), NormalizeError> {
    if value.len() != 8 || value.as_bytes()[2] != b':' || value.as_bytes()[5] != b':' {
        return Err(NormalizeError::InvalidTime);
    }

    let mut parts = value.split(':');
    let hour = parts
        .next()
        .ok_or(NormalizeError::InvalidTime)?
        .parse::<u32>()
        .map_err(|_| NormalizeError::InvalidTime)?;
    let minute = parts
        .next()
        .ok_or(NormalizeError::InvalidTime)?
        .parse::<u32>()
        .map_err(|_| NormalizeError::InvalidTime)?;
    let second = parts
        .next()
        .ok_or(NormalizeError::InvalidTime)?
        .parse::<u32>()
        .map_err(|_| NormalizeError::InvalidTime)?;

    if parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return Err(NormalizeError::InvalidTime);
    }

    Ok((hour, minute, second))
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    i64::from(era) * 146_097 + i64::from(day_of_era) - 719_468
}
