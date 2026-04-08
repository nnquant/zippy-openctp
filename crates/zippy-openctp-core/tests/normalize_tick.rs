use zippy_openctp_core::normalize::{normalize_tick, NormalizeError, RawTickSnapshot};

#[test]
fn normalize_tick_maps_raw_values_into_schema_row() {
    let raw = RawTickSnapshot {
        instrument_id: "IF2506".to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "".to_string(),
        action_day: "20260408".to_string(),
        update_time: "09:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume: 1234,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    };

    let row = normalize_tick(&raw).expect("normalize_tick should succeed");

    assert_eq!(row.instrument_id, "IF2506");
    assert_eq!(row.exchange_id.as_deref(), Some("CFFEX"));
    assert_eq!(row.trading_day, None);
    assert_eq!(row.action_day.as_deref(), Some("20260408"));
    assert_eq!(row.dt_ns, 1_775_611_800_500_000_000);
    assert_eq!(row.last_price, Some(3912.4));
    assert_eq!(row.volume, Some(1234));
    assert_eq!(row.turnover, Some(987654.0));
    assert_eq!(row.open_interest, Some(56789.0));
    assert_eq!(row.bid_price_1, Some(3912.2));
    assert_eq!(row.bid_volume_1, Some(10));
    assert_eq!(row.ask_price_1, Some(3912.6));
    assert_eq!(row.ask_volume_1, Some(8));
}

#[test]
fn normalize_tick_rejects_missing_action_day_after_trimming() {
    let raw = RawTickSnapshot {
        instrument_id: "IF2506".to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "20260408".to_string(),
        action_day: "   ".to_string(),
        update_time: "09:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume: 1234,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    };

    let error = normalize_tick(&raw).expect_err("blank action_day must fail");
    assert_eq!(error, NormalizeError::InvalidDate);
}

#[test]
fn normalize_tick_rejects_non_fixed_width_update_time() {
    let raw = RawTickSnapshot {
        instrument_id: "IF2506".to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "20260408".to_string(),
        action_day: "20260408".to_string(),
        update_time: "9:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume: 1234,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    };

    let error = normalize_tick(&raw).expect_err("non fixed-width update_time must fail");
    assert_eq!(error, NormalizeError::InvalidTime);
}
