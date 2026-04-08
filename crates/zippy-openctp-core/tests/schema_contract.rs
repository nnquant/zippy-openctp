#[test]
fn tick_data_schema_contains_required_columns_in_stable_order() {
    let schema = zippy_openctp_core::schema::tick_data_schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();

    assert_eq!(
        fields,
        vec![
            "instrument_id",
            "exchange_id",
            "trading_day",
            "action_day",
            "dt",
            "last_price",
            "volume",
            "turnover",
            "open_interest",
            "bid_price_1",
            "bid_volume_1",
            "ask_price_1",
            "ask_volume_1",
        ]
    );

    let dt = schema.field_with_name("dt").unwrap();
    assert_eq!(
        dt.data_type(),
        &arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some("UTC".into())
        )
    );
    assert!(!dt.is_nullable());

    let instrument_id = schema.field_with_name("instrument_id").unwrap();
    assert_eq!(instrument_id.data_type(), &arrow::datatypes::DataType::Utf8);
    assert!(!instrument_id.is_nullable());

    let volume = schema.field_with_name("volume").unwrap();
    assert_eq!(volume.data_type(), &arrow::datatypes::DataType::Int64);
    assert!(volume.is_nullable());
}
