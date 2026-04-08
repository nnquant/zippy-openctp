use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickSchemaType {
    Utf8,
    TimestampNsUtc,
    Float64,
    Int64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickSchemaField {
    pub name: &'static str,
    pub data_type: TickSchemaType,
    pub nullable: bool,
}

pub const TICK_SCHEMA_FIELDS: &[TickSchemaField] = &[
    TickSchemaField {
        name: "instrument_id",
        data_type: TickSchemaType::Utf8,
        nullable: false,
    },
    TickSchemaField {
        name: "exchange_id",
        data_type: TickSchemaType::Utf8,
        nullable: true,
    },
    TickSchemaField {
        name: "trading_day",
        data_type: TickSchemaType::Utf8,
        nullable: true,
    },
    TickSchemaField {
        name: "action_day",
        data_type: TickSchemaType::Utf8,
        nullable: true,
    },
    TickSchemaField {
        name: "dt",
        data_type: TickSchemaType::TimestampNsUtc,
        nullable: false,
    },
    TickSchemaField {
        name: "last_price",
        data_type: TickSchemaType::Float64,
        nullable: true,
    },
    TickSchemaField {
        name: "volume",
        data_type: TickSchemaType::Int64,
        nullable: true,
    },
    TickSchemaField {
        name: "turnover",
        data_type: TickSchemaType::Float64,
        nullable: true,
    },
    TickSchemaField {
        name: "open_interest",
        data_type: TickSchemaType::Float64,
        nullable: true,
    },
    TickSchemaField {
        name: "bid_price_1",
        data_type: TickSchemaType::Float64,
        nullable: true,
    },
    TickSchemaField {
        name: "bid_volume_1",
        data_type: TickSchemaType::Int64,
        nullable: true,
    },
    TickSchemaField {
        name: "ask_price_1",
        data_type: TickSchemaType::Float64,
        nullable: true,
    },
    TickSchemaField {
        name: "ask_volume_1",
        data_type: TickSchemaType::Int64,
        nullable: true,
    },
];

pub fn tick_data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        TICK_SCHEMA_FIELDS
            .iter()
            .map(|field| {
                Field::new(
                    field.name,
                    match field.data_type {
                        TickSchemaType::Utf8 => DataType::Utf8,
                        TickSchemaType::TimestampNsUtc => {
                            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
                        }
                        TickSchemaType::Float64 => DataType::Float64,
                        TickSchemaType::Int64 => DataType::Int64,
                    },
                    field.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

pub fn tick_data_schema_name() -> &'static str {
    "tick_data"
}
