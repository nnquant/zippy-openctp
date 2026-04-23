use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan, RowSpanView,
};

use crate::normalize::NormalizedTickRow;

#[derive(Debug, Clone, PartialEq)]
pub struct OpenCtpActiveSegmentSnapshot {
    pub committed_row_count: usize,
    pub dt_ns: Option<i64>,
    pub localtime_ns: Option<i64>,
    pub source_emit_ns: Option<i64>,
    pub instrument_id: Option<String>,
    pub last_price: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OpenCtpSegmentDebugMetrics {
    pub committed_rows: usize,
    pub active_snapshot: Option<OpenCtpActiveSegmentSnapshot>,
}

pub struct OpenCtpSegmentIngress {
    writer: ActiveSegmentWriter,
}

impl OpenCtpSegmentIngress {
    pub fn new_for_source() -> Result<Self, &'static str> {
        Self::new_with_row_capacity(64)
    }

    pub fn for_test() -> Result<Self, &'static str> {
        Self::new_with_row_capacity(64)
    }

    fn new_with_row_capacity(row_capacity: usize) -> Result<Self, &'static str> {
        let schema = compile_schema(&[
            ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
            ColumnSpec::new("localtime_ns", ColumnType::Int64),
            ColumnSpec::new("source_emit_ns", ColumnType::Int64),
            ColumnSpec::new("instrument_id", ColumnType::Utf8),
            ColumnSpec::new("last_price", ColumnType::Float64),
        ])?;
        let layout = LayoutPlan::for_schema(&schema, row_capacity)?;
        let writer = ActiveSegmentWriter::new_for_test(schema, layout)?;
        Ok(Self { writer })
    }

    pub fn write_row(&mut self, row: &NormalizedTickRow) -> Result<(), &'static str> {
        let last_price = row.last_price.ok_or("last_price is required")?;
        self.writer.begin_row()?;
        self.writer.write_i64("dt", row.dt_ns)?;
        self.writer.write_i64("localtime_ns", row.localtime_ns)?;
        self.writer
            .write_i64("source_emit_ns", row.source_emit_ns)?;
        self.writer
            .write_utf8("instrument_id", row.instrument_id.as_str())?;
        self.writer.write_f64("last_price", last_price)?;
        self.writer.commit_row()
    }

    pub fn active_snapshot(&self) -> Result<OpenCtpActiveSegmentSnapshot, &'static str> {
        let committed_row_count = self.writer.committed_row_count();
        if committed_row_count == 0 {
            return Ok(OpenCtpActiveSegmentSnapshot {
                committed_row_count,
                dt_ns: None,
                localtime_ns: None,
                source_emit_ns: None,
                instrument_id: None,
                last_price: None,
            });
        }

        let handle = self.writer.sealed_handle_for_test()?;
        let batch = RowSpanView::new(handle, committed_row_count - 1, committed_row_count)
            .map_err(|_| "failed to build active segment row span")?
            .as_record_batch()
            .map_err(|_| "failed to export active segment snapshot")?;
        let dt_ns = batch
            .column_by_name("dt")
            .and_then(|column| column.as_any().downcast_ref::<TimestampNanosecondArray>())
            .map(|array| array.value(0));
        let localtime_ns = batch
            .column_by_name("localtime_ns")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .map(|array| array.value(0));
        let source_emit_ns = batch
            .column_by_name("source_emit_ns")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .map(|array| array.value(0));
        let instrument_id = batch
            .column_by_name("instrument_id")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .map(|array| array.value(0).to_string());
        let last_price = batch
            .column_by_name("last_price")
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .map(|array| array.value(0));

        Ok(OpenCtpActiveSegmentSnapshot {
            committed_row_count,
            dt_ns,
            localtime_ns,
            source_emit_ns,
            instrument_id,
            last_price,
        })
    }

    pub fn debug_metrics(&self) -> Result<OpenCtpSegmentDebugMetrics, &'static str> {
        let active_snapshot = self.active_snapshot()?;
        Ok(OpenCtpSegmentDebugMetrics {
            committed_rows: self.writer.committed_row_count(),
            active_snapshot: Some(active_snapshot),
        })
    }
}
