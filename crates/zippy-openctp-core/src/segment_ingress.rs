use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, CompiledSchema, LayoutPlan,
    RowSpanView,
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
    schema: CompiledSchema,
    layout: LayoutPlan,
    writer: ActiveSegmentWriter,
    next_segment_id: u64,
    generation: u64,
    committed_rows: usize,
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
        let writer = ActiveSegmentWriter::new_for_runtime(schema.clone(), layout.clone(), 1, 0)?;
        Ok(Self {
            schema,
            layout,
            writer,
            next_segment_id: 2,
            generation: 0,
            committed_rows: 0,
        })
    }

    pub fn write_row(&mut self, row: &NormalizedTickRow) -> Result<(), &'static str> {
        match self.try_write_row(row) {
            Ok(()) => {
                self.committed_rows += 1;
                Ok(())
            }
            Err("segment is full") => {
                self.rollover_writer()?;
                self.try_write_row(row)?;
                self.committed_rows += 1;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn try_write_row(&mut self, row: &NormalizedTickRow) -> Result<(), &'static str> {
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

    fn rollover_writer(&mut self) -> Result<(), &'static str> {
        self.writer = ActiveSegmentWriter::new_for_runtime(
            self.schema.clone(),
            self.layout.clone(),
            self.next_segment_id,
            self.generation,
        )?;
        self.next_segment_id += 1;
        Ok(())
    }

    pub fn active_snapshot(&self) -> Result<OpenCtpActiveSegmentSnapshot, &'static str> {
        let writer_row_count = self.writer.committed_row_count();
        if self.committed_rows == 0 {
            return Ok(OpenCtpActiveSegmentSnapshot {
                committed_row_count: 0,
                dt_ns: None,
                localtime_ns: None,
                source_emit_ns: None,
                instrument_id: None,
                last_price: None,
            });
        }

        let handle = self.writer.sealed_handle_for_test()?;
        let batch = RowSpanView::new(handle, writer_row_count - 1, writer_row_count)
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
            committed_row_count: self.committed_rows,
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
            committed_rows: self.committed_rows,
            active_snapshot: Some(active_snapshot),
        })
    }
}
