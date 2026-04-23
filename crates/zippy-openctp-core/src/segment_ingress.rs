use zippy_segment_store::{
    compile_schema, ActiveSegmentWriter, ColumnSpec, ColumnType, LayoutPlan,
};

use crate::normalize::NormalizedTickRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCtpActiveSegmentSnapshot {
    pub committed_row_count: usize,
    pub last_instrument_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenCtpSegmentDebugMetrics {
    pub committed_rows: usize,
}

pub struct OpenCtpSegmentIngress {
    writer: ActiveSegmentWriter,
}

impl OpenCtpSegmentIngress {
    pub fn for_test() -> Result<Self, &'static str> {
        let schema = compile_schema(&[
            ColumnSpec::new("dt", ColumnType::TimestampNsTz("Asia/Shanghai")),
            ColumnSpec::new("localtime_ns", ColumnType::Int64),
            ColumnSpec::new("source_emit_ns", ColumnType::Int64),
            ColumnSpec::new("instrument_id", ColumnType::Utf8),
            ColumnSpec::new("last_price", ColumnType::Float64),
        ])?;
        let layout = LayoutPlan::for_schema(&schema, 64)?;
        let writer = ActiveSegmentWriter::new_for_test(schema, layout)?;
        Ok(Self { writer })
    }

    pub fn write_row(&mut self, row: &NormalizedTickRow) -> Result<(), &'static str> {
        self.writer.append_tick_for_test(
            row.dt_ns,
            row.instrument_id.as_str(),
            row.last_price.unwrap_or_default(),
        )
    }

    pub fn active_snapshot(&self) -> Result<OpenCtpActiveSegmentSnapshot, &'static str> {
        let committed_row_count = self.writer.committed_row_count();
        let last_instrument_id = if committed_row_count == 0 {
            None
        } else {
            Some(
                self.writer
                    .read_utf8_for_test("instrument_id", committed_row_count - 1)?,
            )
        };

        Ok(OpenCtpActiveSegmentSnapshot {
            committed_row_count,
            last_instrument_id,
        })
    }

    pub fn debug_metrics(&self) -> OpenCtpSegmentDebugMetrics {
        OpenCtpSegmentDebugMetrics {
            committed_rows: self.writer.committed_row_count(),
        }
    }
}
