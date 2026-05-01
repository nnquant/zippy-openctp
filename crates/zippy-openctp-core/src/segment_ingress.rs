use zippy_segment_store::{
    compile_schema, ActiveSegmentReader, ColumnSpec, ColumnType, LayoutPlan, PartitionHandle,
    PartitionRowWriter, PartitionWriterHandle, SegmentStore, SegmentStoreConfig,
    ZippySegmentStoreError,
};

use crate::normalize::NormalizedTickRow;
use crate::schema::{TickSchemaType, TICK_SCHEMA_FIELDS};

const SOURCE_SEGMENT_ROW_CAPACITY: usize = 32768;

#[derive(Debug, Clone, PartialEq)]
pub struct OpenCtpActiveSegmentSnapshot {
    pub committed_row_count: usize,
    pub dt_ns: Option<i64>,
    pub localtime_ns: Option<i64>,
    pub source_emit_ns: Option<i64>,
    pub instrument_id: Option<String>,
    pub last_price: Option<f64>,
}

impl OpenCtpActiveSegmentSnapshot {
    fn empty() -> Self {
        Self {
            committed_row_count: 0,
            dt_ns: None,
            localtime_ns: None,
            source_emit_ns: None,
            instrument_id: None,
            last_price: None,
        }
    }

    fn from_row(row: &NormalizedTickRow, committed_row_count: usize) -> Self {
        Self {
            committed_row_count,
            dt_ns: Some(row.dt_ns),
            localtime_ns: Some(row.localtime_ns),
            source_emit_ns: Some(row.source_emit_ns),
            instrument_id: Some(row.instrument_id.clone()),
            last_price: row.last_price,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OpenCtpSegmentDebugMetrics {
    pub committed_rows: usize,
    pub active_snapshot: Option<OpenCtpActiveSegmentSnapshot>,
}

pub struct OpenCtpSegmentIngress {
    _store: SegmentStore,
    partition: PartitionHandle,
    writer: PartitionWriterHandle,
    row_capacity: usize,
    committed_rows: usize,
    active_snapshot: OpenCtpActiveSegmentSnapshot,
}

impl OpenCtpSegmentIngress {
    pub fn new_for_source() -> Result<Self, &'static str> {
        Self::new_with_row_capacity(SOURCE_SEGMENT_ROW_CAPACITY)
    }

    pub fn for_test() -> Result<Self, &'static str> {
        Self::new_with_row_capacity(64)
    }

    fn new_with_row_capacity(row_capacity: usize) -> Result<Self, &'static str> {
        let schema = openctp_segment_schema()?;
        let store = SegmentStore::new(SegmentStoreConfig {
            default_row_capacity: row_capacity,
        })
        .map_err(map_segment_store_error)?;
        let partition = store
            .open_partition_with_schema("openctp.tick", "default", schema)
            .map_err(map_segment_store_error)?;
        let writer = partition.writer();
        Ok(Self {
            _store: store,
            partition,
            writer,
            row_capacity,
            committed_rows: 0,
            active_snapshot: OpenCtpActiveSegmentSnapshot::empty(),
        })
    }

    pub fn write_row(&mut self, row: &NormalizedTickRow) -> Result<(), &'static str> {
        match self.try_write_row(row) {
            Ok(()) => {
                self.committed_rows += 1;
                self.active_snapshot =
                    OpenCtpActiveSegmentSnapshot::from_row(row, self.committed_rows);
                Ok(())
            }
            Err(ZippySegmentStoreError::Writer("segment is full")) => {
                self.writer.rollover().map_err(map_segment_store_error)?;
                self.try_write_row(row).map_err(map_segment_store_error)?;
                self.committed_rows += 1;
                self.active_snapshot =
                    OpenCtpActiveSegmentSnapshot::from_row(row, self.committed_rows);
                Ok(())
            }
            Err(error) => Err(map_segment_store_error(error)),
        }
    }

    pub fn write_rows(&mut self, rows: &[NormalizedTickRow]) -> Result<bool, &'static str> {
        if rows.is_empty() {
            return Ok(false);
        }

        let mut descriptor_changed = false;
        let mut offset = 0;
        while offset < rows.len() {
            match self.try_write_rows(&rows[offset..]) {
                Ok(0) => return Err("segment batch write made no progress"),
                Ok(written) => {
                    offset += written;
                    self.committed_rows += written;
                    self.active_snapshot = OpenCtpActiveSegmentSnapshot::from_row(
                        &rows[offset - 1],
                        self.committed_rows,
                    );
                }
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    self.writer.rollover().map_err(map_segment_store_error)?;
                    descriptor_changed = true;
                }
                Err(error) => return Err(map_segment_store_error(error)),
            }
        }

        Ok(descriptor_changed)
    }

    fn try_write_row(&self, row: &NormalizedTickRow) -> Result<(), ZippySegmentStoreError> {
        self.writer
            .write_row(|writer| write_normalized_tick_row(writer, row))
    }

    fn try_write_rows(&self, rows: &[NormalizedTickRow]) -> Result<usize, ZippySegmentStoreError> {
        self.writer.write_rows(rows.len(), |writer, index| {
            write_normalized_tick_row(writer, &rows[index])
        })
    }

    pub fn active_snapshot(&self) -> Result<OpenCtpActiveSegmentSnapshot, &'static str> {
        Ok(self.active_snapshot.clone())
    }

    pub fn debug_metrics(&self) -> Result<OpenCtpSegmentDebugMetrics, &'static str> {
        let active_snapshot = self.active_snapshot()?;
        Ok(OpenCtpSegmentDebugMetrics {
            committed_rows: self.committed_rows,
            active_snapshot: Some(active_snapshot),
        })
    }

    pub fn active_descriptor_envelope_bytes(&self) -> Result<Vec<u8>, &'static str> {
        self.partition
            .active_descriptor_envelope_bytes()
            .map_err(map_segment_store_error)
    }

    pub fn active_reader(&self) -> Result<ActiveSegmentReader, ZippySegmentStoreError> {
        let descriptor_envelope = self.partition.active_descriptor_envelope_bytes()?;
        let schema = openctp_segment_schema().map_err(ZippySegmentStoreError::Schema)?;
        let layout = LayoutPlan::for_schema(&schema, self.row_capacity)
            .map_err(ZippySegmentStoreError::Layout)?;
        ActiveSegmentReader::from_descriptor_envelope(&descriptor_envelope, schema, layout)
    }

    pub fn update_active_reader(
        &self,
        reader: &mut ActiveSegmentReader,
    ) -> Result<(), ZippySegmentStoreError> {
        let descriptor_envelope = self.partition.active_descriptor_envelope_bytes()?;
        let schema = openctp_segment_schema().map_err(ZippySegmentStoreError::Schema)?;
        let layout = LayoutPlan::for_schema(&schema, self.row_capacity)
            .map_err(ZippySegmentStoreError::Layout)?;
        reader.update_descriptor_envelope(&descriptor_envelope, schema, layout)
    }

    pub fn active_segment_identity(&self) -> (u64, u64) {
        self.partition.active_segment_identity()
    }

    /// 仅用于测试：返回可供同进程 reader attach 的上下文。
    pub fn reader_context_for_test(&self) -> (SegmentStore, PartitionHandle) {
        (self._store.clone(), self.partition.clone())
    }
}

fn write_normalized_tick_row(
    writer: &mut PartitionRowWriter<'_>,
    row: &NormalizedTickRow,
) -> Result<(), ZippySegmentStoreError> {
    writer.write_utf8("instrument_id", row.instrument_id.as_str())?;
    if let Some(value) = row.exchange_id.as_deref() {
        writer.write_utf8("exchange_id", value)?;
    }
    if let Some(value) = row.trading_day.as_deref() {
        writer.write_utf8("trading_day", value)?;
    }
    if let Some(value) = row.action_day.as_deref() {
        writer.write_utf8("action_day", value)?;
    }
    writer.write_i64("dt", row.dt_ns)?;
    writer.write_i64("localtime_ns", row.localtime_ns)?;
    writer.write_i64("source_emit_ns", row.source_emit_ns)?;
    if let Some(value) = row.last_price {
        writer.write_f64("last_price", value)?;
    }
    if let Some(value) = row.volume {
        writer.write_i64("volume", value)?;
    }
    if let Some(value) = row.turnover {
        writer.write_f64("turnover", value)?;
    }
    if let Some(value) = row.open_interest {
        writer.write_f64("open_interest", value)?;
    }
    if let Some(value) = row.bid_price_1 {
        writer.write_f64("bid_price_1", value)?;
    }
    if let Some(value) = row.bid_volume_1 {
        writer.write_i64("bid_volume_1", value)?;
    }
    if let Some(value) = row.ask_price_1 {
        writer.write_f64("ask_price_1", value)?;
    }
    if let Some(value) = row.ask_volume_1 {
        writer.write_i64("ask_volume_1", value)?;
    }
    Ok(())
}

fn map_segment_store_error(error: ZippySegmentStoreError) -> &'static str {
    match error {
        ZippySegmentStoreError::Schema(reason)
        | ZippySegmentStoreError::Layout(reason)
        | ZippySegmentStoreError::Writer(reason)
        | ZippySegmentStoreError::Lifecycle(reason) => reason,
        ZippySegmentStoreError::Io(_) => "segment store io error",
        ZippySegmentStoreError::Shmem(_) => "segment store shared memory error",
        ZippySegmentStoreError::Arrow(_) => "segment store arrow error",
    }
}

pub fn openctp_segment_schema() -> Result<zippy_segment_store::CompiledSchema, &'static str> {
    let columns = TICK_SCHEMA_FIELDS
        .iter()
        .map(|field| {
            let data_type = match field.data_type {
                TickSchemaType::Utf8 => ColumnType::Utf8,
                TickSchemaType::TimestampNsShanghai => ColumnType::TimestampNsTz("Asia/Shanghai"),
                TickSchemaType::Float64 => ColumnType::Float64,
                TickSchemaType::Int64 => ColumnType::Int64,
            };
            if field.nullable {
                ColumnSpec::nullable(field.name, data_type)
            } else {
                ColumnSpec::new(field.name, data_type)
            }
        })
        .collect::<Vec<_>>();
    compile_schema(&columns)
}
