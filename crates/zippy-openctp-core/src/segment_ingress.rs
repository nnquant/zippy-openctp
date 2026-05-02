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

    fn from_columnar_batch(
        batch: &OpenCtpColumnarTickBatch<'_>,
        row: usize,
        committed_row_count: usize,
    ) -> Self {
        Self {
            committed_row_count,
            dt_ns: Some(batch.dt_ns[row]),
            localtime_ns: Some(batch.localtime_ns[row]),
            source_emit_ns: Some(batch.source_emit_ns[row]),
            instrument_id: Some(batch.instrument_ids[row].to_string()),
            last_price: Some(batch.last_price[row]),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct OpenCtpSegmentDebugMetrics {
    pub committed_rows: usize,
    pub active_snapshot: Option<OpenCtpActiveSegmentSnapshot>,
}

pub struct OpenCtpColumnarTickBatch<'a> {
    pub exchange_id: &'a str,
    pub trading_day: &'a str,
    pub action_day: &'a str,
    pub instrument_ids: Vec<&'a str>,
    pub dt_ns: Vec<i64>,
    pub localtime_ns: Vec<i64>,
    pub source_emit_ns: Vec<i64>,
    pub last_price: Vec<f64>,
    pub volume: Vec<i64>,
    pub turnover: Vec<f64>,
    pub open_interest: Vec<f64>,
    pub bid_price_1: Vec<f64>,
    pub bid_volume_1: Vec<i64>,
    pub ask_price_1: Vec<f64>,
    pub ask_volume_1: Vec<i64>,
}

impl OpenCtpColumnarTickBatch<'_> {
    pub fn len(&self) -> usize {
        self.instrument_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn validate_lengths(&self) -> Result<(), &'static str> {
        let row_count = self.len();
        if self.dt_ns.len() == row_count
            && self.localtime_ns.len() == row_count
            && self.source_emit_ns.len() == row_count
            && self.last_price.len() == row_count
            && self.volume.len() == row_count
            && self.turnover.len() == row_count
            && self.open_interest.len() == row_count
            && self.bid_price_1.len() == row_count
            && self.bid_volume_1.len() == row_count
            && self.ask_price_1.len() == row_count
            && self.ask_volume_1.len() == row_count
        {
            return Ok(());
        }
        Err("columnar tick batch length mismatch")
    }
}

pub struct OpenCtpSegmentIngress {
    _store: SegmentStore,
    partition: PartitionHandle,
    writer: PartitionWriterHandle,
    row_capacity: usize,
    retired_segments: Vec<(u64, u64)>,
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
            retired_segments: Vec::new(),
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
                self.rollover_active_segment()?;
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
                    self.rollover_active_segment()?;
                    descriptor_changed = true;
                }
                Err(error) => return Err(map_segment_store_error(error)),
            }
        }

        Ok(descriptor_changed)
    }

    pub fn write_columnar_batch(
        &mut self,
        batch: &OpenCtpColumnarTickBatch<'_>,
    ) -> Result<bool, &'static str> {
        if batch.is_empty() {
            return Ok(false);
        }
        batch.validate_lengths()?;

        let mut descriptor_changed = false;
        let mut offset = 0;
        while offset < batch.len() {
            match self.try_write_columnar_batch(batch, offset) {
                Ok(0) => return Err("segment columnar write made no progress"),
                Ok(written) => {
                    offset += written;
                    self.committed_rows += written;
                    self.active_snapshot = OpenCtpActiveSegmentSnapshot::from_columnar_batch(
                        batch,
                        offset - 1,
                        self.committed_rows,
                    );
                }
                Err(ZippySegmentStoreError::Writer("segment is full")) => {
                    self.rollover_active_segment()?;
                    descriptor_changed = true;
                }
                Err(error) => return Err(map_segment_store_error(error)),
            }
        }

        Ok(descriptor_changed)
    }

    fn rollover_active_segment(&mut self) -> Result<(), &'static str> {
        let retired_identity = self.active_segment_identity();
        self.writer
            .rollover_without_persistence()
            .map_err(map_segment_store_error)?;
        self.retired_segments.push(retired_identity);
        Ok(())
    }

    pub fn release_retired_segments(&mut self) -> usize {
        let mut released = 0;
        self.retired_segments.retain(|(segment_id, generation)| {
            let removed = self
                .partition
                .release_retired_segment(*segment_id, *generation);
            if removed {
                released += 1;
            }
            !removed
        });
        released
    }

    pub fn retired_segment_count_for_test(&self) -> usize {
        self.retired_segments.len()
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

    fn try_write_columnar_batch(
        &self,
        batch: &OpenCtpColumnarTickBatch<'_>,
        offset: usize,
    ) -> Result<usize, ZippySegmentStoreError> {
        let remaining = batch.len().saturating_sub(offset);
        self.writer.write_columnar_rows(remaining, |columns, rows| {
            let end = offset + rows;
            columns.write_utf8_values("instrument_id", &batch.instrument_ids[offset..end])?;
            columns.write_utf8_repeated("exchange_id", batch.exchange_id, rows)?;
            columns.write_utf8_repeated("trading_day", batch.trading_day, rows)?;
            columns.write_utf8_repeated("action_day", batch.action_day, rows)?;
            columns.write_i64_values("dt", &batch.dt_ns[offset..end])?;
            columns.write_i64_values("localtime_ns", &batch.localtime_ns[offset..end])?;
            columns.write_i64_values("source_emit_ns", &batch.source_emit_ns[offset..end])?;
            columns.write_f64_values("last_price", &batch.last_price[offset..end])?;
            columns.write_i64_values("volume", &batch.volume[offset..end])?;
            columns.write_f64_values("turnover", &batch.turnover[offset..end])?;
            columns.write_f64_values("open_interest", &batch.open_interest[offset..end])?;
            columns.write_f64_values("bid_price_1", &batch.bid_price_1[offset..end])?;
            columns.write_i64_values("bid_volume_1", &batch.bid_volume_1[offset..end])?;
            columns.write_f64_values("ask_price_1", &batch.ask_price_1[offset..end])?;
            columns.write_i64_values("ask_volume_1", &batch.ask_volume_1[offset..end])?;
            Ok(())
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
