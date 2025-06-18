use anyhow::Result;
use arrow::array::ArrayRef;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use std::{
    path::PathBuf,
    sync::Arc,
};

use crate::history::table_history::{HistoryRow, TableHistory};

/// Tracks compaction of individual input files, with start/end times.
pub struct InputCompactionRow {
    pub input_file: String,
    pub partition: NaiveDate,
    pub compaction_start: DateTime<Utc>,
    pub compaction_end: DateTime<Utc>,
    pub thread: u32,
}

impl HistoryRow for InputCompactionRow {
    const KEY_COLUMN: usize = 0; // unique by input_file

    fn partition_date(&self) -> NaiveDate {
        self.compaction_end.date_naive()
    }

    fn schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit};
        ArrowSchema::new(vec![
            Field::new("input_file", ArrowDataType::Utf8, false),
            Field::new("partition", ArrowDataType::Date32, false),
            Field::new(
                "compaction_start",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "compaction_end",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("thread", ArrowDataType::UInt32, false),
        ])
    }

    fn to_arrays(&self) -> Vec<ArrayRef> {
        use arrow::array::{StringArray, TimestampMicrosecondArray, UInt32Array};
        vec![
            Arc::new(StringArray::from(vec![self.input_file.clone()])),
            Arc::new(arrow::array::Date32Array::from(vec![
                self.partition.num_days_from_ce() as i32,
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .compaction_start
                .timestamp_micros()])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .compaction_end
                .timestamp_micros()])),
            Arc::new(UInt32Array::from(vec![self.thread])),
        ]
    }

    fn unique_key(&self) -> String {
        self.input_file.clone()
    }
}

impl TableHistory<InputCompactionRow> {
    pub fn new_compacted(base: impl Into<PathBuf>) -> Result<Arc<Self>> {
        TableHistory::new(base, "compacted")
    }
}
