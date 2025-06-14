use anyhow::Result;
use arrow::{
    array::{ArrayRef, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use std::{path::PathBuf, sync::Arc};

use crate::history::table_history::{HistoryRow, TableHistory};

pub struct ProcessedRow {
    pub filename: String,
    pub total_rows: u64,
    pub size_bytes: u64,
    pub processing_start: DateTime<Utc>,
    pub processing_end: DateTime<Utc>,
    pub thread: u32,
}

impl HistoryRow for ProcessedRow {
    const KEY_COLUMN: usize = 0;

    fn partition_date(&self) -> NaiveDate {
        self.processing_end.date_naive()
    }

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("filename", ArrowDataType::Utf8, false),
            Field::new("total_rows", ArrowDataType::UInt64, false),
            Field::new("size_bytes", ArrowDataType::UInt64, false),
            Field::new(
                "processing_start",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "processing_end",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("thread", ArrowDataType::UInt32, false),
        ])
    }

    fn to_arrays(&self) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(vec![self.filename.clone()])),
            Arc::new(UInt64Array::from(vec![self.total_rows])),
            Arc::new(UInt64Array::from(vec![self.size_bytes])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .processing_start
                .timestamp_micros()])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .processing_end
                .timestamp_micros()])),
            Arc::new(UInt32Array::from(vec![self.thread])),
        ]
    }

    fn unique_key(&self) -> String {
        self.filename.clone()
    }
}

impl TableHistory<ProcessedRow> {
    pub fn new_processed(base: impl Into<PathBuf>) -> Result<Arc<Self>> {
        TableHistory::new(base, "processed")
    }
}
