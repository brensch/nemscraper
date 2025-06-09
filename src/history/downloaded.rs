use anyhow::Result;
use arrow::{
    array::{ArrayRef, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use std::{path::PathBuf, sync::Arc};

use crate::history::table_history::{HistoryRow, TableHistory};

pub struct DownloadedRow {
    pub filename: String,
    pub url: String,
    pub size_bytes: u64,
    pub download_start: DateTime<Utc>,
    pub download_end: DateTime<Utc>,
    pub thread: u32,
}

impl HistoryRow for DownloadedRow {
    const KEY_COLUMN: usize = 0;

    fn partition_date(&self) -> NaiveDate {
        self.download_end.date_naive()
    }

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("filename", ArrowDataType::Utf8, false),
            Field::new("url", ArrowDataType::Utf8, false),
            Field::new("size_bytes", ArrowDataType::UInt64, false),
            Field::new(
                "download_start",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "download_end",
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("thread", ArrowDataType::UInt32, false),
        ])
    }

    fn to_arrays(&self) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(vec![self.filename.clone()])),
            Arc::new(StringArray::from(vec![self.url.clone()])),
            Arc::new(UInt64Array::from(vec![self.size_bytes])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .download_start
                .timestamp_micros()])),
            Arc::new(TimestampMicrosecondArray::from(vec![self
                .download_end
                .timestamp_micros()])),
            Arc::new(UInt32Array::from(vec![self.thread])),
        ]
    }

    fn unique_key(&self) -> String {
        self.filename.clone()
    }
}

impl TableHistory<DownloadedRow> {
    pub fn new_downloaded(base: impl Into<PathBuf>) -> Result<Arc<Self>> {
        TableHistory::new(base, "downloaded")
    }
}
