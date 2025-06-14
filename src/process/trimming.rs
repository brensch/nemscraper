use crate::process::utils::clean_str;
use anyhow::Result;
use arrow::{
    array::{ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Apply trimming to flagged columns
pub fn apply_trimming(batch: &RecordBatch, trim_columns: &[String]) -> Result<RecordBatch> {
    if trim_columns.is_empty() {
        return Ok(batch.clone());
    }

    let mut cols = Vec::with_capacity(batch.num_columns());
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let arr = batch.column(i);
        if trim_columns.contains(field.name()) {
            if let Some(sarr) = arr.as_any().downcast_ref::<StringArray>() {
                let trimmed: StringArray = sarr.iter().map(|opt| opt.map(clean_str)).collect();
                cols.push(Arc::new(trimmed) as ArrayRef);
                continue;
            }
        }
        cols.push(arr.clone());
    }

    RecordBatch::try_new(batch.schema(), cols).map_err(Into::into)
}
