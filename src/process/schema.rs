use crate::process::date_parser;
use crate::process::utils::{clean_str, infer_arrow_dtype_from_str};
use anyhow::Result;
use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};

/// Holds final schema + which cols need date‐parsing or trimming
pub struct SchemaInfo {
    pub schema: Schema,
    pub date_columns: Vec<String>,
    pub trim_columns: Vec<String>,
    pub table_name: String,
}

/// Analyze first batch to detect dates, numbers, and trims
pub fn analyze_batch_for_schema(batch: &RecordBatch, headers: &[String]) -> Result<SchemaInfo> {
    let mut final_fields = Vec::with_capacity(headers.len());
    let mut date_columns = Vec::new();
    let mut trim_columns = Vec::new();

    for (i, name) in headers.iter().enumerate() {
        let col = batch.column(i);
        if let Some(sarr) = col.as_any().downcast_ref::<StringArray>() {
            if let Some(raw) = sarr.iter().find_map(|v| v) {
                let cleaned = clean_str(raw);
                if cleaned != raw {
                    trim_columns.push(name.clone());
                }
                if date_parser::parse_timestamp_millis(&cleaned).is_some() {
                    date_columns.push(name.clone());
                    final_fields.push(Field::new(
                        name,
                        DataType::Timestamp(TimeUnit::Millisecond, Some("+10:00".into())),
                        true,
                    ));
                    continue;
                }
                let ty = infer_arrow_dtype_from_str(&cleaned);
                final_fields.push(Field::new(name, ty, true));
                continue;
            }
        }
        final_fields.push(Field::new(name, DataType::Utf8, true));
    }

    let table_name = if headers.len() >= 4 {
        format!("{}---{}---{}", headers[1], headers[2], headers[3])
    } else {
        "default_table".into()
    };

    Ok(SchemaInfo {
        schema: Schema::new(final_fields),
        date_columns,
        trim_columns,
        table_name,
    })
}
