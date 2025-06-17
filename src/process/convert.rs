use crate::process::schema::SchemaInfo;
use crate::process::{date_parser, utils};
use anyhow::Result;
use arrow::{
    array::{ArrayRef, Float64Builder, StringArray, TimestampMillisecondBuilder},
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Convert string columns into your final types
pub fn convert_to_final_types(
    batch: &RecordBatch,
    schema_info: &SchemaInfo,
) -> Result<RecordBatch> {
    let mut out = Vec::with_capacity(batch.num_columns());

    for (arr, fld) in batch.columns().iter().zip(schema_info.schema.fields()) {
        match (arr.as_any().downcast_ref::<StringArray>(), fld.data_type()) {
            // Date → timestamp
            (Some(sarr), DataType::Timestamp(TimeUnit::Millisecond, _))
                if schema_info.date_columns.contains(fld.name()) =>
            {
                let mut b = TimestampMillisecondBuilder::new();
                for opt in sarr.iter() {
                    let ts = opt.and_then(|s| {
                        let c = utils::clean_str(s);
                        date_parser::parse_timestamp_millis(&c)
                    });
                    b.append_option(ts);
                }
                let col = b.finish().with_timezone("+10:00");
                out.push(Arc::new(col) as ArrayRef);
            }

            // Numeric → f64
            (Some(sarr), DataType::Float64) => {
                let mut b = Float64Builder::new();
                for opt in sarr.iter() {
                    let v = opt.and_then(|s| utils::clean_str(s).parse().ok());
                    b.append_option(v);
                }
                out.push(Arc::new(b.finish()) as ArrayRef);
            }

            // Everything else
            _ => out.push(arr.clone()),
        }
    }

    let schema = Arc::new(schema_info.schema.clone());
    RecordBatch::try_new(schema, out).map_err(Into::into)
}
