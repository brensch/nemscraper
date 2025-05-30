// src/schema/arrow.rs
use super::types::Column;
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
use arrow_schema::TimeUnit;
use std::sync::Arc;

/// Map a CTL column type + format into an Arrow DataType.
///
/// Only handles the types actually present in our schemas:
/// - CHAR*     → Utf8
/// - DATE      → Utf8 (non standard date format, needs post processing)
/// - FLOAT     → Float64
/// - TIMESTAMP → Int64 (epoch micros)
/// - fallback  → Utf8
pub fn map_to_arrow_type(ty: &str, _format: &Option<String>) -> DataType {
    let up = ty.to_ascii_uppercase();
    match up.as_str() {
        s if s.starts_with("CHAR") => DataType::Utf8,
        "DATE" => DataType::Timestamp(TimeUnit::Microsecond, None),
        s if s.starts_with("TIMESTAMP") => DataType::Timestamp(TimeUnit::Microsecond, None),
        "FLOAT" => DataType::Float64,
        _ => DataType::Utf8,
    }
}

/// Build an ArrowSchema (inside an Arc) from a slice of CTL `Column`s.
pub fn build_arrow_schema(cols: &[Column]) -> Arc<ArrowSchema> {
    let fields: Vec<ArrowField> = cols
        .iter()
        .map(|col| {
            let dt = map_to_arrow_type(&col.ty, &col.format);
            ArrowField::new(&col.name, dt, /* nullable = */ true)
        })
        .collect();
    Arc::new(ArrowSchema::new(fields))
}
