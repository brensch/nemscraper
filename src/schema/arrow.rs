// src/schema/arrow.rs

use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

use super::types::Column;

/// Map a CTL column type + format into an Arrow DataType.
///
/// Covers:
/// - DATE (NEM “DATE” includes time) → Timestamp(µs, +10:00)
/// - TIMESTAMP*                        → Timestamp(µs, +10:00)
/// - TIME*                             → Time64(µs)
/// - FLOAT, BINARY_FLOAT               → Float32
/// - DOUBLE, BINARY_DOUBLE             → Float64
/// - NUMBER, DECIMAL*, NUMERIC*        → Float64
/// - INTEGER, INT                      → Int32
/// - BIGINT                            → Int64
/// - SMALLINT                          → Int16
/// - TINYINT                           → Int8
/// - BOOLEAN                           → Boolean
/// - CHAR*, VARCHAR*, VARCHAR2*        → Utf8
/// - CLOB                              → LargeUtf8
/// - BLOB, RAW*                        → Binary
/// - fallback                          → Utf8
pub fn map_to_arrow_type(ty: &str, _format: &Option<String>) -> DataType {
    let upper = ty.to_ascii_uppercase();
    if upper == "DATE" || upper.starts_with("TIMESTAMP") {
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+10:00")))
    } else if upper.starts_with("TIME") {
        DataType::Time64(TimeUnit::Microsecond)
    } else if upper == "FLOAT" || upper == "BINARY_FLOAT" {
        DataType::Float32
    } else if upper == "DOUBLE" || upper == "BINARY_DOUBLE" {
        DataType::Float64
    } else if upper == "NUMBER" || upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
        DataType::Float64
    } else if upper == "INTEGER" || upper == "INT" {
        DataType::Int32
    } else if upper == "BIGINT" {
        DataType::Int64
    } else if upper == "SMALLINT" {
        DataType::Int16
    } else if upper == "TINYINT" {
        DataType::Int8
    } else if upper == "BOOLEAN" {
        DataType::Boolean
    } else if upper.starts_with("CHAR")
        || upper.starts_with("VARCHAR")
        || upper.starts_with("VARCHAR2")
    {
        DataType::Utf8
    } else if upper == "CLOB" {
        DataType::LargeUtf8
    } else if upper == "BLOB" || upper.starts_with("RAW") {
        DataType::Binary
    } else {
        // Catch-all for anything else (e.g. XMLType, etc.)
        DataType::Utf8
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
