use arrow::datatypes::DataType;

/// 1) Trim whitespace + strip outer quotes if present.
pub fn clean_str(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

/// 2) Infer Arrow dtype from a cleaned string
pub fn infer_arrow_dtype_from_str(s: &str) -> DataType {
    if s.parse::<f64>().is_ok() {
        DataType::Float64
    } else {
        DataType::Utf8
    }
}

/// 8) Extract date from filename for partitioning.
pub fn extract_date_from_filename(filename: &str) -> Option<String> {
    // (your existing logic—omitted here for brevity)
    None
}
