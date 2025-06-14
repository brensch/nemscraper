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

/// Extracts a date partition string from the filename, trying:
///  - an 8-digit contiguous `YYYYMMDD`
///  - a 10-char `YYYY-MM-DD` or `YYYY_MM_DD`
/// Returns `Some("YYYY-MM-DD")` if found, else `None`.
pub fn extract_date_from_filename(filename: &str) -> Option<String> {
    let chars: Vec<char> = filename.chars().collect();

    // Try YYYYMMDD
    for i in 0..=chars.len().saturating_sub(8) {
        let slice = &chars[i..i + 8];
        if slice.iter().all(|c| c.is_ascii_digit()) {
            let s: String = slice.iter().collect();
            let (y, m, d) = (
                &s[0..4].parse::<u32>().ok()?,
                &s[4..6].parse::<u32>().ok()?,
                &s[6..8].parse::<u32>().ok()?,
            );
            if (2000..=2030).contains(y) && (1..=12).contains(m) && (1..=31).contains(d) {
                return Some(format!("{:04}-{:02}-{:02}", y, m, d));
            }
        }
    }

    // Try YYYY-MM-DD or YYYY_MM_DD
    for i in 0..=chars.len().saturating_sub(10) {
        let slice: String = chars[i..i + 10].iter().collect();
        if &slice[4..5] == "-" || &slice[4..5] == "_" {
            let sep = &slice[4..5];
            let parts: Vec<&str> = slice.split(sep).collect();
            if parts.len() == 3 {
                let (y, m, d) = (
                    parts[0].parse::<u32>().ok()?,
                    parts[1].parse::<u32>().ok()?,
                    parts[2].parse::<u32>().ok()?,
                );
                if (2000..=2030).contains(&y) && (1..=12).contains(&m) && (1..=31).contains(&d) {
                    return Some(format!("{:04}-{:02}-{:02}", y, m, d));
                }
            }
        }
    }

    None
}
