use chrono::{FixedOffset, NaiveDate, TimeZone};

/// Fast parse of `"YYYY/MM/DD HH:MM:SS"` → millis UTC
pub fn parse_timestamp_millis(s: &str) -> Option<i64> {
    let s = s.trim();
    // minimal length + separators check
    if s.len() < 19 || &s[4..5] != "/" || &s[7..8] != "/" || &s[10..11] != " " {
        return None;
    }
    let year: i32 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;
    let hour: u32 = s[11..13].parse().ok()?;
    let min: u32 = s[14..16].parse().ok()?;
    let sec: u32 = s[17..19].parse().ok()?;

    let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, min, sec)?;
    let offset = FixedOffset::east_opt(10 * 3600).unwrap();
    offset
        .from_local_datetime(&naive)
        .single()
        .map(|dt| dt.timestamp_millis())
}
