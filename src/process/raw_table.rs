#[derive(Debug)]
pub struct RawTable {
    /// Column names, from the “I” row of the specific CSV file.
    /// While SchemaEvolution provides canonical names, these are what the file claims.
    pub headers: Vec<String>,
    /// Each “D” row, as a Vec of Strings (one per field).
    pub rows: Vec<Vec<String>>,
    /// The effective month (YYYYMM) for the data in this table, derived from the 'C' row of the CSV.
    pub effective_month: String,
}
