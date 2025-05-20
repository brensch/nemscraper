// src/schema/types.rs

use arrow::datatypes::Schema as ArrowSchema;
use std::sync::Arc;

use hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A single column definition as parsed from a CTL.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq, Hash)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}

/// One CTL schema: a table, the month, and its columns.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct CtlSchema {
    pub table: String,
    pub month: String,
    pub columns: Vec<Column>,
}

/// All CTL schemas for a given YYYYMM.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MonthSchema {
    pub month: String,
    pub schemas: Vec<CtlSchema>,
}

/// A period during which a table’s schema stayed constant.
#[derive(Debug, Clone)]
pub struct SchemaEvolution {
    pub table_name: String,
    pub fields_hash: String,
    pub start_month: String,
    pub end_month: String,
    pub columns: Vec<Column>,
    pub arrow_schema: Arc<ArrowSchema>,
}

/// Produce a stable SHA256 hash of a column list (sorted by name/type/format).
pub fn calculate_fields_hash(columns: &[Column]) -> String {
    let mut hasher = Sha256::new();
    let mut sorted = columns.to_vec();
    sorted.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| a.ty.cmp(&b.ty))
            .then_with(|| a.format.cmp(&b.format))
    });
    for col in sorted {
        hasher.update(col.name.as_bytes());
        hasher.update(col.ty.as_bytes());
        if let Some(fmt) = &col.format {
            hasher.update(fmt.as_bytes());
        }
        hasher.update(b";");
    }
    hex::encode(hasher.finalize())
}
