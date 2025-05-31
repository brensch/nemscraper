// src/schema/types.rs

use serde::{Deserialize, Serialize};

/// A single column definition as parsed from a CTL.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq, Hash)]
pub struct Column {
    pub name: String,
    pub ty: String,
    pub format: Option<String>,
}
