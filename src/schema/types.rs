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
