// src/schema/mod.rs

//! The `schema` module: download, parse and evolve NEM CTL schemas.

pub mod arrow;
pub mod ctl;
pub mod evolution;
pub mod fetch;
pub mod types;

pub use arrow::{build_arrow_schema, map_to_arrow_type};
pub use ctl::parse_ctl;
pub use evolution::{extract_column_types, find_column_types};
pub use fetch::fetch_all;
pub use types::{Column, CtlSchema, MonthSchema, SchemaEvolution};
