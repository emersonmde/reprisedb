use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct SparseIndex {
    index: HashMap<String, KeyMetadata>,
    // pub bloom_filter: Vec<u8>,
    table_size: u64,
    created: chrono::DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct KeyMetadata {
    offset: u64,
}