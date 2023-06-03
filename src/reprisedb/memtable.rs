use tokio::sync::RwLock;
use tracing::instrument;

use crate::models::{value, ValueKindSize};
use std::{collections::{BTreeMap}, sync::{Arc, atomic::{AtomicUsize, Ordering}}};

/// MemTable is an in-memory data structure for storing key-value pairs.
/// This struct is used as a write buffer in the LSM tree implementation.
#[derive(Debug, Clone)]
pub struct MemTable {
    memtable: Arc<RwLock<BTreeMap<String, value::Kind>>>,
    size: Arc<AtomicUsize>,
}

impl MemTable {
    /// Create a new MemTable.
    pub fn new() -> Self {
        MemTable {
            memtable: Arc::new(RwLock::new(BTreeMap::new())),
            size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Put a key-value pair into the MemTable.
    ///
    /// Arguments:
    /// * `key` - The key to insert.
    /// * `value` - The value to insert.
    pub async fn put(&mut self, key: String, value: value::Kind) {
        let mut memtable = self.memtable.write().await;
        self.size.fetch_add(key.len() + value.size(), Ordering::SeqCst);
        memtable.insert(key, value);
    }

    /// Get a value from the MemTable.
    ///
    /// Arguments:
    /// * `key` - The key to get.
    #[instrument]
    pub async fn get(&self, key: &str) -> Option<value::Kind> {
        let memtable = self.memtable.read().await;
        memtable.get(key).cloned()
    }

    /// Check if the MemTable is empty.
    pub fn is_empty(&self) -> bool {
        self.size.load(Ordering::SeqCst) == 0
    }

    /// Get the size of the MemTable.
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    /// Clear the MemTable.
    pub async fn clear(&mut self) {
        self.memtable.write().await.clear();
        self.size.store(0, Ordering::SeqCst);
    }

    /// Return a reference to the MemTable's BTreeMap.
    pub async fn snapshot(&self) -> BTreeMap<String, value::Kind> {
        // self.memtable.clone().into_iter().collect()
        self.memtable.write().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memtable() {
        let mut memtable = MemTable::new();
        assert!(memtable.is_empty());
        assert_eq!(memtable.size(), 0);
        memtable.put("foo".to_string(), value::Kind::Int(42)).await;
        assert!(!memtable.is_empty());
        assert_eq!(memtable.size(), 3 + 8); // 3 for "foo", 8 for the i64
        assert_eq!(memtable.get("foo").await, Some(value::Kind::Int(42)));
        assert_eq!(memtable.get("bar").await, None);
    }

    #[tokio::test]
    async fn test_clear() {
        let mut memtable = MemTable::new();
        memtable.put("foo".to_string(), value::Kind::Int(42)).await;
        memtable.clear().await;
        assert!(memtable.is_empty());
        assert_eq!(memtable.size(), 0);
    }

    #[tokio::test]
    async fn test_replace_value() {
        let mut memtable = MemTable::new();
        memtable.put("foo".to_string(), value::Kind::Int(42)).await;
        memtable.put("foo".to_string(), value::Kind::Int(100)).await;
        assert_eq!(memtable.get("foo").await, Some(value::Kind::Int(100)));
    }

    #[tokio::test]
    async fn test_size_calculation() {
        let mut memtable = MemTable::new();
        memtable.put("foo".to_string(), value::Kind::Int(42)).await;
        assert_eq!(memtable.size(), 3 + 8); // 3 for "foo", 8 for the i64
        memtable.put("bar".to_string(), value::Kind::Int(100)).await;
        assert_eq!(memtable.size(), 2 * (3 + 8)); // Now two keys and two i64 values
    }
}
