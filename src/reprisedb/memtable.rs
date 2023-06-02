use crate::models::{value, ValueKindSize};
use std::collections::BTreeMap;

/// MemTable is an in-memory data structure for storing key-value pairs.
/// This struct is used as a write buffer in the LSM tree implementation.
#[derive(Debug, Clone)]
pub struct MemTable {
    memtable: BTreeMap<String, value::Kind>,
    size: usize,
}

impl MemTable {
    /// Create a new MemTable.
    pub fn new() -> Self {
        MemTable {
            memtable: BTreeMap::new(),
            size: 0,
        }
    }

    /// Put a key-value pair into the MemTable.
    ///
    /// Arguments:
    /// * `key` - The key to insert.
    /// * `value` - The value to insert.
    pub fn put(&mut self, key: String, value: value::Kind) {
        self.size += key.len() + value.size();
        self.memtable.insert(key, value);
    }

    /// Get a value from the MemTable.
    ///
    /// Arguments:
    /// * `key` - The key to get.
    pub fn get(&self, key: &str) -> Option<&value::Kind> {
        self.memtable.get(key)
    }

    /// Check if the MemTable is empty.
    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Get the size of the MemTable.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Clear the MemTable.
    pub fn clear(&mut self) {
        self.memtable.clear();
        self.size = 0;
    }

    /// Return a reference to the MemTable's BTreeMap.
    pub fn snapshot(&self) -> BTreeMap<String, value::Kind> {
        self.memtable.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable() {
        let mut memtable = MemTable::new();
        assert!(memtable.is_empty());
        assert_eq!(memtable.size(), 0);
        memtable.put("foo".to_string(), value::Kind::Int(42));
        assert!(!memtable.is_empty());
        assert_eq!(memtable.size(), 3 + 8); // 3 for "foo", 8 for the i64
        assert_eq!(memtable.get("foo"), Some(&value::Kind::Int(42)));
        assert_eq!(memtable.get("bar"), None);
    }

    #[test]
    fn test_clear() {
        let mut memtable = MemTable::new();
        memtable.put("foo".to_string(), value::Kind::Int(42));
        memtable.clear();
        assert!(memtable.is_empty());
        assert_eq!(memtable.size(), 0);
    }

    #[test]
    fn test_replace_value() {
        let mut memtable = MemTable::new();
        memtable.put("foo".to_string(), value::Kind::Int(42));
        memtable.put("foo".to_string(), value::Kind::Int(100));
        assert_eq!(memtable.get("foo"), Some(&value::Kind::Int(100)));
    }

    #[test]
    fn test_size_calculation() {
        let mut memtable = MemTable::new();
        memtable.put("foo".to_string(), value::Kind::Int(42));
        assert_eq!(memtable.size(), 3 + 8); // 3 for "foo", 8 for the i64
        memtable.put("bar".to_string(), value::Kind::Int(100));
        assert_eq!(memtable.size(), 2 * (3 + 8)); // Now two keys and two i64 values
    }
}
