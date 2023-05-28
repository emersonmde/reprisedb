use std::collections::BTreeMap;
use crate::models::{value, ValueKindSize};

#[derive(Debug, Clone)]
pub struct MemTable {
    memtable: BTreeMap<String, value::Kind>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable { memtable: BTreeMap::new(), size: 0 }
    }

    pub fn put(&mut self, key: String, value: value::Kind) {
        self.size += key.len() + value.size();
        self.memtable.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&value::Kind> {
        self.memtable.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn clear(&mut self) {
        self.memtable.clear();
    }

    pub fn snapshot(&self) -> &BTreeMap<String, value::Kind> {
        &self.memtable
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
}
