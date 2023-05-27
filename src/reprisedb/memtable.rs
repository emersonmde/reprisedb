use std::collections::BTreeMap;

use crate::reprisedb::value::Value;

#[derive(Debug, Clone)]
pub struct MemTable(BTreeMap<String, Value>);

impl MemTable {
    pub fn new() -> Self {
        MemTable(BTreeMap::new())
    }

    pub fn put(&mut self, key: String, value: Value) {
        self.0.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<String, Value> {
        self.0.iter()
    }

    pub fn clear(&mut self) {
        self.0.clear();
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
        memtable.put("foo".to_string(), Value::Int(42));
        assert!(!memtable.is_empty());
        assert_eq!(memtable.size(), 1);
        assert_eq!(memtable.get("foo"), Some(&Value::Int(42)));
        assert_eq!(memtable.get("bar"), None);
    }
}
