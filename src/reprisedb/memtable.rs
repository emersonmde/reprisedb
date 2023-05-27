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
}
