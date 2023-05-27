use std::collections::BTreeMap;

mod sstable;

#[derive(Debug,Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
}


#[derive(Debug,Clone)]
pub struct MemTable(BTreeMap<String, Value>);

impl MemTable {
    pub fn new() -> Self {
        MemTable(BTreeMap::new())
    }

    pub fn put(&mut self, key: String, value: Value) {
        todo!("Implement MemTable::put");
        // self.0.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        todo!("Implement MemTable::put");
        // self.0.get(key)
    }
}

#[derive(Debug)]
pub struct Database {
    memtable: MemTable,
    sstables: Vec<sstable::SSTable>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            memtable: MemTable::new(),
            sstables: Vec::new(),
        }
    }

    pub fn put(&mut self, key: String, value: Value) {
        self.memtable.put(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.memtable.get(key)
    }
}