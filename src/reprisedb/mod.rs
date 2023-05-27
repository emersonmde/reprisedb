use std::path::Path;
use crate::reprisedb::memtable::MemTable;
use rand::Rng;
use value::Value;

mod sstable;
mod memtable;
pub mod value;

#[derive(Debug)]
pub struct Database {
    memtable: MemTable,
    sstables: Vec<sstable::SSTable>,
    sstable_dir: String,
}

impl Database {
    pub fn new(sstable_dir: &str) -> std::io::Result<Self> {
        let path = Path::new(sstable_dir);
        if !path.exists() {
            std::fs::create_dir(path)?;
        }

        let sstables = Vec::new();
        let memtable = MemTable::new();

        // let sstables = std::fs::read_dir(path)
        //     .unwrap()
        //     .map(|entry| {
        //         let entry = entry.unwrap();
        //         let path = entry.path();
        //         let filename = path.to_str().unwrap().to_string();
        //         sstable::SSTable::load(filename).unwrap()
        //     })
        //     .collect();

        Ok(Database {
            memtable, sstables, sstable_dir: String::from(sstable_dir)
        })
    }

    pub fn put(&mut self, key: String, value: Value) {
        self.memtable.put(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.memtable.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> Database {
        let mut rng = rand::thread_rng();
        let i: u8 = rng.gen();
        let path = format!("/tmp/reprisedb_sstring_testdir{}", i);
        println!("path: {}", path);
        return Database::new(&path).unwrap();
    }

    fn teardown(db: Database) {
        std::fs::remove_dir(db.sstable_dir).unwrap();
    }

    #[test]
    fn test_new_database() {
        let db = Database::new("test_sstable_dir").unwrap();

        assert!(db.memtable.is_empty());
        assert!(db.sstables.is_empty());
        assert_eq!(db.sstable_dir, "test_sstable_dir");

        std::fs::remove_dir("test_sstable_dir").unwrap();
    }

    #[test]
    fn test_put_item() {
        // create database instance, call put, verify that item is added correctly
        let mut db = setup();

        let int_value = Value::Int(44);
        let float_value = Value::Float(12.2);
        let string_value = Value::String("Test".to_string());
        db.put("int".to_string(), int_value.clone());
        db.put("float".to_string(), float_value.clone());
        db.put("string".to_string(), string_value.clone());
        assert_eq!(db.memtable.get("int").unwrap(), &int_value);
        assert_eq!(db.memtable.get("float").unwrap(), &float_value);
        assert_eq!(db.memtable.get("string").unwrap(), &string_value);

        teardown(db);
    }

    #[test]
    fn test_get_item() {
        // create database instance, add item, call get, verify that item is retrieved correctly
        // let mut db = setup();
        let mut db = setup();

        let value = Value::Int(44);
        db.put("int".to_string(), value.clone());
        let res = db.get("int").unwrap();
        assert_eq!(res, &value);

        teardown(db);
    }
}
