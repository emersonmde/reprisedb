use crate::models::value;
use crate::reprisedb::Database;

mod models;
mod reprisedb;


fn main() -> std::io::Result<()> {
    let mut db = Database::new("sstable").expect("Failed to create database");
    println!("sstables: {:?}", db.sstables.read());
    println!("size: {}\n", db.memtable.size());
    println!("put key1");
    db.put("key1".to_string(), value::Kind::Str("value1".to_string())).unwrap();
    println!("key1: {:?}\n", db.get("key1"));
    println!("size: {}\n", db.memtable.size());

    println!("put key2");
    db.put("key2".to_string(), value::Kind::Str("value2".to_string())).unwrap();
    println!("key2: {:?}\n", db.get("key2"));
    println!("size: {}\n", db.memtable.size());

    println!("put key3");
    db.put("key3".to_string(), value::Kind::Str("value3".to_string())).unwrap();
    println!("key3: {:?}\n", db.get("key3"));
    println!("size: {}\n", db.memtable.size());

    db.flush_memtable().unwrap();
    println!("Finshed flushing memtables");
    db.compact_sstables().unwrap();
    println!("Finshed compacting sstables");
    Ok(())
}

