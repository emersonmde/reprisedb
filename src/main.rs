use reprisedb::Database;
use crate::models::value;

mod reprisedb;
mod models;

fn main() {
    println!("Hello, world!");
    let mut db = Database::new("sstable").expect("Failed to create database");
    println!("put key1");
    db.put("key1".to_string(), value::Kind::Str("value1".to_string()));
    println!("key1: {:?}\n", db.get("key1"));

    println!("put key2");
    db.put("key2".to_string(), value::Kind::Str("value2".to_string()));
    println!("key2: {:?}\n", db.get("key2"));

    println!("put key3");
    db.put("key3".to_string(), value::Kind::Str("value3".to_string()));
    println!("key3: {:?}\n", db.get("key3"));
}

