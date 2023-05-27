use reprisedb::Database;
use self::reprisedb::value::Value;

mod reprisedb;

fn main() {
    println!("Hello, world!");
    let mut db = Database::new("sstable").expect("Failed to create database");
    println!("put key");
    db.put("key".to_string(), Value::String("foo".to_string()));
    println!("db {:?}", db);
    println!("key: {:?}", db.get("key"));
    println!("put key1");
    db.put("key1".to_string(), Value::String("bar".to_string()));
    println!("key: {:?}", db.get("key"));
    println!("put key2");
    db.put("key2".to_string(), Value::String("baz".to_string()));
    println!("key: {:?}", db.get("key"));
    println!("key: {:?}", db.get("key"));
    println!("key1: {:?}", db.get("key1"));
    println!("key2: {:?}", db.get("key2"));
}
