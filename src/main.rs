use reprisedb::Database;
use self::reprisedb::value::Value;

mod reprisedb;

fn main() {
    println!("Hello, world!");
    let mut db = Database::new("sstable").expect("Failed to create database");
    println!("db {:?}", db);
    db.put("int".to_string(), Value::Int(44));
    println!("size {:?}", db.memtable.size());
    db.put("float".to_string(), Value::Float(33.33));
    println!("size {:?}", db.memtable.size());
    db.put("string".to_string(), Value::String("bar".to_string()));
    println!("size {:?}", db.memtable.size());
    db.put("stringr1".to_string(), Value::String("bar".to_string()));
    println!("memtable: {:?}", db.memtable);
    println!("size {:?}", db.memtable.size());
    println!("{:?}", db.get("int"));
    println!("{:?}", db.get("float"));
    println!("{:?}", db.get("string"));
    println!("{:?}", db.get("stringr1"));
    println!("{:?}", db.get("none"));
}
