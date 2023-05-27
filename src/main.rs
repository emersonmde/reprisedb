use reprisedb::Database;
use self::reprisedb::value::Value;

mod reprisedb;

fn main() {
    println!("Hello, world!");
    let mut db = Database::new("sstable").expect("Failed to create database");
    db.put("int".to_string(), Value::Int(44));
    db.put("float".to_string(), Value::Float(33.33));
    db.put("string".to_string(), Value::String("bar".to_string()));
    println!("{:?}", db.get("int"));
    println!("{:?}", db.get("float"));
    println!("{:?}", db.get("string"));
    println!("{:?}", db.get("none"));
}
