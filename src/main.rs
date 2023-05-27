use reprisedb::Database;

mod reprisedb;

fn main() {
    println!("Hello, world!");
    let mut db = Database::new();
    db.put("hello".to_string(), reprisedb::Value::Int(42));
    println!("{:?}", db.get("hello"));
}

