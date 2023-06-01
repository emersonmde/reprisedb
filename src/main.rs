use crate::models::value;
use crate::reprisedb::Database;
use crate::reprisedb::DatabaseConfigBuilder;

mod models;
mod reprisedb;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut db = Database::new(DatabaseConfigBuilder::new().build())
        .await
        .expect("Failed to create database");
    println!("sstables: {:?}", db.sstables.read().await);
    println!("size: {}\n", db.memtable.read().await.size());
    println!("put key1");
    db.put("key1".to_string(), value::Kind::Str("value1".to_string()))
        .await
        .unwrap();
    println!("key1: {:?}\n", db.get("key1").await);
    println!("size: {}\n", db.memtable.read().await.size());

    println!("put key2");
    db.put("key2".to_string(), value::Kind::Str("value2".to_string()))
        .await
        .unwrap();
    println!("key2: {:?}\n", db.get("key2").await);
    println!("size: {}\n", db.memtable.read().await.size());

    println!("put key3");
    db.put("key3".to_string(), value::Kind::Str("value3".to_string()))
        .await
        .unwrap();
    println!("key3: {:?}\n", db.get("key3").await);
    println!("size: {}\n", db.memtable.read().await.size());

    db.flush_memtable().await.unwrap();
    println!("Finshed flushing memtables");
    db.compact_sstables().await.unwrap();
    println!("Finshed compacting sstables");

    db.shutdown().await;
    Ok(())
}
