#[cfg(test)]
mod tests {
    use crate::models::value;
    use crate::reprisedb::{Database, DatabaseConfigBuilder};
    use std::fs;
    use uuid::Uuid;

    async fn setup() -> Database {
        let uuid = Uuid::new_v4();
        let path = format!("/tmp/reprisedb_testdir_{}", uuid);
        let config = DatabaseConfigBuilder::new()
            .sstable_dir(path.clone())
            .build();
        return Database::new(config).await.unwrap();
    }

    fn teardown(db: Database) {
        fs::remove_dir_all(&db.sstable_dir).unwrap();
    }

    #[tokio::test]
    async fn test_new_database() {
        let dir = "/tmp/unique_test_sstable_dir";
        let config = DatabaseConfigBuilder::new()
            .sstable_dir(dir.to_string())
            .build();
        let db = Database::new(config).await.unwrap();

        assert!(db.hot_memtable.read().await.is_empty());
        assert!(db.sstables.write().await.is_empty());
        assert_eq!(db.sstable_dir, dir);

        fs::remove_dir(dir).unwrap();
    }

    #[tokio::test]
    async fn test_put_get_item() {
        // create database instance, call put, verify that item is added correctly
        let mut db = setup().await;

        let int_value = value::Kind::Int(44);
        let float_value = value::Kind::Float(12.2);
        let string_value = value::Kind::Str("Test".to_string());
        db.put("int".to_string(), int_value.clone()).await.unwrap();
        db.put("float".to_string(), float_value.clone())
            .await
            .unwrap();
        db.put("string".to_string(), string_value.clone())
            .await
            .unwrap();
        assert_eq!(&db.get("int").await.unwrap().unwrap(), &int_value);
        assert_eq!(&db.get("float").await.unwrap().unwrap(), &float_value);
        assert_eq!(&db.get("string").await.unwrap().unwrap(), &string_value);

        teardown(db);
    }

    #[tokio::test]
    async fn test_flush_memtable() {
        let mut db = setup().await;
        let test_value = value::Kind::Int(44);
        db.put("test".to_string(), test_value.clone())
            .await
            .unwrap();

        db.flush_memtable().await.unwrap();

        assert!(db.hot_memtable.read().await.is_empty());
        assert!(!db.sstables.write().await.is_empty());

        teardown(db);
    }

    #[tokio::test]
    async fn test_get_item_after_memtable_flush() {
        let mut db = setup().await;
        let test_value = value::Kind::Int(44);
        db.put("test".to_string(), test_value.clone())
            .await
            .unwrap();
        db.flush_memtable().await.unwrap();

        assert_eq!(&db.get("test").await.unwrap().unwrap(), &test_value);

        teardown(db);
    }

    #[tokio::test]
    async fn test_compact_sstables() {
        let mut db = setup().await;
        for i in 0..125 {
            let key = format!("key{}", i);
            db.put(key, value::Kind::Int(i as i64)).await.unwrap();
        }
        db.flush_memtable().await.unwrap();

        for i in 75..200 {
            let key = format!("key{}", i);
            db.put(key, value::Kind::Int(i as i64)).await.unwrap();
        }
        db.flush_memtable().await.unwrap();

        let original_sstable_count = db.sstables.write().await.len();
        assert_eq!(original_sstable_count, 2);
        db.compact_sstables().await.unwrap();
        let compacted_sstable_count = db.sstables.write().await.len();
        assert_eq!(compacted_sstable_count, 1);

        for i in 0..200 {
            let key = format!("key{}", i);
            assert_eq!(
                &db.get(&key).await.unwrap().unwrap(),
                &value::Kind::Int(i as i64)
            );
        }

        teardown(db);
    }
}
