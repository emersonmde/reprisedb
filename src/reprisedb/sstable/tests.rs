#[cfg(test)]
mod tests {
    use std::path::Path;

    use rand::Rng;

    use crate::reprisedb::index;

    use super::*;

    async fn setup() -> (String, SSTable, BTreeMap<String, value::Kind>) {
        let sstable_dir = create_test_dir();
        let mut memtable: BTreeMap<String, value::Kind> = BTreeMap::new();
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let i: u8 = rng.gen();
            let key = format!("key{}", i);
            let value = format!("value{}", i);

            memtable.insert(key, value::Kind::Str(value));
        }
        let (sstable, index_handle) = SSTable::create(&sstable_dir, &memtable.clone())
            .await
            .unwrap();
        index_handle.await.unwrap().unwrap();
        (sstable_dir, sstable, memtable)
    }

    async fn teardown(sstable_path: String) {
        println!("Removing {}", sstable_path);
        fs::remove_dir_all(sstable_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_create() {
        let (sstable_path, sstable, _) = setup().await;
        assert!(Path::new(&sstable.path).exists());
        teardown(sstable_path).await;
    }

    #[tokio::test]
    async fn test_sstable_new() {
        let (sstable_path, _, _) = setup().await;
        let sstable = SSTable::new(&sstable_path).await;
        assert!(sstable.is_ok());
        teardown(sstable_path).await;
    }

    #[tokio::test]
    async fn test_sstable_get() {
        let (sstable_path, _, _) = setup().await;

        // Prepare test data
        let mut data = BTreeMap::new();
        data.insert("key1".to_string(), value::Kind::Int(42));
        data.insert("key2".to_string(), value::Kind::Float(4.2));
        data.insert("key3".to_string(), value::Kind::Str("42".to_string()));
        let (sstable, _) = SSTable::create(&sstable_path, &data).await.unwrap();

        assert_eq!(
            sstable.get("key1").await.unwrap().unwrap(),
            value::Kind::Int(42)
        );
        assert_eq!(
            sstable.get("key2").await.unwrap().unwrap(),
            value::Kind::Float(4.2)
        );
        assert_eq!(
            sstable.get("key3").await.unwrap().unwrap(),
            value::Kind::Str("42".to_string())
        );
        teardown(sstable_path).await;
    }

    #[tokio::test]
    async fn test_sstable_merge() {
        let (sstable_path1, sstable1, memtable1) = setup().await;
        let (sstable_path2, sstable2, memtable2) = setup().await;

        let merged_sstable = sstable1.merge(&sstable2, &sstable_path1).await.unwrap();

        for (key, value) in memtable1.iter() {
            let result = merged_sstable.get(key).await.unwrap();
            assert_eq!(result.unwrap(), value.clone());
        }

        for (key, value) in memtable2.iter() {
            let result = merged_sstable.get(key).await.unwrap();
            assert_eq!(result.unwrap(), value.clone());
        }

        teardown(sstable_path1).await;
        teardown(sstable_path2).await;
    }

    fn create_test_dir() -> String {
        let mut rng = rand::thread_rng();
        let i: u8 = rng.gen();
        let path = format!("/tmp/reprisedb_sstable_testdir{}", i);
        let sstable_path = Path::new(&path);
        if !sstable_path.exists() {
            std::fs::create_dir(sstable_path).unwrap();
        }
        path
    }
}
