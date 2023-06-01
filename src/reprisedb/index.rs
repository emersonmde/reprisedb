use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, self};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SparseIndexData {
    index: HashMap<String, KeyMetadata>,
    created: chrono::DateTime<Utc>,
    table_size: u64,
    version: u16,
}

#[derive(Debug)]
pub struct SparseIndex {
    index: Arc<RwLock<HashMap<String, KeyMetadata>>>,
    file: Arc<RwLock<File>>,
    table_size: u64,
    created: chrono::DateTime<Utc>,
    version: u16,
}

impl SparseIndex {
    pub async fn new(sstable_path: &str, table_size: u64) -> SparseIndex {
        let file = File::create(format!("{}-index", sstable_path))
            .await
            .expect("Unable to create index file");
        SparseIndex {
            index: Arc::new(RwLock::new(HashMap::new())),
            file: Arc::new(RwLock::new(file)),
            table_size,
            created: Utc::now(),
            version: 1,
        }
    }

    pub async fn build_index(&self) {
        let index = self.index.write().await;
        // Build index
        // TODO: Build index
    }

    pub async fn flush_index(&self) -> std::io::Result<()> {
        let index = self.index.read().await.clone();
        let created = self.created;
        let table_size = self.table_size;
        let version = self.version;
        let index_data = SparseIndexData {
            index,
            created,
            table_size,
            version,
        };

        let mut file = self.file.write().await;
        let mut writer = Vec::new();
        let result = bincode::serialize_into(&mut writer, &index_data);
        match result {
            Ok(_) => {
                file.write_all(&writer).await?;
                Ok(())
            },
            Err(e) => {
                Err(std::io::Error::new(std::io::ErrorKind::Other, e))
            },
        }
    }

    pub async fn get(&self, key: &str) -> Option<KeyMetadata> {
        let index = self.index.read().await;
        index.get(key).cloned()
    }

    pub async fn len(&self) -> usize {
        let index = self.index.read().await;
        index.len()
    }

    pub async fn is_empty(&self) -> bool {
        let index = self.index.read().await;
        index.is_empty()
    }
    pub fn table_size(&self) -> u64 {
        self.table_size
    }

    pub async fn insert(&self, key: String, metadata: KeyMetadata) {
        let mut index = self.index.write().await;
        index.insert(key, metadata);
    }

    pub async fn remove(&self, key: &str) {
        let mut index = self.index.write().await;
        index.remove(key);
    }
}
