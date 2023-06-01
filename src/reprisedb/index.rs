use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::reprisedb::sstable::iter::{SSTableIter, AsyncIterator};
use crate::reprisedb::sstable::SSTable;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SparseIndexData {
    index: HashMap<String, KeyMetadata>,
    created: chrono::DateTime<Utc>,
    version: u16,
}

#[derive(Debug)]
pub struct SparseIndex {
    index: Arc<RwLock<HashMap<String, KeyMetadata>>>,
    file: Arc<RwLock<File>>,
    sstable: Arc<SSTable>,
    created: chrono::DateTime<Utc>,
    version: u16,
}

#[allow(dead_code)]
impl SparseIndex {
    pub async fn new(sstable: &SSTable) -> io::Result<SparseIndex> {
        let filename = Self::get_index_filename(&sstable.path)?;
        let file = File::create(filename)
            .await
            .expect("Unable to create index file");
        Ok(SparseIndex {
            index: Arc::new(RwLock::new(HashMap::new())),
            file: Arc::new(RwLock::new(file)),
            sstable: Arc::new(sstable.clone()),
            created: Utc::now(),
            version: 1,
        })
    }

    pub async fn build_index(&self) -> io::Result<()> {
        let mut iter: SSTableIter = self.sstable.iter().await?;
        while let Some(result) = iter.next_with_offset().await {
            match result {
                (offset, Ok((key, _))) => {
                    let mut index_guard = self.index.write().await;
                    index_guard.insert(key, KeyMetadata { offset });
                }
                (_, Err(e)) => return Err(e),
            }
        }
        Ok(())
    }

    pub async fn flush_index(&self) -> std::io::Result<()> {
        let index = self.index.read().await.clone();
        let created = self.created;
        let version = self.version;
        let index_data = SparseIndexData {
            index,
            created,
            version,
        };

        let mut file = self.file.write().await;
        let mut writer = Vec::new();
        let result = bincode::serialize_into(&mut writer, &index_data);
        match result {
            Ok(_) => {
                file.write_all(&writer).await?;
                Ok(())
            }
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
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

    pub async fn insert(&self, key: String, metadata: KeyMetadata) {
        let mut index = self.index.write().await;
        index.insert(key, metadata);
    }

    pub async fn remove(&self, key: &str) {
        let mut index = self.index.write().await;
        index.remove(key);
    }

    fn get_index_filename(sstable_path: &PathBuf) -> io::Result<String> {
        match sstable_path.to_str() {
            Some(path_str) => Ok(format!("{}-index", path_str)),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Path is not valid UTF-8",
            )),
        }
    }
}
