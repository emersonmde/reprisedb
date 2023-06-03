use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::RwLock;

use crate::reprisedb::sstable::iter::{AsyncIterator, SSTableIter};
use crate::reprisedb::sstable::SSTable;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SparseIndexData {
    index: BTreeMap<String, KeyMetadata>,
    created: chrono::DateTime<Utc>,
    version: u16,
}

#[derive(Debug)]
pub struct SparseIndex {
    index: Arc<RwLock<BTreeMap<String, KeyMetadata>>>,
    pub file: Arc<RwLock<File>>,
    sstable: Arc<SSTable>,
    created: chrono::DateTime<Utc>,
    version: u16,
}

impl SparseIndex {
    pub async fn new(sstable: &SSTable) -> io::Result<SparseIndex> {
        let filename = Self::get_index_filename(&sstable.path)?;
        let file = File::create(filename)
            .await
            .expect("Unable to create index file");
        Ok(SparseIndex {
            index: Arc::new(RwLock::new(BTreeMap::new())),
            file: Arc::new(RwLock::new(file)),
            sstable: Arc::new(sstable.clone()),
            created: Utc::now(),
            version: 1,
        })
    }

    pub async fn build_index(&self) -> io::Result<()> {
        println!("Building index for {}", self.sstable.path.display());
        let mut iter: SSTableIter = self.sstable.iter().await?;
        let mut i: u64 = 0;
        let mut num_keys: u64 = 0;
        while let Some(result) = iter.next_with_offset().await {
            if i % 2 != 0 {
                i += 1;
                continue;
            }
            match result {
                (offset, Ok((key, _))) => {
                    let mut index_guard = self.index.write().await;
                    index_guard.insert(key, KeyMetadata { offset });
                    num_keys += 1;
                }
                (_, Err(e)) => return Err(e),
            }
            i += 1;
        }
        println!(
            "Completed building index with {} keys for {}",
            num_keys,
            self.sstable.path.display()
        );
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

    pub async fn get_nearest_offset(&self, key: &str) -> Option<u64> {
        let index = self.index.read().await;
        let key = key.to_owned();
        // Get an iterator over all keys up to and including the given key
        let range = index.range(..=key);

        // The last element in the range is the largest key less than or equal to the given key
        if let Some((_, metadata)) = range.last() {
            return Some(metadata.offset);
        }
        None
    }

    pub fn get_index_filename(sstable_path: &PathBuf) -> io::Result<String> {
        match sstable_path.to_str() {
            Some(path_str) => Ok(format!("{}.index", path_str)),
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Path is not valid UTF-8",
            )),
        }
    }
}
