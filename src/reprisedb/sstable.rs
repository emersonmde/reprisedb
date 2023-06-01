use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use prost::Message;
use tokio::fs::{self, File};
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};
use tokio::sync::{RwLock, RwLockWriteGuard};
use uuid::Uuid;

use crate::models;
use crate::models::value;
use crate::reprisedb::index::SparseIndex;

/// Struct representing an SSTable (sorted-string table) which is a key-value
/// storage format with each variable sized row prefixed by a u64 length.
///
/// An SSTable contains the following fields:
/// * `path`: The name of the file where the SSTable is stored.
/// * `size`: The size of the SSTable file.
#[derive(Debug, Clone)]
pub struct SSTable {
    pub(crate) path: PathBuf,
    index: Arc<RwLock<Option<SparseIndex>>>,
    #[allow(dead_code)]
    size: u64,
}

impl SSTable {
    /// Create a new SSTable instance from a file.
    ///
    /// # Arguments
    ///
    /// * `filename` - The name of the file where the SSTable is stored.
    ///
    /// # Returns
    ///
    /// A result that will be an instance of SSTable  or error.
    pub async fn new(filename: &str) -> io::Result<Self> {
        let metadata = fs::metadata(filename).await?;
        Ok(SSTable {
            path: PathBuf::from(filename),
            index: Arc::new(RwLock::new(None)),
            size: metadata.len(),
        })
    }

    /// Create a new SSTable from a memtable reference.
    ///
    /// # Arguments
    ///
    /// * `dir` - The directory where the new file should be created.
    /// * `snapshot` - A reference to the memtable (BTreeMap).
    ///
    /// # Returns
    ///
    /// A result that will be an instance of SSTable or error.
    pub async fn create(dir: &str, snapshot: &BTreeMap<String, value::Kind>) -> io::Result<Self> {
        let filename = Self::get_new_filename(dir);
        let file = File::create(filename.clone()).await?;
        let mut writer = BufWriter::new(file);
        let mut size: u64 = 0;

        for (key, value) in snapshot {
            let row = models::Row {
                key: key.clone(),
                value: Some(models::Value {
                    kind: Some(value.clone()),
                }),
            };

            let mut bytes = Vec::new();
            row.encode(&mut bytes).expect("Unable to encode row");

            // Write the length of the protobuf message as a u64 before the message itself.
            let len = bytes.len() as u64;
            let len_bytes = len.to_be_bytes();
            size += len + len_bytes.len() as u64;
            writer.write_all(&len_bytes).await?;
            writer.write_all(&bytes).await?;
        }

        let index_guard: Arc<RwLock<Option<SparseIndex>>> = Arc::new(RwLock::new(None));
        writer.flush().await?;
        let sstable = SSTable {
            path: PathBuf::from(filename),
            index: index_guard.clone(),
            size,
        };

        let index_clone = Arc::clone(&index_guard);
        let sstable_clone = sstable.clone();
        let index_future = tokio::task::spawn(async move {
            let mut index_clone_guard: RwLockWriteGuard<Option<SparseIndex>> =
                index_clone.write().await;
            let index = match SparseIndex::new(&sstable_clone).await {
                Ok(index) => index,
                Err(e) => {
                    eprintln!("Unable to create index: {}", e);
                    return Err(e);
                }
            };
            // TODO: Update this to take a Result once implmented
            let result = index.build_index().await;
            // if let Err(e) = result {
            //     eprintln!("Unable to flush index: {}", e);
            // }

            let result = index.flush_index().await;
            if let Err(e) = result {
                eprintln!("Unable to flush index: {}", e);
            }

            *index_clone_guard = Some(index);
            Ok(())
        });

        let result = index_future.await?;
        if let Err(e) = result {
            eprintln!("Error in index creation: {}", e);
            return Err(e);
        }

        Ok(sstable)
    }

    /// Get a value from the SSTable based on a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the value to get.
    ///
    /// # Returns
    ///
    /// A result that will be an option containing the value if the key was
    /// found, or an error if the operation failed. The option will be None
    /// if the key was not found in the SSTable.
    pub async fn get(&self, key: &str) -> std::io::Result<Option<models::value::Kind>> {
        let mut iter = self.iter().await?;

        while let Some(result) = iter.next().await {
            match result {
                Ok((row_key, value)) => {
                    if row_key == key {
                        return Ok(Some(value));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        Ok(None)
    }

    /// Merge this SSTable with another one, writing the merged data to a
    /// new SSTable.
    ///
    /// # Arguments
    ///
    /// * `sstable` - The other SSTable to merge with this one.
    /// * `dir` - The directory where the new merged SSTable should be created.
    ///
    /// # Returns
    ///
    /// A result that will be an instance of SSTable if the SSTables were
    /// successfully merged and written to a file, or an error if the
    /// operation failed.
    pub async fn merge(&self, sstable: &SSTable, dir: &str) -> io::Result<SSTable> {
        let mut iter1 = match self.iter().await {
            Ok(iter) => iter,
            Err(e) => return Err(e),
        };

        let mut iter2 = match sstable.iter().await {
            Ok(iter) => iter,
            Err(e) => return Err(e),
        };

        let filename = Self::get_new_filename(dir);

        let file = File::create(&filename).await?;
        let mut writer = BufWriter::new(file);

        let mut kv1 = iter1.next().await;
        let mut kv2 = iter2.next().await;
        while kv1.is_some() || kv2.is_some() {
            match (&kv1, &kv2) {
                (Some(Ok((k1, v1))), Some(Ok((k2, v2)))) => match k1.cmp(k2) {
                    std::cmp::Ordering::Less => {
                        Self::write_row(&mut writer, k1, v1).await?;
                        kv1 = iter1.next().await;
                    }
                    std::cmp::Ordering::Greater => {
                        Self::write_row(&mut writer, k2, v2).await?;
                        kv2 = iter2.next().await;
                    }
                    std::cmp::Ordering::Equal => {
                        Self::write_row(&mut writer, k1, v1).await?;
                        kv1 = iter1.next().await;
                        kv2 = iter2.next().await;
                    }
                },
                (Some(Ok((k1, v1))), None) => {
                    Self::write_row(&mut writer, k1, v1).await?;
                    kv1 = iter1.next().await;
                }
                (None, Some(Ok((k2, v2)))) => {
                    Self::write_row(&mut writer, k2, v2).await?;
                    kv2 = iter2.next().await;
                }
                _ => break,
            }
        }

        writer.flush().await?;

        let sstable = SSTable::new(filename.as_str()).await?;
        Ok(sstable)
    }

    /// Encodes a key and value into a Row protobuf message and writes it to the writer.
    async fn write_row(
        writer: &mut BufWriter<File>,
        key: &str,
        value: &value::Kind,
    ) -> io::Result<()> {
        let row = models::Row {
            key: key.to_string(),
            value: Some(models::Value {
                kind: Some(value.clone()),
            }),
        };

        let mut bytes = Vec::new();
        row.encode(&mut bytes)?;

        // Write the length of the protobuf message as a u64 before the message itself.
        let len = bytes.len() as u64;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&bytes).await?;

        Ok(())
    }

    /// Generates a new filename in the format of "{dir}/Ptimestamp}_{uuid}".
    fn get_new_filename(dir: &str) -> String {
        let now = SystemTime::now();
        let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let filename = format!("{}_{}", since_the_epoch.as_secs(), Uuid::new_v4());
        let path_str = format!("{}/{}", dir, filename);
        path_str
    }

    /// Creates an iterator over the rows in the SSTable.
    pub async fn iter(&self) -> io::Result<SSTableIter> {
        let mut file = File::open(&self.path).await?;
        file.seek(SeekFrom::Start(0)).await?;
        let buf_reader = Arc::new(RwLock::new(BufReader::new(file)));

        Ok(SSTableIter {
            buf_reader,
            buf: Vec::new(),
        })
    }
}

/// An iterator over the rows in an SSTable.
pub struct SSTableIter {
    buf_reader: Arc<RwLock<BufReader<File>>>,
    buf: Vec<u8>,
}

#[async_trait]
pub trait AsyncIterator {
    type Item;
    async fn next(&mut self) -> Option<Self::Item>;
}

#[async_trait]
impl AsyncIterator for SSTableIter {
    type Item = io::Result<(String, models::value::Kind)>;

    async fn next(&mut self) -> Option<Self::Item> {
        self.buf.clear();

        // First, read the length of the protobuf message (assuming it was written as a u64).
        let mut len_buf = [0u8; 8];
        let mut buf_reader = self.buf_reader.write().await;

        match buf_reader.read_exact(&mut len_buf).await {
            Ok(_) => {
                let len = u64::from_be_bytes(len_buf);

                // Then read that number of bytes into the buffer.
                self.buf.resize(len as usize, 0);

                match buf_reader.read_exact(&mut self.buf).await {
                    Ok(_) => {
                        let row: models::Row = match models::Row::decode(self.buf.as_slice()) {
                            Ok(row) => row,
                            Err(e) => return Some(Err(io::Error::new(io::ErrorKind::Other, e))),
                        };

                        Some(Ok((row.key, row.value.map(|v| v.kind.unwrap()).unwrap())))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // EOF
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rand::Rng;

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
        let sstable = SSTable::create(&sstable_dir, &memtable.clone())
            .await
            .unwrap();
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
        let sstable = SSTable::create(&sstable_path, &data).await.unwrap();

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
