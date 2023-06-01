use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use tokio::fs::{self, File};
use tokio::io::{self, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::models;
use crate::models::value;
use crate::reprisedb::index::SparseIndex;

use self::iter::{AsyncIterator, SSTableIter};

pub mod iter;

/// Struct representing an SSTable (sorted-string table) which is a key-value
/// storage format with each variable sized row prefixed by a u64 length.
///
/// An SSTable contains the following fields:
/// * `path`: The name of the file where the SSTable is stored.
/// * `size`: The size of the SSTable file.
#[derive(Debug, Clone)]
pub struct SSTable {
    pub(crate) path: PathBuf,
    #[allow(dead_code)]
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
    pub async fn create(
        dir: &str,
        snapshot: &BTreeMap<String, value::Kind>,
    ) -> io::Result<(Self, JoinHandle<io::Result<()>>)> {
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

        let index: Arc<RwLock<Option<SparseIndex>>> = Arc::new(RwLock::new(None));
        writer.flush().await?;
        let sstable = SSTable {
            path: PathBuf::from(filename),
            index: index.clone(),
            size,
        };

        let index_future = Self::create_index(index, &sstable);

        Ok((sstable, index_future))
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
        let offset = file.seek(SeekFrom::Start(0)).await?;
        let buf_reader = Arc::new(RwLock::new(BufReader::new(file)));

        Ok(SSTableIter {
            buf_reader,
            buf: Vec::new(),
            offset,
        })
    }

    fn create_index(
        index: Arc<RwLock<Option<SparseIndex>>>,
        sstable: &SSTable,
    ) -> JoinHandle<Result<(), io::Error>> {
        let index_clone = Arc::clone(&index);
        let sstable_clone = sstable.clone();
        tokio::task::spawn(async move {
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
            if let Err(e) = result {
                eprintln!("Unable to buildindex: {}", e);
            }

            let result = index.flush_index().await;
            if let Err(e) = result {
                eprintln!("Unable to flush index: {}", e);
            }

            *index_clone_guard = Some(index);
            Ok(())
        })
    }
}
