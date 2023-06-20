use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bloombox::BloomBox;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::fs::{self, File};
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};
use tokio::sync::{Mutex, RwLock, RwLockWriteGuard};
use tokio::task::JoinHandle;
use tracing::instrument;
use uuid::Uuid;

use crate::models;
use crate::models::value;
use crate::reprisedb::index::SparseIndex;

use self::iter::{AsyncIterator, SSTableIter};

pub mod iter;

const BLOOM_FILTER_NUM_ITEMS: usize = 250_000;
const BLOOM_FILTER_FP_RATE: f64 = 0.01;

#[derive(Serialize, Deserialize)]
struct SSTableMetadata {
    path: PathBuf,
    size: u64,
    bloom_filter: BloomBox,
    created_timestamp: Duration,
}

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
    pub index: Arc<RwLock<Option<SparseIndex>>>,
    #[allow(dead_code)]
    size: u64,
    bloom_filter: Arc<RwLock<BloomBox>>,
    #[allow(dead_code)]
    created_timestamp: Duration,
    header_size: u64,
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
    #[instrument]
    pub async fn new(filename: &str) -> io::Result<Self> {
        let fs_metadata = fs::metadata(filename).await?;
        let mut file = File::open(&filename).await?;
        file.seek(SeekFrom::Start(0)).await?;

        let mut len_buf = [0; 8];
        file.read_exact(len_buf.as_mut()).await?;
        let len = u64::from_be_bytes(len_buf);

        let mut metadata_buf = vec![0; len as usize];
        file.read_exact(&mut metadata_buf).await?;
        let metadata: Result<SSTableMetadata, _> = bincode::deserialize(&metadata_buf);
        let metadata = match metadata {
            Ok(metadata) => metadata,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unable to deserialize SSTable metadata",
                ))
            }
        };

        let sstable = SSTable {
            path: PathBuf::from(filename),
            index: Arc::new(RwLock::new(None)),
            size: fs_metadata.len(),
            // bloom_filter,
            bloom_filter: Arc::new(RwLock::new(metadata.bloom_filter)),
            // Load metadata from file
            created_timestamp: metadata.created_timestamp,
            // Include 8 bytes for the stored length of the metadata
            header_size: len + 8,
        };
        Ok(sstable)
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
        let created_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let filename = Self::get_new_filename(dir);
        let file = File::create(filename.clone()).await?;
        println!("Created file: {}", filename);
        let mut writer = BufWriter::new(file);
        let mut size: u64 = 0;
        let mut bloom_filter = BloomBox::with_rate(BLOOM_FILTER_FP_RATE, BLOOM_FILTER_NUM_ITEMS);

        let metadata = Self::write_header(
            &mut writer,
            &filename,
            size,
            &bloom_filter,
            created_timestamp,
        )
        .await?;

        for (key, value) in snapshot {
            bloom_filter.insert(key);
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

        let header_len =
            Self::update_metadata_and_flush(writer, size, &bloom_filter, metadata).await?;
        let sstable = SSTable {
            path: PathBuf::from(filename),
            index: Arc::new(RwLock::new(None)),
            size,
            bloom_filter: Arc::new(RwLock::new(bloom_filter)),
            created_timestamp,
            header_size: header_len,
        };

        let index_future = sstable.create_index();
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
        let mut offset: u64 = self.header_size;
        let mut is_index_hit = false;
        {
            let bloom_filter = self.bloom_filter.read().await;
            if !bloom_filter.contains(&key) {
                return Ok(None);
            }
        }
        let index_opt = self.index.read().await;
        if let Some(index) = index_opt.as_ref() {
            if let Some(nearest_offset) = index.get_nearest_offset(key).await {
                is_index_hit = true;
                offset = nearest_offset
            }
        }
        drop(index_opt);

        let mut iter = self.iter_at_offset(offset).await?;
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

        if is_index_hit {
            // This means the bloom filter returned a false positive
            // TODO: Record metrics
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
        println!("Merging SSTables");
        let mut iter1 = match self.iter().await {
            Ok(iter) => iter,
            Err(e) => return Err(e),
        };

        let mut iter2 = match sstable.iter().await {
            Ok(iter) => iter,
            Err(e) => return Err(e),
        };

        let filename = Self::get_new_filename(dir);

        let created_timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let file = File::create(&filename).await?;
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::Start(0)).await?;
        println!("Created file {}", filename);

        // Write header
        let mut size: u64 = 0;
        let mut bloom_filter = BloomBox::with_rate(BLOOM_FILTER_FP_RATE, BLOOM_FILTER_NUM_ITEMS);
        let metadata = Self::write_header(
            &mut writer,
            &filename,
            size,
            &bloom_filter,
            created_timestamp,
        )
        .await?;

        let mut kv1 = iter1.next().await;
        let mut kv2 = iter2.next().await;
        while kv1.is_some() || kv2.is_some() {
            match (&kv1, &kv2) {
                (Some(Ok((k1, v1))), Some(Ok((k2, v2)))) => match k1.cmp(k2) {
                    std::cmp::Ordering::Less => {
                        bloom_filter.insert(k1);
                        size += Self::write_row(&mut writer, k1, v1).await?;
                        kv1 = iter1.next().await;
                    }
                    std::cmp::Ordering::Greater => {
                        bloom_filter.insert(k2);
                        size += Self::write_row(&mut writer, k2, v2).await?;
                        kv2 = iter2.next().await;
                    }
                    std::cmp::Ordering::Equal => {
                        bloom_filter.insert(k1);
                        size += Self::write_row(&mut writer, k1, v1).await?;
                        kv1 = iter1.next().await;
                        kv2 = iter2.next().await;
                    }
                },
                (Some(Ok((k1, v1))), None) => {
                    bloom_filter.insert(k1);
                    size += Self::write_row(&mut writer, k1, v1).await?;
                    kv1 = iter1.next().await;
                }
                (None, Some(Ok((k2, v2)))) => {
                    bloom_filter.insert(k2);
                    size += Self::write_row(&mut writer, k2, v2).await?;
                    kv2 = iter2.next().await;
                }
                _ => break,
            }
        }

        writer.flush().await?;
        // Update size and bloomfilter in header
        Self::update_metadata_and_flush(writer, size, &bloom_filter, metadata).await?;

        let sstable = SSTable::new(filename.as_str()).await?;
        // TODO: Retry index creation if it fails?
        if let Err(e) = sstable.create_index().await? {
            eprintln!("Failed to create index for merged SSTable: {}", e);
        }

        Ok(sstable)
    }

    /// Encodes a key and value into a Row protobuf message and writes it to the writer.
    #[instrument]
    async fn write_row(
        writer: &mut BufWriter<File>,
        key: &str,
        value: &value::Kind,
    ) -> io::Result<u64> {
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

        Ok(len)
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
    pub async fn iter_at_offset(&self, offset: u64) -> io::Result<SSTableIter> {
        let mut file = File::open(&self.path).await?;
        let offset = file.seek(SeekFrom::Start(offset)).await?;
        let buf_reader = Arc::new(Mutex::new(BufReader::new(file)));

        Ok(SSTableIter {
            buf_reader,
            buf: Arc::new(Mutex::new(Vec::new())),
            offset,
        })
    }

    pub async fn iter(&self) -> io::Result<SSTableIter> {
        self.iter_at_offset(self.header_size).await
    }

    fn create_index(&self) -> JoinHandle<Result<(), io::Error>> {
        let index_clone = Arc::clone(&self.index);
        let sstable_clone = self.clone();
        tokio::task::spawn(async move {
            let index = match SparseIndex::new(&sstable_clone).await {
                Ok(index) => index,
                Err(e) => {
                    eprintln!("Unable to create index: {}", e);
                    return Err(e);
                }
            };
            let result = index.build_index().await;
            if let Err(e) = result {
                eprintln!("Unable to buildindex: {}", e);
            }

            let result = index.flush_index().await;
            if let Err(e) = result {
                eprintln!("Unable to flush index: {}", e);
            }

            {
                let mut index_clone_guard: RwLockWriteGuard<Option<SparseIndex>> =
                    index_clone.write().await;
                *index_clone_guard = Some(index);
            }

            Ok(())
        })
    }

    async fn write_header(
        writer: &mut BufWriter<File>,
        filename: &String,
        size: u64,
        bloom_filter: &BloomBox,
        created_timestamp: Duration,
    ) -> Result<SSTableMetadata, io::Error> {
        let metadata = SSTableMetadata {
            path: PathBuf::from(filename.clone()),
            size,
            // TODO: Avoid the clone?
            bloom_filter: bloom_filter.clone(),
            created_timestamp,
        };
        let serialized_metadata = bincode::serialize(&metadata).unwrap();
        let metadata_buf = vec![0; serialized_metadata.len()];
        let metadata_len = serialized_metadata.len() as u64;
        writer.write_all(&metadata_len.to_be_bytes()).await?;
        writer.write_all(&metadata_buf).await?;
        Ok(metadata)
    }

    async fn update_metadata_and_flush(
        mut writer: BufWriter<File>,
        size: u64,
        bloom_filter: &BloomBox,
        metadata: SSTableMetadata,
    ) -> Result<u64, io::Error> {
        let metadata = SSTableMetadata {
            size,
            bloom_filter: bloom_filter.clone(),
            ..metadata
        };
        let serialized_metadata = bincode::serialize(&metadata).unwrap();
        let metadata_len = serialized_metadata.len() as u64;
        writer.seek(SeekFrom::Start(0)).await?;
        writer.write_all(&metadata_len.to_be_bytes()).await?;
        writer.write_all(&serialized_metadata).await?;
        writer.flush().await?;
        // Include 8 bytes for the length of the metadata
        Ok(serialized_metadata.len() as u64 + 8)
    }
}
