pub mod builder;
pub mod tests;

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::io::Error;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::task::JoinHandle;

use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use tokio::time::Duration;
use tracing::instrument;

use crate::models::value;
use crate::reprisedb::index::SparseIndex;
use crate::reprisedb::memtable::MemTable;
use crate::reprisedb::sstable;

pub struct DatabaseConfig {
    pub memtable_size_target: usize,
    pub sstable_dir: String,
    pub compaction_interval: Duration,
    pub num_concurrent_reads: usize,
    pub max_reads: usize,
}

/// A simple LSM (Log-Structured Merge-tree) database.
///
/// The `Database` struct represents a database, including a memory table (memtable)
/// and a list of sorted string tables (SSTables). The database supports common operations
/// such as `put`, `get`, `flush_memtable`, `compact_sstables`, and `shutdown`.
///
/// The memtable is a BTreeMap that holds data in memory, while the SSTables are used
/// to persist data on the disk. The SSTables are stored in the directory specified
/// during the database initialization (`sstable_dir`).
///
/// `Database` uses `RwLock` to synchronize access to the memtable and the SSTables,
/// ensuring thread-safety for concurrent operations.
///
#[derive(Debug)]
pub struct Database {
    pub memtable: Arc<RwLock<MemTable>>,

    pub sstables: Arc<RwLock<Vec<sstable::SSTable>>>,
    pub compacting_notify: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,

    // Options
    pub sstable_dir: String,
    memtable_size_target: usize,
    compaction_interval: Duration,
    // TODO: Add SSTable target size
}

impl Database {
    /// Creates a new instance of `Database` with the given directory to store SSTables.
    ///
    /// The database instance includes a memtable and a list of SSTables loaded from the specified directory.
    /// If the directory doesn't exist, it will be created. Files within the directory are treated as SSTables
    /// and are loaded during the database initialization.
    ///
    /// If the directory contains invalid file paths or any other IO errors occur, the method will return an `Err`.
    ///
    /// # Arguments
    ///
    /// * `sstable_dir`: A string slice representing the directory path where SSTables will be stored.
    ///
    /// # Errors
    ///
    /// If this function encounters any form of I/O or other error, an error variant will be returned.
    ///
    pub async fn new(config: DatabaseConfig) -> io::Result<Self> {
        let sstable_dir = config.sstable_dir;
        let memtable_size_target = config.memtable_size_target;
        let compaction_interval = config.compaction_interval;
        let num_concurrent_reads = config.num_concurrent_reads;

        let sstable_path = Path::new(&sstable_dir);
        if !sstable_path.exists() {
            fs::create_dir(sstable_path)?;
        }

        let mut sstables = Vec::new();
        for path in Self::get_files_by_modified_date(&sstable_dir)?
            .iter()
            .filter(|file| file.is_file() && file.extension().unwrap_or(OsStr::new("")) != "index")
            .rev()
        {
            let os_path_str = path.clone().into_os_string();
            let path_str = os_path_str.into_string().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid file path: {:?}", e),
                )
            })?;
            sstables.push(sstable::SSTable::new(&path_str).await?);
        }

        let memtable = Arc::new(RwLock::new(MemTable::new()));

        let database = Database {
            memtable,
            sstables: Arc::new(RwLock::with_max_readers(
                sstables,
                num_concurrent_reads as u32,
            )),
            compacting_notify: Arc::new(Mutex::new(None)),
            sstable_dir: sstable_dir.clone(),
            memtable_size_target,
            compaction_interval,
        };
        database.init();
        Ok(database)
    }

    /// Initializes the database
    ///
    /// This method starts a background task that periodically checks the size of the memtable and triggers
    /// compaction every 60 seconds.
    fn init(&self) {
        let mut db_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(db_clone.compaction_interval);
            loop {
                interval.tick().await;
                println!("Starting compaction process...");
                match db_clone.start_compacting().await {
                    Ok(_) => println!("Compaction completed."),
                    Err(e) => eprintln!("Compaction failed: {:?}", e),
                }
            }
        });
    }

    /// Inserts a key-value pair into the database.
    ///
    /// This method inserts the given key-value pair into the in-memory `MemTable`. If the size of the `MemTable`
    /// exceeds the `MEMTABLE_SIZE_TARGET` after the insertion, it automatically triggers the `flush_memtable` method
    /// to persist the in-memory data to disk in an `SSTable`.
    ///
    /// # Arguments
    ///
    /// * `key` - A String that represents the key to be stored in the database.
    /// * `value` - A `value::Kind` that holds the value to be associated with the key.
    ///
    /// # Errors
    ///
    /// This function will return an `io::Error` if the `flush_memtable` operation fails.
    #[instrument]
    pub async fn put(&mut self, key: String, value: value::Kind) -> std::io::Result<()> {
        let mut memtable_guard = self.memtable.write().await;
        memtable_guard.put(key, value).await;
        let memtable_size = memtable_guard.size();
        if memtable_size > self.memtable_size_target {
            println!(
                "Memtable size {} exceeded target {} with {} entries. Flushing memtable.",
                memtable_size,
                self.memtable_size_target,
                memtable_guard.len().await
            );
            let snapshot = {
                println!("Creating new MemTable and updating reference");
                // Create new memtable and swap it with the old one
                let new_memtable = MemTable::new();
                let old_memtable = std::mem::replace(&mut *memtable_guard, new_memtable);

                println!("Taking snapshot");
                old_memtable.snapshot().await
            };

            println!("Snapshot complete, found {} entries", snapshot.len());
            println!("Creating SSTable from MemTable and writing to disk");
            let (sstable, _) = sstable::SSTable::create(&self.sstable_dir, &snapshot).await?;

            println!("Updating SSTable list");
            self.sstables.write().await.push(sstable);

            println!("Finished flushing");
        }
        Ok(())
    }

    /// Retrieves the value associated with a given key from the database.
    ///
    /// This method first checks the in-memory `MemTable` for the requested key. If not found, it iterates
    /// through the `SSTables` on disk in reverse order (newest first) to find the value associated with the key.
    ///
    /// # Arguments
    ///
    /// * `key` - A string slice that holds the key to retrieve its associated value from the database.
    ///
    /// # Return
    ///
    /// This function returns a `Result` containing an `Option<value::Kind>`. If the key is found, an `Option::Some`
    /// variant containing the value is returned. If the key is not found, an `Option::None` variant is returned.
    ///
    /// # Errors
    ///
    /// This function will return an `io::Error` if reading from the `SSTables` fails.
    /// ```
    #[instrument]
    pub async fn get(&self, key: &str) -> io::Result<Option<value::Kind>> {
        let memtable_guard = self.memtable.read().await;
        if let Some(value) = memtable_guard.get(key).await {
            return Ok(Some(value));
        }
        drop(memtable_guard);

        let sstables = self.sstables.read().await;
        for sstable in sstables.iter().rev() {
            if let Some(value) = sstable.get(key).await? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Asynchronously flushes the current memtable into an SSTable on disk.
    ///
    /// The content of the memtable is moved into a new SSTable which is stored on disk in the directory
    /// specified at database creation. After the memtable is successfully flushed, it is cleared and ready for new data.
    ///
    /// If the memtable is empty, this operation is a no-op and immediately returns `Ok(())`.
    ///
    /// # Return
    ///
    /// This function returns an `io::Result<()>`. If the flush process completes successfully, an `Ok(())`
    /// is returned. If an error occurs during the process, an `Err` variant containing the `io::Error` is returned.
    ///
    /// # Errors
    ///
    /// This function will return an `io::Error` if there's a problem creating the SSTable, such as a filesystem I/O error.
    /// ```
    #[instrument]
    pub async fn flush_memtable(&mut self) -> Result<JoinHandle<Result<(), Error>>, Error> {
        println!("Flushing memtable");
        let mut memtable_guard = self.memtable.write().await;
        let snapshot = {
            println!("Creating new MemTable and updating reference");
            // Create new memtable and swap it with the old one
            let new_memtable = MemTable::new();
            let old_memtable = std::mem::replace(&mut *memtable_guard, new_memtable);

            println!("Taking snapshot");
            old_memtable.snapshot().await
        };

        println!("Snapshot complete, found {} entries", snapshot.len());
        println!("Creating SSTable from MemTable and writing to disk");
        let (sstable, index_join_handle) =
            sstable::SSTable::create(&self.sstable_dir, &snapshot).await?;

        println!("Updating SSTable list");
        self.sstables.write().await.push(sstable);

        println!("Finished flushing");
        Ok(index_join_handle)
    }

    /// Initiates compaction of `SSTables` in the database.
    ///
    /// This method goes through the list of `SSTables` in reverse order (from newest to oldest), and merges
    /// the latest `SSTable` with the one just before it. The new merged `SSTable` is then added back to the list,
    /// and the old `SSTables` are removed.
    ///
    /// In case of an error during the merge process, a rollback is performed to restore the state of `SSTables`
    /// to what it was before the start of the operation.
    ///
    /// # Return
    ///
    /// This function returns an `io::Result<()>`. If the compaction process completes successfully, an `Ok(())`
    /// is returned. If an error occurs during the process, an `Err` variant containing the `io::Error` is returned.
    ///
    /// # Errors
    ///
    /// This function will return an `io::Error` if any I/O operation fails during the process.
    pub async fn compact_sstables(&mut self) -> io::Result<()> {
        println!("Starting compact_sstables");
        let sstables_read_guard = self.sstables.read().await;

        let len = sstables_read_guard.len();
        if len < 2 {
            println!("No compaction needed");
            return Ok(());
        }

        println!("Start merge for compaction");
        // Get two oldest SSTables
        // TODO: Use id instead of index
        let latest = sstables_read_guard[1].clone();
        let second_latest = sstables_read_guard[0].clone();
        drop(sstables_read_guard);

        let result = {
            // Merge creates a new SSTable from the two SSTables passed in favoring the latest
            let merged = latest.merge(&second_latest, &self.sstable_dir).await?;

            // Remove the old tables, add the new one
            let mut sstables = self.sstables.write().await;
            sstables[1] = merged;
            sstables.remove(0);
            Ok(())
        };

        match result {
            Ok(_) => {
                println!("SSTables updated with new file, deleting old files");
                // Delete the files associated with the two SSTables that were merged
                fs::remove_file(&latest.path)?;
                fs::remove_file(&second_latest.path)?;

                let mut latest_index_handle = latest.index.write().await;
                if latest_index_handle.as_ref().is_some() {
                    *latest_index_handle = None;
                    let index_file_path = SparseIndex::get_index_filename(&latest.path)?;
                    fs::remove_file(index_file_path)?;
                }
                let mut second_latest_index_handle = second_latest.index.write().await;
                if second_latest_index_handle.as_ref().is_some() {
                    *second_latest_index_handle = None;
                    let index_file_path = SparseIndex::get_index_filename(&second_latest.path)?;
                    fs::remove_file(index_file_path)?;
                }
            }
            Err(err) => {
                println!("Compaction process failed, need to roll back");
                // TODO: Handle the error, perform rollback
                return Err(err);
            }
        }

        println!("Finished compact_sstables");
        Ok(())
    }

    /// Start the compaction process in the background.
    ///
    /// This method spawns a new Tokio task to run the compaction process.
    /// If a compaction process is already running, this method does nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if the compaction process fails.
    #[instrument]
    pub async fn start_compacting(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify_clone = notify.clone();

        let mut db_clone = self.clone();

        {
            let mut compacting_notify_guard = self.compacting_notify.lock().await;
            match &*compacting_notify_guard {
                Some(_) => {
                    println!("Compaction process already running!");
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Compaction process already running!",
                    )
                    .into());
                }
                None => {
                    *compacting_notify_guard = Some(notify);
                }
            }
        }

        println!("Spawning compaction process");
        tokio::spawn(async move {
            let mut num_sstables = {
                let sstables = db_clone.sstables.read().await;
                sstables.len()
            };

            while num_sstables > 2 {
                println!("Continuing compaction process for {} tables", num_sstables);
                let result = db_clone.compact_sstables().await;
                if let Err(e) = &result {
                    eprintln!("Failed to compact sstables: {}", e);
                    return;
                }

                num_sstables = {
                    let sstables = db_clone.sstables.read().await;
                    sstables.len()
                };
            }

            notify_clone.notify_one();
            println!(
                "SSTable compaction completed successfully. SSTables left: {}",
                db_clone.sstables.read().await.len()
            );

            // Reset compacting_notify after compaction is done
            let mut compacting_notify_guard = db_clone.compacting_notify.lock().await;
            *compacting_notify_guard = None;
        });

        Ok(())
    }

    #[instrument]
    pub async fn wait_for_compaction(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let compacting_notify = self.compacting_notify.clone();

        // If a compaction process is running, we wait for it to notify us that it's done
        if let Some(notify) = compacting_notify.lock().await.as_ref() {
            notify.notified().await;
            println!("Compaction process completed successfully.");
        }

        Ok(())
    }

    /// Shuts down the database, ensuring that the current memtable is flushed to disk.
    ///
    /// This method should be called prior to the application exiting to ensure that all in-memory data
    /// is safely written to disk. If the flushing process fails, the error will be printed to the standard error output.
    ///
    /// Note that this method does not currently return any status or error information. As a result,
    /// it is important to ensure that all previous database operations have completed successfully before calling `shutdown`.
    /// ```
    #[instrument]
    pub async fn shutdown(&mut self) {
        match self.flush_memtable().await {
            Ok(_) => (),
            Err(e) => eprintln!("Failed to flush MemTable on shutdown: {}", e),
        }
        match self.wait_for_compaction().await {
            Ok(_) => (),
            Err(e) => eprintln!(
                "Failed to wait for compaction to complete on shutdown: {}",
                e
            ),
        }
    }

    fn get_files_by_modified_date(path: &str) -> io::Result<Vec<PathBuf>> {
        // Read the directory
        let mut entries: Vec<_> = fs::read_dir(path)?
            .map(|res| {
                res.map(|e| {
                    let path = e.path();
                    let metadata = fs::metadata(&path).unwrap();
                    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    (path, modified)
                })
            })
            .collect::<Result<_, io::Error>>()?;

        entries.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        Ok(entries.into_iter().map(|(path, _)| path).collect())
    }
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Database {
            memtable: Arc::clone(&self.memtable),
            sstables: Arc::clone(&self.sstables),
            compacting_notify: Arc::clone(&self.compacting_notify),
            sstable_dir: self.sstable_dir.clone(),
            memtable_size_target: self.memtable_size_target,
            compaction_interval: self.compaction_interval,
        }
    }
}
