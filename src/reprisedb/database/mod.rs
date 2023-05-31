pub mod builder;
pub mod tests;

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

use crate::models::value;
use crate::reprisedb::memtable::MemTable;
use crate::reprisedb::sstable;


pub struct DatabaseConfig {
    pub memtable_size_target: usize,
    pub sstable_dir: String,
    pub compaction_interval: Duration,
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
/// # Example
///
/// ```
/// use reprisedb::reprisedb::Database;
/// use reprisedb::reprisedb::DatabaseConfigBuilder;
/// use reprisedb::models::value::Kind;
/// use std::fs;
///
/// #[tokio::main]
/// async fn main() {
///     let config = DatabaseConfigBuilder::new().sstable_dir("/tmp/mydb".to_string()).build();
///     let mut db = Database::new(config).expect("Database initialization failed");
///
///     db.put("my_key".to_string(), Kind::Str("my_value".to_string())).await.expect("Put operation failed");
///     let value = db.get("my_key").await.expect("Get operation failed");
///
///     assert_eq!(value, Some(Kind::Str("my_value".to_string())));
///
///     db.shutdown().await;
///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
/// }
/// ```
#[derive(Debug)]
pub struct Database {
    pub memtable: Arc<RwLock<MemTable>>,
    pub sstables: Arc<RwLock<Vec<sstable::SSTable>>>,
    pub compacting_handle: Arc<Mutex<Option<JoinHandle<Result<(), io::Error>>>>>,

    // Options
    pub sstable_dir: String,
    memtable_size_target: usize,
    compaction_interval: Duration,
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
    /// # Examples
    ///
    /// ```
    /// use reprisedb::reprisedb::Database;
    /// use reprisedb::reprisedb::DatabaseConfigBuilder;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let config = DatabaseConfigBuilder::new().sstable_dir("/tmp/mydb".to_string()).build();
    ///     let mut db = Database::new(config).expect("Database initialization failed");
    ///     // ...
    ///     db.shutdown().await; // It's good practice to shutdown database before the program exits.
    ///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
    /// }
    /// ```
    pub fn new(config: DatabaseConfig) -> io::Result<Self> {
        let sstable_dir = config.sstable_dir;
        let memtable_size_target = config.memtable_size_target;
        let compaction_interval = config.compaction_interval;

        let sstable_path = Path::new(&sstable_dir);
        if !sstable_path.exists() {
            fs::create_dir(sstable_path)?;
        }

        let mut sstables = Vec::new();
        for path in Self::get_files_by_modified_date(&sstable_dir)?.iter().rev() {
            let os_path_str = path.clone().into_os_string();
            let path_str = os_path_str.into_string().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("Invalid file path: {:?}", e))
            })?;
            sstables.push(sstable::SSTable::new(&path_str)?);
        }

        let memtable = Arc::new(RwLock::new(MemTable::new()));

        let database = Database {
            memtable,
            sstables: Arc::new(RwLock::new(sstables)),
            compacting_handle: Arc::new(Mutex::new(None)),
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use reprisedb::reprisedb::Database;
    /// use reprisedb::reprisedb::DatabaseConfigBuilder;
    /// use reprisedb::models::value::Kind;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let config = DatabaseConfigBuilder::new().sstable_dir("/tmp/mydb".to_string()).build();
    ///     let mut db = Database::new(config).expect("Database initialization failed");
    ///     db.put("my_key".to_string(), Kind::Int(42)).await.expect("Failed to insert key-value pair");
    ///     db.shutdown().await;
    ///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
    /// }
    /// ```
    pub async fn put(&mut self, key: String, value: value::Kind) -> std::io::Result<()> {
        let mut memtable_guard = self.memtable.write().await;
        memtable_guard.put(key, value);
        drop(memtable_guard);
        if self.memtable.read().await.size() > self.memtable_size_target {
            println!("Memtable size exceeded target. Flushing memtable.");
            self.flush_memtable().await?;
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use reprisedb::reprisedb::Database;
    /// use reprisedb::reprisedb::DatabaseConfigBuilder;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let config = DatabaseConfigBuilder::new().sstable_dir("/tmp/mydb".to_string()).build();
    ///     let mut db = Database::new(config).expect("Database initialization failed");
    ///     match db.get("my_key").await {
    ///         Ok(Some(value)) => println!("Retrieved value: {:?}", value),
    ///         Ok(None) => println!("Key not found"),
    ///         Err(e) => eprintln!("Failed to retrieve key: {}", e),
    ///     }
    ///     db.shutdown().await;
    ///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
    /// }
    /// ```
    pub async fn get(&self, key: &str) -> io::Result<Option<value::Kind>> {
        if let Some(value) = self.memtable.read().await.get(key) {
            return Ok(Some(value.clone()));
        }

        for sstable in self.sstables.read().await.iter().rev() {
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use reprisedb::reprisedb::Database;
    /// use reprisedb::reprisedb::DatabaseConfigBuilder;
    /// use reprisedb::models::value::Kind;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let config = DatabaseConfigBuilder::new().sstable_dir("/tmp/mydb".to_string()).build();
    ///     let mut db = Database::new(config).expect("Database initialization failed");
    ///     db.put("key".to_string(), Kind::Str("value".to_string())).await.expect("Failed to put data");
    ///     match db.flush_memtable().await {
    ///         Ok(_) => println!("Memtable flushed successfully"),
    ///         Err(e) => eprintln!("Failed to flush memtable: {}", e),
    ///     }
    ///     db.shutdown().await;
    ///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
    /// }
    /// ```
    pub async fn flush_memtable(&mut self) -> std::io::Result<()> {
        let mut memtable = self.memtable.write().await;
        if memtable.is_empty() {
            return Ok(());
        }
        let sstable = sstable::SSTable::create(&self.sstable_dir, &memtable.get_memtable()).await?;
        self.sstables.write().await.push(sstable);
        memtable.clear();
        Ok(())
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
        let sstables_backup = self.sstables.clone();

        // Get a read lock on self.sstables
        let sstables_guard = self.sstables.read().await;

        // Get the length of sstables once and reuse it
        let len = sstables_guard.len();

        drop(sstables_guard);

        // get_sstable_files returns files sorted by modified date, newest to oldest. Calling
        // reverse here will allow us to pop the newest file
        for i in (1..len).rev() {
            let latest = {
                let sstables_guard = self.sstables.read().await;
                sstables_guard[i].clone()
            };
            let second_latest = {
                let sstables_guard = self.sstables.read().await;
                sstables_guard[i - 1].clone()
            };

            let result = {
                // Merge creates a new SSTable from the two SSTables passed in favoring the latest
                let merged = latest.merge(&second_latest, &self.sstable_dir).await?;

                // Remove the old tables, add the new one
                let mut sstables = self.sstables.write().await;
                sstables.pop();
                sstables.pop();
                sstables.push(merged);
                Ok(())
            };

            match result {
                Ok(_) => {
                    // Delete the files associated with the two SSTables that were merged
                    fs::remove_file(latest.path)?;
                    fs::remove_file(second_latest.path)?;
                }
                Err(err) => {
                    // Handle the error, perform rollback
                    *self.sstables.write().await = sstables_backup.write().await.clone();
                    return Err(err);
                }
            }
        }

        let remaining_len = self.sstables.read().await.len();
        println!("Successfully completed compacting {} SSTables. {} SSTables remaining.", len - remaining_len, remaining_len);

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
    pub async fn start_compacting(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let compacting_handle = self.compacting_handle.clone();
        let mut compacting_handle_guard = self.compacting_handle.lock().await;
        match &*compacting_handle_guard {
            Some(_) => {
                println!("Compaction process already running!");
                return Err(io::Error::new(io::ErrorKind::Other,
                                          "Compaction process already running!").into());
            }
            None => {}
        }

        let mut db_clone = self.clone();
        let handle = tokio::spawn(async move {
            let result = db_clone.compact_sstables().await;
            if let Err(e) = &result {
                eprintln!("Failed to compact sstables: {}", e);
            }
            let mut compacting_handle_guard = compacting_handle.lock().await;
            *compacting_handle_guard = None;
            result
        });
        *compacting_handle_guard = Some(handle);

        Ok(())
    }

    /// Shuts down the database, ensuring that the current memtable is flushed to disk.
    ///
    /// This method should be called prior to the application exiting to ensure that all in-memory data
    /// is safely written to disk. If the flushing process fails, the error will be printed to the standard error output.
    ///
    /// Note that this method does not currently return any status or error information. As a result,
    /// it is important to ensure that all previous database operations have completed successfully before calling `shutdown`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use reprisedb::reprisedb::Database;
    /// use reprisedb::reprisedb::DatabaseConfigBuilder;
    /// use reprisedb::models::value::Kind;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let config = DatabaseConfigBuilder::new().sstable_dir("/tmp/mydb".to_string()).build();
    ///     let mut db = Database::new(config).expect("Database initialization failed");
    ///     db.put("key".to_string(), Kind::Str("value".to_string())).await.expect("Failed to put data");
    ///     db.shutdown().await;
    /// }
    /// ```
    pub async fn shutdown(&mut self) {
        match self.flush_memtable().await {
            Ok(_) => (),
            Err(e) => eprintln!("Failed to flush memtable on shutdown: {}", e),
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
            compacting_handle: Arc::clone(&self.compacting_handle),
            sstable_dir: self.sstable_dir.clone(),
            memtable_size_target: self.memtable_size_target.clone(),
            compaction_interval: self.compaction_interval.clone(),
        }
    }
}

