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

const MEMTABLE_SIZE_TARGET: usize = 1024 * 1024;

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
/// use reprisedb::models::value::Kind;
/// use std::fs;
///
/// #[tokio::main]
/// async fn main() {
///     let mut db = Database::new("/tmp/mydb").expect("Database initialization failed");
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
    pub sstable_dir: String,
    pub compacting_handle: Arc<Mutex<Option<JoinHandle<Result<(), io::Error>>>>>
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
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut db = Database::new("/tmp/mydb").expect("Database initialization failed");
    ///     // ...
    ///     db.shutdown().await; // It's good practice to shutdown database before the program exits.
    ///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
    /// }
    /// ```
    pub fn new(sstable_dir: &str) -> std::io::Result<Self> {
        let sstable_path = Path::new(sstable_dir);
        if !sstable_path.exists() {
            fs::create_dir(sstable_path)?;
        }

        let mut sstables = Vec::new();
        for path in Self::get_files_by_modified_date(sstable_dir)?.iter().rev() {
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
            sstable_dir: String::from(sstable_dir),
            compacting_handle: Arc::new(Mutex::new(None)),
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
        let compaction_interval = Duration::from_secs(10);
        tokio::spawn(async move {
            let mut interval = interval(compaction_interval);
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
    /// use reprisedb::models::value::Kind;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut db = Database::new("/tmp/mydb").expect("Database initialization failed");
    ///     db.put("my_key".to_string(), Kind::Int(42)).await.expect("Failed to insert key-value pair");
    ///     db.shutdown().await;
    ///     fs::remove_dir_all("/tmp/mydb").expect("Failed to remove directory");
    /// }
    /// ```
    pub async fn put(&mut self, key: String, value: value::Kind) -> std::io::Result<()> {
        let mut memtable_guard = self.memtable.write().await;
        memtable_guard.put(key, value);
        if memtable_guard.size() > MEMTABLE_SIZE_TARGET {
            drop(memtable_guard);
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
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut db = Database::new("/tmp/mydb").expect("Database initialization failed");
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
    /// use reprisedb::models::value::Kind;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut db = Database::new("/tmp/mydb").expect("Database initialization failed");
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
                    // TODO: Remove the new SSTable that was created?
                    return Err(err);
                }
            }
        }

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
        let mut compacting_handle = self.compacting_handle.lock().await;
        match &*compacting_handle {
            Some(_) => {
                println!("Compaction process already running!");
                return Ok(());
            }
            None => {}
        }

        let mut db_clone = self.clone();
        let handle = tokio::spawn(async move {
            let result = db_clone.compact_sstables().await;
            if let Err(e) = &result {
                eprintln!("Failed to compact sstables: {}", e);
            }
            result
        });
        *compacting_handle = Some(handle);

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
    /// use reprisedb::models::value::Kind;
    /// use std::fs;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut db = Database::new("/tmp/mydb").expect("Database initialization failed");
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

// TODO: implment non-async methods to flush memtable and shutdown
// impl Drop for Database {
//     fn drop(&mut self) {
//         let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(async || self.flush_memtable().await));
//
//         match result {
//             Ok(res) => {
//                 if let Err(e) = res {
//                     eprintln!("Failed to flush memtable on drop: {}", e);
//                 }
//             }
//             Err(_) => {
//                 eprintln!("Panic occurred while flushing memtable on drop");
//             }
//         }
//     }
// }

impl Clone for Database {
    fn clone(&self) -> Self {
        Database {
            memtable: Arc::clone(&self.memtable),
            sstables: Arc::clone(&self.sstables),
            sstable_dir: self.sstable_dir.clone(),
            compacting_handle: Arc::clone(&self.compacting_handle),
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use super::*;

    fn setup() -> Database {
        let mut rng = rand::thread_rng();
        let i: u8 = rng.gen();
        let path = format!("/tmp/reprisedb_sstring_testdir{}", i);
        return Database::new(&path).unwrap();
    }

    fn teardown(db: Database) {
        fs::remove_dir_all(&db.sstable_dir).unwrap();
    }

    #[tokio::test]
    async fn test_new_database() {
        let db = Database::new("test_sstable_dir").unwrap();

        assert!(db.memtable.read().await.is_empty());
        assert!(db.sstables.write().await.is_empty());
        assert_eq!(db.sstable_dir, "test_sstable_dir");

        std::fs::remove_dir("test_sstable_dir").unwrap();
    }

    #[tokio::test]
    async fn test_put_get_item() {
        // create database instance, call put, verify that item is added correctly
        let mut db = setup();

        let int_value = value::Kind::Int(44);
        let float_value = value::Kind::Float(12.2);
        let string_value = value::Kind::Str("Test".to_string());
        db.put("int".to_string(), int_value.clone()).await.unwrap();
        db.put("float".to_string(), float_value.clone()).await.unwrap();
        db.put("string".to_string(), string_value.clone()).await.unwrap();
        assert_eq!(&db.get("int").await.unwrap().unwrap(), &int_value);
        assert_eq!(&db.get("float").await.unwrap().unwrap(), &float_value);
        assert_eq!(&db.get("string").await.unwrap().unwrap(), &string_value);

        teardown(db);
    }

    #[tokio::test]
    async fn test_flush_memtable() {
        let mut db = setup();
        let test_value = value::Kind::Int(44);
        db.put("test".to_string(), test_value.clone()).await.unwrap();
        db.flush_memtable().await.unwrap();

        assert!(db.memtable.read().await.is_empty());
        assert!(!db.sstables.write().await.is_empty());

        teardown(db);
    }

    #[tokio::test]
    async fn test_get_item_after_memtable_flush() {
        let mut db = setup();
        let test_value = value::Kind::Int(44);
        db.put("test".to_string(), test_value.clone()).await.unwrap();
        db.flush_memtable().await.unwrap();

        assert_eq!(&db.get("test").await.unwrap().unwrap(), &test_value);

        teardown(db);
    }

    #[tokio::test]
    async fn test_compact_sstables() {
        let mut db = setup();
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
            assert_eq!(&db.get(&key).await.unwrap().unwrap(), &value::Kind::Int(i as i64));
        }

        teardown(db);
    }
}
