use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::RwLock;
use rand::Rng;

use crate::models::value;
use crate::reprisedb::memtable::MemTable;
use crate::reprisedb::sstable;

const MEMTABLE_SIZE_TARGET: usize = 1024 * 4;

#[derive(Debug)]
pub struct Database {
    pub memtable: MemTable,
    pub sstables: Arc<RwLock<Vec<sstable::SSTable>>>,
    pub sstable_dir: String,
}

impl Database {
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

        let memtable = MemTable::new();

        Ok(Database {
            memtable,
            sstables: Arc::new(RwLock::new(sstables)),
            sstable_dir: String::from(sstable_dir),
        })
    }

    pub fn put(&mut self, key: String, value: value::Kind) -> std::io::Result<()> {
        self.memtable.put(key, value);
        if self.memtable.size() > MEMTABLE_SIZE_TARGET {
            self.flush_memtable()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> io::Result<Option<value::Kind>> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(Some(value.clone()));
        }

        for sstable in self.sstables.read().iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    pub fn flush_memtable(&mut self) -> std::io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let sstable = sstable::SSTable::create(&self.sstable_dir, &self.memtable.snapshot())?;
        self.sstables.write().push(sstable);

        self.memtable.clear();
        Ok(())
    }

    pub fn compact_sstables(&mut self) -> io::Result<()> {
        let sstables_backup = self.sstables.clone();

        // Get a read lock on self.sstables
        let sstables_guard = self.sstables.read();

        // Get the length of sstables once and reuse it
        let len = sstables_guard.len();

        drop(sstables_guard);

        // get_sstable_files returns files sorted by modified date, newest to oldest. Calling
        // reverse here will allow us to pop the newest file
        for i in (1..len).rev() {
            let latest = {
                let sstables_guard = self.sstables.read();
                sstables_guard[i].clone()
            };
            let second_latest = {
                let sstables_guard = self.sstables.read();
                sstables_guard[i - 1].clone()
            };

            let result = {
                // Merge creates a new SSTable from the two SSTables passed in favoring the latest
                let merged = latest.merge(&second_latest, &self.sstable_dir)?;

                // Remove the old tables, add the new one
                let mut sstables = self.sstables.write();
                sstables.pop();
                sstables.pop();
                sstables.push(merged);
                Ok(())
            };

            match result {
                Ok(_) => {
                    // Delete the files associated with the two SSTables that were merged
                    fs::remove_file(&latest.filename)?;
                    fs::remove_file(&second_latest.filename)?;
                }
                Err(err) => {
                    // Handle the error, perform rollback
                    *self.sstables.write() = sstables_backup.write().clone();
                    // TODO: Remove the new SSTable that was created?
                    return Err(err);
                }
            }
        }

        Ok(())
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

impl Drop for Database {
    fn drop(&mut self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.flush_memtable()));

        match result {
            Ok(res) => {
                if let Err(e) = res {
                    eprintln!("Failed to flush memtable on drop: {}", e);
                }
            }
            Err(_) => {
                eprintln!("Panic occurred while flushing memtable on drop");
            }
        }
    }
}

#[cfg(test)]
mod tests {
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

    #[test]
    fn test_new_database() {
        let db = Database::new("test_sstable_dir").unwrap();

        assert!(db.memtable.is_empty());
        assert!(db.sstables.write().is_empty());
        assert_eq!(db.sstable_dir, "test_sstable_dir");

        std::fs::remove_dir("test_sstable_dir").unwrap();
    }

    #[test]
    fn test_put_get_item() {
        // create database instance, call put, verify that item is added correctly
        let mut db = setup();

        let int_value = value::Kind::Int(44);
        let float_value = value::Kind::Float(12.2);
        let string_value = value::Kind::Str("Test".to_string());
        db.put("int".to_string(), int_value.clone()).unwrap();
        db.put("float".to_string(), float_value.clone()).unwrap();
        db.put("string".to_string(), string_value.clone()).unwrap();
        assert_eq!(&db.get("int").unwrap().unwrap(), &int_value);
        assert_eq!(&db.get("float").unwrap().unwrap(), &float_value);
        assert_eq!(&db.get("string").unwrap().unwrap(), &string_value);

        teardown(db);
    }
}
