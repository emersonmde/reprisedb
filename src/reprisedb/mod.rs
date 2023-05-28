use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use uuid::Uuid;

use crate::models::value;
use crate::reprisedb::memtable::MemTable;

mod sstable;
mod memtable;

const MEMTABLE_SIZE_TARGET: usize = 1024 * 4;

#[derive(Debug)]
pub struct Database {
    pub memtable: MemTable,
    pub sstables: Vec<sstable::SSTable>,
    pub sstable_dir: String,
}

impl Database {
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

    pub fn new(sstable_dir: &str) -> std::io::Result<Self> {
        let sstable_path = Path::new(sstable_dir);
        if !sstable_path.exists() {
            std::fs::create_dir(sstable_path)?;
        }

        let mut sstables = Vec::new();
        for path in Database::get_files_by_modified_date(sstable_dir)? {
            let os_path_str = path.into_os_string();
            let path_str = os_path_str.to_str().unwrap();
            sstables.push(sstable::SSTable::new(path_str)?);
        }

        let memtable = MemTable::new();

        Ok(Database {
            memtable,
            sstables,
            sstable_dir: String::from(sstable_dir),
        })
    }

    pub fn put(&mut self, key: String, value: value::Kind) {
        self.memtable.put(key, value);
        if self.memtable.size() > MEMTABLE_SIZE_TARGET {
            self.flush_memtable();
        }
    }

    pub fn get(&self, key: &str) -> std::io::Result<Option<value::Kind>> {
        let value = self.memtable.get(key);
        if value.is_some() {
            return Ok(Some(value.unwrap().clone()));
        }

        for sstable in self.sstables.iter() {
            let value = sstable.get(key).unwrap_or(None);
            if value.is_some() {
                return Ok(value);
            }
        }

        Ok(None)
    }

    fn flush_memtable(&mut self) -> std::io::Result<()> {
        let now = SystemTime::now();
        let since_the_epoch = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let filename = format!("{}_{}", since_the_epoch.as_secs(), Uuid::new_v4());
        let path_str = format!("{}/{}", self.sstable_dir, filename);
        let sstable = sstable::SSTable::create(&path_str, &self.memtable.snapshot())?;
        self.sstables.push(sstable);

        self.memtable.clear();
        Ok(())
    }

    fn compact_sstables(&mut self) -> std::io::Result<()> {
        todo!("compact_sstables");
    }

    fn get_sstable_files(&self) -> io::Result<Vec<PathBuf>> {
        Database::get_files_by_modified_date(&self.sstable_dir)
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
        println!("path: {}", path);
        return Database::new(&path).unwrap();
    }

    fn teardown(db: Database) {
        for entry in fs::read_dir(db.sstable_dir.clone()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        std::fs::remove_dir(&db.sstable_dir).unwrap();
    }

    #[test]
    fn test_new_database() {
        let db = Database::new("test_sstable_dir").unwrap();

        assert!(db.memtable.is_empty());
        assert!(db.sstables.is_empty());
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
        db.put("int".to_string(), int_value.clone());
        db.put("float".to_string(), float_value.clone());
        db.put("string".to_string(), string_value.clone());
        assert_eq!(&db.get("int").unwrap().unwrap(), &int_value);
        assert_eq!(&db.get("float").unwrap().unwrap(), &float_value);
        assert_eq!(&db.get("string").unwrap().unwrap(), &string_value);

        teardown(db);
    }
}
