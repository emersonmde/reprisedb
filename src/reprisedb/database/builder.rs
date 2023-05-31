use std::time::Duration;
use crate::reprisedb::DatabaseConfig;


pub struct DatabaseConfigBuilder {
    memtable_size_target: Option<usize>,
    sstable_dir: Option<String>,
    compaction_interval: Option<Duration>,
}

#[allow(dead_code)]
impl DatabaseConfigBuilder {
    pub fn new() -> Self {
        Self {
            memtable_size_target: None,
            sstable_dir: None,
            compaction_interval: None,
        }
    }

    /// Sets the memtable size target which will determine the number of bytes
    /// that will be stored before flushing to disk
    pub fn memtable_size_target(mut self, size: usize) -> Self {
        self.memtable_size_target = Some(size);
        self
    }

    /// This method is used to set the sstable storage directory
    pub fn sstable_dir(mut self, dir: String) -> Self {
        self.sstable_dir = Some(dir);
        self
    }

    /// This method is used to set the compaction interval in seconds
    pub fn compaction_interval(mut self, interval: Duration) -> Self {
        self.compaction_interval = Some(interval);
        self
    }

    pub fn build(self) -> DatabaseConfig {
        let memtable_size_target = self.memtable_size_target.unwrap_or(1024 * 1024); // 1MB
        let sstable_dir = self.sstable_dir.unwrap_or("/tmp/reprisedb".to_string());
        let compaction_interval = self.compaction_interval.unwrap_or(Duration::from_secs(10));

        DatabaseConfig {
            memtable_size_target,
            sstable_dir,
            compaction_interval,
        }
    }
}