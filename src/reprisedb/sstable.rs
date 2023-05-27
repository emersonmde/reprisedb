use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};


use crate::reprisedb::value::Value;

#[derive(Debug, Clone)]
pub struct SSTable {
    filename: String,
}

impl SSTable {
    pub fn new(filename: &str, snapshot: &BTreeMap<String, Value>) -> std::io::Result<Self> {
        let file = File::create(filename)?;
        let mut writer = BufWriter::new(file);

        // For simplicity, we'll just write the data as text
        for (key, value) in snapshot {
            // TODO: Write the key and value to the file using ProtoBuf
            writeln!(&mut writer, "{}: {:?}", key, value)?;
        }

        Ok(SSTable { filename: String::from(filename) })
    }

    pub fn load(filename: &str) -> std::io::Result<Self> {
        Ok(SSTable { filename: String::from(filename) })
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        let file = File::open(&self.filename).unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let row = line.expect("Error reading from SSTable");
            let mut kv_pair = row.split('\t');
            if let Some(sstable_key) = kv_pair.next() {
                todo!("Need to check Value type");
                if sstable_key == key {
                    let value = Value::String(String::from(kv_pair.next()?));
                    return Some(value);
                }
            }
        }

        None
    }
}
