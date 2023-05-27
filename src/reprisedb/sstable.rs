use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};

use crate::reprisedb::Value;

#[derive(Debug, Clone)]
pub struct SSTable {
    filename: String,
}

impl SSTable {
    pub fn new(filename: String, snapshot: &BTreeMap<String, Value>) -> std::io::Result<Self> {
        let file = File::create(&filename)?;
        let mut writer = BufWriter::new(file);

        // For simplicity, we'll just write the data as text
        for (key, value) in snapshot {
            // TODO: Write the key and value to the file using ProtoBuf
            writeln!(&mut writer, "{}: {:?}", key, value)?;
        }

        Ok(SSTable { filename })
    }

    pub fn load(filename: String) -> std::io::Result<Self> {
        Ok(SSTable { filename })
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        todo!("Implement SSTable::get");
    }
}
