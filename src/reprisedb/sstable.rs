use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};

use prost::Message;

use crate::models;
use crate::models::value;

#[derive(Debug, Clone)]
pub struct SSTable {
    filename: String,
}

impl SSTable {
    pub fn new(filename: &str, snapshot: &BTreeMap<String, value::Kind>) -> std::io::Result<Self> {
        let file = File::create(filename)?;
        let mut writer = BufWriter::new(file);

        for (key, value) in snapshot {
            let row = models::Row {
                key: key.clone(),
                value: Some(models::Value { kind: Some(value.clone()) }),
            };

            let mut bytes = Vec::new();
            row.encode(&mut bytes).expect("Unable to encode row");

            // Write the length of the protobuf message as a u64 before the message itself.
            let len = bytes.len() as u64;
            writer.write_all(&len.to_be_bytes())?;
            writer.write_all(&bytes)?;
        }

        Ok(SSTable { filename: String::from(filename) })
    }

    pub fn load(filename: &str) -> std::io::Result<Self> {
        Ok(SSTable { filename: String::from(filename) })
    }
    pub fn get(&self, key: &str) -> std::io::Result<Option<models::value::Kind>> {
        let file = File::open(&self.filename)?;
        let mut reader = BufReader::new(file);
        let mut buf = vec![];

        loop {
            buf.clear();

            // First, read the length of the protobuf message (assuming it was written as a u64).
            let mut len_buf = [0u8; 8];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {
                    let len = u64::from_be_bytes(len_buf);

                    // Then read that number of bytes into the buffer.
                    buf.resize(len as usize, 0);
                    reader.read_exact(&mut buf)?;

                    let row: models::Row = models::Row::decode(&*buf)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                    if row.key == key {
                        return Ok(row.value.map(|v| v.kind.unwrap()));
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // We've reached the end of the file.
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(None)
    }
}
