use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use uuid::Uuid;

use crate::models;
use crate::models::value;

#[derive(Debug, Clone)]
pub struct SSTable {
    filename: String,
}

impl SSTable {
    pub fn new(filename: &str) -> std::io::Result<Self> {
        Ok(SSTable { filename: String::from(filename) })
    }

    pub fn create(dir: &str, snapshot: &BTreeMap<String, value::Kind>) -> std::io::Result<Self> {
        let filename = Self::get_new_filename(dir);
        let file = File::create(filename.clone())?;
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


    /// Merges two SSTables returning the filename of the combined SSTable
    #[allow(dead_code)]
    pub fn merge(&self, sstable: SSTable, dir: &str) -> io::Result<String> {
        let mut iter1 = match self.iter() {
            Ok(iter) => iter,
            Err(e) => return Err(e),
        };

        let mut iter2 = match sstable.iter() {
            Ok(iter) => iter,
            Err(e) => return Err(e),
        };

        let filename = Self::get_new_filename(dir);

        let file = File::create(&filename)?;
        let mut writer = BufWriter::new(file);

        let mut kv1 = iter1.next();
        let mut kv2 = iter2.next();
        while kv1.is_some() || kv2.is_some() {
            match (&kv1, &kv2) {
                (Some(Ok((k1, v1))), Some(Ok((k2, v2)))) => {
                    match k1.cmp(k2) {
                        std::cmp::Ordering::Less => {
                            Self::write_row(&mut writer, k1, v1)?;
                            kv1 = iter1.next();
                        },
                        std::cmp::Ordering::Greater => {
                            Self::write_row(&mut writer, k2, v2)?;
                            kv2 = iter2.next();
                        },
                        std::cmp::Ordering::Equal => {
                            Self::write_row(&mut writer, k1, v1)?;
                            kv1 = iter1.next();
                            kv2 = iter2.next();
                        },
                    }
                },
                (Some(Ok((k1, v1))), None) | (None, Some(Ok((k1, v1)))) => {
                    Self::write_row(&mut writer, k1, v1)?;
                    kv1 = iter1.next();
                    kv2 = iter2.next();
                },
                _ => break,
            }
        }

        writer.flush()?;

        // TODO: Update SSTables from Database after merge
        Ok(filename)
    }


    fn write_row(writer: &mut BufWriter<File>, key: &str, value: &value::Kind) -> io::Result<()> {
        let row = models::Row {
            key: key.to_string(),
            value: Some(models::Value { kind: Some(value.clone()) }),
        };

        let mut bytes = Vec::new();
        row.encode(&mut bytes)?;

        // Write the length of the protobuf message as a u64 before the message itself.
        let len = bytes.len() as u64;
        writer.write_all(&len.to_be_bytes())?;
        writer.write_all(&bytes)?;

        Ok(())
    }


    fn get_new_filename(dir: &str) -> String {
        let now = SystemTime::now();
        let since_the_epoch = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let filename = format!("{}_{}", since_the_epoch.as_secs(), Uuid::new_v4());
        let path_str = format!("{}/{}", dir, filename);
        path_str
    }

    pub fn iter(&self) -> io::Result<SSTableIter> {
        let file = File::open(&self.filename)?;
        let reader = BufReader::new(file);
        Ok(SSTableIter { reader, buf: Vec::new() })
    }
}


pub struct SSTableIter {
    reader: BufReader<File>,
    buf: Vec<u8>,
}

impl Iterator for SSTableIter {
    type Item = io::Result<(String, models::value::Kind)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buf.clear();

        // First, read the length of the protobuf message (assuming it was written as a u64).
        let mut len_buf = [0u8; 8];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {
                let len = u64::from_be_bytes(len_buf);

                // Then read that number of bytes into the buffer.
                self.buf.resize(len as usize, 0);
                match self.reader.read_exact(&mut self.buf) {
                    Ok(()) => {
                        let row: models::Row = match models::Row::decode(&*self.buf) {
                            Ok(row) => row,
                            Err(e) => return Some(Err(io::Error::new(io::ErrorKind::Other, e))),
                        };

                        Some(Ok((row.key, row.value.map(|v| v.kind.unwrap()).unwrap())))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // We've reached the end of the file.
                None
            }
            Err(e) => Some(Err(e)),
        }
    }
}