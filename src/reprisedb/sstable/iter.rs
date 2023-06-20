use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;
use tokio::{
    fs::File,
    io::{self, AsyncReadExt, BufReader},
    sync::Mutex,
};

use crate::models;

pub struct SSTableIter {
    pub buf_reader: Arc<Mutex<BufReader<File>>>,
    pub buf: Arc<Mutex<Vec<u8>>>,
    pub offset: u64,
}

#[async_trait]
pub trait AsyncIterator {
    type Item;
    async fn next(&mut self) -> Option<Self::Item>;
    async fn next_with_offset(&mut self) -> Option<(u64, Self::Item)>;
}

#[async_trait]
impl AsyncIterator for SSTableIter {
    type Item = io::Result<(String, models::value::Kind)>;
    async fn next(&mut self) -> Option<Self::Item> {
        self.next_with_offset().await.map(|(_, item)| item)
    }

    async fn next_with_offset(&mut self) -> Option<(u64, Self::Item)> {
        let mut buf_reader = self.buf_reader.lock().await;

        // Read the length of the protobuf message (assuming it was written as a u64)
        let mut len_buf = [0u8; 8];
        match buf_reader.read_exact(&mut len_buf).await {
            Ok(_) => {
                let len = u64::from_be_bytes(len_buf);

                // Then read that number of bytes into the buffer
                // buf.resize(len as usize, 0);
                let mut buf = vec![0u8; len as usize];
                match buf_reader.read_exact(&mut buf).await {
                    Ok(_) => {
                        let row: models::Row = match models::Row::decode(buf.as_slice()) {
                            Ok(row) => row,
                            Err(e) => {
                                return Some((
                                    self.offset,
                                    Err(io::Error::new(io::ErrorKind::Other, e)),
                                ))
                            }
                        };

                        let result = Some((
                            self.offset,
                            Ok((row.key, row.value.map(|v| v.kind.unwrap()).unwrap())),
                        ));
                        self.offset += 8 + len;
                        result
                    }
                    Err(e) => Some((self.offset, Err(e))),
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // EOF
                None
            }
            Err(e) => Some((self.offset, Err(e))),
        }
    }
}
