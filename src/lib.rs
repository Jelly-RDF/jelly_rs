#[macro_use]
extern crate log;

use std::io::Read;

use crate::proto::RdfStreamFrame;
use prost::Message as _;

pub mod deserialize;
pub mod error;
pub mod lookup;
pub mod proto;
pub mod to_rdf;
pub use deserialize::Deserializer;

/// Read a Protobuf varint from an std::io::Read
fn read_varint<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;

    for _ in 0..10 {
        let mut byte = [0u8];
        if reader.read_exact(&mut byte).is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF during varint",
            ));
        }

        let b = byte[0];
        result |= ((b & 0x7F) as u64) << shift;

        if b & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "Varint too long",
    ))
}

pub struct FrameReader<R> {
    reader: R,
}
impl<R> FrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader }
    }
}
pub type Frame = RdfStreamFrame;

impl<R: Read> Iterator for FrameReader<R> {
    type Item = RdfStreamFrame;

    fn next(&mut self) -> Option<Self::Item> {
        // Decode a varint (length prefix)
        let len = match read_varint(&mut self.reader) {
            Ok(l) => l as usize,
            Err(_) => return None,
        };

        let mut buf = vec![0; len];

        // Read the exact number of bytes for the message
        self.reader.read_exact(&mut buf).ok()?;

        // Decode the message from the buffer
        let frame = RdfStreamFrame::decode(&*buf).ok()?;
        Some(frame)
    }
}
