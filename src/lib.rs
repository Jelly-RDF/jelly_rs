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

#[cfg(feature = "futures")]
mod fut {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use crate::{Frame, FrameReader, RdfStreamFrame};
    use futures::Stream;
    use futures::io::{AsyncRead, AsyncReadExt};
    use prost::Message as _;

    /// Async version of varint reading
    async fn read_varint_async<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<u64> {
        let mut shift = 0;
        let mut result = 0u64;
        loop {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf).await?;
            let byte = buf[0];
            result |= ((byte & 0x7F) as u64) << shift;
            if (byte & 0x80) == 0 {
                break;
            }
            shift += 7;
        }
        Ok(result)
    }

    /// Implement as a `Stream` so you can use it in async for-loops
    impl<R: AsyncRead + Unpin> Stream for FrameReader<R> {
        type Item = Frame;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Poll-based wrapper for async fn
            let fut = self.next_frame();
            futures::pin_mut!(fut);
            fut.poll(cx)
        }
    }

    impl<R: AsyncRead + Unpin> FrameReader<R> {
        /// Read a single frame asynchronously
        pub async fn next_frame(&mut self) -> Option<Frame> {
            // Decode a varint (length prefix)
            let len = match read_varint_async(&mut self.reader).await {
                Ok(l) => l as usize,
                Err(_) => return None,
            };

            let mut buf = vec![0; len];

            // Read the exact number of bytes for the message
            if self.reader.read_exact(&mut buf).await.is_err() {
                return None;
            }

            // Decode the message from the buffer
            RdfStreamFrame::decode(&*buf).ok()
        }
    }
}
