use futures::Stream;
use futures::channel::mpsc;
use futures::io::AsyncRead;
use futures::task::{Context, Poll};
use std::collections::VecDeque;
use std::pin::Pin;

/// Our async reader: wraps a channel Receiver<u8 buffers>
pub struct ChannelReader {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pending: VecDeque<u8>, // left-over bytes from the last buffer
}

impl ChannelReader {
    pub fn new(rx: mpsc::UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            rx,
            pending: VecDeque::new(),
        }
    }
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;

        // If we already have pending bytes, drain them first
        if !this.pending.is_empty() {
            let n = buf.len().min(this.pending.len());
            for i in 0..n {
                buf[i] = this.pending.pop_front().unwrap();
            }
            return Poll::Ready(Ok(n));
        }

        // Otherwise, poll the channel for the next buffer
        match Pin::new(&mut this.rx).poll_next(cx) {
            Poll::Ready(Some(chunk)) => {
                let n = buf.len().min(chunk.len());

                // Fill buf with as much as fits
                buf[..n].copy_from_slice(&chunk[..n]);

                // Keep any leftovers for the next poll
                if n < chunk.len() {
                    this.pending.extend(&chunk[n..]);
                }

                Poll::Ready(Ok(n))
            }
            Poll::Ready(None) => {
                // channel closed → EOF
                Poll::Ready(Ok(0))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
