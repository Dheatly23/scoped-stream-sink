//! Make asynchronous [`Stream`](futures_core::Stream) and [`Sink`](futures_sink::Sink) easy.
//!
//! This crate contains [`ScopedStream`] and [`ScopedSink`] type.
//! They use normal Rust lifetime mechanism to ensure safety
//! (eg. sending interior data outside of it's scope).
//! Unlike [`async_stream`](https://docs.rs/async-stream/latest/async_stream/),
//! it doesn't use macro.
//!
//! # Examples
//!
//! ```
//! use std::time::Duration;
//!
//! use anyhow::Error;
//! use futures_sink::Sink;
//! use futures_core::Stream;
//! use futures_util::{SinkExt, StreamExt};
//!
//! use scoped_stream_sink::*;
//!
//! // Function to make a new stream
//! async fn make_stream(n: usize) -> impl Stream<Item = usize> {
//!     // Create new scoped stream
//!     ScopedStream::new(|mut sink| Box::pin(async move {
//!         // We have to Box::pin it because otherwise the trait bounds is too complex
//!         // Interior sink cannot outlast the lifetime of it's outer stream
//!
//!         // This does not work
//!         // tokio::spawn(async move { sink.send(10000).await.unwrap() }).await.unwrap();
//!
//!         // Assume this is a complex task
//!         let (mut a, mut b) = (1, 1);
//!         for _ in 0..n {
//!             sink.send(a).await.unwrap();
//!             (a, b) = (b, a + b);
//!             tokio::time::sleep(Duration::from_millis(100)).await;
//!         }
//!     }))
//! }
//!
//! // Function to make a new sink
//! async fn make_sink() -> impl Sink<usize, Error = Error> {
//!     ScopedSink::new(|mut stream| Box::pin(async move {
//!         // Unlike ScopedStream, this closure will be called over and over again,
//!         // until all values are consumed
//!
//!         // Assume this is a complex task
//!         tokio::time::sleep(Duration::from_millis(100)).await;
//!         if let Some(v) = stream.next().await {
//!             println!("Value: {v}");
//!         }
//!
//!         Ok(())
//!     }))
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     // Using stream
//!     let mut stream = make_stream(10).await;
//!     let mut v = Vec::new();
//!     while let Some(i) = stream.next().await {
//!         v.push(i);
//!     }
//!     println!("{v:?}");
//!
//!     // Using sink
//!     let mut sink = make_sink().await;
//!     for i in 0..10 {
//!         sink.send(i).await?;
//!     }
//!     sink.close().await?;
//!
//!     Ok(())
//! }
//! ```

mod scoped_sink;
mod scoped_stream;
mod scoped_stream_sink;
mod stream_sink;
mod stream_sink_ext;

use std::mem::transmute;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{current, ThreadId};

pub use crate::scoped_sink::*;
pub use crate::scoped_stream::*;
pub use crate::scoped_stream_sink::*;
pub use crate::stream_sink::*;
pub use crate::stream_sink_ext::*;

pub use futures_core::Stream;
pub use futures_sink::Sink;

pub(crate) mod sealed {
    pub(crate) trait Sealed {}
}

/// Protects value within local thread. Call [`Self::get_inner()`] to protect inner access
/// to within thread. Call [`Self::set_inner_ctx()`] to set the context.
pub(crate) struct LocalThread<T> {
    thread: ThreadId,
    lock: AtomicBool,

    inner: T,
}

impl<T> LocalThread<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            thread: current().id(),
            lock: AtomicBool::new(false),
            inner,
        }
    }

    pub(crate) fn get_inner(&self) -> &mut T {
        if !self.lock.swap(true, Ordering::SeqCst) {
            if self.thread == current().id() {
                self.lock.store(false, Ordering::SeqCst);
                // SAFETY: It is current thread.
                #[allow(mutable_transmutes)]
                return unsafe { transmute::<&T, &mut T>(&self.inner) };
            }
        }

        panic!("Called from other thread!");
    }

    pub(crate) fn set_inner_ctx(&mut self) -> &mut T {
        if !self.lock.swap(true, Ordering::SeqCst) {
            self.thread = current().id();
            self.lock.store(false, Ordering::SeqCst);
            return &mut self.inner;
        }

        panic!("Called from other thread!");
    }
}
