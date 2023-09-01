#![allow(clippy::type_complexity)]

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
//! Using [`ScopedStream`]:
//! ```
//! use std::time::Duration;
//!
//! use futures_sink::Sink;
//! use futures_core::Stream;
//! use futures_util::{SinkExt, StreamExt};
//!
//! use scoped_stream_sink::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create new scoped stream
//!     let mut stream = ScopedStream::new(|mut sink| Box::pin(async move {
//!         // We have to Box::pin it because otherwise the trait bounds is too complex
//!         // Interior sink cannot outlast the lifetime of it's outer stream
//!
//!         // This will not work
//!         // tokio::spawn(async move { sink.send(10000).await.unwrap() }).await.unwrap();
//!
//!         // Assume this is a complex task
//!         let (mut a, mut b) = (1usize, 1);
//!         for _ in 0..10 {
//!             sink.send(a).await.unwrap();
//!             (a, b) = (b, a + b);
//!             tokio::time::sleep(Duration::from_millis(100)).await;
//!         }
//!     }));
//!
//!     let mut v = Vec::new();
//!     while let Some(i) = stream.next().await {
//!         v.push(i);
//!     }
//!     println!("{v:?}");
//! }
//! ```
//!
//! Using [`ScopedSink`]:
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
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     // Create new sink
//!     let mut sink = <ScopedSink<usize, Error>>::new(|mut stream| Box::pin(async move {
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
//!     }));
//!
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

#[cfg(feature = "std")]
use core::mem::transmute;

#[cfg(feature = "std")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "std")]
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

#[cfg(feature = "std")]
/// Protects value within local thread. Call [`Self::get_inner()`] to protect inner access
/// to within thread. Call [`Self::set_inner_ctx()`] to set the context.
pub(crate) struct LocalThread<T> {
    thread: ThreadId,
    lock: AtomicBool,

    inner: T,
}

#[cfg(feature = "std")]
impl<T> LocalThread<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            thread: current().id(),
            lock: AtomicBool::new(false),
            inner,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_inner(&self) -> &mut T {
        // Allows nested ifs to help separate operations that must be sequential
        #[allow(clippy::collapsible_if)]
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

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn set_inner_ctx(&mut self) -> &mut T {
        if !self.lock.swap(true, Ordering::SeqCst) {
            self.thread = current().id();
            self.lock.store(false, Ordering::SeqCst);
            return &mut self.inner;
        }

        panic!("Called from other thread!");
    }
}
