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

use std::ops::{Deref, DerefMut};
#[cfg(feature = "std")]
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(all(feature = "std", debug_assertions))]
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

const STATE_OFF: u8 = 0;
const STATE_ENTER: u8 = 1;
const STATE_LOCKED: u8 = 2;

#[cfg(feature = "std")]
/// Protects value within local thread. Call [`Self::get_inner()`] to protect inner access
/// to within thread. Call [`Self::set_inner_ctx()`] to set the context.
pub(crate) struct LocalThread<T> {
    #[cfg(debug_assertions)]
    thread: ThreadId,
    lock: AtomicU8,

    inner: T,
}

fn panic_expected(expect: u8, value: u8) {
    panic!("Inconsistent internal state! (expected lock state to be {:02X}, got {:02X})\nNote: Your code might use inner value across thread", expect, value);
}

#[cfg(feature = "std")]
pub(crate) struct LocalThreadInnerGuard<'a, T>(&'a LocalThread<T>);
#[cfg(feature = "std")]
pub(crate) struct LocalThreadInnerCtxGuard<'a, T>(&'a LocalThread<T>);

#[cfg(feature = "std")]
impl<'a, T> Drop for LocalThreadInnerGuard<'a, T> {
    fn drop(&mut self) {
        if let Err(v) = self.0.lock.compare_exchange(
            STATE_LOCKED,
            STATE_ENTER,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            panic_expected(STATE_LOCKED, v);
        }
    }
}

#[cfg(feature = "std")]
impl<'a, T> Deref for LocalThreadInnerGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.inner
    }
}

#[cfg(feature = "std")]
impl<'a, T> DerefMut for LocalThreadInnerGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: It is locked.
        #[allow(mutable_transmutes)]
        unsafe {
            transmute(&self.0.inner)
        }
    }
}

#[cfg(feature = "std")]
impl<'a, T> Drop for LocalThreadInnerCtxGuard<'a, T> {
    fn drop(&mut self) {
        if let Err(v) = self.0.lock.compare_exchange(
            STATE_ENTER,
            STATE_OFF,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            panic_expected(STATE_ENTER, v);
        }
    }
}

#[cfg(feature = "std")]
impl<'a, T> Deref for LocalThreadInnerCtxGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0.inner
    }
}

#[cfg(feature = "std")]
impl<'a, T> DerefMut for LocalThreadInnerCtxGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: It is locked.
        #[allow(mutable_transmutes)]
        unsafe {
            transmute(&self.0.inner)
        }
    }
}

#[cfg(feature = "std")]
impl<T> LocalThread<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            #[cfg(debug_assertions)]
            thread: current().id(),
            lock: AtomicU8::new(STATE_OFF),

            inner,
        }
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn get_inner<'a>(&'a self) -> LocalThreadInnerGuard<'a, T> {
        if let Err(v) = self.lock.compare_exchange(
            STATE_ENTER,
            STATE_LOCKED,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            panic_expected(STATE_ENTER, v);
        }

        #[cfg(debug_assertions)]
        if self.thread != current().id() {
            panic!("Called from other thread!");
        }

        LocalThreadInnerGuard(self)
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn set_inner_ctx<'a>(&'a mut self) -> LocalThreadInnerCtxGuard<'a, T> {
        if let Err(v) =
            self.lock
                .compare_exchange(STATE_OFF, STATE_ENTER, Ordering::SeqCst, Ordering::Relaxed)
        {
            panic_expected(STATE_OFF, v);
        }

        #[cfg(debug_assertions)]
        {
            self.thread = current().id();
        }

        LocalThreadInnerCtxGuard(&*self)
    }
}
