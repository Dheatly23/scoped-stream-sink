#![no_std]
#![allow(clippy::type_complexity)]

//! Make asynchronous [`Stream`] and [`Sink`] easy.
//!
//! This crate contains [`ScopedStream`] and [`ScopedSink`] type.
//! They use normal Rust lifetime mechanism to ensure safety
//! (eg. sending interior data outside of it's scope).
//! Unlike [`async_stream`](https://docs.rs/async-stream/latest/async_stream/),
//! it doesn't use macro.
//!
//! ## 📌 Plan for 2.0
//!
//! Since AFIT (and RPITIT) is stabilized, i plan to upgrade this library's interface to use them.
//! This _should_ eliminate the [`Box::pin`] requirement, at the cost of complicated type bounds
//! (and harder to use too, maybe).
//! So far i've been unsuccessful to fully reason the type bounds.
//!
//! So here are the (rough) plan for (possible) 2.0:
//! - Eliminate [`Box::pin`] requirement (maybe add type alias for dynamic version).
//! - Beef up [`StreamSink`] functionality (right now it's kinda experimental).
//!
//! ## `no-std` Support
//!
//! Currently, this crate requires `alloc` (because of [`Box`] and such).
//! But it's perfectly usable on platforms like WASM.
//! Do note that the default `std` feature requires stdlib and so is incompatible with `no-std`.
//!
//! # Examples
//!
//! Using [`ScopedStream`]:
//! ```
//! use std::time::Duration;
//!
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
//!
//! These following examples will fail to compile:
//! ```compile_fail
//! # use anyhow::Error;
//! # use futures_util::{SinkExt, StreamExt};
//! # use scoped_stream_sink::*;
//! let sink = <ScopedSink<usize, Error>>::new(|mut stream| Box::pin(async move {
//!     // Moving inner stream into another thread will fail
//!     // because it might live for longer than the sink.
//!     tokio::spawn(async move {
//!         if let Some(v) = stream.next().await {
//!             println!("Value: {v}");
//!         }
//!     }).await?;
//!
//!     Ok(())
//! }));
//! ```
//!
//! ```compile_fail
//! # use anyhow::Error;
//! # use futures_util::{SinkExt, StreamExt};
//! # use scoped_stream_sink::*;
//! let stream = <ScopedTryStream<usize, Error>>::new(|mut sink| Box::pin(async move {
//!     // Moving inner sink into another thread will fail
//!     // because it might live for longer than the stream.
//!     tokio::spawn(async move {
//!         sink.send(1).await.unwrap();
//!     }).await?;
//!
//!     Ok(())
//! }));
//! ```
//!
//! Some very hacky generator out of [`ScopedStream`]:
//! ```
//! use core::pin::pin;
//! use core::ptr::NonNull;
//! use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
//!
//! use futures_util::{SinkExt, StreamExt};
//! use scoped_stream_sink::*;
//!
//! /// Create a null waker. It does nothing when waken.
//! fn nil_waker() -> Waker {
//!     fn raw() -> RawWaker {
//!         RawWaker::new(NonNull::dangling().as_ptr(), &VTABLE)
//!     }
//!
//!     unsafe fn clone(_: *const ()) -> RawWaker {
//!         raw()
//!     }
//!     unsafe fn wake(_: *const ()) {}
//!     unsafe fn wake_by_ref(_: *const ()) {}
//!     unsafe fn drop(_: *const ()) {}
//!
//!     static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
//!
//!     unsafe { Waker::from_raw(raw()) }
//! }
//!
//! // Create a generator
//! let mut stream = ScopedStream::new(|mut sink| Box::pin(async move {
//!     for i in 0usize..10 {
//!         sink.send(i).await.unwrap();
//!     }
//! }));
//! let mut stream = pin!(stream);
//!
//! // Setup waker and context
//! let waker = nil_waker();
//! let mut cx = Context::from_waker(&waker);
//!
//! // The loop
//! loop {
//!     let v = match stream.as_mut().poll_next(&mut cx) {
//!         Poll::Pending => continue, // Should not happen, but continue anyways
//!         Poll::Ready(None) => break, // Stop iteration
//!         Poll::Ready(Some(v)) => v, // Process value
//!     };
//!
//!     println!("{v}");
//! }
//! ```

extern crate alloc;
#[cfg(any(feature = "std", test))]
#[macro_use]
extern crate std;

#[cfg(feature = "std")]
mod local_thread;
mod scoped_sink;
mod scoped_stream;
mod scoped_stream_sink;
mod stream_sink;
mod stream_sink_ext;

pub use crate::scoped_sink::*;
pub use crate::scoped_stream::*;
pub use crate::scoped_stream_sink::*;
pub use crate::stream_sink::*;
pub use crate::stream_sink_ext::*;

#[doc(no_inline)]
pub use futures_core::Stream;
#[doc(no_inline)]
pub use futures_sink::Sink;

#[cfg(feature = "std")]
pub(crate) use local_thread::*;

/* Let's comment out this until ready
pub(crate) mod sealed {
    pub(crate) trait Sealed {}
}
*/

pub mod prelude {
    pub use crate::{
        LocalScopedSink, LocalScopedStream, LocalScopedStreamSink, LocalScopedTryStream,
        StreamSink as _, StreamSinkExt as _,
    };

    #[doc(no_inline)]
    pub use futures_core::Stream as _;
    #[doc(no_inline)]
    pub use futures_sink::Sink as _;

    #[cfg(feature = "std")]
    pub use crate::{ScopedSink, ScopedStream, ScopedStreamSink, ScopedTryStream};
    #[doc(no_inline)]
    #[cfg(feature = "std")]
    pub use futures_util::{SinkExt as _, StreamExt as _};
}
