# scoped-stream-sink

Convenience library to construct streams and sinks.

## How to Use

Simply import this crate to your project and done.
Documentation is already included, and it's very easy to use.

## Examples

```rust
use std::time::Duration;

use anyhow::Error;
use futures_sink::Sink;
use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};

use scoped_stream_sink::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Create new scoped stream
    let mut stream = ScopedStream::new(|mut sink| Box::pin(async move {
        // We have to Box::pin it because otherwise the trait bounds is too complex
        // Interior sink cannot outlast the lifetime of it's outer stream

        // This will not work
        // tokio::spawn(async move { sink.send(10000).await.unwrap() }).await.unwrap();

        // Assume this is a complex task
        let (mut a, mut b) = (1usize, 1);
        for _ in 0..10 {
            sink.send(a).await.unwrap();
            (a, b) = (b, a + b);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }));

    let mut v = Vec::new();
    while let Some(i) = stream.next().await {
        v.push(i);
    }
    println!("{v:?}");

    // Create new sink
    let mut sink = <ScopedSink<usize, Error>>::new(|mut stream| Box::pin(async move {
        // Unlike ScopedStream, this closure will be called over and over again,
        // until all values are consumed

        // Assume this is a complex task
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(v) = stream.next().await {
            println!("Value: {v}");
        }
        Ok(())
    }));

    for i in 0..10 {
        sink.send(i).await?;
    }
    sink.close().await?;

    Ok(())
}
```

## Why?

Because implementing proper `Stream` and `Sink` is hard.
Originally i used it to make a network packet processor (think primitive services).
And since the code has potential, i decided to factor it out into dedicated crate.
Also, it's kinda hacky/cool way to make generators.

```rust
use core::pin::pin;
use core::ptr::NonNull;
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use futures_sink::Sink;
use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};

use scoped_stream_sink::*;

/// Create a null waker. It does nothing when waken.
fn nil_waker() -> Waker {
    fn raw() -> RawWaker {
        RawWaker::new(NonNull::dangling().as_ptr(), &VTABLE)
    }

    unsafe fn clone(_: *const ()) -> RawWaker {
        raw()
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    unsafe { Waker::from_raw(raw()) }
}

fn main() {
    // Create a generator
    //
    // Yes, this is a very hacky way of doing it in stable Rust
    let mut stream = ScopedStream::new(|mut sink| Box::pin(async move {
        for i in 0usize..10 {
            sink.send(i).await.unwrap();
        }
    }));
    let mut stream = pin!(stream);

    // Setup waker and context
    let waker = nil_waker();
    let mut cx = Context::from_waker(&waker);

    // The loop
    loop {
        let v = match stream.as_mut().poll_next(&mut cx) {
            Poll::Pending => continue, // Should not happen, but continue anyways
            Poll::Ready(None) => break, // Stop iteration
            Poll::Ready(Some(v)) => v, // Process value
        };

        println!("{v}");
    }
}
```

## How?

On it's basic, there's a inner pinned value that's mutably accessible
within and outside the sink/stream. Since that's kinda violates aliasing mutable borrow,
it's only permitted to be used within a single thread.
It should be fine however, since the borrow is done sequentially,
"passing" between inner and outer context.
To enforce this the local variants simply does not allow `Send`,
while the non-local ones are using simple locks, which panics if locking failed.

In practice, care must be done to ensure the stream/sink can be awoken from inside and outside,
while not using the waker itself.
There should be no way for the stream/sink to not wait for something,
unless if it's already finished/closed.

## License

This library is licensed under Apache 2.0 license.
