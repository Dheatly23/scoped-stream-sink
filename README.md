# scoped-stream-sink

Convenience library to construct streams and sinks.

## How to Use

Simply import this crate to your project and done.
Documentation is already included, and it's very easy to use.

## Examples

Using [`ScopedStream`]:
```rust
use std::time::Duration;

use futures_sink::Sink;
use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};

use scoped_stream_sink::*;

#[tokio::main]
async fn main() {
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
}
```

Using [`ScopedSink`]:
```rust
use std::time::Duration;

use anyhow::Error;
use futures_sink::Sink;
use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};

use scoped_stream_sink::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
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

## License

This library is licensed under Apache 2.0 license.
