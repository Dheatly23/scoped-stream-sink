use std::convert::Infallible;
use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::transmute;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_sink::Sink;
use pin_project_lite::pin_project;

use crate::LocalThread;

pin_project! {
    /// Stream with a scoped future. It is useful to easily create [`Stream`] type, without
    /// hassle of manually constructing one or using macros
    /// (like [`async_stream`](https://docs.rs/async-stream/latest/async_stream/)).
    /// Safety is guaranteed by carefully scoping [`StreamInner`],
    /// similiar to [`scope`](std::thread::scope).
    pub struct ScopedStream<'env, T> {
        fut: Option<Pin<Box<dyn Future<Output = ()> + Send + 'env>>>,

        data: Pin<Box<StreamInner<'env, 'env, T>>>,
    }
}

pin_project! {
    /// Similiar to [`ScopedStream`], but allows for an error type. Future inside may be fallible,
    /// unlike [`ScopedStream`]. Also, the inner [`TryStreamInner`] allows for either sending
    /// an item or [`Result`] type.
    pub struct ScopedTryStream<'env, T, E> {
        fut: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'env>>>,

        data: Pin<Box<TryStreamInner<'env, 'env, T, E>>>,
    }
}

struct StreamInnerData<T> {
    data: Option<T>,
    closed: bool,
}

struct TryStreamInnerData<T, E> {
    data: Option<Result<T, E>>,
    closed: bool,
}

pin_project! {
    /// Inner type of [`ScopedStream`]. Implements [`Sink`] to send data for the stream.
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`StreamInner`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    pub struct StreamInner<'scope, 'env: 'scope, T> {
        inner: LocalThread<StreamInnerData<T>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env T>,
    }
}

pin_project! {
    /// Inner type of [`ScopedTryStream`]. Implements [`Sink`] for both item type or [`Result`].
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`TryStreamInner`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    pub struct TryStreamInner<'scope, 'env: 'scope, T, E> {
        inner: LocalThread<TryStreamInnerData<T, E>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env (T, E)>,
    }
}

impl<'env, T> ScopedStream<'env, T> {
    /// Create new [`ScopedStream`].
    ///
    /// Future must return unit type. If you want fallible future, use [`ScopedTryStream`].
    ///
    /// # Examples
    ///
    /// ```
    /// // Helper methods for stream
    /// use futures_util::{SinkExt, StreamExt};
    ///
    /// use scoped_stream_sink::ScopedStream;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut stream = <ScopedStream<usize>>::new(|mut sink| Box::pin(async move {
    ///         // Send a value.
    ///         // It is okay to unwrap() because it is infallible.
    ///         sink.send(1).await.unwrap();
    ///
    ///         // (Optional) close the sink. NOTE: sink cannot be used afterwards.
    ///         // sink.close().await.unwrap();
    ///     }));
    ///
    ///     // Receive all values
    ///     while let Some(i) = stream.next().await {
    ///         println!("{i}");
    ///     }
    /// }
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            Pin<&'scope mut StreamInner<'scope, 'env, T>>,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'scope>>,
    {
        let mut data = Box::pin(StreamInner {
            inner: LocalThread::new(StreamInnerData {
                data: None,
                closed: false,
            }),

            pinned: PhantomPinned,
            phantom: PhantomData,
        });

        let ptr = unsafe { transmute::<Pin<&mut StreamInner<T>>, _>(data.as_mut()) };
        let fut = f(ptr);

        Self {
            fut: Some(fut),
            data,
        }
    }
}

impl<'env, T, E> ScopedTryStream<'env, T, E> {
    /// Create new [`ScopedTryStream`].
    ///
    /// Future can fails, and it's sink can receive [`Result`] type too (see [`TryStreamInner`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use anyhow::Error;
    /// // Helper methods for stream
    /// use futures_util::{SinkExt, StreamExt};
    ///
    /// use scoped_stream_sink::ScopedTryStream;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let mut stream = <ScopedTryStream<_, Error>>::new(|mut sink| Box::pin(async move {
    ///         // Send a value.
    ///         sink.send(1).await?;
    ///
    ///         // (Optional) close the sink. NOTE: sink cannot be used afterwards.
    ///         // sink.close().await.unwrap();
    ///
    ///         Ok(())
    ///     }));
    ///
    ///     // Receive all values
    ///     while let Some(i) = stream.next().await.transpose()? {
    ///         println!("{i}");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            Pin<&'scope mut TryStreamInner<'scope, 'env, T, E>>,
        )
            -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>,
    {
        let mut data = Box::pin(TryStreamInner {
            inner: LocalThread::new(TryStreamInnerData {
                data: None,
                closed: false,
            }),

            pinned: PhantomPinned,
            phantom: PhantomData,
        });

        let ptr = unsafe { transmute::<Pin<&mut TryStreamInner<T, E>>, _>(data.as_mut()) };
        let fut = f(ptr);

        Self {
            fut: Some(fut),
            data,
        }
    }
}

impl<'env, T> Stream for ScopedStream<'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let fut = match &mut *this.fut {
            Some(v) => v.as_mut(),
            None => return Poll::Ready(None),
        };

        this.data.as_mut().project().inner.set_inner_ctx();
        if let Poll::Ready(_) = fut.poll(cx) {
            *this.fut = None;
        }

        let inner = this.data.as_mut().project().inner.set_inner_ctx();
        if let Some(v) = inner.data.take() {
            Poll::Ready(Some(v))
        } else if this.fut.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<'env, T, E> Stream for ScopedTryStream<'env, T, E> {
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let fut = match &mut *this.fut {
            Some(v) => v.as_mut(),
            None => return Poll::Ready(None),
        };

        this.data.as_mut().project().inner.set_inner_ctx();
        if let Poll::Ready(v) = fut.poll(cx) {
            *this.fut = None;
            if let Err(e) = v {
                return Poll::Ready(Some(Err(e)));
            }
        }

        let inner = this.data.as_mut().project().inner.set_inner_ctx();
        if let Some(v) = inner.data.take() {
            Poll::Ready(Some(v))
        } else if this.fut.is_none() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<'scope, 'env: 'scope, T> Sink<T> for StreamInner<'scope, 'env, T> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = self.project().inner.get_inner();
        if inner.closed || inner.data.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        let inner = self.project().inner.get_inner();
        if inner.closed {
            panic!("Stream is closed");
        }
        if inner.data.is_some() {
            panic!("poll_ready() is not called yet!");
        }

        inner.data = Some(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = self.project().inner.get_inner();
        inner.closed = true;
        if inner.data.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<'scope, 'env: 'scope, T, E> Sink<Result<T, E>> for TryStreamInner<'scope, 'env, T, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        <Self as Sink<Result<T, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        let inner = self.project().inner.get_inner();
        if inner.closed || inner.data.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Result<T, E>) -> Result<(), Infallible> {
        let inner = self.project().inner.get_inner();
        if inner.closed {
            panic!("Stream is closed");
        }
        if inner.data.is_some() {
            panic!("poll_ready() is not called yet!");
        }

        inner.data = Some(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        let inner = self.project().inner.get_inner();
        inner.closed = true;
        if inner.data.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<'scope, 'env: 'scope, T, E> Sink<T> for TryStreamInner<'scope, 'env, T, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        <Self as Sink<Result<T, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        <Self as Sink<Result<T, E>>>::poll_flush(self, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Infallible> {
        <Self as Sink<Result<T, E>>>::start_send(self, Ok(item))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        <Self as Sink<Result<T, E>>>::poll_close(self, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use anyhow::{bail, Error as AnyError, Result as AnyResult};
    use futures_util::{SinkExt, StreamExt};
    use tokio::task::yield_now;
    use tokio::time::timeout;

    async fn test_helper<F>(f: F) -> AnyResult<()>
    where
        F: Future<Output = AnyResult<()>> + Send,
    {
        match timeout(Duration::from_secs(5), f).await {
            Ok(v) => v,
            Err(_) => bail!("Time ran out"),
        }
    }
    /*
    #[tokio::test]
    async fn test_simple_fail() -> AnyResult<()> {
        use std::pin::pin;

        let mut stream: Pin<&mut ScopedStream<'static, usize>> =
            pin!(ScopedStream::new(|mut src| Box::pin(async move {
                tokio::spawn(async move {
                    src.send(1).await.unwrap();
                    src.send(2).await.unwrap();
                    src.close().await.unwrap();
                })
                .await
                .unwrap();
            })));

        test_helper(async move {
            while let Some(i) = stream.next().await {
                println!("{i}");
            }
            drop(stream);

            Ok(())
        })
        .await
    }
    */
    #[tokio::test]
    async fn test_simple() -> AnyResult<()> {
        let mut stream: ScopedStream<usize> = ScopedStream::new(|_| Box::pin(async {}));

        test_helper(async move {
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_recv_one() -> AnyResult<()> {
        let mut stream: ScopedStream<usize> = ScopedStream::new(|mut src| {
            Box::pin(async move {
                src.send(1).await.unwrap();
            })
        });

        test_helper(async move {
            assert_eq!(stream.next().await, Some(1));
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }
}
