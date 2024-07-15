use alloc::boxed::Box;
use core::convert::Infallible;
use core::future::Future;
use core::marker::{PhantomData, PhantomPinned};
use core::mem::transmute;
use core::ops::DerefMut;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::Stream;
use futures_sink::Sink;
use pin_project_lite::pin_project;

#[cfg(feature = "std")]
use crate::LocalThread;

#[cfg(feature = "std")]
type DynStreamFut<'scope> = Pin<Box<dyn Future<Output = ()> + Send + 'scope>>;
#[cfg(feature = "std")]
type DynTryStreamFut<'scope, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>;

#[cfg(feature = "std")]
pin_project! {
    /// Stream with a scoped future.
    ///
    /// It is useful to easily create [`Stream`] type, without
    /// hassle of manually constructing one or using macros
    /// (like [`async_stream`](https://docs.rs/async-stream/latest/async_stream/)).
    /// Safety is guaranteed by carefully scoping [`StreamInner`],
    /// similiar to [`scope`](std::thread::scope).
    pub struct ScopedStream<'env, T> {
        fut: Option<DynStreamFut<'env>>,

        data: Pin<Box<StreamInner<'env, 'env, T>>>,
    }
}

#[cfg(feature = "std")]
pin_project! {
    /// Fallible stream with a scoped future.
    ///
    /// Similiar to [`ScopedStream`], but allows for an error type. Future inside may fail,
    /// unlike [`ScopedStream`]. Also, the inner [`TryStreamInner`] allows for either sending
    /// an item or [`Result`] type.
    pub struct ScopedTryStream<'env, T, E> {
        fut: Option<DynTryStreamFut<'env, E>>,

        data: Pin<Box<TryStreamInner<'env, 'env, T, E>>>,
    }
}

struct StreamInnerData<T> {
    data: Option<T>,
    closed: bool,
}

#[cfg(feature = "std")]
pin_project! {
    /// Inner type of [`ScopedStream`].
    ///
    /// Implements [`Sink`] to send data for the stream.
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`StreamInner`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    ///
    /// Also do note that some of the check depends on `debug_assertions` build config
    /// (AKA only on debug builds).
    pub struct StreamInner<'scope, 'env: 'scope, T> {
        inner: LocalThread<StreamInnerData<T>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env T>,
    }
}

#[cfg(feature = "std")]
pin_project! {
    /// Inner type of [`ScopedTryStream`].
    ///
    /// Implements [`Sink`] for both item type and a [`Result`],
    /// allowing to send error (if you so choose).
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`TryStreamInner`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    ///
    /// Also do note that some of the check depends on `debug_assertions` build config
    /// (AKA only on debug builds).
    pub struct TryStreamInner<'scope, 'env: 'scope, T, E> {
        inner: LocalThread<StreamInnerData<Result<T, E>>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env (T, E)>,
    }
}

#[cfg(feature = "std")]
impl<'env, T> ScopedStream<'env, T> {
    /// Create new [`ScopedStream`].
    ///
    /// Future must return unit type. If you want fallible future, use [`ScopedTryStream`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Error;
    /// # use futures_util::{SinkExt, StreamExt};
    /// # use scoped_stream_sink::ScopedStream;
    /// # fn main() -> Result<(), Error> {
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
    /// let mut stream = <ScopedStream<usize>>::new(|mut sink| Box::pin(async move {
    ///     // Send a value.
    ///     // It is okay to unwrap() because it is infallible.
    ///     sink.send(1).await.unwrap();
    ///
    ///     // (Optional) close the sink. NOTE: sink cannot be used afterwards.
    ///     // sink.close().await.unwrap();
    /// }));
    ///
    /// // Receive all values
    /// while let Some(i) = stream.next().await {
    ///     println!("{i}");
    /// }
    /// # Ok(()) })}
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

#[cfg(feature = "std")]
impl<'env, T, E> ScopedTryStream<'env, T, E> {
    /// Create new [`ScopedTryStream`].
    ///
    /// Future can fails, and it's sink can receive [`Result`] type too (see [`TryStreamInner`]).
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Error;
    /// # use futures_util::{SinkExt, StreamExt};
    /// # use scoped_stream_sink::ScopedTryStream;
    /// # fn main() -> Result<(), Error> {
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
    /// let mut stream = <ScopedTryStream<_, Error>>::new(|mut sink| Box::pin(async move {
    ///     // Send a value.
    ///     sink.send(1).await?;
    ///
    ///     // (Optional) close the sink. NOTE: sink cannot be used afterwards.
    ///     // sink.close().await.unwrap();
    ///
    ///     Ok(())
    /// }));
    ///
    /// // Receive all values
    /// while let Some(i) = stream.next().await.transpose()? {
    ///     println!("{i}");
    /// }
    ///
    /// # Ok(()) })}
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            Pin<&'scope mut TryStreamInner<'scope, 'env, T, E>>,
        )
            -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>,
    {
        let mut data = Box::pin(TryStreamInner {
            inner: LocalThread::new(StreamInnerData {
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

impl<T, E> StreamInnerData<Result<T, E>> {
    fn next_fallible<U>(
        &mut self,
        cx: &mut Context<'_>,
        fut: &mut Option<Pin<U>>,
    ) -> Poll<Option<Result<T, E>>>
    where
        U: DerefMut,
        U::Target: Future<Output = Result<(), E>>,
    {
        let res = match fut {
            Some(v) => v.as_mut().poll(cx),
            None => return Poll::Ready(None),
        };
        if res.is_ready() {
            *fut = None;

            if let Poll::Ready(Err(e)) = res {
                return Poll::Ready(Some(Err(e)));
            }
        }

        let ret = self.data.take();
        if ret.is_none() && res.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(ret)
        }
    }
}

impl<T> StreamInnerData<T> {
    fn next<F>(&mut self, cx: &mut Context<'_>, fut: &mut Option<Pin<F>>) -> Poll<Option<T>>
    where
        F: DerefMut,
        F::Target: Future<Output = ()>,
    {
        let res = match fut {
            Some(v) => v.as_mut().poll(cx),
            None => return Poll::Ready(None),
        };
        if res.is_ready() {
            *fut = None;
        }

        let ret = self.data.take();
        if ret.is_none() && res.is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(ret)
        }
    }

    fn flush<E>(&mut self) -> Poll<Result<(), E>> {
        if self.closed || self.data.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn send(&mut self, item: T) {
        if self.closed {
            panic!("Stream is closed");
        }
        if self.data.is_some() {
            panic!("poll_ready() is not called yet!");
        }

        self.data = Some(item);
    }

    fn close<E>(&mut self) -> Poll<Result<(), E>> {
        self.closed = true;
        match self.data {
            Some(_) => Poll::Pending,
            None => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(feature = "std")]
impl<'env, T> Stream for ScopedStream<'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .next(cx, this.fut)
    }
}

#[cfg(feature = "std")]
impl<'env, T, E> Stream for ScopedTryStream<'env, T, E> {
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .next_fallible(cx, this.fut)
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env, T> Sink<T> for StreamInner<'scope, 'env, T> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.into_ref().inner.get_inner().flush()
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.into_ref().inner.get_inner().send(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.into_ref().inner.get_inner().close()
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env, T, E> Sink<Result<T, E>> for TryStreamInner<'scope, 'env, T, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        <Self as Sink<Result<T, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.into_ref().inner.get_inner().flush()
    }

    fn start_send(self: Pin<&mut Self>, item: Result<T, E>) -> Result<(), Infallible> {
        self.into_ref().inner.get_inner().send(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.into_ref().inner.get_inner().close()
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env, T, E> Sink<T> for TryStreamInner<'scope, 'env, T, E> {
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

type DynLocalStreamFut<'scope> = Pin<Box<dyn Future<Output = ()> + 'scope>>;
type DynLocalTryStreamFut<'scope, E> = Pin<Box<dyn Future<Output = Result<(), E>> + 'scope>>;

pin_project! {
    /// Local stream with a scoped future.
    ///
    /// Unlike [`ScopedStream`] it is not [`Send`], so it can work in no-std environment.
    pub struct LocalScopedStream<'env, T> {
        fut: Option<DynLocalStreamFut<'env>>,

        data: Pin<Box<LocalStreamInner<'env, 'env, T>>>,
    }
}

pin_project! {
    /// Local stream with a scoped future.
    ///
    /// Unlike [`ScopedTryStream`] it is not [`Send`], so it can work in no-std environment.
    pub struct LocalScopedTryStream<'env, T, E> {
        fut: Option<DynLocalTryStreamFut<'env, E>>,

        data: Pin<Box<LocalTryStreamInner<'env, 'env, T, E>>>,
    }
}

pin_project! {
    /// Inner type of [`LocalScopedStream`].
    ///
    /// Similiar to [`StreamInner`], but not [`Send`].
    pub struct LocalStreamInner<'scope, 'env: 'scope, T> {
        inner: StreamInnerData<T>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<(&'scope mut &'env T, *mut u8)>,
    }
}

pin_project! {
    /// Inner type of [`LocalScopedTryStream`].
    ///
    /// Similiar to [`TryStreamInner`], but not [`Send`].
    pub struct LocalTryStreamInner<'scope, 'env: 'scope, T, E> {
        inner: StreamInnerData<Result<T, E>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<(&'scope mut &'env (T, E), *mut u8)>,
    }
}

impl<'env, T> LocalScopedStream<'env, T> {
    /// Create new [`LocalScopedStream`].
    ///
    /// Future must return unit type. If you want fallible future, use [`LocalScopedTryStream`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Error;
    /// # use futures_util::{SinkExt, StreamExt};
    /// # use scoped_stream_sink::LocalScopedStream;
    /// # fn main() -> Result<(), Error> {
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
    /// let mut stream = <LocalScopedStream<usize>>::new(|mut sink| Box::pin(async move {
    ///     // Send a value.
    ///     // It is okay to unwrap() because it is infallible.
    ///     sink.send(1).await.unwrap();
    ///
    ///     // (Optional) close the sink. NOTE: sink cannot be used afterwards.
    ///     // sink.close().await.unwrap();
    /// }));
    ///
    /// // Receive all values
    /// while let Some(i) = stream.next().await {
    ///     println!("{i}");
    /// }
    /// # Ok(()) })}
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            Pin<&'scope mut LocalStreamInner<'scope, 'env, T>>,
        ) -> Pin<Box<dyn Future<Output = ()> + 'scope>>,
    {
        let mut data = Box::pin(LocalStreamInner {
            inner: StreamInnerData {
                data: None,
                closed: false,
            },

            pinned: PhantomPinned,
            phantom: PhantomData,
        });

        let ptr = unsafe { transmute::<Pin<&mut LocalStreamInner<T>>, _>(data.as_mut()) };
        let fut = f(ptr);

        Self {
            fut: Some(fut),
            data,
        }
    }
}

impl<'env, T, E> LocalScopedTryStream<'env, T, E> {
    /// Create new [`LocalScopedTryStream`].
    ///
    /// Future can fails, and it's sink can receive [`Result`] type too (see [`LocalTryStreamInner`]).
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Error;
    /// # use futures_util::{SinkExt, StreamExt};
    /// # use scoped_stream_sink::LocalScopedTryStream;
    /// # fn main() -> Result<(), Error> {
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
    /// let mut stream = <LocalScopedTryStream<_, Error>>::new(|mut sink| Box::pin(async move {
    ///     // Send a value.
    ///     sink.send(1).await?;
    ///
    ///     // (Optional) close the sink. NOTE: sink cannot be used afterwards.
    ///     // sink.close().await.unwrap();
    ///
    ///     Ok(())
    /// }));
    ///
    /// // Receive all values
    /// while let Some(i) = stream.next().await.transpose()? {
    ///     println!("{i}");
    /// }
    ///
    /// # Ok(()) })}
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            Pin<&'scope mut LocalTryStreamInner<'scope, 'env, T, E>>,
        ) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'scope>>,
    {
        let mut data = Box::pin(LocalTryStreamInner {
            inner: StreamInnerData {
                data: None,
                closed: false,
            },

            pinned: PhantomPinned,
            phantom: PhantomData,
        });

        let ptr = unsafe { transmute::<Pin<&mut LocalTryStreamInner<T, E>>, _>(data.as_mut()) };
        let fut = f(ptr);

        Self {
            fut: Some(fut),
            data,
        }
    }
}

impl<'env, T> Stream for LocalScopedStream<'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.data.as_mut().project().inner.next(cx, this.fut)
    }
}

impl<'env, T, E> Stream for LocalScopedTryStream<'env, T, E> {
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.data
            .as_mut()
            .project()
            .inner
            .next_fallible(cx, this.fut)
    }
}

impl<'scope, 'env, T> Sink<T> for LocalStreamInner<'scope, 'env, T> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.flush()
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        self.project().inner.send(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.close()
    }
}

impl<'scope, 'env, T, E> Sink<Result<T, E>> for LocalTryStreamInner<'scope, 'env, T, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        <Self as Sink<Result<T, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.project().inner.flush()
    }

    fn start_send(self: Pin<&mut Self>, item: Result<T, E>) -> Result<(), Infallible> {
        self.project().inner.send(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.project().inner.close()
    }
}

impl<'scope, 'env, T, E> Sink<T> for LocalTryStreamInner<'scope, 'env, T, E> {
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

    use std::pin::pin;
    use std::prelude::rust_2021::*;
    use std::ptr::NonNull;
    use std::task::{Context, RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;

    use anyhow::{bail, Error as AnyError, Result as AnyResult};
    use futures_util::{join, pending, SinkExt, StreamExt};
    use tokio::sync::mpsc::channel;
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tokio::{select, spawn};

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

    #[tokio::test]
    async fn test_recv_yield() -> AnyResult<()> {
        let mut stream = <ScopedStream<usize>>::new(|_| {
            Box::pin(async move {
                for _ in 0..5 {
                    yield_now().await;
                }
            })
        });

        test_helper(async move {
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_recv_many() -> AnyResult<()> {
        let mut stream = <ScopedStream<usize>>::new(|mut sink| {
            Box::pin(async move {
                for i in 0..10 {
                    sink.send(i).await.unwrap();
                }
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await, Some(i));
            }
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_recv_many_yield() -> AnyResult<()> {
        let mut stream = <ScopedStream<usize>>::new(|mut sink| {
            Box::pin(async move {
                for i in 0..10 {
                    sink.send(i).await.unwrap();
                    for _ in 0..i {
                        yield_now().await;
                    }
                }
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await, Some(i));
            }
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_double_scoped() -> AnyResult<()> {
        let mut stream = <ScopedStream<usize>>::new(|mut sink| {
            Box::pin(async move {
                let mut stream2 = <ScopedStream<usize>>::new(|mut sink2| {
                    let sink = &mut sink;
                    Box::pin(async move {
                        for i in 0..10 {
                            sink.send(i + 100).await.unwrap();
                            sink2.send(i).await.unwrap();
                        }
                    })
                });

                for i in 0..10 {
                    assert_eq!(stream2.next().await, Some(i));
                }
                assert_eq!(stream2.next().await, None);
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await, Some(i + 100));
            }
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_double_scoped2() -> AnyResult<()> {
        let mut stream = <ScopedStream<usize>>::new(|mut sink| {
            Box::pin(async move {
                let mut stream2 = <ScopedStream<usize>>::new(|mut sink2| {
                    let sink = &mut sink;
                    Box::pin(async move {
                        for i in 0..10 {
                            assert_eq!(join!(sink.send(i + 100), sink2.send(i)), (Ok(()), Ok(())));
                        }
                    })
                });

                for i in 0..10 {
                    assert_eq!(stream2.next().await, Some(i));
                }
                assert_eq!(stream2.next().await, None);
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await, Some(i + 100));
            }
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_simple() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|_| Box::pin(async { Ok(()) }));

        test_helper(async move {
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_recv_one() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|mut src| {
            Box::pin(async move {
                src.send(1).await.unwrap();

                Ok(())
            })
        });

        test_helper(async move {
            assert_eq!(stream.next().await.transpose()?, Some(1));
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_recv_yield() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|_| {
            Box::pin(async move {
                for _ in 0..5 {
                    yield_now().await;
                }

                Ok(())
            })
        });

        test_helper(async move {
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_recv_many() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|mut sink| {
            Box::pin(async move {
                for i in 0..10 {
                    sink.send(i).await.unwrap();
                }

                Ok(())
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await.transpose()?, Some(i));
            }
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_recv_many_yield() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|mut sink| {
            Box::pin(async move {
                for i in 0..10 {
                    sink.send(i).await?;
                    for _ in 0..i {
                        yield_now().await;
                    }
                }

                Ok(())
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await.transpose()?, Some(i));
            }
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_double_scoped() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|mut sink| {
            Box::pin(async move {
                let mut stream2 = <ScopedTryStream<usize, AnyError>>::new(|mut sink2| {
                    let sink = &mut sink;
                    Box::pin(async move {
                        for i in 0..10 {
                            sink.send(i + 100).await?;
                            sink2.send(i).await?;
                        }

                        Ok(())
                    })
                });

                for i in 0..10 {
                    assert_eq!(stream2.next().await.transpose()?, Some(i));
                }
                assert_eq!(stream2.next().await.transpose()?, None);

                Ok(())
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await.transpose()?, Some(i + 100));
            }
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_double_scoped2() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, AnyError>>::new(|mut sink| {
            Box::pin(async move {
                let mut stream2 = <ScopedTryStream<usize, AnyError>>::new(|mut sink2| {
                    let sink = &mut sink;
                    Box::pin(async move {
                        for i in 0..10 {
                            let (r1, r2) = join!(sink.send(i + 100), sink2.send(i));
                            r1?;
                            r2?;
                        }

                        Ok(())
                    })
                });

                for i in 0..10 {
                    assert_eq!(stream2.next().await.transpose()?, Some(i));
                }
                assert_eq!(stream2.next().await.transpose()?, None);

                Ok(())
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await.transpose()?, Some(i + 100));
            }
            assert_eq!(stream.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_fail() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, usize>>::new(|mut sink| {
            Box::pin(async move {
                for i in 0..10 {
                    sink.send(Ok(i)).await.unwrap();
                }

                Err(500)
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await, Some(Ok(i)));
            }
            assert_eq!(stream.next().await, Some(Err(500)));
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_try_fail2() -> AnyResult<()> {
        let mut stream = <ScopedTryStream<usize, usize>>::new(|mut sink| {
            Box::pin(async move {
                for i in 0..10 {
                    sink.send(Ok(i)).await.unwrap();
                }

                for i in 0..10 {
                    sink.send(Err(i)).await.unwrap();
                }

                Err(500)
            })
        });

        test_helper(async move {
            for i in 0..10 {
                assert_eq!(stream.next().await, Some(Ok(i)));
            }

            for i in 0..10 {
                assert_eq!(stream.next().await, Some(Err(i)));
            }

            assert_eq!(stream.next().await, Some(Err(500)));
            assert_eq!(stream.next().await, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_spawn_mpsc() -> AnyResult<()> {
        let (s1, mut r1) = channel::<(usize, usize)>(4);
        let (s2, mut r2) = channel::<(usize, usize)>(4);

        let mut stream = ScopedStream::new(|mut sink| {
            Box::pin(async move {
                loop {
                    let r;
                    select! {
                        Some(v) = r1.recv() => r = v,
                        Some(v) = r2.recv() => r = v,
                        else => return,
                    }

                    println!("Received: {r:?}");
                    sink.feed(r).await.unwrap();
                }
            })
        });

        let it = [0..10, 5..20, 10..100, 25..100, 50..75];
        let mut handles = Vec::new();

        let mut it_ = it.clone();
        handles.push(spawn(test_helper(async move {
            while let Some((i, v)) = stream.next().await {
                assert_eq!(it_[i].next(), Some(v));
            }

            for mut v in it_ {
                assert_eq!(v.next(), None);
            }

            Ok(())
        })));

        for (i, v) in it.into_iter().enumerate() {
            let s = if i % 2 == 0 { s1.clone() } else { s2.clone() };
            handles.push(spawn(test_helper(async move {
                for j in v {
                    s.send((i, j)).await.unwrap();

                    for _ in 0..i {
                        yield_now().await
                    }
                }

                Ok(())
            })));
        }
        drop((s1, s2));

        let mut has_error = false;
        for f in handles {
            if let Err(e) = f.await? {
                eprintln!("{e:?}");
                has_error = true;
            }
        }

        if has_error {
            bail!("Some error has happened");
        }

        Ok(())
    }

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

    #[test]
    fn test_generator() {
        let mut stream = pin!(ScopedStream::new(|mut sink| {
            Box::pin(async move {
                for i in 0usize..10 {
                    sink.send(i).await.unwrap();
                }
            })
        }));

        let waker = nil_waker();
        let mut cx = Context::from_waker(&waker);
        for j in 0usize..10 {
            assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(j)));
        }

        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn test_generator_yield() {
        let mut stream = pin!(ScopedStream::new(|mut sink| {
            Box::pin(async move {
                for i in 0usize..10 {
                    sink.send(i).await.unwrap();
                    pending!();
                }
            })
        }));

        let waker = nil_waker();
        let mut cx = Context::from_waker(&waker);
        for j in 0usize..10 {
            assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(j)));
            assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Pending);
        }

        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(None));
    }
}
