use core::future::Future;
use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll};
use std::ops::DerefMut;

use futures_core::Stream;
use futures_sink::Sink;
use pin_project_lite::pin_project;

#[cfg(feature = "std")]
use crate::LocalThread;

#[cfg(feature = "std")]
/// Erased type for the scope function.
///
/// It accepts a [`SinkInner`] reference and returns a future, capturing it's parameter.
///
/// # Examples
///
/// ```
/// # use futures_util::StreamExt;
/// let func: scoped_stream_sink::DynSinkFn<usize, ()> = Box::new(|mut stream| Box::pin(async move {
///     while let Some(v) = stream.next().await {
///         println!("Value: {v}");
///     }
///     Ok(())
/// }));
/// ```
pub type DynSinkFn<'env, T, E> = Box<
    dyn 'env
        + Send
        + for<'scope> FnMut(Pin<&'scope mut SinkInner<'scope, 'env, T>>) -> DynSinkFuture<'scope, E>,
>;

/// Erased type for the scoped future.
#[cfg(feature = "std")]
pub type DynSinkFuture<'scope, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>;

#[cfg(feature = "std")]
pin_project! {
    /// Sink with a scoped future.
    ///
    /// It is useful to easily create [`Sink`] type, without
    /// hassle of manually constructing one.
    /// Safety is guaranteed by the inner reference cannot be moved outside the future,
    /// similiar to [`scope`](std::thread::scope).
    #[must_use = "Sink will not do anything if not used"]
    pub struct ScopedSink<'env, T, E> {
        f: DynSinkFn<'env, T, E>,
        inner: Option<DynSinkFuture<'env, E>>,

        #[pin]
        data: SinkInner<'env, 'env, T>,
    }
}

struct SinkInnerData<T> {
    data: Option<T>,
    closed: bool,
}

#[cfg(feature = "std")]
pin_project! {
    /// Inner type for [`ScopedSink`].
    ///
    /// `'scope` defines the lifetime of it's scope,
    /// and `'env` defines the lifetime of it's environment. Lifetimes are constrained
    /// such that the reference cannot be sent outside it's scope.
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`SinkInner`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    ///
    /// Also do note that some of the check depends on `debug_assertions` build config
    /// (AKA only on debug builds).
    pub struct SinkInner<'scope, 'env: 'scope, T> {
        inner: LocalThread<SinkInnerData<T>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env T>,
    }
}

#[cfg(feature = "std")]
impl<'env, T: 'env, E: 'env> ScopedSink<'env, T, E> {
    /// Create new [`ScopedSink`] from a [`DynSinkFn`].
    ///
    /// You should probably use [`new`](Self::new) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Error;
    /// # use scoped_stream_sink::ScopedSink;
    /// let mut sink: ScopedSink<usize, Error> = ScopedSink::new_dyn(Box::new(|_| {
    ///     Box::pin(async { Ok(()) })
    /// }));
    /// ```
    pub fn new_dyn(f: DynSinkFn<'env, T, E>) -> Self {
        Self {
            data: SinkInner {
                inner: LocalThread::new(SinkInnerData {
                    data: None,
                    closed: false,
                }),

                pinned: PhantomPinned,
                phantom: PhantomData,
            },

            f,
            inner: None,
        }
    }

    /// Create new [`ScopedSink`].
    ///
    /// Future should be consuming all items of the stream.
    /// If not, the function will be called again to restart itself.
    /// It is guaranteed if the sink is closed, the future will never restarts.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::pin::pin;
    /// # use anyhow::Error;
    /// # use futures_util::{SinkExt, StreamExt};
    /// # use scoped_stream_sink::ScopedSink;
    /// # fn main() -> Result<(), Error> {
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
    /// let mut sink = pin!(<ScopedSink<_, Error>>::new(|mut stream| Box::pin(async move {
    ///     // Reads a value. If future returns before sink is closed, it will be restarted.
    ///     if let Some(v) = stream.next().await {
    ///         println!("Value: {v}");
    ///     }
    ///     Ok(())
    /// })));
    ///
    /// // Send a value
    /// sink.send(1).await?;
    ///
    /// // Close the sink
    /// sink.close().await?;
    /// # Ok(()) })}
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: 'env
            + Send
            + FnMut(
                Pin<&'scope mut SinkInner<'scope, 'env, T>>,
            ) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>,
    {
        Self::new_dyn(Box::new(f))
    }
}

impl<T> SinkInnerData<T> {
    fn flush<E, U, F>(
        &mut self,
        cx: &mut Context<'_>,
        fut: &mut Option<Pin<U>>,
        mut f: F,
    ) -> Poll<Result<(), E>>
    where
        U: DerefMut,
        U::Target: Future<Output = Result<(), E>>,
        F: FnMut() -> Pin<U>,
    {
        loop {
            if self.data.is_none() {
                // No need to poll future.
                return Poll::Ready(Ok(()));
            }

            let fp = if let Some(v) = fut {
                v
            } else if self.closed {
                return Poll::Ready(Ok(()));
            } else {
                fut.get_or_insert_with(&mut f)
            };

            if let Poll::Ready(v) = fp.as_mut().poll(cx) {
                // Dispose future.
                *fut = None;

                if v.is_err() {
                    return Poll::Ready(v);
                }

                // We have to repoll the future, otherwise it will never be awoken.
                continue;
            }

            return match self.data {
                Some(_) => Poll::Pending,
                None => Poll::Ready(Ok(())),
            };
        }
    }

    fn send(&mut self, item: T) {
        if self.closed {
            panic!("Sink is closed!");
        }
        if self.data.is_some() {
            panic!("poll_ready() is not called yet!");
        }
        self.data = Some(item);
    }

    fn close<E, U, F>(
        &mut self,
        cx: &mut Context<'_>,
        fut: &mut Option<Pin<U>>,
        f: F,
    ) -> Poll<Result<(), E>>
    where
        U: DerefMut,
        U::Target: Future<Output = Result<(), E>>,
        F: FnMut() -> Pin<U>,
    {
        self.closed = true;

        // There is still some data
        if self.data.is_some() {
            let ret = self.flush(cx, &mut *fut, f);
            if let Poll::Ready(Err(_)) = ret {
                return ret;
            }
            return match fut {
                // Must have been pending then.
                Some(_) => Poll::Pending,
                None => ret,
            };
        }

        let ret = match fut {
            Some(p) => p.as_mut().poll(cx),
            None => return Poll::Ready(Ok(())),
        };
        if ret.is_ready() {
            *fut = None;
        }
        ret
    }

    fn next(&mut self) -> Poll<Option<T>> {
        match self.data.take() {
            v @ Some(_) => Poll::Ready(v),
            None if self.closed => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
}

unsafe fn make_future<'a, T: 'a, R, F>(mut ptr: NonNull<T>, mut f: F) -> impl FnMut() -> R
where
    F: FnMut(Pin<&'a mut T>) -> R,
{
    move || f(Pin::new_unchecked(ptr.as_mut()))
}

#[cfg(feature = "std")]
impl<'env, T: 'env, E: 'env> ScopedSink<'env, T, E> {
    fn future_wrapper(
        self: Pin<&mut Self>,
    ) -> (
        impl DerefMut<Target = SinkInnerData<T>> + '_,
        &mut Option<DynSinkFuture<'env, E>>,
        impl FnMut() -> DynSinkFuture<'env, E> + '_,
    ) {
        let mut this = self.project();
        // SAFETY: We constrained data lifetime to be 'scope.
        // Since 'scope is contained within self, it is safe to extend it.
        let f = unsafe {
            make_future(
                NonNull::from(this.data.as_mut().get_unchecked_mut()),
                this.f,
            )
        };

        (this.data.project().inner.set_inner_ctx(), this.inner, f)
    }
}

#[cfg(feature = "std")]
impl<'env, T: 'env, E: 'env> Sink<T> for ScopedSink<'env, T, E> {
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let (mut data, fut, f) = self.future_wrapper();

        data.flush(cx, fut, f)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), E> {
        self.project()
            .data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .send(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let (mut data, fut, f) = self.future_wrapper();

        data.close(cx, fut, f)
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env: 'scope, T> Stream for SinkInner<'scope, 'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.into_ref().inner.get_inner().next()
    }
}

/// Erased type for the local scope function.
///
/// It accepts a [`LocalSinkInner`] reference and returns a future, capturing it's parameter.
///
/// # Examples
///
/// ```
/// # use futures_util::StreamExt;
/// let func: scoped_stream_sink::DynLocalSinkFn<usize, ()> = Box::new(|mut stream| Box::pin(async move {
///     while let Some(v) = stream.next().await {
///         println!("Value: {v}");
///     }
///     Ok(())
/// }));
/// ```
pub type DynLocalSinkFn<'env, T, E> = Box<
    dyn 'env
        + for<'scope> FnMut(
            Pin<&'scope mut LocalSinkInner<'scope, 'env, T>>,
        ) -> DynLocalSinkFuture<'scope, E>,
>;

/// Erased type for the locally scoped future.
pub type DynLocalSinkFuture<'scope, E> = Pin<Box<dyn Future<Output = Result<(), E>> + 'scope>>;

pin_project! {
    /// Local sink with a scoped future.
    ///
    /// Unlike [`ScopedSink`] it is not [`Send`], so it can work in no-std environment.
    #[must_use = "Sink will not do anything if not used"]
    pub struct LocalScopedSink<'env, T, E> {
        f: DynLocalSinkFn<'env, T, E>,
        inner: Option<DynLocalSinkFuture<'env, E>>,

        #[pin]
        data: LocalSinkInner<'env, 'env, T>,
    }
}

pin_project! {
    /// Inner type for [`LocalScopedSink`].
    ///
    /// Similiar to [`SinkInner`], but not [`Send`].
    pub struct LocalSinkInner<'scope, 'env: 'scope, T> {
        inner: SinkInnerData<T>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<(&'scope mut &'env T, *mut u8)>,
    }
}

impl<'env, T: 'env, E: 'env> LocalScopedSink<'env, T, E> {
    /// Create new [`LocalScopedSink`] from a [`DynLocalSinkFn`].
    ///
    /// You should probably use [`new`](Self::new) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Error;
    /// # use scoped_stream_sink::LocalScopedSink;
    /// let mut sink: LocalScopedSink<usize, Error> = LocalScopedSink::new_dyn(Box::new(|_| {
    ///     Box::pin(async { Ok(()) })
    /// }));
    /// ```
    pub fn new_dyn(f: DynLocalSinkFn<'env, T, E>) -> Self {
        Self {
            data: LocalSinkInner {
                inner: SinkInnerData {
                    data: None,
                    closed: false,
                },

                pinned: PhantomPinned,
                phantom: PhantomData,
            },

            f,
            inner: None,
        }
    }

    /// Create new [`LocalScopedSink`].
    ///
    /// Future should be consuming all items of the stream.
    /// If not, the function will be called again to restart itself.
    /// It is guaranteed if the sink is closed, the future will never restarts.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::pin::pin;
    /// # use anyhow::Error;
    /// # use futures_util::{SinkExt, StreamExt};
    /// # use scoped_stream_sink::LocalScopedSink;
    /// # fn main() -> Result<(), Error> {
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
    /// let mut sink = pin!(<LocalScopedSink<_, Error>>::new(|mut stream| Box::pin(async move {
    ///     // Reads a value. If future returns before sink is closed, it will be restarted.
    ///     if let Some(v) = stream.next().await {
    ///         println!("Value: {v}");
    ///     }
    ///     Ok(())
    /// })));
    ///
    /// // Send a value
    /// sink.send(1).await?;
    ///
    /// // Close the sink
    /// sink.close().await?;
    /// # Ok(()) })}
    /// ```
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: 'env
            + FnMut(Pin<&'scope mut LocalSinkInner<'scope, 'env, T>>) -> DynLocalSinkFuture<'scope, E>,
    {
        Self::new_dyn(Box::new(f))
    }
}

impl<'env, T: 'env, E: 'env> LocalScopedSink<'env, T, E> {
    fn future_wrapper(
        self: Pin<&mut Self>,
    ) -> (
        &mut SinkInnerData<T>,
        &mut Option<DynLocalSinkFuture<'env, E>>,
        impl FnMut() -> DynLocalSinkFuture<'env, E> + '_,
    ) {
        let mut this = self.project();
        // SAFETY: We constrained data lifetime to be 'scope.
        // Since 'scope is contained within self, it is safe to extend it.
        let f = unsafe {
            make_future(
                NonNull::from(this.data.as_mut().get_unchecked_mut()),
                this.f,
            )
        };

        (this.data.project().inner, this.inner, f)
    }
}

impl<'env, T: 'env, E: 'env> Sink<T> for LocalScopedSink<'env, T, E> {
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let (data, fut, f) = self.future_wrapper();

        data.flush(cx, fut, f)
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), E> {
        self.project().data.as_mut().project().inner.send(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let (data, fut, f) = self.future_wrapper();

        data.close(cx, fut, f)
    }
}

impl<'scope, 'env: 'scope, T> Stream for LocalSinkInner<'scope, 'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::pin;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;
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

        let mut sink: Pin<&mut ScopedSink<'static, usize, AnyError>> =
            pin!(ScopedSink::new(|mut src| Box::pin(async move {
                tokio::spawn(async move {
                    println!("{:?}", src.next().await);
                    println!("{:?}", src.next().await);
                })
                .await?;
                Ok(())
            })));

        test_helper(async move {
            let mut sink = pin!(sink);
            sink.send(1).await?;
            drop(sink);

            Ok(())
        })
        .await
    }
    */
    #[tokio::test]
    async fn test_simple() -> AnyResult<()> {
        let sink = <ScopedSink<usize, AnyError>>::new(|_| Box::pin(async { Ok(()) }));

        test_helper(async move {
            let mut sink = pin!(sink);

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_one() -> AnyResult<()> {
        let sink = <ScopedSink<usize, AnyError>>::new(|mut src| {
            Box::pin(async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                }
                println!("Stopping sink");

                Ok(())
            })
        });

        test_helper(async move {
            let mut sink = pin!(sink);

            sink.feed(1).await?;

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_many() -> AnyResult<()> {
        let sink = <ScopedSink<usize, AnyError>>::new(|mut src| {
            Box::pin(async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                }
                println!("Stopping sink");

                Ok(())
            })
        });

        test_helper(async move {
            let mut sink = pin!(sink);

            for i in 0..10 {
                println!("Sending: {i}");
                sink.feed(i).await?;
            }

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_yield() -> AnyResult<()> {
        let sink = <ScopedSink<usize, AnyError>>::new(|mut src| {
            Box::pin(async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                    for _ in 0..5 {
                        yield_now().await;
                    }
                }
                println!("Stopping sink");

                Ok(())
            })
        });

        test_helper(async move {
            let mut sink = pin!(sink);

            for i in 0..10 {
                println!("Sending: {i}");
                sink.feed(i).await?;
            }

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_yield2() -> AnyResult<()> {
        let sink = <ScopedSink<usize, AnyError>>::new(|mut src| {
            Box::pin(async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                    for _ in 0..3 {
                        yield_now().await;
                    }
                }
                println!("Stopping sink");

                Ok(())
            })
        });

        test_helper(async move {
            let mut sink = pin!(sink);

            for i in 0..10 {
                println!("Sending: {i}");
                sink.feed(i).await?;

                for _ in 0..5 {
                    yield_now().await;
                }
            }

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_many_flush() -> AnyResult<()> {
        let sink = <ScopedSink<usize, AnyError>>::new(|mut src| {
            Box::pin(async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                }
                println!("Stopping sink");

                Ok(())
            })
        });

        test_helper(async move {
            let mut sink = pin!(sink);

            for i in 0..10 {
                println!("Sending: {i}");
                sink.feed(i).await?;
            }

            println!("Flushing");
            sink.flush().await?;

            for i in 10..20 {
                println!("Sending: {i}");
                sink.feed(i).await?;
            }

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_return_then_receive() -> AnyResult<()> {
        let v = Arc::new(AtomicU8::new(0));
        let sink = <ScopedSink<usize, AnyError>>::new(move |mut src| {
            let v = v.clone();
            Box::pin(async move {
                let mut v_ = v.load(Ordering::SeqCst);
                v_ = if v_ == 8 {
                    // Should never receive empty as any closure will be returned early.
                    assert_eq!(src.next().await, Some(1));
                    0
                } else {
                    v_ + 1
                };
                v.store(v_, Ordering::SeqCst);

                Ok(())
            })
        });

        test_helper(async move {
            let mut sink = pin!(sink);

            for _ in 0..10 {
                println!("Sending");
                sink.feed(1).await?;
            }

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }
}
