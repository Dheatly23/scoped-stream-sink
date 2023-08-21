mod stream_sink;

use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::transmute;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread::{current, ThreadId};

use futures_core::Stream;
use futures_sink::Sink;
use pin_project_lite::pin_project;

pub use crate::stream_sink::*;

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

/// Erased type for the scope function. It accepts a [`SinkInner`] reference
/// that implements [`Stream`].
///
/// # Examples
///
/// ```
/// // Helper methods for stream
/// use futures_util::StreamExt;
///
/// let func: scoped_sink::DynSinkFn<usize, ()> = Box::new(|mut stream| Box::pin(async move {
///     while let Some(v) = stream.next().await {
///         println!("Value: {v}");
///     }
///     Ok(())
/// }));
/// ```
pub type DynSinkFn<'env, T, E> = Box<
    dyn 'env
        + Send
        + for<'scope> FnMut(
            Pin<&'scope mut SinkInner<'scope, 'env, T>>,
        ) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>,
>;

pin_project! {
    /// Sink with a scoped future. It is useful to simply creates stateful sinks.
    /// Safety is guaranteed by the inner reference cannot be moved outside the future,
    /// similiar to [`scope`](https://doc.rust-lang.org/std/thread/fn.scope.html).
    #[must_use = "Sink will not do anything if not used"]
    pub struct ScopedSink<'env, T, E> {
        f: DynSinkFn<'env, T, E>,
        inner: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'env>>>,

        data: Pin<Box<SinkInner<'env, 'env, T>>>,
    }
}

struct SinkInnerData<T> {
    data: Option<T>,
    closed: bool,
}

pin_project! {
    /// Inner type for [`ScopedSink`]. `'scope` defines the lifetime of it's scope,
    /// and `'env` defines the lifetime of it's environment. Lifetimes are constrained
    /// such that the reference cannot be sent outside it's scope.
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`SinkInner`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](https://doc.rust-lang.org/std/thread/fn.scope.html).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    pub struct SinkInner<'scope, 'env: 'scope, T> {
        inner: LocalThread<SinkInnerData<T>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env T>,
    }
}

impl<'env, T: 'env, E: 'env> ScopedSink<'env, T, E> {
    /// Create new [`ScopedSink`] from a [`DynSinkFn`].
    /// You should probably use [`new`](`ScopedSink::new`) instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use scoped_sink::ScopedSink;
    /// let mut sink: ScopedSink<usize, ()> = ScopedSink::new_dyn(Box::new(|_| {
    ///     Box::pin(async { Ok(()) })
    /// }));
    /// ```
    pub fn new_dyn(f: DynSinkFn<'env, T, E>) -> Self {
        Self {
            data: Box::pin(SinkInner {
                inner: LocalThread::new(SinkInnerData {
                    data: None,
                    closed: false,
                }),

                pinned: PhantomPinned,
                phantom: PhantomData,
            }),

            f,
            inner: None,
        }
    }

    /// Create new [`ScopedSink`].
    /// Future should be consuming all items of the stream.
    /// If not, the function will be called again to restart itself.
    /// It is guaranteed if the sink is closed, the future will never restarts.
    ///
    /// # Examples
    ///
    /// ```
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// # use scoped_sink::ScopedSink;
    /// use anyhow::Error;
    /// // Helper methods for stream
    /// use futures_util::{SinkExt, StreamExt};
    ///
    /// let mut sink = <ScopedSink<usize, Error>>::new(|mut stream| Box::pin(async move {
    ///     // Reads a value. If future returns before sink is closed, it will be restarted.
    ///     if let Some(v) = stream.next().await {
    ///         println!("Value: {v}");
    ///     }
    ///     Ok(())
    /// }));
    ///
    /// // Send a value
    /// sink.send(1).await?;
    ///
    /// // Close the sink
    /// sink.close().await?;
    /// # Ok::<(), Error>(()) });
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

impl<'env, T: 'env, E: 'env> Sink<T> for ScopedSink<'env, T, E> {
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let this = self.project();
        let data = this.data.as_mut().project().inner.set_inner_ctx();

        if data.data.is_none() {
            // No need to poll future.
            return Poll::Ready(Ok(()));
        }
        let closed = data.closed;

        let fut = loop {
            if let Some(v) = this.inner {
                break v.as_mut();
            }
            if closed {
                return Poll::Ready(Ok(()));
            }

            // SAFETY: We constrained data lifetime to be 'scope.
            // Since 'scope is contained within self, it is safe to extend it.
            let inner = unsafe {
                transmute::<Pin<&mut SinkInner<T>>, Pin<&mut SinkInner<T>>>(this.data.as_mut())
            };

            let f = &mut *this.f;
            *this.inner = Some(f(inner));
        };

        if let Poll::Ready(v) = fut.poll(cx) {
            // Dispose future.
            *this.inner = None;

            if let Err(_) = v {
                return Poll::Ready(v);
            }
        }

        match this
            .data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .data
            .is_none()
        {
            true => Poll::Ready(Ok(())),
            false => Poll::Pending,
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), E> {
        let data = self.project().data.as_mut().project().inner.set_inner_ctx();
        if data.closed {
            panic!("Sink is closed!");
        }

        if data.data.is_some() {
            panic!("poll_ready() is not called yet!");
        }
        data.data = Some(item);

        Ok(())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let data = self
            .as_mut()
            .project()
            .data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx();
        data.closed = true;

        // There is still some data
        if data.data.is_some() {
            if let v @ (Poll::Pending | Poll::Ready(Err(_))) = self.as_mut().poll_flush(cx) {
                return v;
            }
        }

        // At this point, no data must reside.
        // There is possibility future is polled twice, but that shouldn't matter.

        let mut this = self.project();
        let Some(fut) = &mut this.inner else {
            return Poll::Ready(Ok(()));
        };
        let ret = fut.as_mut().poll(cx);
        if ret.is_ready() {
            *this.inner = None;
        }
        ret
    }
}

impl<'scope, 'env: 'scope, T> Stream for SinkInner<'scope, 'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let temp = self.into_ref();
        let this = temp.inner.get_inner();
        match this.data.take() {
            Some(v) => Poll::Ready(Some(v)),
            None if this.closed => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
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
            sink.send(1).await?;
            drop(sink);

            Ok(())
        })
        .await
    }
    */
    #[tokio::test]
    async fn test_simple() -> AnyResult<()> {
        let mut sink: ScopedSink<usize, AnyError> = ScopedSink::new(|_| Box::pin(async { Ok(()) }));

        test_helper(async move {
            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_one() -> AnyResult<()> {
        let mut sink: ScopedSink<usize, AnyError> = ScopedSink::new(|mut src| {
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
            sink.feed(1).await?;

            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_many() -> AnyResult<()> {
        let mut sink: ScopedSink<usize, AnyError> = ScopedSink::new(|mut src| {
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
        let mut sink: ScopedSink<usize, AnyError> = ScopedSink::new(|mut src| {
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
        let mut sink: ScopedSink<usize, AnyError> = ScopedSink::new(|mut src| {
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
        let mut sink: ScopedSink<usize, AnyError> = ScopedSink::new(|mut src| {
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
}
