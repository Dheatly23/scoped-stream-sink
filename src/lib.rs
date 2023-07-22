use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use futures_core::Stream;
use futures_sink::Sink;
use parking_lot::Mutex;
use pin_project_lite::pin_project;

pub(crate) mod sealed {
    pub(crate) trait Sealed {}
}

pin_project! {
    #[must_use = "Sink will not do anything if not used"]
    pub struct ScopedSink<'env, T, E, F, Ft> {
        f: F,
        #[pin]
        inner: Option<Ft>,

        data: SinkInner<'env, T>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<E>,
    }
}

pub struct SinkInner<'env, T> {
    inner: Mutex<SinkInnerData<T>>,
    closed: AtomicBool,

    phantom: PhantomData<&'env T>,
}

struct SinkInnerData<T> {
    data: Option<T>,
    waker: Option<Waker>,
}

impl<'env, T: 'env, E: 'env, F, Ft> ScopedSink<'env, T, E, F, Ft>
where
    F: FnMut(&'env SinkInner<'env, T>) -> Ft,
    Ft: Future<Output = Result<(), E>> + 'env,
    Self: 'env,
{
    pub fn new(f: F) -> Self {
        Self {
            data: SinkInner {
                inner: Mutex::new(SinkInnerData {
                    data: None,
                    waker: None,
                }),
                closed: AtomicBool::new(false),

                phantom: PhantomData,
            },

            f,
            inner: None,

            pinned: PhantomPinned,
            phantom: PhantomData,
        }
    }
}

pub type DynScopedSink<'env, T, E> = ScopedSink<
    'env,
    T,
    E,
    Box<
        dyn 'env + FnMut(&'env SinkInner<T>) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'env>>,
    >,
    Pin<Box<dyn Future<Output = Result<(), E>> + 'env>>,
>;

impl<'env, T: 'env, E: 'env> DynScopedSink<'env, T, E>
where
    Self: 'env,
{
    pub fn new_dyn<F, Ft>(mut f: F) -> Self
    where
        F: 'env + FnMut(&'env SinkInner<T>) -> Ft,
        Ft: Future<Output = Result<(), E>> + 'env,
    {
        Self::new(Box::new(move |v| Box::pin(f(v))))
    }
}

impl<'env, T: 'env, E: 'env, F, Ft> Sink<T> for ScopedSink<'env, T, E, F, Ft>
where
    F: FnMut(&'env SinkInner<'env, T>) -> Ft,
    Ft: Future<Output = Result<(), E>> + 'env,
    Self: 'env,
{
    type Error = E;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        self.poll_flush(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let mut this = self.project();
        let fut = loop {
            if let Some(v) = this.inner.as_mut().as_pin_mut() {
                break v;
            }
            if this.data.closed.load(Ordering::SeqCst) {
                return Poll::Ready(Ok(()));
            }

            // SAFETY: We constrained data lifetime to be 'scope.
            // Since 'scope is contained within self, it is safe to extend it.
            let data = unsafe { &*(this.data as *const SinkInner<T>) };

            let f = &mut *this.f;
            this.inner.set(Some(f(data)));
        };

        match fut.poll(cx) {
            Poll::Pending => {
                let guard = this.data.inner.lock();

                if guard.data.is_none() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
            v => {
                this.inner.set(None);
                v
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), E> {
        let data = self.project().data;
        if data.closed.load(Ordering::SeqCst) {
            panic!("Sink is closed!");
        }

        let mut guard = data.inner.lock();
        if guard.data.is_some() {
            panic!("poll_ready() is not called yet!");
        }
        guard.data = Some(item);
        if let Some(waker) = guard.waker.take() {
            waker.wake();
        }

        Ok(())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let data = self.as_mut().project().data;
        data.closed.store(true, Ordering::SeqCst);

        if data.inner.lock().data.is_some() {
            return self.poll_flush(cx);
        }

        let mut this = self.project();
        let Some(fut) = this.inner.as_mut().as_pin_mut() else { return Poll::Ready(Ok(()))};
        let ret = fut.poll(cx);
        if ret.is_ready() {
            this.inner.set(None);
        }
        ret
    }
}

impl<'scope, 'env: 'scope, T> Stream for &'scope SinkInner<'env, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { *self.get_unchecked_mut() };
        let mut guard = this.inner.lock();
        match guard.data.take() {
            Some(v) => Poll::Ready(Some(v)),
            None if this.closed.load(Ordering::SeqCst) => Poll::Ready(None),
            None => {
                guard.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::pin::pin;
    use std::time::Duration;

    use anyhow::{bail, Error as AnyError, Result as AnyResult};
    use futures_util::{SinkExt, StreamExt};
    use tokio::task::yield_now;
    use tokio::time::timeout;

    async fn test_helper<F>(f: F) -> AnyResult<()>
    where
        F: Future<Output = AnyResult<()>>,
    {
        match timeout(Duration::from_secs(5), f).await {
            Ok(v) => v,
            Err(_) => bail!("Time ran out"),
        }
    }

    #[tokio::test]
    async fn test_simple() -> AnyResult<()> {
        let mut sink: Pin<&mut ScopedSink<usize, AnyError, _, _>> =
            pin!(ScopedSink::new(|_| async { Ok(()) }));

        test_helper(async move {
            println!("Closing");
            sink.close().await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_send_one() -> AnyResult<()> {
        let mut sink: Pin<&mut ScopedSink<usize, AnyError, _, _>> =
            pin!(ScopedSink::new(|mut src| async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                }
                println!("Stopping sink");

                Ok(())
            }));

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
        let mut sink: Pin<&mut ScopedSink<usize, AnyError, _, _>> =
            pin!(ScopedSink::new(|mut src| async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                }
                println!("Stopping sink");

                Ok(())
            }));

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
        let mut sink: Pin<&mut ScopedSink<usize, AnyError, _, _>> =
            pin!(ScopedSink::new(|mut src| async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                    for _ in 0..5 {
                        yield_now().await;
                    }
                }
                println!("Stopping sink");

                Ok(())
            }));

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
        let mut sink: Pin<&mut ScopedSink<usize, AnyError, _, _>> =
            pin!(ScopedSink::new(|mut src| async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                    for _ in 0..3 {
                        yield_now().await;
                    }
                }
                println!("Stopping sink");

                Ok(())
            }));

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
        let mut sink: Pin<&mut ScopedSink<usize, AnyError, _, _>> =
            pin!(ScopedSink::new(|mut src| async move {
                println!("Starting sink");
                while let Some(v) = src.next().await {
                    println!("Value: {v}");
                }
                println!("Stopping sink");

                Ok(())
            }));

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
