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
use crate::{State, StreamSink};

#[cfg(feature = "std")]
pin_project! {
    /// Scoped version of [`StreamSink`]. Makes building [`StreamSink`] much easier to do.
    #[must_use = "StreamSink will not do anything if not used"]
    pub struct ScopedStreamSink<'env, SI, RI, E> {
        fut: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'env>>>,

        data: Pin<Box<StreamSinkInner<'env, 'env, SI, RI, E>>>,
    }
}

struct StreamSinkInnerData<SI, RI, E> {
    send: Option<Result<SI, E>>,
    recv: Option<RI>,
    close_send: bool,
    close_recv: bool,
}

#[cfg(feature = "std")]
pin_project! {
    struct StreamSinkInner<'scope, 'env: 'scope, SI, RI, E> {
        inner: LocalThread<StreamSinkInnerData<SI, RI, E>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env (SI, RI, E)>,
    }
}

#[cfg(feature = "std")]
pin_project! {
    /// [`Stream`] half of inner [`ScopedStreamSink`].
    /// Produce receive type values.
    /// Can only be closed from it's outer [`ScopedStreamSink`].
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`StreamPart`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    ///
    /// Also do note that some of the check depends on `debug_assertions` build config
    /// (AKA only on debug builds).
    #[must_use = "Stream will not do anything if not used"]
    pub struct StreamPart<'scope, 'env: 'scope, SI, RI, E> {
        ptr: Pin<&'scope mut StreamSinkInner<'scope, 'env, SI, RI, E>>,
    }
}

#[cfg(feature = "std")]
pin_project! {
    /// [`Sink`] half of inner [`ScopedStreamSink`].
    /// Can receive both send type or a [`Result`] type.
    /// Closing will complete when outer [`ScopedStreamSink`] is closed and received all data.
    ///
    /// # Note About Thread-safety
    ///
    /// Even though [`SinkPart`] is both [`Send`] and [`Sink`], it's reference
    /// **should** not be sent across thread. This is currently impossible, due to
    /// lack of async version of [`scope`](std::thread::scope).
    /// To future-proof that possibility, any usage of it will panic if called from different
    /// thread than the outer thread. It also may panics outer thread too.
    ///
    /// Also do note that some of the check depends on `debug_assertions` build config
    /// (AKA only on debug builds).
    #[must_use = "Sink will not do anything if not used"]
    pub struct SinkPart<'scope, 'env: 'scope, SI, RI, E> {
        ptr: Pin<&'scope mut StreamSinkInner<'scope, 'env, SI, RI, E>>,
    }
}

#[cfg(feature = "std")]
impl<'env, SI, RI, E> ScopedStreamSink<'env, SI, RI, E> {
    /// Creates new [`ScopedStreamSink`].
    /// Safety is guaranteed by scoping both [`StreamPart`] and [`SinkPart`].
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            StreamPart<'scope, 'env, SI, RI, E>,
            SinkPart<'scope, 'env, SI, RI, E>,
        )
            -> Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'scope>>,
    {
        let mut data = Box::pin(StreamSinkInner {
            inner: LocalThread::new(StreamSinkInnerData {
                send: None,
                recv: None,
                close_send: false,
                close_recv: false,
            }),

            pinned: PhantomPinned,
            phantom: PhantomData,
        });

        let (stream, sink);
        // SAFETY: Borrow is scoped, so it can't get out of scope.
        // Also, StreamPart and SinkPart write access is separated.
        unsafe {
            stream = StreamPart {
                ptr: transmute::<Pin<&mut StreamSinkInner<SI, RI, E>>, _>(data.as_mut()),
            };
            sink = SinkPart {
                ptr: transmute::<Pin<&mut StreamSinkInner<SI, RI, E>>, _>(data.as_mut()),
            };
        }
        let fut = f(stream, sink);

        Self {
            fut: Some(fut),
            data,
        }
    }
}

impl<SI, RI, E> StreamSinkInnerData<SI, RI, E> {
    fn stream_sink<F>(&mut self, cx: &mut Context<'_>, fut: &mut Option<Pin<F>>) -> State<SI, E>
    where
        F: DerefMut,
        F::Target: Future<Output = Result<(), E>>,
    {
        let ret = match fut {
            Some(f) => f.as_mut().poll(cx),
            None => Poll::Ready(Ok(())),
        };

        if let Poll::Ready(v) = ret {
            *fut = None;
            self.close_send = true;
            self.close_recv = true;

            if let Err(e) = v {
                return State::Error(e);
            }
        }

        match (self.send.take(), !self.close_recv && self.recv.is_none()) {
            (Some(Err(e)), _) => State::Error(e),
            (Some(Ok(i)), true) => State::SendRecvReady(i),
            (Some(Ok(i)), false) => State::SendReady(i),
            (None, _) if fut.is_none() => State::End,
            (None, true) => State::RecvReady,
            (None, false) => State::Pending,
        }
    }

    fn send_outer(&mut self, item: RI) {
        if self.close_recv {
            panic!("ScopedStreamSink is closed!");
        }
        if self.recv.is_some() {
            panic!("ScopedStreamSink is not ready to receive!");
        }

        self.recv = Some(item);
    }

    fn close_outer<F>(
        &mut self,
        cx: &mut Context<'_>,
        fut: &mut Option<Pin<F>>,
    ) -> Poll<Result<Option<SI>, E>>
    where
        F: DerefMut,
        F::Target: Future<Output = Result<(), E>>,
    {
        self.close_recv = true;
        let ret = match fut {
            Some(f) => f.as_mut().poll(cx),
            None => Poll::Ready(Ok(())),
        };

        if let Poll::Ready(v) = ret {
            *fut = None;

            if let Err(e) = v {
                return Poll::Ready(Err(e));
            }
        }

        let ret = self.send.take();
        if ret.is_none() && fut.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(ret.transpose())
        }
    }

    fn next(&mut self) -> Poll<Option<RI>> {
        match self.recv.take() {
            v @ Some(_) => Poll::Ready(v),
            None if self.close_recv => Poll::Ready(None),
            None => Poll::Pending,
        }
    }

    fn flush<E2>(&mut self) -> Poll<Result<(), E2>> {
        if !self.close_send && self.send.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn send_inner(&mut self, item: Result<SI, E>) {
        if self.close_send {
            panic!("ScopedStreamSink is closed!");
        }
        if self.send.is_some() {
            panic!("poll_ready() is not called first!");
        }

        self.send = Some(item);
    }

    fn close_inner<E2>(&mut self) -> Poll<Result<(), E2>> {
        self.close_send = true;
        if self.send.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(feature = "std")]
impl<'env, SI, RI, E> StreamSink<SI, RI> for ScopedStreamSink<'env, SI, RI, E> {
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        let this = self.project();
        this.data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .stream_sink(cx, this.fut)
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        self.project()
            .data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .send_outer(item);
        Ok(())
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        let this = self.project();
        this.data
            .as_mut()
            .project()
            .inner
            .set_inner_ctx()
            .close_outer(cx, this.fut)
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env, SI, RI, E> Stream for StreamPart<'scope, 'env, SI, RI, E> {
    type Item = RI;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.into_ref().ptr.inner.get_inner().next()
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env, SI, RI, E> Sink<Result<SI, E>> for SinkPart<'scope, 'env, SI, RI, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.into_ref().ptr.inner.get_inner().flush()
    }

    fn start_send(self: Pin<&mut Self>, item: Result<SI, E>) -> Result<(), Self::Error> {
        self.into_ref().ptr.inner.get_inner().send_inner(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.into_ref().ptr.inner.get_inner().close_inner()
    }
}

#[cfg(feature = "std")]
impl<'scope, 'env, SI, RI, E> Sink<SI> for SinkPart<'scope, 'env, SI, RI, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: SI) -> Result<(), Self::Error> {
        <Self as Sink<Result<SI, E>>>::start_send(self, Ok(item))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_close(self, cx)
    }
}

pin_project! {
    /// Locally scoped version of [`StreamSink`]. Does not implement [`Send`].
    #[must_use = "StreamSink will not do anything if not used"]
    pub struct LocalScopedStreamSink<'env, SI, RI, E> {
        fut: Option<Pin<Box<dyn Future<Output = Result<(), E>> + 'env>>>,

        data: Pin<Box<LocalStreamSinkInner<'env, 'env, SI, RI, E>>>,
    }
}

pin_project! {
    struct LocalStreamSinkInner<'scope, 'env: 'scope, SI, RI, E> {
        inner: StreamSinkInnerData<SI, RI, E>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<(&'scope mut &'env (SI, RI, E), *mut u8)>,
    }
}

pin_project! {
    /// [`Stream`] half of inner [`LocalScopedStreamSink`].
    /// Produce receive type values.
    /// Can only be closed from it's outer [`LocalScopedStreamSink`].
    #[must_use = "Stream will not do anything if not used"]
    pub struct LocalStreamPart<'scope, 'env: 'scope, SI, RI, E> {
        ptr: Pin<&'scope mut LocalStreamSinkInner<'scope, 'env, SI, RI, E>>,
    }
}

pin_project! {
    /// [`Sink`] half of inner [`LocalScopedStreamSink`].
    /// Can receive both send type or a [`Result`] type.
    /// Closing will complete when outer [`LocalScopedStreamSink`] is closed and received all data.
    #[must_use = "Sink will not do anything if not used"]
    pub struct LocalSinkPart<'scope, 'env: 'scope, SI, RI, E> {
        ptr: Pin<&'scope mut LocalStreamSinkInner<'scope, 'env, SI, RI, E>>,
    }
}

impl<'env, SI, RI, E> LocalScopedStreamSink<'env, SI, RI, E> {
    /// Creates new [`LocalScopedStreamSink`].
    /// Safety is guaranteed by scoping both [`LocalStreamPart`] and [`LocalSinkPart`].
    pub fn new<F>(f: F) -> Self
    where
        for<'scope> F: FnOnce(
            LocalStreamPart<'scope, 'env, SI, RI, E>,
            LocalSinkPart<'scope, 'env, SI, RI, E>,
        ) -> Pin<Box<dyn Future<Output = Result<(), E>> + 'scope>>,
    {
        let mut data = Box::pin(LocalStreamSinkInner {
            inner: StreamSinkInnerData {
                send: None,
                recv: None,
                close_send: false,
                close_recv: false,
            },

            pinned: PhantomPinned,
            phantom: PhantomData,
        });

        let (stream, sink);
        // SAFETY: Borrow is scoped, so it can't get out of scope.
        // Also, StreamPart and SinkPart write access is separated.
        unsafe {
            stream = LocalStreamPart {
                ptr: transmute::<Pin<&mut LocalStreamSinkInner<SI, RI, E>>, _>(data.as_mut()),
            };
            sink = LocalSinkPart {
                ptr: transmute::<Pin<&mut LocalStreamSinkInner<SI, RI, E>>, _>(data.as_mut()),
            };
        }
        let fut = f(stream, sink);

        Self {
            fut: Some(fut),
            data,
        }
    }
}

impl<'env, SI, RI, E> StreamSink<SI, RI> for LocalScopedStreamSink<'env, SI, RI, E> {
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        let this = self.project();
        this.data.as_mut().project().inner.stream_sink(cx, this.fut)
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        self.project()
            .data
            .as_mut()
            .project()
            .inner
            .send_outer(item);
        Ok(())
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        let this = self.project();
        this.data.as_mut().project().inner.close_outer(cx, this.fut)
    }
}

impl<'scope, 'env, SI, RI, E> Stream for LocalStreamPart<'scope, 'env, SI, RI, E> {
    type Item = RI;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().ptr.as_mut().project().inner.next()
    }
}

impl<'scope, 'env, SI, RI, E> Sink<Result<SI, E>> for LocalSinkPart<'scope, 'env, SI, RI, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().ptr.as_mut().project().inner.flush()
    }

    fn start_send(self: Pin<&mut Self>, item: Result<SI, E>) -> Result<(), Self::Error> {
        self.project().ptr.as_mut().project().inner.send_inner(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().ptr.as_mut().project().inner.close_inner()
    }
}

impl<'scope, 'env, SI, RI, E> Sink<SI> for LocalSinkPart<'scope, 'env, SI, RI, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn start_send(self: Pin<&mut Self>, item: SI) -> Result<(), Self::Error> {
        <Self as Sink<Result<SI, E>>>::start_send(self, Ok(item))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_close(self, cx)
    }
}
