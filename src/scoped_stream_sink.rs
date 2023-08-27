use std::convert::Infallible;
use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::mem::transmute;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_sink::Sink;
use pin_project_lite::pin_project;

use crate::{LocalThread, State, StreamSink};

pin_project! {
    /// Scoped version of [`StreamSink`]. Makes building [`StreamSink`] much easier to do.
    pub struct ScopedStreamSink<'env, SI, RI, E> {
        fut: Option<Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'env>>>,

        data: Pin<Box<StreamSinkInner<'env, 'env, SI, RI, E>>>,
    }
}

struct StreamSinkInnerData<SI, RI, E> {
    send: Option<Result<SI, E>>,
    recv: Option<RI>,
    closed: bool,
}

pin_project! {
    struct StreamSinkInner<'scope, 'env: 'scope, SI, RI, E> {
        inner: LocalThread<StreamSinkInnerData<SI, RI, E>>,

        #[pin]
        pinned: PhantomPinned,
        phantom: PhantomData<&'scope mut &'env (SI, RI, E)>,
    }
}

pin_project! {
    /// [`Stream`] half of inner [`ScopedStreamSink`].
    /// Produce receive type values.
    /// Can only be closed from it's outer [`ScopedStreamSink`].
    pub struct StreamPart<'scope, 'env: 'scope, SI, RI, E> {
        ptr: Pin<&'scope mut StreamSinkInner<'scope, 'env, SI, RI, E>>,
    }
}

pin_project! {
    /// [`Sink`] half of inner [`ScopedStreamSink`].
    /// Can receive both send type or a [`Result`] type.
    /// Closing will complete when outer [`ScopedStreamSink`] is closed and received all data.
    pub struct SinkPart<'scope, 'env: 'scope, SI, RI, E> {
        ptr: Pin<&'scope mut StreamSinkInner<'scope, 'env, SI, RI, E>>,
    }
}

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
                closed: false,
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

impl<'env, SI, RI, E> StreamSink<SI, RI> for ScopedStreamSink<'env, SI, RI, E> {
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        let this = self.project();

        let ret = if let Some(fut) = this.fut {
            this.data.as_mut().project().inner.set_inner_ctx();
            fut.as_mut().poll(cx)
        } else {
            Poll::Ready(Ok(()))
        };

        if let Poll::Ready(v) = ret {
            *this.fut = None;
            if let Err(e) = v {
                return State::Error(e);
            }
        }

        let inner = this.data.as_mut().project().inner.set_inner_ctx();
        if this.fut.is_none() {
            inner.closed = true;
        }
        match (inner.send.take(), !inner.closed && inner.recv.is_none()) {
            (Some(Err(e)), _) => State::Error(e),
            (Some(Ok(i)), true) => State::SendRecvReady(i),
            (Some(Ok(i)), false) => State::SendReady(i),
            (None, _) if this.fut.is_none() => State::End,
            (None, true) => State::RecvReady,
            (None, false) => State::Pending,
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        let this = self.project();
        if this.fut.is_none() {
            panic!("ScopedStreamSink is closed!");
        }

        let inner = this.data.as_mut().project().inner.set_inner_ctx();
        if inner.recv.is_some() {
            panic!("ScopedStreamSink is not ready to receive!");
        }

        inner.recv = Some(item);
        Ok(())
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        let this = self.project();

        this.data.as_mut().project().inner.set_inner_ctx().closed = true;
        let ret = match this.fut {
            Some(fut) => fut.as_mut().poll(cx),
            None => Poll::Ready(Ok(())),
        };

        if let Poll::Ready(v) = ret {
            *this.fut = None;
            if let Err(e) = v {
                return Poll::Ready(Err(e));
            }
        }

        let inner = this.data.as_mut().project().inner.set_inner_ctx();
        match inner.send.take() {
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok(i)) => Poll::Ready(Ok(Some(i))),
            None if this.fut.is_none() => Poll::Ready(Ok(None)),
            None => Poll::Pending,
        }
    }
}

impl<'scope, 'env: 'scope, SI, RI, E> Stream for StreamPart<'scope, 'env, SI, RI, E> {
    type Item = RI;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.project().ptr.as_mut().project().inner.get_inner();

        if let i @ Some(_) = inner.recv.take() {
            Poll::Ready(i)
        } else if inner.closed {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl<'scope, 'env: 'scope, SI, RI, E> Sink<Result<SI, E>> for SinkPart<'scope, 'env, SI, RI, E> {
    type Error = Infallible;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Self as Sink<Result<SI, E>>>::poll_flush(self, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = self.project().ptr.as_mut().project().inner.get_inner();

        match inner.send {
            Some(_) => Poll::Ready(Ok(())),
            None => Poll::Pending,
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Result<SI, E>) -> Result<(), Self::Error> {
        let inner = self.project().ptr.as_mut().project().inner.get_inner();
        if inner.send.is_some() {
            panic!("poll_ready() is not called first!");
        }

        inner.send = Some(item);
        Ok(())
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let inner = self.project().ptr.as_mut().project().inner.get_inner();

        inner.closed = true;
        if inner.recv.is_none() && inner.send.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl<'scope, 'env: 'scope, SI, RI, E> Sink<SI> for SinkPart<'scope, 'env, SI, RI, E> {
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
