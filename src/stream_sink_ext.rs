use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

use futures_core::{FusedStream, Stream};
use pin_project_lite::pin_project;

use crate::{State, StreamSink};

pin_project! {
    /// Return type of [`StreamSinkExt::map_send()`].
    pub struct MapSend<T, F, I> {
        #[pin]
        t: T,
        f: F,
        phantom: PhantomData<I>,
    }
}

impl<T, F, SI, RI, I> StreamSink<SI, RI> for MapSend<T, F, I>
where
    T: StreamSink<I, RI>,
    F: FnMut(I) -> SI,
{
    type Error = T::Error;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        let zelf = self.project();
        let f = zelf.f;
        match zelf.t.poll_stream_sink(cx) {
            State::Error(e) => State::Error(e),
            State::Pending => State::Pending,
            State::End => State::End,
            State::RecvReady => State::RecvReady,
            State::SendReady(i) => State::SendReady(f(i)),
            State::SendRecvReady(i) => State::SendRecvReady(f(i)),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        self.project().t.start_send(item)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        let zelf = self.project();
        let f = zelf.f;
        match zelf.t.poll_close(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(Some(i))) => Poll::Ready(Ok(Some(f(i)))),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::map_recv()`].
    pub struct MapRecv<T, F, I> {
        #[pin]
        t: T,
        f: F,
        phantom: PhantomData<I>,
    }
}

impl<T, F, SI, RI, I> StreamSink<SI, RI> for MapRecv<T, F, I>
where
    T: StreamSink<SI, I>,
    F: FnMut(RI) -> I,
{
    type Error = T::Error;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        self.project().t.poll_stream_sink(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        let zelf = self.project();
        let f = zelf.f;
        zelf.t.start_send(f(item))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        self.project().t.poll_close(cx)
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::map_error()`].
    pub struct MapError<T, F> {
        #[pin]
        t: T,
        f: F,
    }
}

impl<T, F, SI, RI, E> StreamSink<SI, RI> for MapError<T, F>
where
    T: StreamSink<SI, RI>,
    F: FnMut(T::Error) -> E,
{
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        let zelf = self.project();
        let f = zelf.f;
        match zelf.t.poll_stream_sink(cx) {
            State::Error(e) => State::Error(f(e)),
            State::Pending => State::Pending,
            State::End => State::End,
            State::RecvReady => State::RecvReady,
            State::SendReady(i) => State::SendReady(i),
            State::SendRecvReady(i) => State::SendRecvReady(i),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        let zelf = self.project();
        let f = zelf.f;
        match zelf.t.start_send(item) {
            Ok(v) => Ok(v),
            Err(e) => Err(f(e)),
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        let zelf = self.project();
        let f = zelf.f;
        match zelf.t.poll_close(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(f(e))),
            Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::error_cast()`].
    pub struct ErrorCast<T, E> {
        #[pin]
        t: T,
        phantom: PhantomData<E>,
    }
}

impl<T, SI, RI, E> StreamSink<SI, RI> for ErrorCast<T, E>
where
    T: StreamSink<SI, RI>,
    T::Error: Into<E>,
{
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        match self.project().t.poll_stream_sink(cx) {
            State::Error(e) => State::Error(e.into()),
            State::Pending => State::Pending,
            State::End => State::End,
            State::RecvReady => State::RecvReady,
            State::SendReady(i) => State::SendReady(i),
            State::SendRecvReady(i) => State::SendRecvReady(i),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        match self.project().t.start_send(item) {
            Ok(v) => Ok(v),
            Err(e) => Err(e.into()),
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        match self.project().t.poll_close(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ChainState {
    Pending,
    Ready,
    UEnd,
    VEnd,
    Done,
}

pin_project! {
    /// Return type of [`StreamSinkExt::chain()`].
    pub struct Chain<U, V, II> {
        #[pin]
        u: U,
        #[pin]
        v: V,

        state: ChainState,
        phantom: PhantomData<II>,
    }
}

impl<U, V, SI, II, RI> StreamSink<SI, RI> for Chain<U, V, II>
where
    U: StreamSink<II, RI>,
    V: StreamSink<SI, II, Error = U::Error>,
{
    type Error = U::Error;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, Self::Error> {
        let mut zelf = self.project();

        loop {
            match *zelf.state {
                ChainState::Pending => match zelf.v.as_mut().poll_stream_sink(&mut *cx) {
                    State::Pending => return State::Pending,
                    State::Error(e) => return State::Error(e),
                    State::End => *zelf.state = ChainState::VEnd,
                    State::SendReady(i) => return State::SendReady(i),
                    State::RecvReady => *zelf.state = ChainState::Ready,
                    State::SendRecvReady(i) => {
                        *zelf.state = ChainState::Ready;
                        return State::SendReady(i);
                    }
                },
                ChainState::Ready => match zelf.u.as_mut().poll_stream_sink(&mut *cx) {
                    State::Pending => return State::Pending,
                    State::Error(e) => return State::Error(e),
                    State::End => *zelf.state = ChainState::UEnd,
                    State::SendReady(i) => match zelf.v.as_mut().start_send(i) {
                        Err(e) => return State::Error(e),
                        Ok(_) => *zelf.state = ChainState::Pending,
                    },
                    State::RecvReady => return State::RecvReady,
                    State::SendRecvReady(i) => match zelf.v.as_mut().start_send(i) {
                        Err(e) => return State::Error(e),
                        Ok(_) => {
                            *zelf.state = ChainState::Pending;
                            return State::RecvReady;
                        }
                    },
                },
                ChainState::VEnd => match zelf.u.as_mut().poll_stream_sink(&mut *cx) {
                    State::Pending => return State::Pending,
                    State::Error(e) => return State::Error(e),
                    State::End => *zelf.state = ChainState::Done,
                    State::SendReady(_) => (),
                    State::RecvReady | State::SendRecvReady(_) => return State::RecvReady,
                },
                ChainState::UEnd => match zelf.v.as_mut().poll_close(&mut *cx) {
                    Poll::Pending => return State::Pending,
                    Poll::Ready(Err(e)) => return State::Error(e),
                    Poll::Ready(Ok(Some(_))) => (),
                    Poll::Ready(Ok(None)) => *zelf.state = ChainState::Done,
                },
                ChainState::Done => return State::End,
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), Self::Error> {
        self.project().u.start_send(item)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SI>, Self::Error>> {
        let mut zelf = self.project();

        loop {
            match *zelf.state {
                ChainState::Pending => match zelf.v.as_mut().poll_stream_sink(&mut *cx) {
                    State::Pending => return Poll::Pending,
                    State::Error(e) => return Poll::Ready(Err(e)),
                    State::End => *zelf.state = ChainState::VEnd,
                    State::SendReady(i) => return Poll::Ready(Ok(Some(i))),
                    State::RecvReady => *zelf.state = ChainState::Ready,
                    State::SendRecvReady(i) => {
                        *zelf.state = ChainState::Ready;
                        return Poll::Ready(Ok(Some(i)));
                    }
                },
                ChainState::Ready => match zelf.u.as_mut().poll_close(&mut *cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(Some(i))) => match zelf.v.as_mut().start_send(i) {
                        Err(e) => return Poll::Ready(Err(e)),
                        Ok(_) => *zelf.state = ChainState::Pending,
                    },
                    Poll::Ready(Ok(None)) => *zelf.state = ChainState::UEnd,
                },
                ChainState::VEnd => match zelf.u.as_mut().poll_close(&mut *cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(Some(_))) => (),
                    Poll::Ready(Ok(None)) => *zelf.state = ChainState::Done,
                },
                ChainState::UEnd => match zelf.v.as_mut().poll_close(&mut *cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(Some(_))) => (),
                    Poll::Ready(Ok(None)) => *zelf.state = ChainState::Done,
                },
                ChainState::Done => return Poll::Ready(Ok(None)),
            }
        }
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::send_one()`].
    pub struct SendOne<'a, T: ?Sized, SI, RI, E> {
        value: Option<(Pin<&'a mut T>, RI)>,
        error: Option<E>,
        phantom: PhantomData<SI>,
    }
}

impl<'a, T, SI, RI, E> Stream for SendOne<'a, T, SI, RI, E>
where
    T: StreamSink<SI, RI, Error = E> + ?Sized,
{
    type Item = Result<SI, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let zelf = self.project();
        if let Some(e) = zelf.error.take() {
            return Poll::Ready(Some(Err(e)));
        }

        let Some((t, _)) = zelf.value else {
            return Poll::Ready(None);
        };
        match t.as_mut().poll_stream_sink(cx) {
            State::Error(e) => {
                *zelf.value = None;
                Poll::Ready(Some(Err(e)))
            }
            State::Pending => Poll::Pending,
            State::SendReady(v) => Poll::Ready(Some(Ok(v))),
            State::SendRecvReady(v) => {
                // SAFETY: zelf.value has value
                let (t, i) = zelf.value.take().unwrap();
                *zelf.error = t.start_send(i).err();
                Poll::Ready(Some(Ok(v)))
            }
            State::RecvReady => {
                // SAFETY: zelf.value has value
                let (t, i) = zelf.value.take().unwrap();
                match t.start_send(i) {
                    Ok(_) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            State::End => {
                *zelf.value = None;
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self { error: Some(_), .. } => (1, Some(1)),
            Self { value: None, .. } => (0, Some(0)),
            _ => (0, None),
        }
    }
}

impl<'a, T, SI, RI, E> FusedStream for SendOne<'a, T, SI, RI, E>
where
    T: StreamSink<SI, RI, Error = E> + ?Sized,
{
    fn is_terminated(&self) -> bool {
        self.value.is_none() && self.error.is_none()
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::send_iter()`].
    pub struct SendIter<'a, T: ?Sized, SI, RI, IT, E> {
        value: Option<(Pin<&'a mut T>, IT)>,
        error: Option<E>,
        phantom: PhantomData<(SI, RI)>,
    }
}

impl<'a, T, SI, RI, IT, E> Stream for SendIter<'a, T, SI, RI, IT, E>
where
    T: StreamSink<SI, RI, Error = E> + ?Sized,
    IT: Iterator<Item = RI>,
{
    type Item = Result<SI, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let zelf = self.as_mut().project();
        if let Some(e) = zelf.error.take() {
            *zelf.value = None;
            return Poll::Ready(Some(Err(e)));
        }

        let Some((t, it)) = zelf.value else {
            return Poll::Ready(None);
        };
        match t.as_mut().poll_stream_sink(cx) {
            State::Error(e) => {
                *zelf.value = None;
                Poll::Ready(Some(Err(e)))
            }
            State::Pending => Poll::Pending,
            State::SendReady(v) => Poll::Ready(Some(Ok(v))),
            State::SendRecvReady(v) => {
                *zelf.error = if let Some(i) = it.next() {
                    t.as_mut().start_send(i).err()
                } else {
                    *zelf.value = None;
                    None
                };
                Poll::Ready(Some(Ok(v)))
            }
            State::RecvReady => {
                let r = if let Some(i) = it.next() {
                    t.as_mut().start_send(i).err()
                } else {
                    *zelf.value = None;
                    None
                };
                if let Some(e) = r {
                    *zelf.value = None;
                    Poll::Ready(Some(Err(e)))
                } else {
                    self.poll_next(cx)
                }
            }
            State::End => {
                *zelf.value = None;
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self { error: Some(_), .. } => (1, Some(1)),
            Self { value: None, .. } => (0, Some(0)),
            _ => (0, None),
        }
    }
}

impl<'a, T, SI, RI, IT, E> FusedStream for SendIter<'a, T, SI, RI, IT, E>
where
    T: StreamSink<SI, RI, Error = E> + ?Sized,
    IT: Iterator<Item = RI>,
{
    fn is_terminated(&self) -> bool {
        self.value.is_none() && self.error.is_none()
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::send_try_iter()`].
    pub struct SendTryIter<'a, T: ?Sized, SI, RI, IT, E> {
        value: Option<(Pin<&'a mut T>, IT)>,
        error: Option<E>,
        phantom: PhantomData<(SI, RI)>,
    }
}

impl<'a, T, SI, RI, IT, E> Stream for SendTryIter<'a, T, SI, RI, IT, E>
where
    T: StreamSink<SI, RI, Error = E> + ?Sized,
    IT: Iterator<Item = Result<RI, E>>,
{
    type Item = Result<SI, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let zelf = self.as_mut().project();
        if let Some(e) = zelf.error.take() {
            *zelf.value = None;
            return Poll::Ready(Some(Err(e)));
        }

        let Some((t, it)) = zelf.value else {
            return Poll::Ready(None);
        };
        match t.as_mut().poll_stream_sink(cx) {
            State::Error(e) => Poll::Ready(Some(Err(e))),
            State::Pending => Poll::Pending,
            State::SendReady(v) => Poll::Ready(Some(Ok(v))),
            State::SendRecvReady(v) => {
                *zelf.error = match it.next() {
                    Some(Ok(i)) => t.as_mut().start_send(i).err(),
                    Some(Err(e)) => Some(e),
                    None => {
                        *zelf.value = None;
                        None
                    }
                };
                Poll::Ready(Some(Ok(v)))
            }
            State::RecvReady => {
                let r = match it.next() {
                    Some(Ok(i)) => t.as_mut().start_send(i).err(),
                    Some(Err(e)) => Some(e),
                    None => {
                        *zelf.value = None;
                        None
                    }
                };
                if let Some(e) = r {
                    *zelf.value = None;
                    Poll::Ready(Some(Err(e)))
                } else {
                    self.poll_next(cx)
                }
            }
            State::End => {
                *zelf.value = None;
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self { error: Some(_), .. } => (1, Some(1)),
            Self { value: None, .. } => (0, Some(0)),
            _ => (0, None),
        }
    }
}

impl<'a, T, SI, RI, IT, E> FusedStream for SendTryIter<'a, T, SI, RI, IT, E>
where
    T: StreamSink<SI, RI, Error = E> + ?Sized,
    IT: Iterator<Item = Result<RI, E>>,
{
    fn is_terminated(&self) -> bool {
        self.value.is_none() && self.error.is_none()
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::close()`].
    pub struct Close<'a, T: ?Sized, SI, RI> {
        ptr: Option<Pin<&'a mut T>>,
        phantom: PhantomData<(SI, RI)>,
    }
}

impl<'a, T, SI, RI> Stream for Close<'a, T, SI, RI>
where
    T: StreamSink<SI, RI> + ?Sized,
{
    type Item = Result<SI, T::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let ptr = self.project().ptr;
        let Some(t) = ptr else {
            return Poll::Ready(None);
        };

        let r = match t.as_mut().poll_close(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(v) => v.transpose(),
        };
        if matches!(r, None | Some(Err(_))) {
            *ptr = None;
        }
        Poll::Ready(r)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self { ptr: None, .. } => (0, Some(0)),
            _ => (0, None),
        }
    }
}

impl<'a, T, SI, RI> FusedStream for Close<'a, T, SI, RI>
where
    T: StreamSink<SI, RI> + ?Sized,
{
    fn is_terminated(&self) -> bool {
        self.ptr.is_none()
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::ready()`].
    pub struct Ready<'a, T: ?Sized, SI, RI> {
        ptr: Pin<&'a mut T>,
        phantom: PhantomData<(SI, RI)>,
    }
}

impl<'a, T, SI, RI> Future for Ready<'a, T, SI, RI>
where
    T: ?Sized + StreamSink<SI, RI>,
{
    type Output = Result<(Option<SI>, bool), T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let zelf = self.project();

        match zelf.ptr.as_mut().poll_stream_sink(cx) {
            State::Pending => Poll::Pending,
            State::Error(e) => Poll::Ready(Err(e)),
            State::End => Poll::Ready(Ok((None, false))),
            State::RecvReady => Poll::Ready(Ok((None, true))),
            State::SendReady(i) => Poll::Ready(Ok((Some(i), false))),
            State::SendRecvReady(i) => Poll::Ready(Ok((Some(i), true))),
        }
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::try_send_one()`].
    pub struct TrySendOne<'a, T: ?Sized, SI, F> {
        ptr: Pin<&'a mut T>,
        f: Option<F>,
        phantom: PhantomData<SI>,
    }
}

impl<'a, T, SI, RI, F> Future for TrySendOne<'a, T, SI, F>
where
    F: FnOnce() -> RI,
    T: ?Sized + StreamSink<SI, RI>,
{
    type Output = Result<Option<SI>, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let zelf = self.project();

        let mut f = || {
            zelf.f
                .take()
                .expect("Future should not be polled after completion")()
        };
        match zelf.ptr.as_mut().poll_stream_sink(cx) {
            State::Pending => Poll::Pending,
            State::Error(e) => Poll::Ready(Err(e)),
            State::End => Poll::Ready(Ok(None)),
            State::SendReady(i) => Poll::Ready(Ok(Some(i))),
            State::RecvReady => match zelf.ptr.as_mut().start_send(f()) {
                Ok(_) => Poll::Ready(Ok(None)),
                Err(e) => Poll::Ready(Err(e)),
            },
            State::SendRecvReady(i) => match zelf.ptr.as_mut().start_send(f()) {
                Ok(_) => Poll::Ready(Ok(Some(i))),
                Err(e) => Poll::Ready(Err(e)),
            },
        }
    }
}

pin_project! {
    /// Return type of [`StreamSinkExt::try_send_future()`].
    pub struct TrySendFuture<'a, T: ?Sized, SI, F> {
        ptr: Pin<&'a mut T>,
        #[pin]
        fut: F,
        send: Option<SI>,
        ready: bool,
    }
}

impl<'a, T, SI, RI, F> Future for TrySendFuture<'a, T, SI, F>
where
    T: ?Sized + StreamSink<SI, RI>,
    F: Future<Output = Result<RI, T::Error>>,
{
    type Output = Result<Option<SI>, T::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let zelf = self.project();

        while !*zelf.ready {
            return match zelf.ptr.as_mut().poll_stream_sink(&mut *cx) {
                State::Pending => Poll::Pending,
                State::Error(e) => Poll::Ready(Err(e)),
                State::End => Poll::Ready(Ok(None)),
                State::RecvReady => {
                    *zelf.ready = true;
                    continue;
                }
                State::SendReady(i) => Poll::Ready(Ok(Some(i))),
                State::SendRecvReady(i) => {
                    *zelf.ready = true;
                    *zelf.send = Some(i);
                    continue;
                }
            };
        }

        match zelf.fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(r)) => match zelf.ptr.as_mut().start_send(r) {
                Err(e) => Poll::Ready(Err(e)),
                Ok(_) => Poll::Ready(Ok(zelf.send.take())),
            },
        }
    }
}

/// Extension trait for [`StreamSink`]. Contains helper methods for using [`StreamSink`].
pub trait StreamSinkExt<SendItem, RecvItem = SendItem>: StreamSink<SendItem, RecvItem> {
    /// Maps the `SendItem`.
    fn map_send<F, I>(self, f: F) -> MapSend<Self, F, SendItem>
    where
        Self: Sized,
        F: FnMut(SendItem) -> I,
    {
        MapSend {
            t: self,
            f,
            phantom: PhantomData,
        }
    }

    /// Maps the `RecvItem`.
    fn map_recv<F, I>(self, f: F) -> MapRecv<Self, F, I>
    where
        Self: Sized,
        F: FnMut(I) -> RecvItem,
    {
        MapRecv {
            t: self,
            f,
            phantom: PhantomData,
        }
    }

    /// Maps the error type.
    fn map_error<F, E>(self, f: F) -> MapError<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Error) -> E,
    {
        MapError { t: self, f }
    }

    /// Cast the error type.
    fn error_cast<E>(self) -> ErrorCast<Self, E>
    where
        Self: Sized,
        Self::Error: Into<E>,
    {
        ErrorCast {
            t: self,
            phantom: PhantomData,
        }
    }

    /// Chain two [`StreamSink`].
    ///
    /// If either of the [`StreamSink`] ends, the other one will try to be closed.
    /// Some data may gets lost because of that.
    fn chain<Other, Item>(self, other: Other) -> Chain<Self, Other, SendItem>
    where
        Self: Sized,
        Other: StreamSink<Item, SendItem, Error = Self::Error>,
    {
        Chain {
            u: self,
            v: other,
            state: ChainState::Pending,
            phantom: PhantomData,
        }
    }

    /// Send one item.
    ///
    /// The resulting [`Stream`] may be dropped at anytime, with only consequence is loss of item.
    /// To not lose the item, use [`try_send_one`](Self::try_send_one).
    fn send_one<'a>(
        self: Pin<&'a mut Self>,
        item: RecvItem,
    ) -> SendOne<'a, Self, SendItem, RecvItem, Self::Error> {
        SendOne {
            value: Some((self, item)),
            error: None,
            phantom: PhantomData,
        }
    }

    /// Send items from an [`IntoIterator`].
    ///
    /// The resulting [`Stream`] may be dropped at anytime, with no loss of item.
    fn send_iter<'a, I: IntoIterator<Item = RecvItem>>(
        self: Pin<&'a mut Self>,
        iter: I,
    ) -> SendIter<'a, Self, SendItem, RecvItem, I::IntoIter, Self::Error> {
        SendIter {
            value: Some((self, iter.into_iter())),
            error: None,
            phantom: PhantomData,
        }
    }

    /// Send items from a fallible [`IntoIterator`].
    ///
    /// The resulting [`Stream`] may be dropped at anytime, with no loss of item.
    fn send_try_iter<'a, I: IntoIterator<Item = Result<RecvItem, Self::Error>>>(
        self: Pin<&'a mut Self>,
        iter: I,
    ) -> SendTryIter<'a, Self, SendItem, RecvItem, I::IntoIter, Self::Error> {
        SendTryIter {
            value: Some((self, iter.into_iter())),
            error: None,
            phantom: PhantomData,
        }
    }

    /// Closes the [`StreamSink`].
    ///
    /// You must handle all items that came out of the resulting [`Stream`].
    fn close<'a>(self: Pin<&'a mut Self>) -> Close<'a, Self, SendItem, RecvItem> {
        Close {
            ptr: Some(self),
            phantom: PhantomData,
        }
    }

    /// Polls until it's ready. It is safe drop the [`Future`] before it's ready (cancel-safety).
    ///
    /// Possible return value (after awaited):
    /// - `Err(error)` : Error happened.
    /// - `Ok((None, false))` : [`StreamSink`] is closed.
    /// - `Ok((Some(item), false))` : An item is sent.
    /// - `Ok((None, true))` : Ready to receive item.
    /// - `Ok((Some(item), true))` : Item is sent and it's ready to receive another item.
    fn ready<'a>(self: Pin<&'a mut Self>) -> Ready<'a, Self, SendItem, RecvItem> {
        Ready {
            ptr: self,
            phantom: PhantomData,
        }
    }

    /// Try to send an item.
    /// It is safe to drop the [`Future`] before it's ready.
    fn try_send_one<'a, F: FnOnce() -> RecvItem>(
        self: Pin<&'a mut Self>,
        f: F,
    ) -> TrySendOne<'a, Self, SendItem, F> {
        TrySendOne {
            ptr: self,
            f: Some(f),
            phantom: PhantomData,
        }
    }

    /// Try to send an item using [`Future`].
    /// It is cancen-safe if and only if the inner [`Future`] is also cancel-safe.
    fn try_send_future<'a, F: Future<Output = Result<RecvItem, Self::Error>>>(
        self: Pin<&'a mut Self>,
        fut: F,
    ) -> TrySendFuture<'a, Self, SendItem, F> {
        TrySendFuture {
            ptr: self,
            fut,
            send: None,
            ready: false,
        }
    }
}

impl<SI, RI, T> StreamSinkExt<SI, RI> for T where T: StreamSink<SI, RI> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ScopedStreamSink;

    use std::pin::pin;
    use std::prelude::rust_2021::*;
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

    #[tokio::test]
    async fn test_ready_simple() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|_, _| Box::pin(async { Ok(()) }));

        test_helper(async move {
            let v = pin!(v);
            assert_eq!(v.ready().await?, (None, false));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_ready_send() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|_, mut sink| {
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
            let mut v = pin!(v);
            for i in 0..10 {
                assert_eq!(v.as_mut().ready().await?, (Some(i), true));
                for _ in 0..i {
                    assert_eq!(v.as_mut().ready().await?, (None, true));
                }
            }
            assert_eq!(v.as_mut().ready().await?, (None, false));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_close_send() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|_, mut sink| {
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
            let mut v = pin!(v);
            let mut s = v.as_mut().close();
            for i in 0..10 {
                assert_eq!(s.next().await.transpose()?, Some(i));
            }
            assert_eq!(s.next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_transform() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|mut stream, mut sink| {
            Box::pin(async move {
                while let Some(v) = stream.next().await {
                    sink.send(v * 2).await?;
                }

                Ok(())
            })
        });

        test_helper(async move {
            let mut v = pin!(v);
            for i in 0..10 {
                let mut s = v.as_mut().send_one(i);
                assert_eq!(s.next().await.transpose()?, None);
                assert_eq!(v.as_mut().ready().await?.0, Some(i * 2));
            }
            assert_eq!(v.close().next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_transform_iter() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|mut stream, mut sink| {
            Box::pin(async move {
                while let Some(v) = stream.next().await {
                    sink.send(v * 2).await?;
                }

                Ok(())
            })
        });

        test_helper(async move {
            let mut v = pin!(v);
            let mut s = v.as_mut().send_iter(0..10);
            for i in 0..10 {
                assert_eq!(s.next().await.transpose()?, Some(i * 2));
            }
            assert_eq!(s.next().await.transpose()?, None);
            assert_eq!(v.close().next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_map_send() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|mut stream, mut sink| {
            Box::pin(async move {
                while let Some(v) = stream.next().await {
                    sink.send(v * 2).await?;
                }

                Ok(())
            })
        })
        .map_send(|v| v + 10);

        test_helper(async move {
            let mut v = pin!(v);
            for i in 0..10 {
                let mut s = v.as_mut().send_one(i);
                assert_eq!(s.next().await.transpose()?, None);
                assert_eq!(v.as_mut().ready().await?.0, Some(i * 2 + 10));
            }
            assert_eq!(v.close().next().await.transpose()?, None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_map_recv() -> AnyResult<()> {
        let v = <ScopedStreamSink<usize, usize, AnyError>>::new(|mut stream, mut sink| {
            Box::pin(async move {
                while let Some(v) = stream.next().await {
                    sink.send(v * 2).await?;
                }

                Ok(())
            })
        })
        .map_recv(|v| v + 10);

        test_helper(async move {
            let mut v = pin!(v);
            for i in 0..10 {
                let mut s = v.as_mut().send_one(i);
                assert_eq!(s.next().await.transpose()?, None);
                assert_eq!(v.as_mut().ready().await?.0, Some((i + 10) * 2));
            }
            assert_eq!(v.close().next().await.transpose()?, None);

            Ok(())
        })
        .await
    }
}
