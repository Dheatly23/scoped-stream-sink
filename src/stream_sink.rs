use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::{FusedStream, Stream};
use futures_sink::Sink;
use pin_project_lite::pin_project;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State<T, E> {
    Pending,
    Error(E),
    RecvReady,
    SendReady(T),
    SendRecvReady(T),
    End,
}

impl<T: fmt::Display, E: fmt::Display> fmt::Display for State<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Error(e) => write!(f, "Error({e})"),
            Self::RecvReady => write!(f, "RecvReady"),
            Self::SendReady(t) => write!(f, "SendReady({t})"),
            Self::SendRecvReady(t) => write!(f, "SendRecvReady({t})"),
            Self::End => write!(f, "End"),
        }
    }
}

impl<T, E> From<Result<T, E>> for State<T, E> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(t) => Self::SendReady(t),
            Err(e) => Self::Error(e),
        }
    }
}

impl<T, E> From<Poll<Result<T, E>>> for State<T, E> {
    fn from(value: Poll<Result<T, E>>) -> Self {
        match value {
            Poll::Pending => Self::Pending,
            Poll::Ready(Ok(t)) => Self::SendReady(t),
            Poll::Ready(Err(e)) => Self::Error(e),
        }
    }
}

impl<T, E> From<Result<Option<T>, E>> for State<T, E> {
    fn from(value: Result<Option<T>, E>) -> Self {
        match value {
            Ok(Some(t)) => Self::SendReady(t),
            Ok(None) => Self::End,
            Err(e) => Self::Error(e),
        }
    }
}

impl<T, E> From<Poll<Result<Option<T>, E>>> for State<T, E> {
    fn from(value: Poll<Result<Option<T>, E>>) -> Self {
        match value {
            Poll::Pending => Self::Pending,
            Poll::Ready(Ok(Some(t))) => Self::SendReady(t),
            Poll::Ready(Ok(None)) => Self::End,
            Poll::Ready(Err(e)) => Self::Error(e),
        }
    }
}

impl<T, E> From<Result<(Option<T>, bool), E>> for State<T, E> {
    fn from(value: Result<(Option<T>, bool), E>) -> Self {
        match value {
            Ok((Some(t), true)) => Self::SendReady(t),
            Ok((Some(t), false)) => Self::SendRecvReady(t),
            Ok((None, _)) => Self::End,
            Err(e) => Self::Error(e),
        }
    }
}

impl<T, E> From<Poll<Result<(Option<T>, bool), E>>> for State<T, E> {
    fn from(value: Poll<Result<(Option<T>, bool), E>>) -> Self {
        match value {
            Poll::Pending => Self::Pending,
            Poll::Ready(Ok((Some(t), true))) => Self::SendReady(t),
            Poll::Ready(Ok((Some(t), false))) => Self::SendRecvReady(t),
            Poll::Ready(Ok((None, _))) => Self::End,
            Poll::Ready(Err(e)) => Self::Error(e),
        }
    }
}

impl<T, E> From<Result<(Poll<Option<T>>, Poll<()>), E>> for State<T, E> {
    fn from(value: Result<(Poll<Option<T>>, Poll<()>), E>) -> Self {
        match value {
            Err(e) => Self::Error(e),
            Ok((Poll::Pending, Poll::Pending)) => Self::Pending,
            Ok((Poll::Pending, Poll::Ready(()))) => Self::RecvReady,
            Ok((Poll::Ready(None), _)) => Self::End,
            Ok((Poll::Ready(Some(t)), Poll::Pending)) => Self::SendReady(t),
            Ok((Poll::Ready(Some(t)), Poll::Ready(()))) => Self::SendRecvReady(t),
        }
    }
}

impl<T, E> From<State<T, E>> for Result<(Poll<Option<T>>, Poll<()>), E> {
    fn from(value: State<T, E>) -> Self {
        match value {
            State::Pending => Ok((Poll::Pending, Poll::Pending)),
            State::Error(e) => Err(e),
            State::RecvReady => Ok((Poll::Pending, Poll::Ready(()))),
            State::End => Ok((Poll::Ready(None), Poll::Pending)),
            State::SendReady(t) => Ok((Poll::Ready(Some(t)), Poll::Pending)),
            State::SendRecvReady(t) => Ok((Poll::Ready(Some(t)), Poll::Ready(()))),
        }
    }
}

impl<T, E> State<T, E> {
    #[inline]
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    #[inline]
    pub fn is_recv(&self) -> bool {
        matches!(self, Self::RecvReady | Self::SendRecvReady(_))
    }

    #[inline]
    pub fn is_send(&self) -> bool {
        matches!(self, Self::SendReady(_) | Self::SendRecvReady(_))
    }

    #[inline]
    pub fn is_end(&self) -> bool {
        matches!(self, Self::End)
    }

    pub fn unwrap_content(self) -> Result<Option<T>, E> {
        match self {
            Self::Error(e) => Err(e),
            Self::SendReady(t) | Self::SendRecvReady(t) => Ok(Some(t)),
            _ => Ok(None),
        }
    }
}

pub trait StreamSink<SendItem, RecvItem = SendItem> {
    type Error;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> State<SendItem, Self::Error>;
    fn start_send(self: Pin<&mut Self>, item: RecvItem) -> Result<(), Self::Error>;
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SendItem>, Self::Error>>;
}

impl<T, SendItem, RecvItem> StreamSink<SendItem, RecvItem> for &mut T
where
    T: StreamSink<SendItem, RecvItem> + ?Sized,
{
    type Error = T::Error;

    fn poll_stream_sink(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> State<SendItem, Self::Error> {
        // SAFETY: Repin the borrow immediately.
        unsafe { Pin::new_unchecked(&mut **self.get_unchecked_mut()).poll_stream_sink(cx) }
    }

    fn start_send(self: Pin<&mut Self>, item: RecvItem) -> Result<(), Self::Error> {
        // SAFETY: Repin the borrow immediately.
        unsafe { Pin::new_unchecked(&mut **self.get_unchecked_mut()).start_send(item) }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SendItem>, Self::Error>> {
        // SAFETY: Repin the borrow immediately.
        unsafe { Pin::new_unchecked(&mut **self.get_unchecked_mut()).poll_close(cx) }
    }
}

impl<T, SendItem, RecvItem> StreamSink<SendItem, RecvItem> for Pin<&mut T>
where
    T: StreamSink<SendItem, RecvItem> + ?Sized,
{
    type Error = T::Error;

    fn poll_stream_sink(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> State<SendItem, Self::Error> {
        // SAFETY: Inner is pinned.
        unsafe { self.get_unchecked_mut().as_mut().poll_stream_sink(cx) }
    }

    fn start_send(self: Pin<&mut Self>, item: RecvItem) -> Result<(), Self::Error> {
        // SAFETY: Inner is pinned.
        unsafe { self.get_unchecked_mut().as_mut().start_send(item) }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SendItem>, Self::Error>> {
        // SAFETY: Inner is pinned.
        unsafe { self.get_unchecked_mut().as_mut().poll_close(cx) }
    }
}

pin_project! {
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let zelf = self.project();
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
                    Poll::Ready(None)
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

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let zelf = self.project();
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
                    Poll::Ready(None)
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
                    break;
                }
                State::SendReady(i) => Poll::Ready(Ok(Some(i))),
                State::SendRecvReady(i) => {
                    *zelf.ready = true;
                    *zelf.send = Some(i);
                    break;
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

pub trait StreamSinkExt<SendItem, RecvItem = SendItem>: StreamSink<SendItem, RecvItem> {
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

    fn close<'a>(self: Pin<&'a mut Self>) -> Close<'a, Self, SendItem, RecvItem> {
        Close {
            ptr: Some(self),
            phantom: PhantomData,
        }
    }

    fn ready<'a>(self: Pin<&'a mut Self>) -> Ready<'a, Self, SendItem, RecvItem> {
        Ready {
            ptr: self,
            phantom: PhantomData,
        }
    }

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

pin_project! {
    pub struct StreamSinkPair<S, R> {
        #[pin]
        stream: Option<S>,

        #[pin]
        sink: R,
    }
}

impl<S, R> StreamSinkPair<S, R> {
    #[inline]
    pub fn new(stream: S, sink: R) -> Self {
        Self {
            stream: Some(stream),
            sink,
        }
    }

    #[inline]
    pub fn get_pair(&mut self) -> (Option<&mut S>, &mut R) {
        (self.stream.as_mut(), &mut self.sink)
    }

    #[inline]
    pub fn get_pinned_pair(self: Pin<&mut Self>) -> (Option<Pin<&mut S>>, Pin<&mut R>) {
        let zelf = self.project();
        (zelf.stream.as_pin_mut(), zelf.sink)
    }

    #[inline]
    pub fn unwrap_pair(self) -> (Option<S>, R) {
        (self.stream, self.sink)
    }
}

impl<S, R, Is, Ir, E> StreamSink<Is, Ir> for StreamSinkPair<S, R>
where
    S: Stream<Item = Result<Is, E>>,
    R: Sink<Ir, Error = E>,
{
    type Error = E;

    fn poll_stream_sink(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<Is, Self::Error> {
        let mut zelf = self.as_mut().project();

        let Some(stream) = zelf.stream.as_mut().as_pin_mut() else {
            return match zelf.sink.poll_close(cx) {
                Poll::Pending => State::Pending,
                Poll::Ready(Ok(_)) => State::End,
                Poll::Ready(Err(e)) => State::Error(e),
            };
        };

        let ready = match zelf.sink.poll_ready(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return State::Error(e),
        };
        match (stream.poll_next(cx), ready) {
            (Poll::Pending, true) => State::RecvReady,
            (Poll::Pending, false) => State::Pending,
            (Poll::Ready(Some(Ok(t))), true) => State::SendRecvReady(t),
            (Poll::Ready(Some(Ok(t))), false) => State::SendReady(t),
            (Poll::Ready(None), _) => {
                zelf.stream.set(None);
                self.poll_stream_sink(cx)
            }
            (Poll::Ready(Some(Err(e))), _) => State::Error(e),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Ir) -> Result<(), Self::Error> {
        let zelf = self.project();
        zelf.sink.start_send(item)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Is>, Self::Error>> {
        let mut zelf = self.project();

        let ready = match zelf.sink.poll_close(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        };

        let Some(stream) = zelf.stream.as_mut().as_pin_mut() else {
            return if ready {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            };
        };

        match stream.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(t))) => Poll::Ready(Ok(Some(t))),
            Poll::Ready(None) => {
                zelf.stream.set(None);
                if ready {
                    Poll::Ready(Ok(None))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
        }
    }
}

pin_project! {
    pub struct StreamSinkWrapper<SI, RI, E, T: ?Sized> {
        phantom: PhantomData<(SI, RI, E)>,

        stream_done: bool,

        #[pin]
        inner: T,
    }
}

impl<SI, RI, E, T> StreamSinkWrapper<SI, RI, E, T>
where
    T: Stream<Item = SI> + Sink<RI, Error = E> + Sized,
{
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            stream_done: false,
            inner: value,
            phantom: PhantomData,
        }
    }
}

impl<SI, RI, E, T> StreamSinkWrapper<SI, RI, E, T> {
    #[inline]
    pub fn unwrap_inner(self) -> T {
        self.inner
    }
}

impl<SI, RI, E, T: ?Sized> StreamSinkWrapper<SI, RI, E, T> {
    #[inline]
    pub fn get_inner(&mut self) -> &mut T {
        &mut self.inner
    }

    #[inline]
    pub fn get_pinned_inner(self: Pin<&mut Self>) -> Pin<&mut T> {
        self.project().inner
    }
}

pin_project! {
    pub struct StreamSinkFallibleWrapper<SI, RI, E, T: ?Sized> {
        phantom: PhantomData<(SI, RI, E)>,

        stream_done: bool,

        #[pin]
        inner: T,
    }
}

impl<SI, RI, E, T> StreamSinkFallibleWrapper<SI, RI, E, T>
where
    T: Stream<Item = Result<SI, E>> + Sink<RI, Error = E> + Sized,
{
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            stream_done: false,
            inner: value,
            phantom: PhantomData,
        }
    }
}

impl<SI, RI, E, T> StreamSinkFallibleWrapper<SI, RI, E, T> {
    #[inline]
    pub fn unwrap_inner(self) -> T {
        self.inner
    }
}

impl<SI, RI, E, T: ?Sized> StreamSinkFallibleWrapper<SI, RI, E, T> {
    #[inline]
    pub fn get_inner(&mut self) -> &mut T {
        &mut self.inner
    }

    #[inline]
    pub fn get_pinned_inner(self: Pin<&mut Self>) -> Pin<&mut T> {
        self.project().inner
    }
}

impl<SI, RI, E, T> StreamSink<SI, RI> for StreamSinkWrapper<SI, RI, E, T>
where
    T: Stream<Item = SI> + Sink<RI, Error = E> + ?Sized,
{
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, E> {
        let mut zelf = self.project();

        let stream_res = if *zelf.stream_done {
            Poll::Ready(None)
        } else {
            zelf.inner.as_mut().poll_next(&mut *cx)
        };
        *zelf.stream_done = *zelf.stream_done || matches!(stream_res, Poll::Ready(None));

        if *zelf.stream_done {
            return match zelf.inner.poll_close(cx) {
                Poll::Pending => State::Pending,
                Poll::Ready(Ok(_)) => State::End,
                Poll::Ready(Err(e)) => State::Error(e),
            };
        }

        let ready = match zelf.inner.poll_ready(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return State::Error(e),
        };
        match (stream_res, ready) {
            (Poll::Pending, true) => State::RecvReady,
            (Poll::Pending, false) => State::Pending,
            (Poll::Ready(Some(t)), true) => State::SendRecvReady(t),
            (Poll::Ready(Some(t)), false) => State::SendReady(t),
            (Poll::Ready(None), _) => unreachable!("Stream end should be handled earlier"),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), E> {
        self.project().inner.start_send(item)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Option<SI>, E>> {
        let mut zelf = self.project();

        let ready = match zelf.inner.as_mut().poll_close(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        };

        let stream_res = if *zelf.stream_done {
            Poll::Ready(None)
        } else {
            zelf.inner.poll_next(cx)
        };

        match stream_res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(t)) => Poll::Ready(Ok(Some(t))),
            Poll::Ready(None) => {
                *zelf.stream_done = true;
                if ready {
                    Poll::Ready(Ok(None))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl<SI, RI, E, T> StreamSink<SI, RI> for StreamSinkFallibleWrapper<SI, RI, E, T>
where
    T: Stream<Item = Result<SI, E>> + Sink<RI, Error = E> + ?Sized,
{
    type Error = E;

    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<SI, E> {
        let mut zelf = self.project();

        let stream_res = if *zelf.stream_done {
            Poll::Ready(None)
        } else {
            zelf.inner.as_mut().poll_next(&mut *cx)
        };
        *zelf.stream_done = *zelf.stream_done || matches!(stream_res, Poll::Ready(None));

        if *zelf.stream_done {
            return match zelf.inner.poll_close(cx) {
                Poll::Pending => State::Pending,
                Poll::Ready(Ok(_)) => State::End,
                Poll::Ready(Err(e)) => State::Error(e),
            };
        }

        let ready = match zelf.inner.poll_ready(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return State::Error(e),
        };
        match (stream_res, ready) {
            (Poll::Pending, true) => State::RecvReady,
            (Poll::Pending, false) => State::Pending,
            (Poll::Ready(Some(Ok(t))), true) => State::SendRecvReady(t),
            (Poll::Ready(Some(Ok(t))), false) => State::SendReady(t),
            (Poll::Ready(None), _) => unreachable!("Stream end should be handled earlier"),
            (Poll::Ready(Some(Err(e))), _) => State::Error(e),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: RI) -> Result<(), E> {
        self.project().inner.start_send(item)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Option<SI>, E>> {
        let mut zelf = self.project();

        let ready = match zelf.inner.as_mut().poll_close(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        };

        let stream_res = if *zelf.stream_done {
            Poll::Ready(None)
        } else {
            zelf.inner.poll_next(cx)
        };

        match stream_res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(t))) => Poll::Ready(Ok(Some(t))),
            Poll::Ready(None) => {
                *zelf.stream_done = true;
                if ready {
                    Poll::Ready(Ok(None))
                } else {
                    Poll::Pending
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
        }
    }
}
