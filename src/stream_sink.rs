use std::fmt;
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
}

impl<SI, RI, T> StreamSinkExt<SI, RI> for T where T: StreamSink<SI, RI> {}

pin_project! {
    pub struct StreamSinkPair<S, R> {
        #[pin]
        stream: S,

        #[pin]
        sink: R,
    }
}

impl<S, R> StreamSinkPair<S, R> {
    #[inline]
    pub fn new(stream: S, sink: R) -> Self {
        Self { stream, sink }
    }

    #[inline]
    pub fn get_pair(&mut self) -> (&mut S, &mut R) {
        (&mut self.stream, &mut self.sink)
    }

    #[inline]
    pub fn get_pinned_pair(self: Pin<&mut Self>) -> (Pin<&mut S>, Pin<&mut R>) {
        let zelf = self.project();
        (zelf.stream, zelf.sink)
    }

    #[inline]
    pub fn unwrap_pair(self) -> (S, R) {
        (self.stream, self.sink)
    }
}

impl<S, R, Is, Ir, E> StreamSink<Is, Ir> for StreamSinkPair<S, R>
where
    S: Stream<Item = Result<Is, E>> + FusedStream,
    R: Sink<Ir, Error = E>,
{
    type Error = E;

    fn poll_stream_sink(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> State<Is, Self::Error> {
        let zelf = self.as_mut().project();

        if zelf.stream.is_terminated() {
            return match zelf.sink.poll_close(cx) {
                Poll::Pending => State::Pending,
                Poll::Ready(Ok(_)) => State::End,
                Poll::Ready(Err(e)) => State::Error(e),
            };
        }

        let ready = match zelf.sink.poll_ready(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return State::Error(e),
        };
        match (zelf.stream.poll_next(cx), ready) {
            (Poll::Pending, true) => State::RecvReady,
            (Poll::Pending, false) => State::Pending,
            (Poll::Ready(Some(Ok(t))), true) => State::SendRecvReady(t),
            (Poll::Ready(Some(Ok(t))), false) => State::SendReady(t),
            (Poll::Ready(None), _) => self.poll_stream_sink(cx),
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
        let zelf = self.project();

        let ready = match zelf.sink.poll_close(&mut *cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(_)) => true,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        };

        if zelf.stream.is_terminated() {
            return if ready {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            };
        }

        match (zelf.stream.poll_next(cx), ready) {
            (Poll::Pending, _) => Poll::Pending,
            (Poll::Ready(Some(Ok(t))), _) => Poll::Ready(Ok(Some(t))),
            (Poll::Ready(None), true) => Poll::Ready(Ok(None)),
            (Poll::Ready(None), false) => Poll::Pending,
            (Poll::Ready(Some(Err(e))), _) => Poll::Ready(Err(e)),
        }
    }
}
