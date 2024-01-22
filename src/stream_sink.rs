use core::fmt;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};

#[cfg(feature = "either")]
use either::{for_both, Either};
use futures_core::Stream;
use futures_sink::Sink;
use pin_project_lite::pin_project;

/// State enum for [`StreamSink`].
///
/// Due to it's complexity, the state of [`StreamSink`] is represented by this enum.
/// Should be equivalent to `Poll<Result<(Option<T>, bool), E>>`.
///
/// Implements many useful [`From`] traits to ease conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum State<T, E> {
    /// Is pending.
    Pending,

    /// Is erroring.
    Error(E),

    /// Ready to receive item.
    RecvReady,

    /// Has item to send.
    SendReady(T),

    /// Combines [`RecvReady`](Self::RecvReady) and [`SendReady`](Self::SendReady).
    SendRecvReady(T),

    /// Has no more data to send and cannot receive data anymore.
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
    /// Returns `true` if state is pending.
    #[inline]
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    /// Returns `true` if state is ready to receive item.
    #[inline]
    pub fn is_recv(&self) -> bool {
        matches!(self, Self::RecvReady | Self::SendRecvReady(_))
    }

    /// Returns `true` if state has an item to send.
    #[inline]
    pub fn is_send(&self) -> bool {
        matches!(self, Self::SendReady(_) | Self::SendRecvReady(_))
    }

    /// Returns `true` if state is ended
    #[inline]
    pub fn is_end(&self) -> bool {
        matches!(self, Self::End)
    }

    /// Unwraps it's content (if any).
    pub fn unwrap_content(self) -> Result<Option<T>, E> {
        match self {
            Self::Error(e) => Err(e),
            Self::SendReady(t) | Self::SendRecvReady(t) => Ok(Some(t)),
            _ => Ok(None),
        }
    }
}

/// Combines [`Stream`] and [`Sink`] into one trait.
///
/// Unlike each of it's part, [`StreamSink`] is capable of simultaneously poll
/// for sending and receiving. This allows for workflow such as packet processor to be done
/// without making separate pipe for sending and receiving.
pub trait StreamSink<SendItem, RecvItem = SendItem> {
    /// The error type that it may return.
    type Error;

    /// Poll the [`StreamSink`].
    ///
    /// Due to complexity of it's state, it does not return the usual [`Poll`] enum
    /// like other poll-like methods. See [`State`] for more info.
    fn poll_stream_sink(self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> State<SendItem, Self::Error>;

    /// Starts sending item into [`StreamSink`].
    ///
    /// **WARNING: May panics if [`StreamSink`] is not ready to receive.**
    fn start_send(self: Pin<&mut Self>, item: RecvItem) -> Result<(), Self::Error>;

    /// Close the [`StreamSink`] from outside. Implementation must be idempotent.
    ///
    /// Meaning of value returned:
    /// - `Poll::Pending` : Pending to be closed, poll again in the future.
    /// - `Poll::Ready(Err(error))` : Error happened.
    /// - `Poll::Ready(Ok(Some(item)))` : [`StreamSink`] wants to send more item as cleanup.
    /// - `Poll::Ready(Ok(None))` : [`StreamSink`] is finalized.
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

#[cfg(feature = "either")]
impl<L, R, SendItem, RecvItem, E> StreamSink<SendItem, RecvItem> for Either<L, R>
where
    L: StreamSink<SendItem, RecvItem, Error = E>,
    R: StreamSink<SendItem, RecvItem, Error = E>,
{
    type Error = E;

    fn poll_stream_sink(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> State<SendItem, Self::Error> {
        for_both!(self.as_pin_mut(), v => v.poll_stream_sink(cx))
    }

    fn start_send(self: Pin<&mut Self>, item: RecvItem) -> Result<(), Self::Error> {
        for_both!(self.as_pin_mut(), v => v.start_send(item))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<SendItem>, Self::Error>> {
        for_both!(self.as_pin_mut(), v => v.poll_close(cx))
    }
}

pin_project! {
    /// Wraps a pair of [`Stream`] and [`Sink`].
    /// Implements [`StreamSink`].
    pub struct StreamSinkPair<S, R> {
        #[pin]
        stream: Option<S>,

        #[pin]
        sink: R,
    }
}

impl<S, R> StreamSinkPair<S, R> {
    /// Creates new [`StreamSinkPair`].
    #[inline]
    pub fn new(stream: S, sink: R) -> Self {
        Self {
            stream: Some(stream),
            sink,
        }
    }

    /// Gets reference for inner data.
    #[inline]
    pub fn get_pair(&mut self) -> (Option<&mut S>, &mut R) {
        (self.stream.as_mut(), &mut self.sink)
    }

    /// Gets pinned reference for inner data.
    #[inline]
    pub fn get_pinned_pair(self: Pin<&mut Self>) -> (Option<Pin<&mut S>>, Pin<&mut R>) {
        let zelf = self.project();
        (zelf.stream.as_pin_mut(), zelf.sink)
    }

    /// Unwraps self into it's content.
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
    /// Wraps a type that implements both [`Stream`] and [`Sink`].
    /// Although such type is not very usable, both implementation is combined here
    /// into [`StreamSink`].
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
    /// Creates a new [`StreamSinkWrapper`].
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
    /// Wraps a type that implements both [`Stream`] and [`Sink`]
    /// where the [`Stream`] half returns a [`Result`].
    /// Although such type is not very usable, both implementation is combined here
    /// into [`StreamSink`].
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
    /// Creates a new [`StreamSinkFallibleWrapper`].
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
