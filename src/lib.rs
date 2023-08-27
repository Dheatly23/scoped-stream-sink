mod scoped_sink;
mod scoped_stream;
mod scoped_stream_sink;
mod stream_sink;
mod stream_sink_ext;

use std::mem::transmute;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{current, ThreadId};

pub use crate::scoped_sink::*;
pub use crate::scoped_stream::*;
pub use crate::scoped_stream_sink::*;
pub use crate::stream_sink::*;
pub use crate::stream_sink_ext::*;

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
