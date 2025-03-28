use std::cell::{Cell, UnsafeCell};
use std::ops::Deref;
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(debug_assertions)]
use std::thread::{current, ThreadId};

const STATE_OFF: u8 = 0;
const STATE_ENTER: u8 = 1;
const STATE_LOCKED: u8 = 2;

/// Protects value within local thread. Call [`Self::get_inner()`] to protect inner access
/// to within thread. Call [`Self::set_inner_ctx()`] to set the context.
pub(crate) struct LocalThread<T> {
    #[cfg(debug_assertions)]
    thread: Cell<ThreadId>,
    lock: AtomicU8,

    inner: UnsafeCell<T>,
}

// SAFETY: Local thread lock is Sync.
unsafe impl<T: Sync> Sync for LocalThread<T> {}

fn panic_expected(expect: u8, value: u8) {
    panic!("Inconsistent internal state! (expected lock state to be {:02X}, got {:02X})\nNote: Your code might use inner value across thread", expect, value);
}

/// Guard for inner context.
pub(crate) struct LocalThreadInnerGuard<'a, T> {
    parent: &'a LocalThread<T>,
}

impl<'a, T> Drop for LocalThreadInnerGuard<'a, T> {
    fn drop(&mut self) {
        if let Err(v) = self.parent.lock.compare_exchange(
            STATE_LOCKED,
            STATE_ENTER,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            panic_expected(STATE_LOCKED, v);
        }
    }
}

impl<'a, T> Deref for LocalThreadInnerGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: It is locked.
        unsafe { &*self.parent.inner.get() }
    }
}

/// Guard for outer context.
pub(crate) struct LocalThreadInnerCtxGuard<'a, T> {
    parent: &'a LocalThread<T>,
}

impl<'a, T> Drop for LocalThreadInnerCtxGuard<'a, T> {
    fn drop(&mut self) {
        if let Err(v) = self.parent.lock.compare_exchange(
            STATE_ENTER,
            STATE_OFF,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            panic_expected(STATE_ENTER, v);
        }
    }
}

impl<'a, T> Deref for LocalThreadInnerCtxGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: It is locked.
        unsafe { &*self.parent.inner.get() }
    }
}

impl<T> LocalThread<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            #[cfg(debug_assertions)]
            thread: Cell::new(current().id()),
            lock: AtomicU8::new(STATE_OFF),

            inner: UnsafeCell::new(value),
        }
    }

    /// Enters inner context.
    pub(crate) fn get_inner(&self) -> LocalThreadInnerGuard<'_, T> {
        if let Err(v) = self.lock.compare_exchange(
            STATE_ENTER,
            STATE_LOCKED,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            panic_expected(STATE_ENTER, v);
        }

        #[cfg(debug_assertions)]
        if self.thread.get() != current().id() {
            panic!("Called from other thread!");
        }

        LocalThreadInnerGuard { parent: self }
    }

    /// Enters outer context.
    pub(crate) fn set_inner_ctx(&self) -> LocalThreadInnerCtxGuard<'_, T> {
        if let Err(v) =
            self.lock
                .compare_exchange(STATE_OFF, STATE_ENTER, Ordering::SeqCst, Ordering::Relaxed)
        {
            panic_expected(STATE_OFF, v);
        }

        #[cfg(debug_assertions)]
        {
            self.thread.set(current().id());
        }

        LocalThreadInnerCtxGuard { parent: self }
    }
}
