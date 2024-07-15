use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
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
    thread: ThreadId,
    lock: AtomicU8,

    inner: T,
}

fn panic_expected(expect: u8, value: u8) {
    panic!("Inconsistent internal state! (expected lock state to be {:02X}, got {:02X})\nNote: Your code might use inner value across thread", expect, value);
}

/// Guard for inner context.
pub(crate) struct LocalThreadInnerGuard<'a, T> {
    ptr: NonNull<LocalThread<T>>,
    phantom: PhantomData<&'a LocalThread<T>>,
}

impl<'a, T> Drop for LocalThreadInnerGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: Pointer is valid.
        let p = unsafe { self.ptr.as_ref() };
        if let Err(v) = p.lock.compare_exchange(
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
        unsafe { &self.ptr.as_ref().inner }
    }
}

impl<'a, T> DerefMut for LocalThreadInnerGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: It is locked.
        unsafe { &mut self.ptr.as_mut().inner }
    }
}

/// Guard for outer context.
pub(crate) struct LocalThreadInnerCtxGuard<'a, T> {
    ptr: NonNull<LocalThread<T>>,
    phantom: PhantomData<&'a LocalThread<T>>,
}

impl<'a, T> Drop for LocalThreadInnerCtxGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: Pointer is valid.
        let p = unsafe { self.ptr.as_ref() };
        if let Err(v) =
            p.lock
                .compare_exchange(STATE_ENTER, STATE_OFF, Ordering::Release, Ordering::Relaxed)
        {
            panic_expected(STATE_ENTER, v);
        }
    }
}

impl<'a, T> Deref for LocalThreadInnerCtxGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: It is locked.
        unsafe { &self.ptr.as_ref().inner }
    }
}

impl<'a, T> DerefMut for LocalThreadInnerCtxGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: It is locked.
        unsafe { &mut self.ptr.as_mut().inner }
    }
}

impl<T> LocalThread<T> {
    pub(crate) fn new(inner: T) -> Self {
        Self {
            #[cfg(debug_assertions)]
            thread: current().id(),
            lock: AtomicU8::new(STATE_OFF),

            inner,
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
        if self.thread != current().id() {
            panic!("Called from other thread!");
        }

        LocalThreadInnerGuard {
            ptr: self.into(),
            phantom: PhantomData,
        }
    }

    /// Enters outer context.
    pub(crate) fn set_inner_ctx(&mut self) -> LocalThreadInnerCtxGuard<'_, T> {
        if let Err(v) =
            self.lock
                .compare_exchange(STATE_OFF, STATE_ENTER, Ordering::SeqCst, Ordering::Relaxed)
        {
            panic_expected(STATE_OFF, v);
        }

        #[cfg(debug_assertions)]
        {
            self.thread = current().id();
        }

        LocalThreadInnerCtxGuard {
            ptr: self.into(),
            phantom: PhantomData,
        }
    }
}
