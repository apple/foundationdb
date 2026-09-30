//! Asynchronous runtime module
//!
//! This module defines the `fdb_spawn` method to run to completion asynchronous tasks containing
//! FoundationDB futures

#![doc = include_str!("../docs/fdb_rt.md")]

use std::{
    cell::{RefCell, UnsafeCell},
    future::Future,
    mem,
    pin::Pin,
    sync::Arc,
    task::{Context, RawWaker, RawWakerVTable, Waker},
};

thread_local! {
    static TASKS: RefCell<Vec<Arc<Task>>> = const { RefCell::new(Vec::new()) };
}

struct Task {
    f: UnsafeCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,
}

impl Task {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::clone, Self::wake, Self::wake_by_ref, Self::drop);

    fn poll(self: Arc<Self>) {
        let waker = unsafe { Waker::from_raw(self.clone().into_waker()) };
        let cx = &mut Context::from_waker(&waker);
        let slot = unsafe { &mut *self.f.get() };
        if let Some(mut f) = slot.take() {
            if f.as_mut().poll(cx).is_pending() {
                *slot = Some(f);
            }
        }
    }

    fn from_ptr(ptr: *const ()) -> Arc<Self> {
        unsafe { Arc::from_raw(&*(ptr as *const Self)) }
    }
    fn into_waker(self: Arc<Self>) -> RawWaker {
        RawWaker::new(Arc::into_raw(self) as *const (), &Self::VTABLE)
    }

    fn clone(ptr: *const ()) -> RawWaker {
        let task = Self::from_ptr(ptr);
        mem::forget(task.clone());
        task.into_waker()
    }
    fn wake(ptr: *const ()) {
        let task = Self::from_ptr(ptr);
        TASKS.with_borrow_mut(|tasks| tasks.push(task))
    }
    fn wake_by_ref(ptr: *const ()) {
        let task = Self::from_ptr(ptr);
        TASKS.with_borrow_mut(|tasks| tasks.push(task.clone()));
        mem::forget(task);
    }
    fn drop(ptr: *const ()) {
        drop(Self::from_ptr(ptr))
    }
}

/// Poll all tasks in the pending queue
pub fn poll_pending_tasks() {
    let mut tasks = TASKS.with_borrow_mut(mem::take);
    tasks.drain(..).for_each(Task::poll);
}

/// Spawn an async block and resolve all contained FoundationDB futures
pub(crate) fn fdb_spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    // The simulation executor is single-threaded, but `Waker` requires the task
    // pointer to be usable as `Send + Sync`, so we use `Arc` (not `Rc`) to uphold the
    // `RawWaker` safety contract even though `Task` is neither `Send` nor `Sync`.
    #[allow(clippy::arc_with_non_send_sync)]
    let task = Arc::new(Task {
        f: UnsafeCell::new(Some(Box::pin(future))),
    });
    task.poll();
}
