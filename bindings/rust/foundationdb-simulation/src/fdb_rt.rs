//! Asynchronous runtime module
//!
//! This module defines the `fdb_spawn` method to run to completion asynchronous tasks containing
//! FoundationDB futures

#![doc = include_str!("../docs/fdb_rt.md")]

use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Weak, atomic::Ordering},
    task::{Context, Wake, Waker},
};

#[cfg(all(test, fdb_simulation_loom))]
use loom::sync::{Mutex, atomic::AtomicBool};
#[cfg(not(all(test, fdb_simulation_loom)))]
use std::sync::{Mutex, atomic::AtomicBool};

pub(crate) type TaskId = usize;
type Task = Pin<Box<dyn Future<Output = ()>>>;
type Queue = Mutex<VecDeque<Notification>>;

thread_local! {
    static EXECUTOR: RefCell<Executor> = RefCell::new(Executor::default());
    static POLLING: Cell<bool> = const { Cell::new(false) };
    static CURRENT_TASK: Cell<Option<TaskId>> = const { Cell::new(None) };
}

#[derive(Default)]
struct Executor {
    tasks: BTreeMap<TaskId, Task>,
    queue: Arc<Queue>,
    next_id: TaskId,
}

enum Notification {
    Ready(Arc<TaskWaker>),
    Cancel(TaskId),
}

/// Wakers may cross threads, but must never own or access an executor's futures.
struct TaskWaker {
    id: TaskId,
    queue: Weak<Queue>,
    queued: AtomicBool,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(queue) = self.queue.upgrade() else {
            return;
        };
        queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_back(Notification::Ready(self));
    }
}

impl Drop for TaskWaker {
    fn drop(&mut self) {
        // A pending future with no remaining wakers cannot make progress. Only
        // its owner may destroy it, even when its last waker dies on another thread.
        if let Some(queue) = self.queue.upgrade() {
            queue
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push_back(Notification::Cancel(self.id));
        }
    }
}

fn next_notification(queue: &Queue) -> Option<Notification> {
    let notification = queue
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .pop_front();
    if let Some(Notification::Ready(waker)) = &notification {
        // Coalesced wakes skip the queue lock. Acquire their publication before
        // polling; a later wake sees false and queues another notification.
        waker.queued.swap(false, Ordering::AcqRel);
    }
    notification
}

struct PollingTaskGuard;

impl Drop for PollingTaskGuard {
    fn drop(&mut self) {
        CURRENT_TASK.with(|current| current.set(None));
    }
}

struct PollingGuard(bool);

impl Drop for PollingGuard {
    fn drop(&mut self) {
        let _ = POLLING.try_with(|polling| polling.set(self.0));
    }
}

/// Poll this thread's queued tasks until no notifications remain.
///
/// Wakes from other threads are delivered here, on the thread that spawned the
/// future. Calling this function on a different thread cannot poll those tasks.
/// Nested calls defer to the outer drain, so a future is never polled reentrantly.
pub fn poll_pending_tasks() {
    // Cancelling a future during thread teardown can invoke the callback hook.
    // Its executor is already being destroyed and must not be entered again.
    let Ok(queue) = EXECUTOR.try_with(|executor| executor.borrow().queue.clone()) else {
        return;
    };
    let Ok(false) = POLLING.try_with(|polling| polling.replace(true)) else {
        return;
    };
    let _guard = PollingGuard(false);
    loop {
        let Some(notification) = next_notification(&queue) else {
            return;
        };
        // Neither the queue lock nor the registry borrow may span user polling
        // or destruction: either can synchronously wake or spawn another task.
        match notification {
            Notification::Ready(notification) => {
                let id = notification.id;
                let future = EXECUTOR.with_borrow_mut(|executor| executor.tasks.remove(&id));
                if let Some(mut future) = future {
                    CURRENT_TASK.with(|current| current.set(Some(id)));
                    let _task_guard = PollingTaskGuard;
                    let waker = Waker::from(notification);
                    let mut cx = Context::from_waker(&waker);
                    if future.as_mut().poll(&mut cx).is_pending() {
                        EXECUTOR.with_borrow_mut(|executor| {
                            executor.tasks.insert(id, future);
                        });
                    }
                }
            }
            Notification::Cancel(id) => {
                let future = EXECUTOR.with_borrow_mut(|executor| executor.tasks.remove(&id));
                drop(future);
            }
        }
    }
}

/// Queue a task without polling it, so its owner can record the cancellation ID.
pub(crate) fn enqueue<F>(future: F) -> TaskId
where
    F: Future<Output = ()> + 'static,
{
    let notification = EXECUTOR.with_borrow_mut(|executor| {
        let id = executor.next_id;
        executor.next_id = id.checked_add(1).expect("simulation task IDs exhausted");
        executor.tasks.insert(id, Box::pin(future));
        Arc::new(TaskWaker {
            id,
            queue: Arc::downgrade(&executor.queue),
            queued: AtomicBool::new(false),
        })
    });
    let id = notification.id;
    notification.wake();
    id
}

/// Synchronously release a suspended task before its borrowed native state.
pub(crate) fn cancel(id: TaskId) {
    CURRENT_TASK.with(|current| {
        assert_ne!(
            current.get(),
            Some(id),
            "the native caller cannot free a workload during its phase poll"
        );
    });
    let _guard = PollingGuard(POLLING.with(|polling| polling.replace(true)));
    let future = EXECUTOR.with_borrow_mut(|executor| executor.tasks.remove(&id));
    drop(future);
}

#[cfg(all(test, not(fdb_simulation_loom)))]
fn fdb_spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    enqueue(future);
    poll_pending_tasks();
}

#[cfg(all(test, not(fdb_simulation_loom)))]
mod tests {
    use super::*;
    use std::{
        future::poll_fn,
        rc::Rc,
        sync::mpsc,
        task::Poll,
        thread::{self, ThreadId},
    };

    struct LocalFuture {
        owner: ThreadId,
        polls: Rc<Cell<usize>>,
        drops: Arc<Mutex<Vec<ThreadId>>>,
        wakers: Arc<Mutex<Vec<Waker>>>,
        ready: Arc<AtomicBool>,
    }

    impl Future for LocalFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            assert_eq!(thread::current().id(), self.owner);
            self.polls.set(self.polls.get() + 1);
            if self.ready.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                self.wakers.lock().unwrap().push(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    impl Drop for LocalFuture {
        fn drop(&mut self) {
            assert_eq!(thread::current().id(), self.owner);
            // FDB cancellation can synchronously invoke this hook, including
            // when the owner thread is destroying its executor registry.
            poll_pending_tasks();
            self.drops.lock().unwrap().push(thread::current().id());
        }
    }

    #[test]
    fn foreign_wakes_only_poll_and_drop_the_future_on_its_owner() {
        let owner = thread::current().id();
        let polls = Rc::new(Cell::new(0));
        let drops = Arc::new(Mutex::new(Vec::new()));
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let ready = Arc::new(AtomicBool::new(false));
        fdb_spawn(LocalFuture {
            owner,
            polls: polls.clone(),
            drops: drops.clone(),
            wakers: wakers.clone(),
            ready: ready.clone(),
        });
        let waker = wakers.lock().unwrap().pop().unwrap();
        let other_waker = waker.clone();
        ready.store(true, Ordering::Release);
        let first = thread::spawn(move || {
            waker.wake_by_ref();
            waker.wake_by_ref();
            waker.wake();
            poll_pending_tasks();
        });
        let second = thread::spawn(move || {
            other_waker.wake();
            poll_pending_tasks();
        });
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(polls.get(), 1);
        assert!(drops.lock().unwrap().is_empty());

        poll_pending_tasks();
        assert_eq!(polls.get(), 2);
        assert_eq!(*drops.lock().unwrap(), [owner]);
    }

    #[test]
    fn dropping_the_last_foreign_waker_cancels_on_the_owner() {
        let owner = thread::current().id();
        let polls = Rc::new(Cell::new(0));
        let drops = Arc::new(Mutex::new(Vec::new()));
        let wakers = Arc::new(Mutex::new(Vec::new()));
        fdb_spawn(LocalFuture {
            owner,
            polls: polls.clone(),
            drops: drops.clone(),
            wakers: wakers.clone(),
            ready: Arc::new(AtomicBool::new(false)),
        });
        let waker = wakers.lock().unwrap().pop().unwrap();
        thread::spawn(move || {
            drop(waker);
            poll_pending_tasks();
        })
        .join()
        .unwrap();
        assert!(drops.lock().unwrap().is_empty());

        poll_pending_tasks();
        assert_eq!(polls.get(), 1);
        assert_eq!(*drops.lock().unwrap(), [owner]);
    }

    #[test]
    fn duplicate_and_reentrant_wakes_are_polled_after_the_current_poll() {
        let polls = Rc::new(Cell::new(0));
        let observed_polls = polls.clone();
        fdb_spawn(poll_fn(move |cx| {
            let count = observed_polls.get() + 1;
            observed_polls.set(count);
            if count == 1 {
                cx.waker().wake_by_ref();
                cx.waker().wake_by_ref();
                poll_pending_tasks();
                assert_eq!(observed_polls.get(), 1);
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }));
        assert_eq!(polls.get(), 2);
    }

    #[test]
    fn spawning_during_a_poll_defers_the_child_until_the_parent_returns() {
        let events = Rc::new(RefCell::new(Vec::new()));
        let observed_events = events.clone();
        fdb_spawn(async move {
            observed_events.borrow_mut().push("parent enter");
            let child_events = observed_events.clone();
            fdb_spawn(async move {
                child_events.borrow_mut().push("child");
            });
            observed_events.borrow_mut().push("parent exit");
        });
        assert_eq!(*events.borrow(), ["parent enter", "parent exit", "child"]);
    }

    #[test]
    fn a_pending_future_without_a_waker_is_released() {
        let held = Rc::new(());
        let future_held = held.clone();
        fdb_spawn(poll_fn(move |_| {
            let _ = &future_held;
            Poll::Pending
        }));
        assert_eq!(Rc::strong_count(&held), 1);
    }

    #[test]
    fn completed_task_wakers_do_not_retain_or_repoll_its_future() {
        let held = Rc::new(());
        let future_held = held.clone();
        let wakers = Arc::new(Mutex::new(Vec::new()));
        let captured_wakers = wakers.clone();
        fdb_spawn(poll_fn(move |cx| {
            let _ = &future_held;
            captured_wakers.lock().unwrap().push(cx.waker().clone());
            Poll::Ready(())
        }));
        assert_eq!(Rc::strong_count(&held), 1);
        let waker = wakers.lock().unwrap().pop().unwrap();
        thread::spawn(move || waker.wake()).join().unwrap();
        poll_pending_tasks();
        assert_eq!(Rc::strong_count(&held), 1);
    }

    #[test]
    fn owner_exit_releases_local_futures_while_foreign_wakers_remain_valid() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let observed_drops = drops.clone();
        let (sender, receiver) = mpsc::channel();
        let owner = thread::spawn(move || {
            let owner = thread::current().id();
            let wakers = Arc::new(Mutex::new(Vec::new()));
            fdb_spawn(LocalFuture {
                owner,
                polls: Rc::new(Cell::new(0)),
                drops: observed_drops,
                wakers: wakers.clone(),
                ready: Arc::new(AtomicBool::new(false)),
            });
            sender.send(wakers.lock().unwrap().pop().unwrap()).unwrap();
            owner
        })
        .join()
        .unwrap();
        let waker = receiver.recv().unwrap();
        assert_eq!(*drops.lock().unwrap(), [owner]);
        waker.wake_by_ref();
        drop(waker);
        poll_pending_tasks();
    }
}

#[cfg(all(test, fdb_simulation_loom))]
mod loom_tests {
    use super::*;
    use std::{future::poll_fn, task::Poll};

    #[test]
    fn coalesced_wake_publishes_before_poll() {
        loom::model(|| {
            let queue = Arc::new(Queue::default());
            let notification = Arc::new(TaskWaker {
                id: 0,
                queue: Arc::downgrade(&queue),
                queued: AtomicBool::new(false),
            });
            let retained_waker = Waker::from(notification.clone());
            retained_waker.wake_by_ref();

            let ready = Arc::new(AtomicBool::new(false));
            let wake_done = Arc::new(AtomicBool::new(false));
            let producer_ready = ready.clone();
            let producer_done = wake_done.clone();
            let producer_waker = retained_waker.clone();
            let producer = loom::thread::spawn(move || {
                producer_ready.store(true, Ordering::Relaxed);
                producer_waker.wake_by_ref();
                producer_done.store(true, Ordering::Relaxed);
            });

            // Schedule the second wake while the first is still queued, without
            // acquiring its writes through a join, channel, or readiness flag.
            while !wake_done.load(Ordering::Relaxed) {
                loom::thread::yield_now();
            }
            let Some(Notification::Ready(waker)) = next_notification(&queue) else {
                panic!("expected the queued wake");
            };
            let waker = Waker::from(waker);
            let mut future = std::pin::pin!(poll_fn(|_| {
                if ready.load(Ordering::Relaxed) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }));
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_ready(),
                "the coalesced wake must publish readiness before polling"
            );
            producer.join().unwrap();

            // std Arc/Waker preserve the real Wake implementation. These
            // retained references prevent final-drop synchronization from
            // supplying the acquire that the queue must provide itself.
            drop(retained_waker);
            drop(notification);
        });
    }
}
