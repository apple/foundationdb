Here is an explanation of how `fdb_spawn` communicates with the simulation through the
foundationdb-rs SDK.
```rs
    fdb_spawn(async move {
        ... // code that relies on polling FdbFutures
    }
```

The executor uses a two-phase model: **wakeup** (which queues tasks) and **polling** (which
executes them). This separation enables proper handling of shared futures and prevents
re-entrancy issues.

## Core Data Structures

A thread-local queue holds tasks that have been woken and are waiting to be polled:
```rs
thread_local! {
    static TASKS: RefCell<Vec<Arc<Task>>> = const { RefCell::new(Vec::new()) };
}
```

The `Task` struct wraps a pinned future. The `Option` allows the future to be taken out
during polling and put back if it returns `Pending`:
```rs
struct Task {
    f: UnsafeCell<Option<Pin<Box<dyn Future<Output = ()>>>>>,
}
```

## Spawning a Task

`fdb_spawn` takes in a Rust Future, wraps it in a `Task`, and immediately polls it:
```rs
pub(crate) fn fdb_spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    // Create a Task containing the pinned future wrapped in Option
    let task = Arc::new(Task {
        f: UnsafeCell::new(Some(Box::pin(future))),
    });
    // Immediately poll the task to start execution
    task.poll();
}
```

## Polling a Task

`Task::poll` creates a waker from itself and polls the future:
```rs
impl Task {
    fn poll(self: Arc<Self>) {
        // Create a Waker that references this Task
        let waker = unsafe { Waker::from_raw(self.clone().into_waker()) };
        let cx = &mut Context::from_waker(&waker);
        // Take the future out of the slot
        let slot = unsafe { &mut *self.f.get() };
        if let Some(mut f) = slot.take() {
            // Poll the future
            if f.as_mut().poll(cx).is_pending() {
                // If Pending, put the future back for later polling
                *slot = Some(f);
            }
            // If Ready, the future is dropped (not put back)
        }
    }
}
```

## Waking a Task

The key insight of this architecture: **waking a task does not poll it**. Instead, it
queues the task for later polling:
```rs
impl Task {
    // Called when Waker::wake() is invoked (consumes the waker)
    fn wake(ptr: *const ()) {
        let task = Self::from_ptr(ptr);
        // Just push to the queue - don't poll yet
        TASKS.with_borrow_mut(|tasks| tasks.push(task))
    }

    // Called when Waker::wake_by_ref() is invoked (doesn't consume the waker)
    fn wake_by_ref(ptr: *const ()) {
        let task = Self::from_ptr(ptr);
        TASKS.with_borrow_mut(|tasks| tasks.push(task.clone()));
        mem::forget(task);  // Don't decrement ref count since we cloned
    }
}
```

## Draining the Queue

`poll_pending_tasks` drains the queue and polls all waiting tasks:
```rs
pub fn poll_pending_tasks() {
    let mut tasks = TASKS.with_borrow_mut(mem::take);
    tasks.drain(..).for_each(Task::poll);
}
```

## FdbFuture Polling

`poll()` runs the closure until encountering an `await`. The case we're interested in takes
place when foundationdb-rs issues a call to fdbserver using the C-API through `fdb_sys`.
Calls that return an `FDBFuture*` are wrapped into `FdbFuture` which implements poll:
```rs
pub(crate) struct FdbFuture<T> {
    f: Option<FdbFutureHandle>,
    waker: Option<Arc<AtomicWaker>>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> Future for FdbFuture<T>
where
    T: TryFrom<FdbFutureHandle, Error = FdbError> + Unpin,
{
    type Output = FdbResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<FdbResult<T>> {
        let f = self.f.as_ref().expect("cannot poll after resolve");
        let ready = unsafe { fdb_sys::fdb_future_is_ready(f.as_ptr()) };
        if ready == 0 {
            // Future not ready yet - set up the callback
            let f_ptr = f.as_ptr();
            let mut register = false;
            let waker = self.waker.get_or_insert_with(|| {
                register = true;
                Arc::new(AtomicWaker::new())
            });
            // Register our Task's waker into the AtomicWaker
            // This clones the waker
            waker.register(cx.waker());
            if register {
                // First poll: set up the FDB callback
                let network_waker: Arc<AtomicWaker> = waker.clone();
                let network_waker_ptr = Arc::into_raw(network_waker);
                unsafe {
                    fdb_sys::fdb_future_set_callback(
                        f_ptr,
                        Some(fdb_future_callback),
                        network_waker_ptr as *mut _,
                    );
                }
            }
            Poll::Pending
        } else {
            // Future is ready - extract the result
            Poll::Ready(
                error::eval(unsafe { fdb_sys::fdb_future_get_error(f.as_ptr()) })
                    .and_then(|()| T::try_from(self.f.take().expect("self.f.is_some()"))),
            )
        }
    }
}
```

## Initial Spawn Summary

To summarize what happens when `fdb_spawn` is called:
- `fdb_spawn` creates a `Task` wrapping the pinned future and immediately calls `task.poll()`
- `Task::poll` creates a waker and polls the future
- When an `FdbFuture` is polled, it:
  - Registers our `Waker` into an `AtomicWaker` (cloning it)
  - Sets `fdb_future_callback` as callback with the `AtomicWaker` as argument
  - Returns `Poll::Pending`
- `Task::poll` sees `Pending` and puts the future back in the slot
- `fdb_spawn` returns

```
fdb_spawn
  └─ Task::poll
       └─ FdbFuture::poll
            ├─ waker.register(cx.waker())  // clones our waker into AtomicWaker
            ├─ fdb_future_set_callback     // registers callback with FDB
            └─ returns Pending
       └─ future put back in slot
  └─ returns
```

After this, the future has been handed to the simulation and `fdb_spawn` returns. If your
Workload is valid, execution is returned to the simulation. The fdbserver runs, internally
resolving the future and, when done, notifies `FdbFutureHandle` by calling its callback.

## The Callback and Executor Hook

When FDB resolves a future, it calls `fdb_future_callback`. This is where the two-phase
model comes together:
```rs
pub static CUSTOM_EXECUTOR_HOOK: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

extern "C" fn fdb_future_callback(
    _f: *mut fdb_sys::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    // Phase 1: Wakeup - resolve all wakeup chain
    let network_waker: Arc<AtomicWaker> = unsafe { Arc::from_raw(callback_parameter as *const _) };
    network_waker.wake();  // This calls Task::wake(), which queues the task

    // Phase 2: Polling - poll all queued tasks
    if let Some(poll_pending_tasks) = CUSTOM_EXECUTOR_HOOK.get() {
        poll_pending_tasks();
    }
}
```

The simulation registers `poll_pending_tasks` as the executor hook during workload
initialization (in `register_workload!` or `register_factory!`):
```rs
foundationdb::future::CUSTOM_EXECUTOR_HOOK
    .set(foundationdb_simulation::internals::poll_pending_tasks)
    .unwrap();
```

## Callback Execution Flow

When `fdb_future_callback` is called:
1. `network_waker.wake()` calls the waker stored in `AtomicWaker`
2. This invokes `Task::wake()`, which pushes the task to `TASKS` queue
3. `poll_pending_tasks()` drains the queue and polls all tasks
4. `Task::poll` polls the future, which finds `fdb_future_is_ready` returns non-zero
5. `FdbFuture::poll` returns `Poll::Ready` with the result

```
fdb_future_callback
  ├─ network_waker.wake()
  │    └─ Task::wake() -> pushes task to TASKS queue
  └─ poll_pending_tasks()
       └─ Task::poll
            └─ FdbFuture::poll
                 └─ fdb_future_is_ready == true
                 └─ returns Ready(result)
            └─ future continues executing...
```

## Encountering Another FdbFuture

If the closure encounters another `FdbFuture`, the cycle continues:
- `FdbFuture::poll` is called which:
  - Registers the `Waker` (cloning it)
  - Sets `fdb_future_callback` as callback
  - Returns `Poll::Pending`
- `Task::poll` puts the future back in the slot
- Later, when FDB resolves the new future, `fdb_future_callback` is called again

```
fdb_future_callback (for first future)
  ├─ network_waker.wake() -> queues task
  └─ poll_pending_tasks()
       └─ Task::poll
            └─ FdbFuture::poll (first)
                 └─ returns Ready
            └─ ... more code runs ...
            └─ FdbFuture::poll (second)
                 ├─ waker.register()
                 ├─ fdb_future_set_callback
                 └─ returns Pending
            └─ future put back in slot

... later, fdbserver resolves second future ...

fdb_future_callback (for second future)
  ├─ network_waker.wake() -> queues task
  └─ poll_pending_tasks()
       └─ Task::poll
            └─ ... continues from where it left off ...
```

## Completion

When the task's future finishes without setting a new callback (no more `FdbFuture`s to
await), `Task::poll` sees `Poll::Ready(())` and does not put the future back in the slot.
The future is dropped, and when all references to the `Task` are dropped, the `Task` itself
is freed:

```
fdb_future_callback
  ├─ network_waker.wake() -> queues task
  └─ poll_pending_tasks()
       └─ Task::poll
            └─ future.poll() returns Ready(())
            └─ future is dropped (not put back)
       └─ Task dropped (if no more references)
```
