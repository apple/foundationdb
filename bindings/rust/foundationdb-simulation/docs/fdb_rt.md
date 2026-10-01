The simulator executor runs Rust futures on the thread that creates them. The futures
may contain `Rc`, thread-affine simulator state, and other values that are not `Send`.
Their wakers, however, are ordinary Rust `Waker`s and may safely be cloned, woken, or
dropped on any thread.

## Ownership and scheduling

Each thread has its own executor registry. Only this registry owns the pinned futures;
neither a waker nor the shared notification queue contains a future or a pointer to one.
Task IDs are never reused within an executor.

A waker contains a task ID, an atomic queued flag, and a weak reference to its owner's
notification queue. The standard library's `Wake` implementation constructs the `Waker`
from this `Send + Sync` state without a custom unsafe `RawWaker` implementation.

Waking adds a notification to the owner's queue. Duplicate wakes before the next poll
are coalesced. The queue holds the notification state alive until it is consumed, so
even a consuming `Waker::wake()` schedules a poll before releasing the final waker.
Waking never polls the future directly.

Before polling, the owner acquires the queued flag while clearing it. A coalesced
wake does not take the queue lock, so this acquire makes its producer's writes
visible to the poll even when no second notification was enqueued.

`fdb_spawn` inserts a future into the current thread's registry, queues its first poll,
and calls `poll_pending_tasks`. The drain removes each future from the registry while
polling it and restores it only if it returns `Pending`. No queue lock or registry borrow
spans user polling or destruction. A thread-local guard defers nested drains, including
synchronous FoundationDB callbacks, until the current poll has returned. Tasks spawned
during a poll also wait for that poll to finish.

The drain continues until the queue is empty. A wake during a poll schedules another poll
after the current one, including when the wake comes from another thread. As with other
executors, a future must not wake itself indefinitely without making progress.

## FoundationDB callbacks

The workload registration macros install `poll_pending_tasks` in
`foundationdb::future::CUSTOM_EXECUTOR_HOOK`. A pending `FdbFuture` registers the task's
waker in its callback state. When the C future completes, its callback first wakes that
state and then invokes the executor hook:

```text
FDB future callback on the simulator thread
  -> wake task -> enqueue notification
  -> CUSTOM_EXECUTOR_HOOK -> poll_pending_tasks
      -> remove future from owner registry
      -> poll future
          -> Ready: destroy it on its owner
          -> Pending: restore it to owner registry
```

The simulator's normal callbacks run on the owner thread and therefore resume work
immediately. A wake from another thread is safe but does not create a simulator event
or move execution to that thread: the owner must next call `poll_pending_tasks` to make
progress. Calling the hook on a foreign thread drains only that thread's own executor.
This executor is not a general-purpose cross-thread event loop.

## Completion and cancellation

A completed future is destroyed on its owner even if external wakers still exist. Later
wakes for its task ID are harmless. If a pending future loses its final waker without
being woken, the waker's destructor queues cancellation. The owner drops that future on
its next drain. This preserves owner-thread destruction when the last waker is dropped
by another thread and avoids permanently retaining an unreachable pending task.

If the owner does not drain again, cancellation remains deferred until its executor is
destroyed at thread exit. Exiting the owner thread drops all remaining futures locally;
outstanding wakers contain only weak queue references and remain safe to use afterward.
Cancellation callbacks that reenter the executor during this teardown return immediately.
The executor has no independent timer or background thread to drive foreign wakes.
