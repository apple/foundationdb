# Subsystem 1: Flow Runtime

**[Diagrams](diagram_01_flow_runtime.md)**

**Location:** [`flow/`](https://github.com/apple/foundationdb/tree/main/flow)
**Size:** ~31K implementation + headers  
**Role:** The custom async programming framework that is the execution model for all FoundationDB code.

---

## Overview

Flow is not a library you can opt out of -- it **is** the execution model. Every server component, every client operation, every test runs as a collection of cooperatively-scheduled coroutines (called "actors") on a single-threaded event loop. There are no application-level threads; all concurrency comes from Future resolution and callback dispatch.

---

## Key Data Structures

### SAV (SingleAssignmentVar) -- [`flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h)`:730-914`

The core primitive underlying all async communication. A reference-counted cell that holds either a value or an error, set exactly once.

```
struct SAV<T> : Callback<T>, FastAllocated<SAV<T>> {
    int promises;       // Promise reference count
    int futures;        // Future reference count
    aligned_storage<T>  value_storage;
    Error error_state;  // UNSET_ERROR_CODE (-3), SET_ERROR_CODE, NEVER_ERROR_CODE, or actual error
};
```

**Key methods:**
- `send(U&& value)` -- sets value, fires all callbacks in the chain
- `sendError(Error)` -- sets error, fires error callbacks
- `isSet()` / `canBeSet()` / `isError()` -- state queries
- `addCallbackAndDelFutureRef()` -- registers a continuation
- `delPromiseRef()` / `delFutureRef()` -- reference counting; destroys when both reach 0

**Callback chain:** SAV inherits from `Callback<T>`, which forms a **doubly-linked circular list**. Multiple waiters can register on one SAV. When the SAV fires, it walks the chain calling `fire(value)` or `error(err)` on each callback, then unlinks them.

### Future<T> -- [`flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h)`:973-1068`

Read-only handle to a SAV. Cannot set the value, only observe it.

```
Future<T> f;           // default: points to nothing
Future<T> f(value);    // immediately ready
Future<T> f(Never());  // never completes
Future<T> f(error);    // immediately errored
```

**Key methods:**
- `get()` -- returns value or throws error (must be ready)
- `isReady()` -- true if set (value or error)
- `canGet()` -- ready and not error
- `cancel()` -- calls `sav->cancel()`, used by actor system
- `addCallbackAndClear(cb)` -- transfers ownership of callback to SAV

### Promise<T> -- [`flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h)`:1091-1157`

Write-only handle to a SAV. Delivers the value exactly once.

**Key methods:**
- `send(value)` -- delivers value
- `sendError(error)` -- delivers error
- `getFuture()` -- returns the associated Future<T>
- `isSet()` / `canBeSet()` -- state queries

**Lifecycle:** Creating a Promise creates a SAV with `promises=1, futures=0`. Calling `getFuture()` increments `futures`. When the Promise is destroyed without sending, it sends `broken_promise()` error.

### FutureStream<T> / PromiseStream<T> -- [`flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h)`:1159-1456`

Multi-value async channels for request/reply patterns.

**NotifiedQueue<T>** (backing structure):
- `std::queue<T, Deque<T>> queue` -- buffered values
- Single callback chain (not doubly-linked)
- `send(value)` -- if callback waiting, delivers directly; else queues
- `pop()` -- dequeues or throws error

**PromiseStream<T>** key pattern -- self-addressed envelope:
```cpp
struct MyRequest {
    int param;
    ReplyPromise<MyReply> reply;  // sender embeds a reply channel
};
PromiseStream<MyRequest> stream;
stream.send(MyRequest{42, ReplyPromise<MyReply>()});
// receiver calls req.reply.send(result)
```

No explicit backpressure -- values accumulate in the queue. Implicit pressure: if the consumer doesn't drain, memory grows.

### Error / ErrorOr<T> -- [`Error.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Error.h)`:44-81`, [`flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h)`:124-317`

**Error class:**
- `uint16_t error_code` + `uint16_t flags` (includes `FLAG_INJECTED_FAULT`)
- Propagates through Futures: `SAV::sendError()` stores the error; `Future::get()` throws it
- Defined via macros in `error_definitions.h`: `ERROR(name, number, description)`

**ErrorOr<T>:**
- `std::variant<Error, T>` -- holds either an error or a value
- `present()` / `get()` / `getError()` -- access methods
- Used by `tryGetReply()` to return errors without throwing

---

## Actors and C++20 Coroutines

### Coroutine Functions

FDB's asynchronous functions use standard C++20 coroutines in `.cpp` and `.h` files. A coroutine returns a `Future<T>` and uses `co_await` to await asynchronous results:

```cpp
Future<int> myActor(Future<int> input) {
    int x = 42;
    int val = co_await input;
    co_return val + x;
}
```

The C++ compiler preserves parameters and local variables needed across suspension in the coroutine frame. Flow connects that frame to its existing Future/Promise and callback machinery; these asynchronous tasks are still called actors.

**Common operations:**
- `co_await future` -- obtain a Future's value, suspending only if it is not ready
- `co_await stream` -- consume the next value from a `FutureStream<T>`
- `co_return value` -- complete a `Future<T>` coroutine; use `co_return;` for `Future<Void>`
- `co_await race(a, b)` -- await the first of several futures through `flow/CoroUtils.h`
- `while (true)` -- repeat an asynchronous service loop

See [Writing Coroutines in FoundationDB](../coroutines.md) for usage and lifetime rules.

### C++20 Coroutine Integration -- [`CoroutinesImpl.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/CoroutinesImpl.h)

Flow supplies the promise and awaiter types used by the C++ coroutine machinery:

**CoroPromise<T, ...>** ([`CoroutinesImpl.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/CoroutinesImpl.h)):
- The `promise_type` for `Future<T>` coroutines
- Embeds a `CoroActor<T>` which inherits from `Actor<T>` (which inherits from SAV)
- `get_return_object()` returns `Future<T>` backed by the coroutine's SAV
- `initial_suspend()` returns `suspend_never` (eager start)
- `await_transform()` overloads convert `Future<U>` into `AwaitableFuture`

**AwaitableFuture** ([`CoroutinesImpl.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/CoroutinesImpl.h)):
- The awaiter type for `co_await Future<T>`
- `await_ready()` -- checks if future is ready or coroutine cancelled
- `await_suspend()` -- registers callback with the future's SAV
- `await_resume()` -- extracts value or throws error
- Inherits from `Callback<T>` so it can be inserted into SAV's callback chain

**Cancellation:** For an ordinary cancellable coroutine, `CoroActor::cancel()` sets `ACTOR_WAIT_STATE_CANCELLED` and resumes the coroutine if it is suspended. The awaiter's resume path throws `actor_cancelled()` into the coroutine. If cancellation is already set when a new await begins, the readiness check routes directly to that error path.

---

## Net2 Event Loop -- [`Net2.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Net2.cpp)

The real (non-simulated) event loop. Single-threaded, priority-based task scheduling over Boost.ASIO.

### Net2 Class (line 139)

```
class Net2 : public INetwork, public INetworkConnections {
    ASIOReactor reactor;              // Boost.ASIO wrapper
    TaskQueue<PromiseTask> taskQueue; // Priority queue of pending tasks
    std::atomic<double> currentTime;
    std::atomic<bool> stopped;
};
```

### Run Loop

The `run()` method:
1. Polls ASIO for I/O events (socket readable/writable, timers)
2. Processes ready tasks from the priority queue (highest priority first)
3. Tracks starvation metrics per priority level
4. Yields back to ASIO periodically

### Task Priority

`TaskPriority` enum defines ~30 priority levels from `Min` to `Max`. Examples:
- `RunLoop` (highest) -- event loop housekeeping
- `ReadSocket` -- incoming network data
- `DefaultPromiseEndpoint` -- RPC message delivery
- `DefaultYield` -- cooperative yielding
- `DefaultDelay` -- timer-based delays
- `DiskWrite` / `DiskRead` -- I/O completions

### Key Methods

- `delay(seconds, taskId)` -- returns `Future<Void>` that fires after `seconds` at priority `taskId`
- `yield(taskId)` -- suspends current actor, re-enqueues at `taskId` priority
- `check_yield(taskId)` -- returns true if the current task has been running too long
- `onMainThread(promise, taskId)` -- schedules work from a thread pool thread back onto the main thread

### Boost.ASIO Integration

- TCP/TLS connections via `boost::asio::ip::tcp::socket`
- UDP sockets for connectionless messaging
- Timer operations for `delay()` implementation
- Thread pool integration for async file I/O

---

## Arena Memory System -- [`Arena.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Arena.h)

### Arena

A bump-pointer allocator. Memory is allocated in blocks (`ArenaBlock`), never freed individually -- the entire Arena is freed at once.

```
class Arena {
    Reference<ArenaBlock> impl;   // first block in chain
    
    Arena();                       // empty
    Arena(size_t reservedSize);    // pre-allocate
    void dependsOn(const Arena&); // link arenas (for reference counting)
    void* operator new(size_t, Arena&);  // allocate in arena
};
```

**ArenaBlock** ([`Arena.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Arena.h)`:171-212`):
- Fixed block sizes: `SMALL=64`, `LARGE=8193`
- Bump pointer allocation: `bigUsed` tracks next free offset
- Blocks chain together; all freed when Arena destroyed
- `secure` flag for zero-on-dealloc (sensitive data)

### StringRef / KeyRef / ValueRef

Non-owning pointer+length references. Do not own memory.

```
struct StringRef {
    const uint8_t* data;
    int length;
    
    StringRef(Arena& a, const StringRef& toCopy);  // copy into arena
    StringRef substr(int start, int size);
    int compare(StringRef const& other);           // lexicographic
};

using KeyRef = StringRef;
using ValueRef = StringRef;
```

### Standalone<T>

Bundles a T with its own Arena, creating an owning wrapper:

```
template<class T>
class Standalone : private Arena, public T {
    Arena& arena();     // access the arena
    T& contents();      // access the value
};
// Example: Standalone<StringRef> owns the bytes it points to
```

### VectorRef<T>

Non-owning reference to an array of T. Like StringRef but for structured data.

---

## FastAlloc -- `FastAlloc.h`

Thread-local pool allocator for common sizes (16, 32, 64, 96, 128, 256 bytes).

- `FastAllocator<Size>::allocate()` -- O(1) from thread-local free list
- `FastAllocator<Size>::release()` -- O(1) return to free list
- Magazine system: when free list empty, swap with a full magazine from global pool
- `FastAllocated<Object>` trait: overloads `operator new/delete` to use FastAllocator
- No locks for allocation when free list has items
- 128KB magazines exchanged between thread-local and global pools

---

## Serialization -- [`serialize.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/serialize.h)

Unified framework for binary serialization:

- `is_binary_serializable<T>` trait for fixed-size types (int, double, UID, etc.)
- `Serializer<Archive, T>` template: calls `t.serialize(ar)` by default
- Specializations for containers: `std::vector`, `std::map`, `std::variant`, etc.
- `BinaryWriter` / `BinaryReader` archives
- Protocol version support: `_IncludeVersion`, `_AssumeVersion`, `_Unversioned`
- Flatbuffers integration via `ObjectSerializer` for network messages

---

## Tracing -- `Trace.h`, [`Trace.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Trace.cpp)

Structured event logging used everywhere:

```cpp
TraceEvent("MyEvent", self->dbgid)
    .detail("Key", key)
    .detail("Version", version)
    .detail("Duration", timer() - startTime);
```

- Fluent `.detail()` chain builds key-value fields
- Severity levels: `SevVerbose(0)`, `SevDebug(5)`, `SevInfo(10)`, `SevWarn(20)`, `SevError(40)`
- `AuditedEvent` for security-critical events that bypass suppression
- `.suppressFor(duration)` -- rate-limit repeated events
- `.trackLatest(key)` -- keep latest value of a recurring event

---

## Generic Actors -- [`genericactors.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/genericactors.h)

Key utility actors:

| Actor | Purpose |
|-------|---------|
| `timeout(Future<T>, seconds, defaultValue)` | Race future against timer |
| `timeoutError(Future<T>, seconds)` | Timeout throws `timed_out()` |
| `delayed(Future<T>, seconds)` | Add delay after completion |
| `errorOr(Future<T>)` | Catch error into `ErrorOr<T>` |
| `store(X& out, Future<T>)` | Store result in variable |
| `waitForAll(vector<Future<T>>)` | Wait for all futures |
| `waitForAllReady(vector<Future<T>>)` | Wait ignoring errors |
| `map(Future<T>, func)` | Transform value |
| `mapAsync(Future<T>, actorFunc)` | Async transform |
| `holdWhile(object, Future<T>)` | Keep object alive until future resolves |
| `uncancellable(Future<T>)` | Prevent cancellation |

**AsyncVar<T>** ([`genericactors.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/genericactors.h)`:753-785`):
- Reactive variable: holds a value, notifies on change
- `get()` -- current value
- `onChange()` -- returns `Future<Void>` that fires on next change
- `set(value)` -- updates value and triggers onChange if different
- Used extensively for cluster state (e.g., `AsyncVar<ServerDBInfo>`)

---

## Data Flow Summary

### Actor Execution Cycle

```
1. Coroutine called → frame and SAV created, code runs eagerly until it suspends or completes
2. co_await pendingFuture → registers callback on the future's SAV and suspends
3. Event fires (network data, timer, another actor sends) → SAV::send(value)
4. Callback chain walked → actor's callback called
5. Coroutine resumes from suspension, runs until another pending await or completion
6. co_return value → completes the coroutine's own SAV → caller's callback fires
```

### Memory Flow

```
1. Arena allocated (bump pointer in ArenaBlock)
2. StringRef/KeyRef/ValueRef created referencing arena memory
3. Standalone<T> bundles T + Arena for ownership transfer
4. When Standalone destroyed, Arena freed, all StringRefs invalidated
5. No per-object deallocation -- whole Arena freed at once
```

---

## Principal Files

| File | Purpose |
|------|---------|
| [`flow/include/flow/flow.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/flow.h) | Master include: Future, Promise, SAV, Callback, ErrorOr |
| [`flow/include/flow/Arena.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Arena.h) | Arena, StringRef, Standalone, VectorRef |
| [`flow/include/flow/Error.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/Error.h) | Error class and error code definitions |
| [`flow/include/flow/CoroutinesImpl.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/CoroutinesImpl.h) | C++20 coroutine integration (CoroPromise, AwaitableFuture) |
| [`flow/include/flow/genericactors.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/genericactors.h) | Utility actors (delay, timeout, waitForAll, AsyncVar) |
| [`flow/include/flow/serialize.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/serialize.h) | Serialization framework |
| `flow/include/flow/FastAlloc.h` | Thread-local pool allocator |
| [`flow/Net2.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Net2.cpp) | Real event loop (Boost.ASIO based) |
| [`flow/Trace.cpp`](https://github.com/apple/foundationdb/blob/main/flow/Trace.cpp) | Structured event tracing |
| [`flow/include/flow/CoroUtils.h`](https://github.com/apple/foundationdb/blob/main/flow/include/flow/CoroUtils.h) | Coroutine selection helpers (Choose, race) |
