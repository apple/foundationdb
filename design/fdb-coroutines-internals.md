# How C++ Coroutines Work in FoundationDB

*Caveat: work in progress. This mentions code not yet submitted. Accuracy is believed to be good,
but ongoing work to increase clarity is needed.*

This document explains the coroutine infrastructure in FoundationDB, from the
C++20 standard machinery down to FDB-specific implementation details.

## C++20 Coroutine Fundamentals

Any function whose body contains `co_await`, `co_return`, or `co_yield` is a
coroutine. The compiler rewrites it into a state machine. The programmer does
**not** call the coroutine machinery directly — the compiler generates all the
calls. The programmer's job is to implement the **policy** types that the
compiler calls into.

### What the compiler generates

Given:

```cpp
Future<int> foo(int x) {
    int y = co_await bar();
    co_return y + 1;
}
```

The compiler emits roughly:

```cpp
Future<int> foo(int x) {
    // 1. Look up the promise type via coroutine_traits<Future<int>, int>
    using promise_type = std::coroutine_traits<Future<int>, int>::promise_type;

    // 2. Allocate the coroutine frame on the heap
    //    (calls promise_type::operator new if defined)
    //    The frame holds: the promise object, all local variables,
    //    bookkeeping for each suspend point, and the function arguments.

    // 3. Construct the promise
    promise_type promise;

    // 4. Get the return object — this is what the caller receives.
    //    Called BEFORE the body executes.
    Future<int> retval = promise.get_return_object();

    // 5. co_await promise.initial_suspend()
    //    FDB returns suspend_never → body starts immediately.

    try {
        // === user's body ===

        // co_await bar() expands to:
        auto&& awaitable = promise.await_transform(bar());
        if (!awaitable.await_ready()) {
            awaitable.await_suspend(handle_to_this_coroutine);
            // <-- coroutine suspends here, control returns to caller -->
            // <-- later: something calls handle.resume() -->
        }
        int y = awaitable.await_resume();

        // co_return y + 1 expands to:
        promise.return_value(y + 1);
        goto final_suspend;

    } catch (...) {
        promise.unhandled_exception();
    }

final_suspend:
    // 6. co_await promise.final_suspend()
    //    The compiler generates the same await_ready / await_suspend /
    //    await_resume sequence here.

    return retval;  // Already returned to caller at step 4/5
}
```

Key insight: `retval` is returned to the caller when the coroutine first
suspends (or completes without suspending). The caller never sees the
`co_return` value directly — it goes through `promise.return_value()` into
whatever result cell the promise manages.

### The coroutine frame

The compiler heap-allocates a "coroutine frame" containing:

- The promise object
- Copies of all function parameters
- All local variables (live across any suspend point)
- Internal state for tracking which suspend point the coroutine is at
- Awaiter objects for each `co_await` (since they must survive suspension)

FDB overrides `operator new`/`operator delete` on promise types to use its
`allocateFast`/`freeFast` allocator.

### `coroutine_handle<>`

An opaque pointer to the coroutine frame. The compiler passes it to
`await_suspend`. Key operations:

- `handle.resume()` — resume the coroutine at its current suspend point
- `handle.destroy()` — destroy the coroutine frame (runs destructors for
  in-scope locals, deallocates)
- `handle.done()` — true if the coroutine is at its final suspend point

### `await_suspend` return types

| Return type | Meaning |
|---|---|
| `void` | Always suspend |
| `bool` | `true` → suspend; `false` → don't suspend (resume immediately) |
| `coroutine_handle<U>` | Symmetric transfer: suspend this coroutine, resume that one |

### `final_suspend` rules

- The expression `co_await promise.final_suspend()` **must not throw**
  (compiler-enforced). Mark `final_suspend()` and the awaiter's methods
  `noexcept`.
- If `await_suspend` returns `void` or `true`: the coroutine is suspended at
  the final suspend point. The frame stays alive until someone calls
  `handle.destroy()`. Calling `handle.resume()` on a coroutine suspended at
  its final suspend point is **undefined behavior**.
- If `await_suspend` returns `false`: the coroutine was never suspended. The
  runtime auto-destroys the frame after `await_resume()` completes. This is
  well-defined.

### `promise_type` required members

| Method | Required? | Purpose |
|---|---|---|
| `get_return_object()` | Yes | Create the value returned to the caller |
| `initial_suspend()` | Yes | Controls whether body starts immediately |
| `final_suspend()` | Yes, `noexcept` | Controls frame lifetime after body completes |
| `unhandled_exception()` | Yes | Called if an exception escapes the body |
| `return_value(T)` or `return_void()` | Exactly one | Stores the co_return value |
| `await_transform(U)` | Optional | Intercepts every `co_await` expression |
| `yield_value(T)` | Optional | Enables `co_yield` |
| `operator new` / `operator delete` | Optional | Custom frame allocation |

---

## FDB's Type Hierarchy

### `Callback<T>` — intrusive linked list node

```
flow/include/flow/flow.h:468
```

A doubly-linked list node with virtual `fire(T const&)`, `fire(T&&)`,
`error(Error)`, and `unwait()`. When a `SAV<T>` has no value yet, it serves as
the sentinel of a circular list of waiting callbacks.

When a callback is `remove()`d and it was the last one, `unwait()` is called on
the sentinel (the SAV), which decrements the future refcount.

### `SAV<T>` — Single Assignment Variable

```
flow/include/flow/flow.h:732
```

The core result cell. Inherits privately from `Callback<T>` (to serve as the
list sentinel) and `FastAllocated<SAV<T>>`.

**Two independent refcounts:**

- `promises` — one per `Promise<T>` pointing at it, plus one for an active
  actor (the producer side)
- `futures` — one per `Future<T>` pointing at it, plus one if any callbacks
  are registered

**State machine** via `error_state.code()`:

| Code | Constant | Meaning |
|---|---|---|
| -3 | `UNSET_ERROR_CODE` | Not yet assigned. `canBeSet()` is true. |
| -2 | `NEVER_ERROR_CODE` | Will never complete (set to `Never`). |
| -1 | `SET_ERROR_CODE` | Successfully set with a value. |
| > 0 | (error code) | Set with an error. |

**Lifecycle rules:**

- `delPromiseRef()`: if promises drops to 1 and the SAV is still unset, sends
  `broken_promise()` to any waiting callbacks, then sets promises to 0. If
  futures is also 0, calls `destroy()`.
- `delFutureRef()`: if futures drops to 0 and promises > 0, calls `cancel()`
  (this is how dropping the last Future cancels an actor). If promises is also
  0, calls `destroy()`.
- `destroy()` and `cancel()` are virtual — subclasses override them.

**Callback management:** `addCallbackAndDelFutureRef(cb)` transfers a future
reference into the callback list. If this is the first callback, the future
count stays the same (the "+1 for having callbacks" replaces the future ref).
If callbacks already exist, the future count is decremented.

### `Actor<ReturnValue>` — actor base class

```
flow/include/flow/flow.h:1500
```

Inherits from `SAV<ReturnValue>`. Adds one field:

```cpp
int8_t actor_wait_state;
```

| Value | Constant | Meaning |
|---|---|---|
| > 0 | `ACTOR_WAIT_STATE_WAITING` (1) | Suspended, waiting on a callback |
| 0 | `ACTOR_WAIT_STATE_NOT_WAITING` | Running or completed |
| -1 | `ACTOR_WAIT_STATE_CANCELLED` | Cancellation requested |
| -2 | `ACTOR_WAIT_STATE_CANCELLED_DURING_READY_CHECK` | Cancelled detected during `await_ready` |

Constructed with `SAV(1, 1)`: starts with futures=1 and promises=1. The
futures=1 is consumed by the `Future<T>` returned from `get_return_object()`.
The promises=1 represents the actor itself as the producer.

`Actor<void>` is a separate specialization with no SAV base — for
fire-and-forget actors.

### `Promise<T>` and `Future<T>`

```
flow/include/flow/flow.h:1091, 982
```

`Promise<T>` wraps a `SAV<T>*`. Constructor creates `SAV(0, 1)` — zero
futures, one promise. `getFuture()` calls `addFutureRef()` then returns
`Future<T>(sav)`.

`Future<T>` wraps a `SAV<T>*`. Copy constructor calls `addFutureRef()`.
Destructor calls `delFutureRef()`. The explicit constructor
`Future(SAV<T>* sav)` does **not** increment the refcount — it assumes
the count is already correct (used by `get_return_object()`).

---

## FDB's Coroutine Promise: `CoroPromise`

```
flow/include/flow/CoroutinesImpl.h
```

### `coroutine_traits` specialization

```
flow/include/flow/Coroutines.h
```

```cpp
template <typename ReturnValue, typename... Args>
struct coroutine_traits<Future<ReturnValue>, Args...> {
    using promise_type = coro::CoroPromise<ReturnValue,
                                           !coro::hasUncancellable<Args...>,
                                           coro::hasExplicitVoid<Args...>,
                                           coro::hasNoThrowOnCancel<Args...>>;
};
```

The `Args...` includes all function parameter types. Marker types like
`Uncancellable`, `NoThrowOnCancel`, and `ExplicitVoid` are detected via
template metaprogramming on the parameter list. They are always defaulted
parameters (`NoThrowOnCancel = {}`) so callers don't pass them.

### `CoroActor<T, IsCancellable>`

```
flow/include/flow/CoroutinesImpl.h:124
```

Inherits from `Actor<T>`. Embedded **inside** the coroutine frame (the promise
holds it as a member). This means the frame allocation also allocates the
SAV — a single allocation for both.

Adds a `coroutine_handle<>` and overrides:

- `cancel()`: sets `actor_wait_state = CANCELLED`, then resumes the coroutine
  if it was waiting. The resumed coroutine sees the cancelled state in
  `resumeImpl()` and throws `actor_cancelled()`, which unwinds through the
  coroutine body (running destructors and catch blocks), reaches
  `unhandled_exception()`, and the error is stored on the SAV.
- `destroy()`: calls `handle.destroy()` to free the coroutine frame (which
  also frees the embedded CoroActor, since it's part of the frame).

### `get_return_object()`

```cpp
ReturnFutureType get_return_object() noexcept {
    coroActor.handle = handle();
    return ReturnFutureType(&coroActor);
}
```

Stores the coroutine handle in the actor, then constructs a `Future<T>`
pointing at the actor's SAV. The SAV was initialized with `futures=1` by the
`Actor` constructor, and the explicit `Future(SAV*)` constructor does not
increment the refcount — so the returned Future consumes that initial
reference.

### `initial_suspend()`

Returns `suspend_never` — the coroutine body starts executing immediately when
called. The `Future<T>` has already been returned to the caller via
`get_return_object()`.

### `return_value()` / `return_void()`

Implemented via the `CoroReturn<T, Promise>` CRTP mixin. Calls
`actor()->set(value)` which placement-news the value into the SAV's storage
and sets `error_state = SET_ERROR_CODE`.

### `final_suspend()`

Returns a custom `FinalAwaitable`:

- `await_ready()` returns `false` — always enter `await_suspend`
- `await_suspend()` calls `finishSendAndDelPromiseRef()` (or the error
  variant), which fires all waiting callbacks and decrements the promise count.
  Returns `void` — the coroutine is left suspended. The frame stays alive
  until the SAV's `destroy()` virtual is called (when both refcounts hit 0),
  which calls `handle.destroy()`.

### `unhandled_exception()`

Catches the exception, extracts the `Error`, and stores it on the SAV via
`setError()`. If the exception isn't an `Error`, stores `unknown_error()`.

### `await_transform()`

Intercepts every `co_await` expression and wraps it in an FDB-specific awaiter
type:

| Awaited type | Awaiter created |
|---|---|
| `Future<U>` | `AwaitableFuture<..., false>` |
| `FutureStream<U>` | `AwaitableFuture<..., true>` |
| `AsyncResult<U>` | `AwaitableAsyncResult` |
| `ThreadFutureStream<U>` | `ThreadAwaitableFutureStream` |
| `coro::FutureIgnore<U>` | `AwaitableFutureIgnore` |
| `coro::FutureErrorOr<U>` | `AwaitableFutureErrorOr` |

---

## Awaiters: How `co_await` Suspends and Resumes

### `AwaitableFuture<PromiseType, ValueType, IsStream>`

The most common awaiter. Used for `co_await someFuture` and
`co_await someStream`.

Inherits from `Callback<T>` (or `SingleCallback<T>` for streams), which means
the awaiter itself is the callback node that gets inserted into the Future's
callback list.

**`await_ready()`**: Returns `true` if the Future is already resolved (skip
suspension). Also checks if the actor has been cancelled — if so, sets
`CANCELLED_DURING_READY_CHECK` and returns `true` to proceed to
`await_resume()` where the cancellation is handled.

**`await_suspend(handle)`**:
1. Stores the coroutine handle via `pt->setHandle(h)`
2. Sets `actor_wait_state = ACTOR_WAIT_STATE_WAITING`
3. Registers this awaiter as a callback on the Future via
   `addCallbackAndClear(this)` — this transfers the Future's reference to
   the callback list

**`fire(value)` / `error(err)`** (inherited from Callback): Called by the
Future's SAV when it is resolved. For streams, stores the value in a local
`AwaitableFutureStore`. Then calls `pt->resume()` which calls
`handle.resume()`, resuming the coroutine.

**`resumeImpl()`**: Called at the start of `await_resume()`. Checks
`actor_wait_state`:
- If cancelled: detaches from the callback list and throws
  `actor_cancelled()`
- If still waiting (future wasn't ready yet when we entered await_resume via
  the non-ready-check path): detaches from callback list, resets wait state
- Returns whether the future was already ready (affects how the value is
  retrieved for streams)

**`await_resume()`**: Implemented via the `AwaitableResume` CRTP mixin.
Calls `resumeImpl()`, then returns the value from the resolved Future.
Throws if the Future completed with an error.

### The full `co_await` sequence, step by step

```cpp
Future<Void> example() {
    Promise<int> p;
    Future<int> f = p.getFuture();

    // ... elsewhere, someone will call p.send(42)

    int val = co_await f;
    //        ^^^^^^^^^
    // 1. Compiler calls promise.await_transform(f)
    //    → creates AwaitableFuture<..., int, false>{f, &promise}
    //
    // 2. Compiler calls awaiter.await_ready()
    //    → f is not resolved yet → returns false
    //
    // 3. Compiler calls awaiter.await_suspend(handle)
    //    → stores handle in actor
    //    → sets actor_wait_state = WAITING
    //    → registers awaiter as callback on f's SAV
    //    → coroutine suspends, control returns to caller
    //
    // ... time passes, someone calls p.send(42) ...
    //
    // 4. SAV::send() fires all callbacks → awaiter.fire(42)
    //    → calls promise.resume() → handle.resume()
    //    → coroutine resumes
    //
    // 5. Compiler calls awaiter.await_resume()
    //    → resumeImpl() checks wait state, detaches callback
    //    → returns f.get() → 42
    //
    // 6. val = 42

    co_return val;
}
```

---

## Cancellation

### How cancellation is triggered

Cancellation flows through the SAV refcount system:

1. The caller drops the last `Future<T>` reference to an actor
2. `Future::~Future()` calls `sav->delFutureRef()`
3. `delFutureRef()` sees futures hit 0 and promises > 0 → calls `cancel()`
4. `cancel()` is virtual, dispatched to the actor subclass

Or explicitly: the caller calls `f.cancel()` which calls `sav->cancel()`.

### Normal cancellation (CoroActor)

```cpp
void cancel() override {
    auto prev_wait_state = actor_wait_state;
    actor_wait_state = ACTOR_WAIT_STATE_CANCELLED;
    if (actorWaitStateIsWaiting(prev_wait_state)) {
        handle.resume();  // resume the coroutine
    }
}
```

If the coroutine is suspended at a `co_await`, it is resumed. The awaiter's
`resumeImpl()` sees `CANCELLED` and throws `actor_cancelled()`. The exception
unwinds through the coroutine body — running destructors, entering catch
blocks — until it reaches `unhandled_exception()`, which stores the error on
the SAV.

If the coroutine is not waiting (between co_await points, which shouldn't
happen in cooperative scheduling), the flag is just set. The next
`await_ready()` will detect it.

### `Uncancellable` coroutines

`CoroActor<T, false>` — the `cancel()` body is compiled away
(`if constexpr (IsCancellable)` is false). The coroutine runs to completion
regardless of whether futures are dropped.

### `NoThrowOnCancel` coroutines

Uses `NoThrowOnCancelCoroActor<T>` instead of `CoroActor`. Key differences:

1. **The actor is heap-allocated separately** from the coroutine frame
   (via `new ActorType()` in the promise). This is necessary because cancel
   destroys the frame while the SAV must survive for Future observers.

2. **`cancel()` destroys the frame directly** instead of resuming:

```cpp
void cancel() override {
    if (!canBeSet()) return;

    if (cancelHandler) {
        // Detach the awaiter's callback before destroying the frame
        cancelHandler->cancelWait();
    }

    destroyFrame();  // handle.destroy() — RAII destructors run

    if (futures) {
        sendErrorAndDelPromiseRef(actor_cancelled());
    } else {
        promises = 0;
        delete this;
    }
}
```

3. **`final_suspend` returns `false`** from `await_suspend`. After completing
   the SAV and clearing the handle, the frame auto-destructs (the coroutine was
   never suspended at the final suspend point). This is well-defined per the
   C++ standard: returning `false` means the coroutine resumes, control flows
   off the end, and the frame is destroyed.

4. **`AwaitCancelHandler`**: A virtual interface that awaiters implement. Each
   awaiter registers itself via `pt->setCancelHandler(this)` in
   `await_suspend` and clears itself in `resumeImpl`. When `cancel()` fires,
   it calls `cancelHandler->cancelWait()` to detach the awaiter's callback
   from whatever Future it's waiting on, *before* destroying the frame. Without
   this, the destroyed awaiter would be a dangling node in the Future's
   callback list.

**Behavioral difference from normal cancellation:**

- RAII destructors run (frame destruction invokes ~locals)
- `catch` blocks inside the coroutine are **not** entered for cancellation
- The coroutine never "sees" the cancellation — it simply ceases to exist

---

## `AsyncResult<T>`

```
flow/include/flow/Coroutines.h, CoroutinesImpl.h
```

An alternative to `Future<T>` that transfers ownership of the result value
through `co_await`. Unlike `Future<T>` where `co_await` returns `T const&`,
`co_await AsyncResult<T>` returns `T` by value (moved out). This avoids copies
for expensive payloads.

`AsyncResult<T>` is backed by `AsyncResultState<T>` (separate from SAV) with
its own reference counting. The coroutine promise is `AsyncResultPromise`,
which manages `producerHandle` and `producerWaitState` directly rather than
inheriting from Actor.

Cancellation for AsyncResult coroutines goes through
`AsyncResultState::cancelProducer()`, which mirrors the Actor cancel logic:
either resumes the producer to throw `actor_cancelled()` or (with
`noThrowOnCancel`) destroys the producer frame directly.

---

## `choose` / `when` for Coroutines

```
flow/include/flow/CoroUtils.h
```

The coroutine equivalent of the actor compiler's `choose { when(...) { ... } }`
pattern. Two forms:

### `Choose().When(future, handler).When(...).run()`

Callback-based. Creates a `ChooseImplActor` that registers callbacks on all
futures simultaneously. When the first one fires, it removes all other
callbacks and runs the handler. Returns `Future<Void>`.

**Limitations:** handlers are synchronous functions — you cannot `co_await`
inside a `When` handler.

### `co_await race(future1, future2, ...)`

Template-based alternative that supports heterogeneous future types. Returns
`Future<std::variant<T1, T2, ...>>` where each `Ti` is the value type of the
corresponding future. When the first future completes, `race` cancels the
others and constructs the variant with `std::in_place_index<N>` for the
winning future's position.

Use `result.index()` to determine which future won, and `std::get<N>(result)`
or `std::visit(...)` to extract the value:

```cpp
auto result = co_await race(futureInt, futureString);
if (result.index() == 0) {
    int val = std::get<0>(result);
} else {
    std::string val = std::get<1>(result);
}
```

---

## Marker Parameters

FDB uses defaulted function parameters as compile-time markers:

```cpp
Future<Void> foo(int x, NoThrowOnCancel = {}) {
    co_await bar();
    co_return;
}
```

The compiler sees the parameter types `(int, NoThrowOnCancel)` and passes them
through `coroutine_traits<Future<Void>, int, NoThrowOnCancel>`, which selects
the appropriate promise type.

| Marker | Effect |
|---|---|
| (none) | Normal cancellable coroutine |
| `Uncancellable` | `cancel()` is a no-op; coroutine runs to completion |
| `NoThrowOnCancel` | `cancel()` destroys the frame directly; no `actor_cancelled` thrown |
| `ExplicitVoid` | `co_await Future<Void>` produces `Void` instead of `void` |

`Uncancellable` and `NoThrowOnCancel` are mutually exclusive (enforced by
`static_assert`). Neither is compatible with `AsyncGenerator`.

---

## Lifecycle Summary

### Normal completion

1. Coroutine is called → frame allocated, `get_return_object()` returns
   `Future<T>` to caller
2. `initial_suspend()` returns `suspend_never` → body runs immediately
3. Body executes, suspending/resuming at `co_await` points
4. `co_return value` → `promise.return_value(value)` stores result in SAV
5. `final_suspend()` → `await_suspend` fires callbacks via
   `finishSendAndDelPromiseRef()`, decrements promise count
6. Frame is either left suspended (normal) or auto-destroyed
   (NoThrowOnCancel)
7. When both refcounts hit 0, `destroy()` frees the frame (if still alive)
   and the SAV

### Cancellation (normal)

1. Last `Future<T>` dropped → `delFutureRef()` → `cancel()` on the actor
2. `cancel()` sets `actor_wait_state = CANCELLED`, resumes the coroutine
3. Awaiter's `resumeImpl()` throws `actor_cancelled()`
4. Exception unwinds through the body (running destructors and catch blocks)
5. `unhandled_exception()` stores the error on the SAV
6. `final_suspend()` completes the SAV, fires callbacks
7. Frame cleanup proceeds as in normal completion

### Cancellation (NoThrowOnCancel)

1. Last `Future<T>` dropped → `delFutureRef()` → `cancel()` on the actor
2. `cancel()` calls `cancelHandler->cancelWait()` to detach the awaiter
3. `cancel()` calls `handle.destroy()` — frame is destroyed, RAII runs
4. `cancel()` sends `actor_cancelled()` to any remaining Futures via SAV
5. Actor (heap-allocated) survives until all Future references are dropped

### Refcount flow

```
Actor constructor:     promises=1  futures=1
get_return_object():   Future<T> takes the futures=1 ref (no increment)
Caller copies Future:  futures=2
co_await registers cb: futures stays same (cb replaces future ref)
Future fires cb:       coroutine resumes
final_suspend:         promises-- (via finishSendAndDelPromiseRef)
Caller drops Future:   futures-- → if both 0, destroy()
```
