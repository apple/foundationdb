# Coroutines in Flow

* [Introduction](#introduction)
* [Basic Types](#basic-types)
* [Waiting for Multiple Futures](#waiting-for-multiple-futures)
    * [Ordered Evaluation with Choose](#ordered-evaluation-with-choose)
    * [Concurrent Request Handling](#concurrent-request-handling)
* [Generators](#generators)
    * [Generators and Ranges](#generators-and-ranges)
    * [Execution and Value Ownership](#execution-and-value-ownership)
    * [Generators vs Promise Streams](#generators-vs-promise-streams)
* [Cancellation](#cancellation)
    * [Uncancellable](#uncancellable)
    * [NoThrowOnCancel](#nothrowoncancel)
* [Lifetime and Ownership](#lifetime-and-ownership)
    * [Locals and Scope](#locals-and-scope)
    * [Parameters and Objects](#parameters-and-objects)
* [Error Handlers](#error-handlers)
* [Direct Await Expressions](#direct-await-expressions)
* [Internals Reference](fdb-coroutines-internals.md) — coroutine runtime, awaiters, and cancellation

## Introduction

Flow uses standard C++20 coroutines for asynchronous code. Coroutines work with Flow's network loop, futures,
RPC layer, and deterministic simulator. They use ordinary C++ control flow around `co_await`, `co_return`, and,
for generators, `co_yield`.

A simple coroutine looks like this:

```c++
Future<double> simpleCoroutine() {
    double begin = now();
    co_await delay(1.0);
    co_return now() - begin;
}
```

The function starts executing when called. It returns a `Future<double>` that becomes ready when the coroutine
returns its result or fails. Awaiting a ready future does not suspend; awaiting a pending future registers a
continuation with Flow and suspends until the value or error is available.

This guide assumes familiarity with Flow's basic types. Their coroutine support is defined in
[`Coroutines.h`](../flow/include/flow/Coroutines.h) and
[`CoroutinesImpl.h`](../flow/include/flow/CoroutinesImpl.h).

## Basic Types

A function is a coroutine if its body contains `co_await`, `co_yield`, or `co_return`. Its return type must provide
a compatible coroutine implementation. The main types covered here are:

* `Future<T>` returns one asynchronous result. A coroutine can await other Flow futures and return its value with
  `co_return value;`. An unhandled error becomes the future's error. `co_yield` is not supported.
* `Future<Void>` represents asynchronous completion without a result value. Use `co_return;`, or reach the end of
  the function body. By default, awaiting a `Future<Void>` also produces no value.
* `Generator<T>` produces values synchronously, using `co_yield`. It provides an input iterator interface.
* `AsyncGenerator<T>` produces values on demand and can both `co_await` asynchronous work and `co_yield` results.
  Calling the generator requests its next value and returns a `Future<T>`.

`AsyncResult<T>` is also available for a single consumer that does not need a copyable `Future<T>`; it has a distinct
ownership contract. See the [internals reference](fdb-coroutines-internals.md) for that API and the runtime types
such as `SAV` and `Actor` that support coroutine execution.

## Waiting for Multiple Futures

Prefer `race(...)` when waiting for one of several operations and then branching on the winner. It returns a
`std::variant` whose index matches the winning argument. For example:

```c++
Future<int> withDeadline(Future<int> result, double timeoutSeconds) {
    auto winner = co_await race(result, delay(timeoutSeconds));
    if (winner.index() == 0) {
        co_return std::get<0>(winner);
    }
    throw io_timeout();
}
```

If multiple inputs are already ready, the first argument that is ready wins. An error from the winning input is
propagated rather than returned in the variant. For a `FutureStream<T>`, the winning branch consumes one value.
Losing inputs are detached from the race, not explicitly cancelled, but dropping their last reference can cancel
them. All argument expressions are evaluated before `race` is called; use separate statements when their evaluation
order matters.

Use helpers such as `quorum`, `waitForAll`, `waitForAllReady`, `timeoutError`, or `operator||` when they express the
required behavior more directly. `race` and `Choose` are defined in
[`CoroUtils.h`](../flow/include/flow/CoroUtils.h); the other helpers are in
[`genericactors.h`](../flow/include/flow/genericactors.h).

### Ordered Evaluation with Choose

`Choose` supports synchronous callbacks for the winning future:

```c++
co_await Choose()
    .When(future1, [](Void const&) {
        // Handle the first result.
    })
    .When(foo(), [](Foo const& value) {
        // Handle the second result.
    }).run();
```

Each `When` checks readiness in call order. A handler for an already-ready future can run immediately while the
chain is being constructed. Passing `foo()` still calls it even if an earlier branch has already won, because
it is an ordinary C++ argument expression. Pass a factory to avoid creating a later future unnecessarily:

```c++
co_await Choose()
    .When(future1, [](Void const&) {
        // Handle the first result.
    })
    .When([]() { return foo(); }, [](Foo const& value) {
        // Handle the second result.
    }).run();
```

The factory is called only if no earlier branch received a ready future. Use `Choose` when this ordered, lazy
creation matters, or when synchronous callbacks make the code clearer. Handlers must return `void`; they cannot
suspend with `co_await`. If the winner needs asynchronous follow-up work, use `race` and await it in the outer
coroutine.

### Concurrent Request Handling

Awaiting a request handler inside a receive loop serializes request handling. This can be intentional, but when
requests should run concurrently, retain each handler's future in an `ActorCollection` and observe the collection's
result to propagate failures:

```c++
ActorCollection actors(false);
while (true) {
    auto request = co_await race(requestAStream.getFuture(), requestBStream.getFuture(), actors.getResult());
    if (request.index() == 0) {
        actors.add(handleRequestA(std::get<0>(request)));
    } else if (request.index() == 1) {
        actors.add(handleRequestB(std::get<1>(request)));
    } else {
        // With returnWhenEmptied=false, the collection only completes with an error.
        UNREACHABLE();
    }
}
```

Include [`ActorCollection.h`](../flow/include/flow/ActorCollection.h) for this pattern. Handlers must own any request
data they use after suspension; references into the local variant do not outlive the loop iteration. Bound
concurrency when needed rather than allowing an unbounded collection of outstanding work.

## Generators

Generators separate value production from consumption. Use `Generator<T>` for synchronous computation and
`AsyncGenerator<T>` when producing the next value requires asynchronous work.

### Generators and Ranges

A `Generator<T>` exposes its current value through `*generator` and advances with `++generator`. Copies share the
same coroutine and iteration position; they do not create independent sequences. This makes it an input iterator,
not a multipass iterator.

```c++
// Produces base^0, base^1, base^2, ...
Generator<double> powersOf(double base) {
    double current = 1;
    while (true) {
        co_yield current;
        current *= base;
    }
}
```

Use `std::ranges::subrange` with `Generator<T>::end()` to apply range adaptors:

```c++
auto powers = std::ranges::subrange(powersOf(2), Generator<double>::end());
for (double value : powers
                    | std::views::filter([](double value) { return value > 10; })
                    | std::views::take(10)) {
    fmt::print("{}\n", value);
}
```

This prints ten powers of two, from 16 through 8192. Include `<ranges>` for the standard range facilities.

An asynchronous generator requests its next value with `co_await generator()`:

```c++
AsyncGenerator<int> delayedValues(int count) {
    for (int i = 0; i < count; ++i) {
        co_await delay(0.01);
        co_yield i;
    }
}
```

Keep the generator alive until each request finishes, and await that request before making another one. Do not copy
an `AsyncGenerator` or abandon an outstanding request. When the generator reaches the end of its body or executes
`co_return;`, the pending request reports `end_of_stream`. Checking `if (generator)` only tells you whether it has
already finished; the next request can still discover the end of the stream.

[`toGenerator`](../flow/include/flow/CoroUtils.h) adapts a `FutureStream<T>` to an `AsyncGenerator<T>`, translating the
stream's `end_of_stream` into generator completion while preserving other errors.

### Execution and Value Ownership

The execution policy depends on the return type:

* `Future<T>` coroutines begin immediately and run until suspension or completion.
* `Generator<T>` begins immediately and runs to its first `co_yield` or completion. Incrementing it resumes production.
* `AsyncGenerator<T>` initially suspends. Calling its `()` operator resumes production until a value, error, or
  asynchronous suspension is reached.

Both generator types suspend at `co_yield` until the next value is requested. This permits a generator to reuse
storage between values, but the consumer must respect that storage's lifetime. For example:

```c++
Generator<StringRef> randomStrings(int minLen, int maxLen) {
    Arena arena;
    auto buffer = new (arena) uint8_t[maxLen + 1];
    while (true) {
        auto size = deterministicRandom()->randomInt(minLen, maxLen + 1);
        for (int i = 0; i < size; ++i) {
            buffer[i] = deterministicRandom()->randomAlphaNumeric();
        }
        co_yield StringRef(buffer, size);
    }
}
```

Each `StringRef` points into the generator's arena. Its contents can change when the generator advances, and the
storage is freed when the generator is destroyed. Copy a value into owning storage, such as `Standalone<StringRef>`,
if it must survive either event. The same requirement applies when a consumer passes a view to background work.

### Generators vs Promise Streams

`AsyncGenerator<T>` internally uses `PromiseStream<T>`, but the two interfaces express different production policies.
A generator produces another value only when requested. A promise stream lets a producer enqueue values independently
of the consumer.

Prefer a synchronous generator for simple computation. Use an asynchronous generator for demand-driven IO, such as
reading the next block of a file. Use a promise stream when production should run ahead, when multiple sources feed
a stream, or when an existing stream interface fits the operation. For example, prefetching file blocks can hide IO
latency while the consumer processes earlier blocks.

A producer that runs ahead needs explicit bounds or backpressure, an owner for its future, and error propagation to
the consumer. Send owning values when the producer will reuse or release the underlying storage. A `PromiseStream`
does not by itself bound its queue or keep a producer coroutine alive.

## Cancellation

By default, explicitly cancelling a coroutine's future or dropping its last future reference requests cancellation.
A suspended coroutine resumes and its await throws `actor_cancelled`. If cancellation is requested while it is
running, the next Flow await observes it. Subsequent Flow awaits also throw cancellation rather than waiting.

Use RAII for cleanup, and rethrow cancellation from error handlers unless the coroutine's contract explicitly
requires something else. Do not silently consume `broken_promise` or other failures either. Keeping a future in a
local or an `ActorCollection` makes the lifetime of asynchronous work explicit.

### Uncancellable

Some operations must finish even if their caller stops waiting. Add an `Uncancellable` marker parameter to make
cancellation of the returned future a no-op:

```c++
Future<Void> finishOperation(Future<Void> operation, Uncancellable = {}) {
    co_await operation;
}
```

Dropping all references to the returned future does not cancel this coroutine. It continues until completion or
failure, so it must own any resources and data it needs for that duration. Use this marker only when required by the
operation's lifetime contract; it does not prevent an awaited operation from failing.

### NoThrowOnCancel

`NoThrowOnCancel` keeps cancellation enabled but destroys the coroutine frame without resuming it to throw
`actor_cancelled` inside the coroutine:

```c++
Future<Void> waitForSignal(Future<Void> signal, NoThrowOnCancel = {}) {
    co_await signal;
}
```

Cancellation runs normal RAII cleanup for locals in scope, but it does not enter the coroutine's `catch` handlers.
Observers of its returned future still receive `actor_cancelled`. `NoThrowOnCancel` and `Uncancellable` are mutually
exclusive, and neither marker is supported by `AsyncGenerator`.

## Lifetime and Ownership

### Locals and Scope

Coroutine locals follow normal C++ scoping rules. A local remains alive across suspension until its scope exits.
For objects such as lock releasers, make sure that this is the intended lifetime: retaining a lock across a wait
can block other work, while leaving a scope releases it.

The same rule applies to futures. A future declared inside an `if` or `try` block is destroyed at that block's end.
If it is the last reference to pending cancellable work, that work is cancelled. Declare the future in the scope
that must retain it, or add it to an `ActorCollection`; observe its result as well as retaining it.

Initialize local values explicitly. For a passive aggregate with primitive fields, `SomeStruct value{};` initializes
those fields, while `SomeStruct value;` may leave them uninitialized. Coroutine frames do not change C++ initialization
rules.

### Parameters and Objects

Prefer owning parameters passed by value for data used after suspension. Coroutine reference parameters remain
references; the frame does not copy the referenced object. A temporary argument or caller-local object can therefore
be destroyed while the coroutine is suspended.

```c++
Future<Void> printLater(Key key) {
    co_await delay(1.0);
    fmt::print("{}\n", key.toString());
}
```

Here the frame owns a `Key`. Passing a `KeyRef`, `ValueRef`, or `StringRef` by value only copies a view and does not
extend its arena's lifetime. Use an owning type such as `Key`, `Value`, or `Standalone<T>` when bytes must outlive
the caller.

If an API must take a reference, establish the required ownership before suspension and use only the owned copy
thereafter. Copying inside the body before the first await works for an eager `Future<T>` coroutine, but not for an
initially suspended `AsyncGenerator<T>` whose caller may already have destroyed the argument before the body starts.

A coroutine can be a non-static member function, but its `this` pointer does not keep the object alive. The owner must
outlive the coroutine, or the coroutine must retain an appropriate owning reference. Similarly, a coroutine lambda's
captures belong to its closure object; keep that object alive or pass owned values as coroutine parameters instead.

## Error Handlers

C++ does not allow `co_await` inside a `catch` handler. Save the error and await recovery after leaving the handler.
For a transaction retry loop, for example:

```c++
Future<Void> writeKey(Database db, Key key, Value value) {
    ReadYourWritesTransaction tr(db);
    while (true) {
        Error error;
        try {
            tr.set(key, value);
            co_await tr.commit();
            co_return;
        } catch (Error& e) {
            if (e.code() == error_code_actor_cancelled) {
                throw;
            }
            error = e;
        }
        co_await tr.onError(error);
    }
}
```

The successful path returns before recovery; only the failed path calls `onError`. That call applies the transaction's
retry policy and propagates non-retryable errors. Include
[`ReadYourWrites.h`](../fdbclient/include/fdbclient/ReadYourWrites.h) for this transaction API.

## Direct Await Expressions

Await a future directly when no separate helper is needed:

```c++
co_await future;                 // Wait and discard a non-Void result.
value = co_await anotherFuture;  // Wait and store the result.
consume(co_await nextValue);     // Use the result in an expression.
```

Use named locals when they clarify ownership or control flow. Helpers remain useful when adapting futures for other
APIs, but wrapping a future in `success` or `store` is unnecessary just to await it in a coroutine.
