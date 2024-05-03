# Coroutines in Flow

* [Introduction](#Introduction)
* [Coroutines vs ACTORs](#coroutines-vs-actors)
* [Basic Types](#basic-types)
* [Choose-When](#choose-when)
    * [Execution in when-expressions](#execution-in-when-expressions)
    * [Waiting in When-Blocks](#waiting-in-when-blocks)
* [Generators](#generators)
    * [Generators and ranges](#generators-and-ranges)
    * [Eager vs Lazy Execution](#eager-vs-lazy-execution)
    * [Generators vs Promise Streams](#generators-vs-promise-streams)
* [Uncancellable](#uncancellable)
* [Porting ACTOR's to C++ Coroutines](#porting-actors-to-c-coroutines)
    * [Lifetime of Locals](#lifetime-of-locals)
    * [Unnecessary Helper Actors](#unnecessary-helper-actors)
    * [Replace Locals with Temporaries](#replace-locals-with-temporaries)
    * [Don't Wait in Error-Handlers](#dont-wait-in-error-handlers)
    * [Make Static Functions Class Members](#make-static-functions-class-members)
    * [Initialization of Locals](#initialization-of-locals)

## Introduction

In the past Flow implemented an actor mode by shipping its own compiler which would
extend the C++ language with a few additional keywords. This, while still supported,
is deprecated in favor of the standard C++20 coroutines.

Coroutines are meant to be simple, look like serial code, and be easy to reason about.
As simple example for a coroutine function can look like this:

```c++
Future<double> simpleCoroutine() {
    double begin = now();
    co_await delay(1.0);
    co_return now() - begin;
}
```

This document assumes some familiarity with Flow. As of today, actors and coroutines
can be freely mixed, but new code should be written using coroutines.

## Coroutines vs ACTORs

It is important to understand that C++ coroutine support doesn't change anything in Flow: they are not a replacement
of Flow but they replace the actor compiler with a C++ compiler. This means, that the network loop, all Flow types,
the RPC layer, and the simulator all remain unchanged. A coroutine simply returns a special `SAV<T>` which has handle
to a coroutine.

## Basic Types

As defined in the C++20 standard, a function is a coroutine if its body contains at least one `co_await`, `co_yield`,
or `co_return` statement. However, in order for this to work, the return type needs an underlying coroutine
implementation. Flow provides these for the following types:

* `Future<T>` is the primary type we use for coroutines. A coroutine returning
  `Future<T>` is allowed to `co_await` other coroutines and it can `co_return`
  a single value. `co_yield` is not implemented by this type.
    * A special case is `Future<Void>`. Void-Futures are what a user would probably
      expect `Future<>` to be (it has this type for historical reasons and to
      provide compatibility with old Flow `ACTOR`s). A coroutine with return type
      `Future<Void>` must not return anything. So either the coroutine can run until
      the end, or it can be terminated by calling `co_return`.
* `Generator<T>` can return a stream of values. However, they can't `co_await`
  other coroutines. These are useful for streams where the values are lazily
  computed but don't involve any IO.
* `AsyncGenerator<T>` is similar to `Generator<T>` in that it can return a stream
  of values, but in addition to that it can also `co_await` other coroutines.
  Due to that, they're slightly less efficient than `Generator<T>`.
  `AsyncGenerator<T>` should be used whenever values should be lazily generated
  AND need IO. It is an alternative to `PromiseStream<T>`, which can be more efficient, but is
  more intuitive to use correctly.

A more detailed explanation of `Generator<T>` and `AsyncGenerator<T>` can be
found further down.

## Choose-When

In actor compiled code we were able to use the keywords `choose` and `when` to wait on a
statically known number of futures and execute corresponding code. Something like this:

```c++
choose {
    when(wait(future1)) {
        // do something
    }
    when(Foo f = wait(foo())) {
        // do something else
    }
}
```

Since this is a compiler functionality, we can't use this with C++ coroutines. We could
keep only this feature around, but only using standard C++ is desirable. So instead, we
introduce a new `class` called `Choose` to achieve something very similar:

```c++
co_await Choose()
    .When(future1, [](Void& const) {
        // do something
    })
    .When(foo(), [](Foo const& f) {
        // do something else
    }).Run();
```

While `Choose` and `choose` behave very similarly, there are some minor differences between
the two. These are explained below.

### Execution in when-expressions

In the above example, there is one, potentially important difference between the old and new
style: in the statement `when(Foo f = wait(foo()))` is only executed if `future1` is not ready.
Depending on what the intent of the statement is, this could be desirable. Since `Choose::When`
is a normal method, `foo()` will be evaluated whether the statement is already done or not.
This can be worked around by passing a lambda that returns a Future instead:

```c++
co_await Choose()
    .When(future1, [](Void& const) {
        // do something
    })
    .When([](){ return foo() }, [](Foo const& f) {
        // do something else
    }).Run();
```

The implementation of `When` will guarantee that this lambda will only be executed if all previous
`When` calls didn't receive a ready future.

## Waiting in When-Blocks

In FDB we sometimes see this pattern:

```c++
loop {
  choose {
      when(RequestA req = waitNext(requestAStream.getFuture())) {
          wait(handleRequestA(req));
      }
      when(RequestB req = waitNext(requestBStream.getFuture())) {
          wait(handleRequestb(req));
      }
      //...
  }
}
```

This is not possible to do with `Choose`. However, this is done deliberately as the above is
considered an antipattern: This means that we can't serve two requests concurrently since the loop
won't execute until the request has been served. Instead, this should be written like this:

```c++
state ActorCollection actors(false);
loop {
  choose {
      when(RequestA req = waitNext(requestAStream.getFuture())) {
          actors.add(handleRequestA(req));
      }
      when(RequestB req = waitNext(requestBStream.getFuture())) {
          actors.add(handleRequestb(req));
      }
      //...
      when(wait(actors.getResult())) {
          // this only makes sure that errors are thrown correctly
          UNREACHABLE();
      }
  }
}
```

And so the above can easily be rewritten using `Choose`:

```c++
ActorCollection actors(false);
loop {
    co_await Choose()
        .When(requestAStream.getFuture(), [&actors](RequestA const& req) {
            actors.add(handleRequestA(req));  
        })
        .When(requestBStream.getFuture(), [&actors](RequestB const& req) {
            actors.add(handleRequestB(req));  
        })
        .When(actors.getResult(), [](Void const&) {
            UNREACHABLE();
        }).run();
}
```

However, often using `choose`-`when` (or `Choose().When`) is overkill and other facilities like `quorum` and
`operator||` should be used instead. For example this:

```c++
choose {
    when(R res = wait(f1)) {
        return res;
    }
    when(wait(timeout(...))) {
        throw io_timeout();
    }
}
```

Should be written like this:

```c++
co_await (f1 || timeout(...));
if (f1.isReady()) {
    co_return f1.get();
}
throw io_timeout();
```

(The above could also be packed into a helper function in `genericactors.actor.h`).

## Generators

With C++ coroutines we introduce two new basic types in Flow: `Generator<T>` and `AsyncGenerator<T>`. A generator is a
special type of coroutine, which can return multiple values.

`Generator<T>` and `AsyncGenerator<T>` implement a different interface and serve a very different purpose.
`Generator<T>` conforms to the `input_iterator` trait -- so it can be used like a normal iterator (with the exception
that copying the iterator has a different semantics). This also means that it can be used with the new `ranges`
library in STL which was introduced in C++20.

`AsyncGenerator<T>` implements the `()` operator which returns a new value every time it is called. However, this value
HAS to be waited for (dropping it and attempting to call `()` again will result in undefined behavior!). This semantic
difference allows an author to mix `co_await` and `co_yield` statements in a coroutine returning `AsyncGenerator<T>`.

Since generators can produce infinitely long streams, they can be useful to use in places where we'd otherwise use a
more complex in-line loop. For example, consider the code in `masterserver.actor.cpp` that is responsible generate
version numbers. The logic for this code is currently in a long function. With a `Generator<T>` it can be isolated to
one simple coroutine (which can be a direct member of `MasterData`). A simplified version of such a generator could
look as follows:

```c++
Generator<Version> MasterData::versionGenerator() {
    auto prevVersion = lastEpochEnd;
    auto lastVersionTime = now();
    while (true) {
        auto t1 = now();
        Version toAdd =
			    std::max<Version>(1,
			                      std::min<Version>(SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS,
			                                        SERVER_KNOBS->VERSIONS_PER_SECOND * (t1 - self->lastVersionTime)));
        lastVersionTime = t1;
        co_yield prevVersion + toAdd;
        prevVersion += toAdd;
    }
}
```

Now that the logic to compute versions is separated, `MasterData` can simply create an instance of `Generator<Version>`
by calling `auto vGenerator = MasterData::versionGenerator();` (and possibly storing that as a class member). It can
then access the current version by calling `*vGenerator` and go to the next generator by incrementing the iterator
(`++vGenerator`).

`AsyncGenerator<T>` should be used in some places where we used promise streams before (though not all of them, this
topic is discussed a bit later). For example:

```c++
template <class T, class F>
AsyncGenerator<T> filter(AsyncGenerator<T> gen, F pred) {
    while (gen) {
        auto val = co_await gen();
        if (pred(val)) {
            co_yield val;
        }
    }
}
```

Note how much simpler this function is compared to the old flow function:

```c++
ACTOR template <class T, class F>
Future<Void> filter(FutureStream<T> input, F pred, PromiseStream<T> output) {
	loop {
		try {
			T nextInput = waitNext(input);
			if (pred(nextInput))
				output.send(nextInput);
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			} else
				throw;
		}
	}

	output.sendError(end_of_stream());

	return Void();
}
```

A `FutureStream` can be converted into an `AsyncGenerator` by using a simple helper function:

```c++
template <class T>
AsyncGenerator<T> toGenerator(FutureStream<T> stream) {
    loop {
        try {
            co_yield co_await stream;
        } catch (Error& e) {
            if (e.code() == error_code_end_of_stream) {
                co_return;
            }
            throw;
        }
    }
}
```

### Generators and Ranges

`Generator<T>` can be used like an input iterator. This means, that it can also be used with `std::ranges`. Consider
the following coroutine:

```c++
// returns base^0, base^1, base^2, ...
Generator<double> powersOf(double base) {
    double curr = 1;
    loop {
        co_yield curr;
        curr *= base;
    }
}
```

We can use this now to generate views. For example:

```c++
for (auto v : generatorRange(powersOf(2)) 
            | std::ranges::views::filter([](auto v) { return v > 10; })
            | std::ranges::views::take(10)) {
    fmt::print("{}\n", v);
}
```

The above would print all powers of two between 10 and 2^10.

### Eager vs Lazy Execution

One major difference between async generators and tasks (coroutines returning only one value through `Future`) is the
execution policy: An async generator will immediately suspend when it is called while a task will immediately start
execution and needs to be explicitly scheduled.

This is a conscious design decision. Lazy execution makes it much simpler to reason about memory ownership. For example,
the following is ok:

```c++
Generator<StringRef> randomStrings(int minLen, int maxLen) {
    Arena arena;
    auto buffer = new (arena) uint8_t[maxLen + 1];
    while (true) {
        auto sz = deterministicRandom()->randomInt(minLen, maxLen + 1);
        for (int i = 0; i < sz; ++i) {
            buffer[i] = deterministicRandom()->randomAlphaNumeric();
        }
        co_yield StringRef(buffer, sz);
    }
}
```

The above coroutine returns a stream of random strings. The memory is owned by the coroutine and so it always returns
a `StringRef` and then reuses the memory in the next iteration. This makes this generator very cheap to use, as it only
does one allocation in its lifetime. With eager execution, this would be much harder to write (and reason about): the
coroutine would immediately generate a string and then eagerly compute the next one when the string is retrieved.
However, in Flow a `co_yield` is guarantee to suspend the coroutine until the value was consumed (this is not generally
a guarantee with `co_yield` -- C++ coroutines give the implementor a great degree of freedom over decisions like this).

### Generators vs Promise Streams

Flow provides another mechanism to send streams of messages between actors: `PromiseStream<T>`. In fact,
`AsyncGenerator<T>` uses `PromiseStream<T>` internally. So when should one be used over the other?

As a general rule of thumb: whenever possible, use `Generator<T>`, if not, use `AsyncGenerator<T>` if in doubt.

For pure computation it almost never makes sense to use a `PromiseStream<T>` (the only exception is if computation
can be expensive enough that `co_await yield()` becomes necessary). `Generator<T>` is more lightweight and therefore
usually more efficient. It is also easier to use.

When it comes to IO it becomes a bit more tricky. Assume we want to scan a file on disk, and we want to read it in
4k blocks. This can be done quite elegantly using a coroutine:

```c++
AsyncGenerator<Standalone<StringRef>> blockScanner(Reference<IAsyncFile> file) {
    auto sz = co_await file->size();
    decltype(sz) offset = 0;
    constexpr decltype(sz) blockSize = 4*1024;
    while (offset < sz) {
        Arena arena;
        auto block = new (arena) int8_t[blockSize];
        auto toRead = std::min(sz - offset, blockSize);
        auto r = co_await file->read(block, toRead, offset);
        co_yield Standalone<StringRef>(StringRef(block, r), arena);
        offset += r;
    }
}
```

The problem with the above generator though, is that we only start reading when the generator is invoked. If consuming
the block takes sometimes a long time (for example because it has to be written somewhere), each call will take as long
as the disk latency is for a read.

What if we want to hide this latency? In other words: what if we want to improve throughput and end-to-end latency by
prefetching?

Doing this with a generator, while not trivial, is possible. But here it might be easier to use a `PromiseStream`
(we can even reuse the above generator):

```c++
Future<Void> blockScannerWithPrefetch(Reference<IAsyncFile> file,
                                      PromiseStream<Standalone<StringRef> promise,
                                      FlowLock lock) {
    auto generator = blockScanner(file);
    while (generator) {
        {
            FlowLock::Releaser _(co_await lock.take());
            try {
                promise.send(co_await generator());
            } catch (Error& e) {
                promise.sendError(e);
                co_return;
            }
        }
        // give caller opportunity to take the lock
        co_await yield();
    }
}
```

With the above the caller can control the prefetching dynamically by taking the lock if the queue becomes too full.

## Uncancellable

By default, a coroutine runs until it is either done (reaches the end of the function body, a `co_return` statement,
or throws an exception) or the last `Future<T>` object referencing that object is being dropped. The second use-case is
implemented as follows:

1. When the future count of a coroutine goes to `0`, the coroutine is immediately resumed and `actor_cancelled` is
   thrown within that coroutine (this allows the coroutine to do some cleanup work).
2. Any attempt to run `co_await expr` will immediately throw `actor_cancelled`.

However, some coroutines aren't safe to be cancelled. This usually concerns disk IO operations. With `ACTOR` we could
either have a return-type `void` or use the `UNCANCELLABLE` keyword to change this behavior: in this case, calling
`Future<T>::cancel()` would be a no-op and dropping all futures wouldn't cause cancellation.

However, with C++ coroutines, this won't work:

* We can't introduce new keywords in pure C++ (so `UNCANCELLABLE` would require some preprocessing).
* Implementing a `promise_type` for `void` isn't a good idea, as this would make any `void`-function potentially a
  coroutine.

However, this can also be seen as an opportunity: uncancellable actors are always a bit tricky to use, since we need to
make sure that the caller keeps all memory alive that the uncancellable coroutine might reference until it is done.
Because of that, whenever someone calls a coroutine, they need to be extra careful. However, someone might not know that
the coroutine they call is uncancellable.

We address this problem with the following definition:

---
*Definition*:

A coroutine is uncancellable if the first argument (or the second, if the coroutine is a class-member) is of type
`Uncancellable`

---

The definition of `Uncancellable` is trivial: `struct Uncancellable {};` -- it is simply used as a marker. So now, if
a user calls an uncancellable coroutine, it will be obvious on the caller side. For example the following is *never*
uncancellable:

```c++
co_await foo();
```

But this one is:

```c++
co_await bar(Uncancellable());
```

## Porting `ACTOR`'s to C++ Coroutines

If you have an existing `ACTOR`, you can port it to a C++ coroutine by following these steps:

1. Remove `ACTOR` keyword.
2. If the actor is marked with `UNCANCELLABLE`, remove it and make the first argument `Uncancellable`. If the return
   type of the actor is `void` make it `Future<Void>` instead and add an `Uncancellable` as the first argument.
3. Remove all `state` modifiers from local variables.
4. Replace all `wait(expr)` with `co_await expr`.
5. Remove all `waitNext(expr)` with `co_await expr`.
6. Rewrite existing `choose-when` statements using the `Choose` class.

In addition, the following things should be looked out for:

### Lifetime of locals

Consider this code:

```c++
Local foo;
wait(bar());
...
```

`foo` will be destroyed right after the `wait`-expression. However, after making this a coroutine:

```c++
Local foo;
co_await bar();
...
```

`foo` will stay alive until we leave the scope. This is better (as it is more intuitive and follows standard C++), but
in some weird corner-cases code might depend on the semantic that locals get destroyed when we call into `wait`. Look
out for things where destructors do semantically important work (like in `FlowLock::Releaser`).

### Unnecessary Helper Actors

In `flow/genericactors.actor.h` we have a number of useful helpers. Some of them are also useful with C++ coroutines,
others add unnecessary overhead. Look out for those and remove calls to it. The most important ones are `success` and
`store`.

```c++
wait(success(f));
```

becomes

```c++
co_await f;
```

and

```c++
wait(store(v, f));
```

becomes

```c++
v = co_await f;
```

### Replace Locals with Temporaries

In certain places we use locals just to work around actor compiler limitations. Since locals use up space in the
coroutine object they should be removed wherever it makes sense (only if it doesn't make the code less readable!).

For example:

```c++
Foo f = wait(foo);
bar(f);
```

might become

```c++
bar(co_await foo);
```

### Don't Wait in Error-Handlers

Using `co_await` in an error-handler produces a compilation error in C++. However, this was legal with `ACTOR`. There
is no general best way of addressing this issue, but usually it's quite easy to move the `co_await` expression out of
the `catch`-block.

One place where we use this pattern a lot if in our transaction retry loop:

```c++
state ReadYourWritesTransaction tr(db);
loop {
    try {
        Value v = wait(tr.get(key));
        tr.set(key2, val2);
        wait(tr.commit());
        return Void();
    } catch (Error& e) {
        wait(tr.onError(e));
    }
}
```

Luckily, with coroutines, we can do one better: generalize the retry loop. The above could look like this:

```c++
co_await db.run([&](ReadYourWritesTransaction* tr) -> Future<Void> {
    Value v = wait(tr.get(key));
    tr.set(key2, val2);
    wait(tr.commit());
});
```

A possible implementation of `Database::run` would be:

```c++
template <std:invocable<ReadYourWritesTransaction*> Fun>
Future<Void> Database::run(Fun fun) {
    ReadYourWritesTransaction tr(*this);
    Future<Void> onError;
    while (true) {
        if (onError.isValid()) {
            co_await onError;
            onError = Future<Void>();
        }
        try {
            co_await fun(&tr);
        } catch (Error& e) {
            onError = tr.onError(e);
        }
    }
}
```

### Make Static Functions Class Members

With actors, we often see the following pattern:

```c++
struct Foo : IFoo {
    ACTOR static Future<Void> bar(Foo* self) {
        // use `self` here to access members of `Foo`
    }
    
    Future<Void> bar() override {
        return bar(this);
    }
};
```

This boilerplate is necessary, because `ACTOR`s can't be class members: the actor compiler will generate another
`struct` and move the code there -- so `this` will point to the actor state and not to the class instance.

With C++ coroutines, this limitation goes away. So a cleaner (and slightly more efficient) implementation of the above
is:

```c++
struct Foo : IFoo {
    Future<Void> bar() override {
        // `this` can be used like in any non-coroutine. `co_await` can be used.
    }
};
```

### Initialization of Locals

There is one very subtle and hard to spot difference between `ACTOR` and a coroutine: the way some local variables are
initialized. Consider the following code:

```c++
struct SomeStruct {
    int a;
    bool b;
};

ACTOR Future<Void> someActor() {
    // beginning of body
    state SomeStruct someStruct;
    // rest of body
}
```

For state variables, the actor-compiler generates the following code to initialize `SomeStruct someStruct`:

```c++
someStruct = SomeStruct();
```

This, however, is different from what might expect since now the default constructor is explicitly called. This means
if the code is translated to:

```c++
Future<Void> someActor() {
    // beginning of body
    SomeStruct someStruct;
    // rest of body
}
```

initialization will be different. The exact equivalent instead would be something like this:

```c++
Future<Void> someActor() {
    // beginning of body
    SomeStruct someStruct{}; // auto someStruct = SomeStruct();
    // rest of body
}
```

If the struct `SomeStruct` would initialize its primitive members explicitly (for example by using `int a = 0;` and
`bool b = false`) this would be a non-issue. And explicit initialization is probably the right fix here. Sadly, it
doesn't seem like UBSAN finds these kind of subtle bugs.

Another difference is, that if a `state` variables might be initialized twice: once at the creation of the actor using
the default constructor and a second time at the point where the variable is initialized in the code. With C++
coroutines we now get the expected behavior, which is better, but nonetheless a potential behavior change.