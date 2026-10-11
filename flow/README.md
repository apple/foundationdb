Flow Tutorial
=============

   * [Using Flow](#using-flow)
      * [Primitives](#primitives)
         * [Promise, Future](#promise-future)
         * [Network messaging](#network-messaging)
         * [co_await](#co_await)
         * [Coroutines](#coroutines)
         * [Local variables and lifetimes](#local-variables-and-lifetimes)
         * [Void](#void)
         * [PromiseStream&lt;&gt;, FutureStream&lt;&gt;](#promisestream-futurestream)
         * [Racing futures](#racing-futures)
         * [Future composition](#future-composition)
      * [Design Patterns](#design-patterns)
         * [Request/reply](#requestreply)
         * [Flatbuffers/ObjectSerializer](#flatbuffersobjectserializer)
         * [Coroutine return values](#coroutine-return-values)
      * [“gotchas”](#gotchas)
         * [Exception handling](#exception-handling)
         * [Coroutine cancellation](#coroutine-cancellation)
   * [Memory Management](#memory-management)
      * [Reference Counting](#reference-counting)
         * [Potential Gotchas](#potential-gotchas)
            * [Reference Cycles](#reference-cycles)
      * [Arenas](#arenas)
         * [Potential Gotchas](#potential-gotchas-1)
            * [Function Creating and Returning a non-Standalone Ref Object](#function-creating-and-returning-a-non-standalone-ref-object)
            * [Assigning Returned Standalone Object to non Standalone Variable](#assigning-returned-standalone-object-to-non-standalone-variable)
            * [Use of Standalone Objects in Coroutines](#use-of-standalone-objects-in-coroutines)

# Using Flow

Flow provides asynchronous communication and cooperative scheduling using standard C++
coroutines. Include `flow/flow.h` for the runtime types and `flow/CoroUtils.h` for
`race()`. Coroutine code lives in ordinary `.cpp` and `.h` files.

See [the coroutine design guide](../design/coroutines.md) for more details and
[the runnable coroutine tutorial](../documentation/coro_tutorial/tutorial.cpp) for
examples that use the network and FoundationDB client APIs.

## Primitives

The essence of Flow is the capability of passing messages asynchronously between
components. The basic data types that connect asynchronous senders and receivers are
`Promise<>` and `Future<>`. The sender holds a `Promise<X>` to, sometime in the future, deliver
a value of type `X` to the holder of the `Future<X>`. A receiver, holding a `Future<X>`, at some point
needs the `X` to continue computation, and uses `co_await` to suspend until the value is
delivered. Other coroutines can run while it is suspended. When a component wants to deal
with a series of asynchronously delivered values, it uses `PromiseStream<>` and
`FutureStream<>`.

### Promise<T>, Future<T>

`Promise<T>` and `Future<T>` are intrinsically linked (they go in pairs) and are two wrappers
around a construct called `SingleAssignmentVar`, a variable that can be set only once. A
Promise is a handle to a `SingleAssignmentVar` that allows for a one-time set of the value; a
Future is a read-only handle to the variable that only allows reading of the value.

The following example uses these two simple types:

```c++
Promise<int> p;
Future<int> f = p.getFuture();
p.send( 4 );
printf( "%d\n", f.get() ); // f is already set
```

### Network messaging

`Promise<T>` and `Future<T>` are local process handles. FoundationDB's RPC layer builds
network communication on top of these primitives with `RequestStream<T>` and
`ReplyPromise<T>`. A request can carry a reply promise to another process; sending the reply
there makes the caller's future ready. See the [request/reply example](#requestreply) below
for the local form of this pattern.

### co_await

`co_await` suspends a coroutine until a future becomes ready. If the future is already ready,
execution continues without suspending. Awaiting a failed future throws its `Error` at the
`co_await` expression.

The following snippet waits on a `Future<int>` inside a coroutine:

```c++
Future<int> f = asyncCalculation(); // defined elsewhere
int count = co_await f;
printf( "%d\n", count );
```

Await a `Future<Void>` without assigning the result:

```c++
Future<Void> asyncTask(); //defined elsewhere
co_await asyncTask();
```

### Coroutines

A function containing `co_await` or `co_return` is a coroutine. A Flow coroutine returning
`Future<T>` produces its result with `co_return value;`. The C++ compiler preserves its
execution state across suspension points.

Calling a coroutine that returns `Future<T>` starts it immediately. It runs until it completes
or awaits something that is not ready. Retain the returned future for work that must continue; see
[coroutine cancellation](#coroutine-cancellation).

The following function waits for a value, adds `offset`, and returns the result:

```c++
Future<int> asyncAdd(Future<int> f, int offset) {
    int value = co_await f;
    co_return value + offset;
}
```

### Local variables and lifetimes

Local variables follow normal C++ scope rules and remain alive across suspension while
their scope is active. The following function retains `value1` while waiting for `f2`:

```c++
Future<int> asyncCalculation(Future<int> f1, Future<int> f2, int offset) {
    int value1 = co_await f1;
    int value2 = co_await f2;
    co_return value1 + value2 + offset;
}
```

Parameters passed by value are stored in the coroutine frame. A reference, pointer, or
non-owning view does not keep the referenced object alive; its owner must outlive all uses,
including uses after suspension. Prefer owning parameters such as `Reference<T>` or
`Standalone<T>` when a coroutine must retain an object or its bytes.

Captures in a coroutine lambda belong to the lambda's closure, which may be destroyed while
the coroutine is suspended. Prefer a named coroutine with explicit value parameters when
the work can outlive the call that starts it.

### Void

The `Void `type is used as a signalling-only type for coordination of asynchronous processes.
The following function waits on an input, sends an output to a `Promise`, and signals completion:

```c++
Future<Void> asyncCalculation(Future<int> f, Promise<int> p, int offset) {
    int value = co_await f;
    p.send( value + offset );
    co_return;
}
```

### PromiseStream<>, FutureStream<>

`PromiseStream<T>` sends a series of values, and `FutureStream<T>` receives them. Await a
`FutureStream<T>` directly to consume its next value. If a value is already queued, execution
continues without suspension. The following server waits for input and sends the result to a
`PromiseStream<int>`:

```c++
Future<Void> asyncCalculation(FutureStream<int> f, PromiseStream<int> p, int offset) {
    while (true) {
        int value = co_await f;
        p.send( value + offset );
    }
}
```

### Racing futures

`race()` waits for the first ready input and returns a `std::variant` whose index identifies
the winning argument. Inputs can be futures or streams; a winning stream consumes one
element. If multiple inputs are already ready, the lowest argument index wins.

```c++
auto result = co_await race(futureStreamA, futureB);
if (result.index() == 0) {
    int number = std::get<0>(result);
    // Handle the stream value.
} else {
    std::string text = std::get<1>(result);
    // Handle the future value.
}
```

Errors propagate from the winning input. Losing inputs are detached from the race, not
explicitly cancelled. They can still be cancelled if releasing the race drops their last future
reference. Retain a future separately when its operation must continue after losing a race.

Put the race in a loop to process a sequence of events. A completed non-stream future remains
ready, so replace it or remove it from the race after handling it.

### Future composition

Futures can be chained together with the result of one depending on the output of another.

```c++
Future<int> asyncAddition(Future<int> f, int offset) {
    int value = co_await f;
    co_return value + offset;
}

Future<int> asyncDivision(Future<int> f, int divisor) {
    int value = co_await f;
    co_return value / divisor;
}

Future<int> asyncCalculation(Future<int> f) {
    co_return co_await asyncDivision(asyncAddition(f, 10), 2);
}
```


## Design Patterns

### Request/reply

Many logical servers expose one request stream per request type. This local example uses
promise streams to maintain a count. A network interface uses the RPC types described
[above](#network-messaging) and also needs serialization.

```c++
struct CountingServerInterface {
    PromiseStream<int> addCount;
    PromiseStream<int> subtractCount;
    PromiseStream<Promise<int>> getCount;
};
```

Clients can then pass messages to the server with calls such as this:

```c++
Future<int> updateAndReadCount(CountingServerInterface csi) {
    csi.addCount.send(5);
    csi.subtractCount.send(2);
    Promise<int> finalCount;
    csi.getCount.send(finalCount);
    co_return co_await finalCount.getFuture();
}
```

A single server coroutine handles requests by repeatedly racing the request streams:

```c++
Future<Void> serveCountingServerInterface(CountingServerInterface csi) {
    int count = 0;
    while (true) {
        auto request = co_await race(csi.addCount.getFuture(),
                                     csi.subtractCount.getFuture(),
                                     csi.getCount.getFuture());
        switch (request.index()) {
        case 0:
            count += std::get<0>(request);
            break;
        case 1:
            count -= std::get<1>(request);
            break;
        case 2:
            std::get<2>(request).send(count);
            break;
        }
    }
}
```

The caller must keep the server's returned `Future<Void>` alive while the server is needed.
The add and subtract interfaces modify the count, which remains alive across each suspension.
The get interface takes a `Promise<int>` instead of just an
int. In the interface class, you can see a `PromiseStream<Promise<int>>`. This is a common
construct that is analogous to sending someone a self-addressed envelope. You send a
promise to a someone else, who then unpacks it and send the answer back to you, because
you are holding the corresponding future.

### Flatbuffers/ObjectSerializer

1. Introduction

    The goal is to have a more robust serialization protocol.  One feature of
    flatbuffers is that you can add a new field to a network message without
    requiring a protocol-incompatible upgrade. In order for this to work,
    correctness must not depend on that field always being present. This can be
    tested in simulation by randomly (use buggify) default-initializing that
    field when deserializing. Once you make a protocol-incompatible upgrade you
    can rely on the field always being present in the new protocol, just like
    before. Currently we are using a custom flatbuffers implementation so to
    that we can present (roughly) the same serialization api as before.
    Currently the ObjectSerializer is only used for network messages, but that
    may change.  Flatbuffers was selected because it is (relatively) simple
    among protocols providing forwards/backwards compatibility, and its binary
    format is [well
    documented](https://github.com/dvidelabs/flatcc/blob/master/doc/binary-format.md)

1. Correspondence to flatbuffers IDL
    - Tables
    ```
    // Flow type
    struct A {
        constexpr static FileIdentifier file_identifier = 12345;
        int a;
        template <class Ar>
        void serialize(Ar& ar) {
            serializer(ar, a);
        }
    }

    // IDL equivalent
    table A {
        a:int;
    }
    ```
    - Unions
    ```
    // Flow type
    using T = std::variant<A, B, C>;

    // IDL equivalent
    union T { A, B, C}
    ```
    - Strings (there's a string type in the idl that guarantees null termination, but flow does not, so it's comparable to a vector of bytes)
    ```
    // Flow type
    StringRef, std::string

    // IDL equivalent
    [ubyte]
    ```
    - Vectors
    ```
    // Flow type
    VectorRef<T>, std::vector<T>

    // IDL equivalent
    [T]
    ```

1. Flatbuffers Traits

    In order to serialize a type as a flatbuffers vector, struct, or union, you can implement the appropriate trait for your type.
    - `scalar_traits` corresponds to a flatbuffers struct. See `UID` for an example.
    - `vector_like_traits` corresponds to a flatbuffers vector. See `VectorRef` for an example.
    - `dynamic_size_traits` corresponds to a flatbuffers vector of uint8_t. See `StringRef` for an example.
    - `union_like_traits` corresponds to a flatbuffers union. See `std::variant` for an example.

1. Potential Gotchas
    - Flatbuffers 'vtables' are collected from default-constructed instances of
      each type. Consequently types serialized by flatbuffers should have cheap
      default constructors. Future work: we may be able to collect vtables
      without an instance of a type using `declval`.

    - `T::serialize` may get called multiple times when serializing `T`. It is
      guaranteed to be called only once for deserialization though, and thus
      the `Ar::isDeserializing` idiom is appropriate. Future work: in theory we
      don't need to call `T::serialize` multiple times when serializing, but
      this would complicate the implementation.

   - In a call to `serializer`, arenas must come after any members whose memory
     the arena owns. It's safe to reorder an arena in a `serializer` call
     because arenas are ignored for the flatbuffers schema. (Future work)
     Enforce that no fields appear after an arena at compile time.

1. File identifiers

    [File identifiers](https://google.github.io/flatbuffers/md__schemas.html)
    are used to sanity check that the message you're deserializing is of the
    schema you expect. You can give a type `T` a file identifier by making
    `T::file_identifier` a static member of type `FileIdentifier`. If you don't
    control `T`, you can specialize the `FileIdentifierFor` template. See
    `flow/FileIdentifier.h` for examples. You don't need to change the file
    identifier for a type when evolving its schema.

1. Schema evolution

    Two schemas are forward/backward compatible if they meet the following
    requirements. (Future work) Any fields that are not common to both schemas should be
    default-initialized in deserialized messages. Currently they will be
    uninitialized if their default constructor doesn't initialize.

    - Two tables are compatible if one table's fields are all compatible with a prefix of the other table's fields.
    - Two vectors are compatible if their element types are compatible.
    - Two unions are compatible if one union's fields are all compatible with a prefix of the other union's fields.
    - Two scalar types are only compatible if they are equal.

1. Deprecation

    Flatbuffers allows fields to be deprecated, and a deprecated field consumes
    only two bytes on the wire. (Future work) Introduce `Deprecated<...>`
    template or something similar so that we can write smaller messages for
    deprecated fields.

### Coroutine return values

A coroutine's returned future completes only once. Use a promise stream to send repeated
results while the coroutine is running:

```c++
Future<Void> periodically(PromiseStream<Void> ps, int seconds) {
    while (true) {
        co_await delay(seconds);
        ps.send(Void());
    }
}
```

Keep the returned `Future<Void>` alive for as long as periodic notifications are needed.
Its lifetime controls the work; the stream carries the notifications.

## “gotchas”

### Exception handling

An error from an awaited future is thrown at the `co_await` expression. Catch errors around
the operation that can fail, and propagate errors that the coroutine cannot handle. C++ does
not allow `co_await` inside a `catch` handler. If recovery itself is asynchronous, save the
error and await the recovery operation after leaving the handler.

### Coroutine cancellation

By default, dropping the last reference to a pending coroutine's returned `Future` cancels
that coroutine. An explicit `Future::cancel()` also requests cancellation. A suspended
coroutine resumes by throwing `actor_cancelled` from its await; local objects are destroyed
as their scopes unwind. Do not discard a future when its work must continue.

Do not swallow `actor_cancelled` in an error or retry handler. Rethrow it after any required
synchronous cleanup so that cancellation can finish. Preserve `broken_promise` and other
errors unless the caller's contract explicitly handles them.

# Memory Management

## Reference Counting

The FoundationDB solution uses reference counting to manage the lifetimes of many of its
constituent classes. In order for a class `T` to be reference counted, the following two globally
defined methods must be defined (see [FastRef.h](include/flow/FastRef.h)):


```c++
void addref(T*);
void delref(T*);
```

The easiest way to implement these methods is by making your class a descendant of
`ReferenceCounted`.

NOTE: Any descendants of `ReferenceCounted` should either have virtual destructors or be
sealed. If you fail to meet these criteria, then references to descendants of your class will never
be deleted.

If you choose not to inherit from `ReferenceCounted`, you will have to manage the reference
count yourself. One way this can be done is to define `void addref()` and `void delref()`
methods on your class, which will make it compatible with the existing global `addref` and
`delref` methods. Otherwise, you will need to create the global `addref` and `delref` methods
for your class, as mentioned above. In either case, you will need to manage the reference
count on your object and delete it when the count reaches 0. Note that the reference count
should usually be initialized to 1, as the `addRef(T*)` function is not called when the object is
created.

To create a reference counted instance of a class `T`, you instantiate a `Reference<T>` on the
stack with a pointer to your `T` object:

```c++
auto refCountedInstance = makeReference<T>();
```
The `Reference<T>` class automatically calls addref on your `T` instance every time it is copied
(such as by argument passing or assignment), but not when the object is initially created
(consequently, `ReferenceCounted` classes are initialized with reference count 1). It will call
`delref` on your `T` instance whenever a particular `Reference<T>` instance gets deleted (usually
by going out of scope). When no more instances of `Reference<T>` holding a particular `T`
instance exist, then that `T` instance will be destroyed.

### Potential Gotchas

#### Reference Cycles

You must be cautious about creating reference cycles when using reference counting. For
example, if two `Reference<T>` objects refer to each other, then without specific intervention
their reference counts will never reach 0 and the objects will never be deleted.

## Arenas


In addition to using reference counting, the FoundationDB solution also uses memory pools to
allocate buffers. In this scheme, buffers are allocated from a common pool, called an `Arena`,
and remain valid for the entire lifetime of that `Arena`. When the `Arena` is destroyed, all of the
memory it held for the buffers is deallocated along with it. As a general convention, types which
can use these `Arenas` and do not manage their own memory are given the "`Ref`" suffix. When
a `*Ref` object is being used, consideration should be given to how its buffers are being
managed (much in the same way that you would consider memory management when you see
a `T*`).

As an example, consider the `StringRef` class. A `StringRef` is an object which contains a
pointer to a sequence of bytes, but does not actually manage that buffer. Thus, if a `StringRef`
is deallocated, the data remains intact. Conversely, if the data is deallocated, the `StringRef`
becomes invalid. In order for the `StringRef` to manage its own buffer, we need to create an
instance of the `Standalone<StringRef>` class:

```c++
Standalone<StringRef> str("data");
```

A `Standalone<T>` object has its own arena (technically, it is an `Arena`), and for classes like
`StringRef` which support the use of arenas, the memory buffers used by the class are
allocated from that arena. `Standalone<T>` is also a subclass of `T`, and so for all other purposes
operates just like a `T`.

There are a number of classes which support the use of arenas, and some which have
convenience types for their `Standalone` versions (not a complete list):

|        T         | Standalone<T> alias |
|:----------------:|:-------------------:|
| StringRef        | N/A                 |
| KeyRef           | Key                 |
| ValueRef         | Value               |
| KeyValueRef      | KeyValue            |
| KeyRangeRef      | KeyRange            |
| KeySelectorRef   | KeySelector         |
| VectorRef        | N/A                 |

The `VectorRef<T>` class is an `std::vector`-like object which is used to manage a list of these
`*Ref` objects. A `Standalone<VectorRef<T>>` has its own arena which can be used to store
the buffers held by its constituents. In order for that to happen, one of the two deep insertion
methods (`push_back_deep` or `append_deep`) should be used when placing items in the vector.
The shallow insertion methods will hold the objects only; any arena-managed memory is not
copied. Thus, the `Standalone<VectorRef<T>>` will hold the `T` objects without managing their
memory. Note that the arena(s) used by the `VectorRef` need not be its own (and cannot be
unless the `VectorRef` is a `Standalone` object), and are determined by arguments to the
functions that insert items.

`VectorRef<T>` can also be used with types besides the standard `Ref` types, in which case the
deep copy methods should not be used. In this case, the `VectorRef<T>` object holds the items
in an arena much like a normal vector would hold the items in its buffer. Again, the arena used
by the `VectorRef<T>` need not be its own.

When a `Standalone<T>` is copied (e.g. by argument passing or assignment) to another
`Standalone<T>`, they will share the same memory. The actual memory contents of the arena
are stored in a reference counted structure (`ArenaBlock`), so the memory will persist until all
instances of `Arena` holding that memory are destroyed. If instead a `T` object is copied to a
`Standalone<T>`, then its entire contents are copied into the arena of the new `Standalone<T>`
object using a deep copy. Thus, it is generally more efficient to consistently use `*Ref` objects
and manage the memory with something external, or to consistently use `Standalone<T>`
objects (where assignments just increment reference counters) to avoid memory copies.

### Potential Gotchas

#### Function Creating and Returning a non-Standalone Ref Object

A function which creates a `Ref` object should generally return a `Standalone` version of that
object. Otherwise, make certain that the `Arena` on which that `Ref` object was created still exists
when the caller uses the returned `Ref`.

#### Assigning Returned Standalone Object to non Standalone Variable

A caller which receives a `Standalone` return value should assign that return value to a
`Standalone` variable. Consider the following example:

```c++
Standalone<StringRef> foo() {
    return Standalone<StringRef>("string");
}

void bar() {
    StringRef val = foo();
}
```

When `val` is copy-assigned in `bar`, its data is stored in the `Arena` of the `StringRef` that was
returned from `foo`. When this returned `StringRef` is subsequently deallocated, `val` will no
longer be valid.


#### Use of Standalone Objects in Coroutines

An owning local remains alive across suspension while its scope is active. A non-owning
`StringRef` still does not retain its arena. When a coroutine needs to own bytes independently
of its caller, pass a `Standalone<StringRef>` by value:

```c++
Future<Void> printLater(Standalone<StringRef> text) {
    co_await delay(1.0);
    printf("%s\n", text.toString().c_str());
    co_return;
}

Future<Void> printMessage() {
    Standalone<StringRef> text("string"_sr);
    co_await printLater(text);
    co_return;
}
```

Both coroutines retain the arena in this example. Passing a `StringRef` instead would be safe
only if its owner remained alive until the callee finished using it. The same rule applies to
`KeyRef`, `ValueRef`, and other views into arena-backed storage.
