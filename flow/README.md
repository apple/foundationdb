Flow Tutorial
=============

   * [Using Flow](#using-flow)
      * [Keywords/primitives](#keywordsprimitives)
         * [Promise, Future](#promise-future)
         * [Network traversal](#network-traversal)
         * [wait()](#wait)
         * [ACTOR](#actor)
         * [State Variables](#state-variables)
         * [Void](#void)
         * [PromiseStream&lt;&gt;, FutureStream&lt;&gt;](#promisestream-futurestream)
         * [waitNext()](#waitnext)
         * [choose / when](#choose--when)
         * [Future composition](#future-composition)
      * [Design Patterns](#design-patterns)
         * [RPC](#rpc)
         * [ACTOR return values](#actor-return-values)
      * [“gotchas”](#gotchas)
         * [Actor compiler](#actor-compiler)
            * [Switch statements](#switch-statements)
            * [try/catch with no wait()](#trycatch-with-no-wait)
         * [ACTOR cancellation](#actor-cancellation)
   * [Memory Management](#memory-management)
      * [Reference Counting](#reference-counting)
         * [Potential Gotchas](#potential-gotchas)
            * [Reference Cycles](#reference-cycles)
      * [Arenas](#arenas)
         * [Potential Gotchas](#potential-gotchas-1)
            * [Function Creating and Returning a non-Standalone Ref Object](#function-creating-and-returning-a-non-standalone-ref-object)
            * [Assigning Returned Standalone Object to non Standalone Variable](#assigning-returned-standalone-object-to-non-standalone-variable)
            * [Use of Standalone Objects in ACTOR Functions](#use-of-standalone-objects-in-actor-functions)

# Using Flow

Flow introduces some new keywords and flow controls. Combining these into workable units
also introduces some new design patterns to C++ programmers.

## Keywords/primitives

The essence of Flow is the capability of passing messages asynchronously between
components. The basic data types that connect asynchronous senders and receivers are
`Promise<>` and `Future<>`. The sender holds a `Promise<X>` to, sometime in the future, deliver
a value of type `X` to the holder of the `Future<X>`. A receiver, holding a `Future<X>`, at some point
needs the `X` to continue computation, and invokes the `wait(Future<> f)` statement to pause
until the value is delivered. To use the `wait()` statement, a function needs to be declared as an
ACTOR function, a special flow keyword which directs the flow compiler to create the necessary
internal callbacks, etc. Similarly, When a component wants to deal not with one asynchronously
delivered value, but with a series, there are `PromiseStream<>` and `FutureStream<>`. These
two constructs allow for “reliable delivery” of messages, and play an important role in the design
patterns.

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

### Network traversal

Promises and futures can be used within a single process, but their real strength in a distributed
system is that they can traverse the network. For example, one computer could create a
promise/future pair, then send the promise to another computer over the network. The promise
and future will still be connected, and when the promise is fulfilled by the remote computer, the
original holder of the future will see the value appear.

[TODO: network delivery guarantees]

### wait()

Wait allows for the calling code to pause execution while the value of a `Future` is set. This
statement is called with a `Future<T>` as its parameter and returns a `T`; the eventual value of the
`Future`. Errors that are generated in the code that is setting the `Future`, will be thrown from
the location of the `wait()`, so `Error`s must be caught at this location.

The following example shows a snippet (from an ACTOR) of waiting on a `Future`:

```c++
Future<int> f = asyncCalculation(); // defined elsewhere
int count = wait( f );
printf( "%d\n", count );
```

It is worth nothing that, although the function `wait()` is declared in [actorcompiler.h](actorcompiler.h), this
“function” is compiled by the Actor Compiler into a complex set of integrated statements and
callbacks. It is therefore never present in generated code or at link time.
**Note** : because of the way that the actor compiler is built, `wait()` must always assign the
resulting value to a _newly declared variable._

From 6.1, `wait()` on `Void` actors shouldn't assign the resulting value. So, the following code

```c++
Future<Void> asyncTask(); //defined elsewhere
Void _ = _wait(asyncTask());
```

becomes

```c++
Future<Void> asyncTask(); //defined elsewhere
wait(asyncTask());
```

### ACTOR

The only code that can call the `wait()` function are functions that are themselves labeled with
the “ACTOR” tag. This is the essential unit of asynchronous work that can be chained together
to create complex message-passing systems.
An actor, although declared as returning a `Future<T>`, simply returns a `T`. Because an actor
may wait on the results of other actors, an actor must return either a `Future `or `void`. In most
cases returning `void `is less advantageous than returning a `Future`, since there are
implications for actor cancellation. See the Actor Cancellation section for details.

The following simple actor function waits on the `Future` to be ready, when it is ready adds `offset` and returns the result:

```c++
ACTOR Future<int> asyncAdd(Future<int> f, int offset) {
    int value = wait( f );
    return value + offset;
}
```

### State Variables

Since ACTOR-labeled functions are compiled into a c++ class and numerous supporting
functions, the variable scoping rules that normally apply are altered. The differences arise out
of the fact that control flow is broken at wait() statements. Generally the compiled code is
broken into chunks at wait statements, so scoping variables so that they can be seen in multiple
“chunks” requires the `state `keyword.
The following function waits on two inputs and outputs the sum with an offset attached:

```c++
ACTOR Future<int> asyncCalculation(Future<int> f1, Future<int> f2, int offset ) {
    state int value1 = wait( f1 );
    int value2 = wait( f2 );
    return value1 + value2 + offset;
}
```

### Void

The `Void `type is used as a signalling-only type for coordination of asynchronous processes.
The following function waits on an input, send an output to a `Promise`, and signals completion:

```c++
ACTOR Future<Void> asyncCalculation(Future<int> f, Promise<int> p, int offset ) {
    int value = wait( f );
    p.send( value + offset );
    return Void();
}
```

### PromiseStream<>, FutureStream<>

PromiseStream ​and `FutureStream` are groupings of a series of asynchronous messages.


These allow for two important features: multiplexing and network reliability, discussed later.
They can be waited on with the `waitNext()` function.

### waitNext()

Like `wait()`, `waitNext()` pauses program execution and awaits the next value in a
`FutureStream`. If there is a value ready in the stream, execution continues without delay. The
following “server” waits on input, send an output to a `PromiseStream`:

```c++
ACTOR void asyncCalculation(FutureStream<int> f, PromiseStream<int> p, int offset ) {
    while( true ) {
        int value = waitNext( f );
        p.send( value + offset );
    }
}
```

### choose / when

The `choose / when` construct allows an Actor to wait for multiple `Future `events at once in a
ordered and predictable way. Only the `when` associated with the first future to become ready
will be executed. The following shows the general use of choose and when:

```c++
choose {
    when( int number = waitNext( futureStreamA ) ) {
        // clause A
    }
    when( std::string text = wait( futureB ) ) {
        // clause B
    }
}
```

You can put this construct in a loop if you need multiple `when` clauses to execute.

### Future composition

Futures can be chained together with the result of one depending on the output of another.

```c++
ACTOR Future<int> asyncAddition(Future<int> f, int offset ) {
    int value = wait( f );
    return value + offset;
}

ACTOR Future<int> asyncDivision(Future<int> f, int divisor ) {
    int value = wait( f );
    return value / divisor;
}

ACTOR Future<int> asyncCalculation( Future<int> f ) {
    int value = wait( asyncDivision(
    asyncAddition( f, 10 ), 2 ) );
    return value;
}
```


## Design Patterns

### RPC

Many of the “servers” in FoundationDB that communicate over the network expose their interfaces as a struct of PromiseStreams--one for each request type. For instance, a logical server that keeps a count could look like this:

```c++
struct CountingServerInterface {
    PromiseStream<int> addCount;
    PromiseStream<int> subtractCount;
    PromiseStream<Promise<int>> getCount;

    // serialization code required for use on a network
    template <class Ar>
    void serialize( Ar& ar ) {
        serializer(ar, addCount, subtractCount, getCount);
    }
};
```

Clients can then pass messages to the server with calls such as this:

```c++
CountingServerInterface csi = ...; // comes from somewhere
csi.addCount.send(5);
csi.subtractCount.send(2);
Promise<int> finalCount;
csi.getCount.send(finalCount);
int value = wait( finalCount.getFuture() );
```

There is even a utility function to take the place of the last three lines: [TODO: And is necessary
when sending requests over a real network to ensure delivery]

```c++
CountingServerInterface csi = ...; // comes from somewhere
csi.addCount.send(5);
csi.subtractCount.send(2);
int value = wait( csi.getCount.getReply<int>() );
```

Canonically, a single server ACTOR that implements the interface is a loop with a choose
statement between all of the request types:

```c++
ACTOR void serveCountingServerInterface(CountingServerInterface csi) {
    state int count = 0;
    loop {
        choose {
            when (int x = waitNext(csi.addCount.getFuture())){
                count += x;
            }
            when (int x = waitNext(csi.subtractCount.getFuture())){
                count -= x;
            }
            when (Promise<int> r = waitNext(csi.getCount.getFuture())){
                r.send( count ); // goes to client
            }
        }
    }
}
```

In this example, the add and subtract interfaces modify the count itself, stored with a state
variable. The get interface is a bit more complicated, taking a `Promise<int>` instead of just an
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
    Note that, when A, B, C are tables, this is generally compatible with other languages.  If A, B, C are scalars, strings or structs, the corresponding IDL contains things like `struct S { .. }; union T { S, ... }`, which is only partially supported by flatcc (and, in particular, does not work in Rust).  See:  https://github.com/google/flatbuffers/issues/7220
    
    The .fbs files we've checked in work around this by hardcoding one of the variant types; as in the workaround below.

    As luck would have it, `union U { [ubyte], ... }` is invalid .fbs syntax, as vectors cannot be directly embedded into union types.  Fortunately, we do not use IDL strings in FDB, and vectors of bytes cannot be embedded in structs.  This forces unions involving StringRefs to be unions of tables, which are supported in Rust.  See the `OptionalString` definition in `common.fbs` for an example.
    ```
    using T = std::variant<A, B, C>;

    // IDL equivalent
    union T { A, B, C}

    // IDL workaround for Rust, etc -- will fail to parse if it encounters anything structurally incompatible with A.
    // Only needed if A is *not* a table; unions of tables work normally in Rust.
    struct T {
        tag_hack: ubyte;  // Must be 1 (or 2 if hardcoding B, 3 for C, etc...)
        a: A;
    }

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
    - Optional
    (Only tested when T is a table; not sure whether Void will be a struct or table in other circumstances)
    ```
    // Flow type
    Optional<T>

    // IDL equivalent
    table Void {}
    union U { T, Void }
    ```
    - Maps
    ```
    // Flow type
    std::map<T, U>

    // IDL equivalent
    table PairTU { t: T, u: U, }
    [PairTU]
    ```
Note that fields inherited from superclasses appear last in the IDL definition.

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

    Compatibility note:  FlatBuffer file identifiers must be a 4 byte array that is
    also valid UTF-8.  FDB does not currently enforce this, and uses file identifiers
    that violate this rule.  As a workaround, third party tools need to serialize
    with a fake file identifier, then overwrite bytes [4..8) of the serialized buffer
    before sending it over the wire to FDB.  When deserializing, they need to
    perform the reverse translation.  Note that flatbuffer file identiers are
    optional, and that including one changes the layout of the serialized flatbuffer
    data.  Therefore, telling the serialization logic to omit the file_identifier,
    or telling the deserializer to simply ignore it is *NOT* adequate.  You must
    manually rewrite the serialized data before passing it to/from standard
    flatbuffer implementations.

    This may be fixed in the future, but doing so will force us to rewrite FDB's
    file identifiers to be valid UTF-8, and audit the usage of (or replace)
    ComposedIdentifier.  The above was written during the 7.2 release cycle.

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

### ACTOR return values

An actor can have only one returned Future, so there is a case that one actor wants to perform
some operation more than once:

```c++
ACTOR Future<Void> periodically(PromiseStream<Void> ps, int seconds) {
    loop {
        wait( delay( seconds ) );
        ps.send(Void());
    }
}
```

In this example, the `PromiseStream `is actually a way for the actor to return data from some
operation that it ongoing.

By default it is a compiler error to discard the result of a cancellable actor. If you don't think this is appropriate for your actor you can use the `[[flow_allow_discard]]` attribute.
This does not apply to UNCANCELLABLE actors.

## “gotchas”

### Actor compiler

There are some things about the actor compiler that can confuse and may change over time

#### Switch statements

Do not use these with wait statements inside!

#### try/catch with no wait()

When a `try/catch` block does not `wait()` the blocks are still decomposed into separate
functions. This means that variables that you want to access both before and after such a block
will need to be declared state.

### ACTOR cancellation

When the reference to the returned `Future` of an actor is dropped, that actor will be cancelled.
Cancellation of an actor means that any `wait()`s that were currently active (the callback was
currently registered) will be delivered an exception (`actor_cancelled`). In almost every case
this exception should not be caught, though there are certainly exceptions!

# Memory Management

## Reference Counting

The FoundationDB solution uses reference counting to manage the lifetimes of many of its
constituent classes. In order for a class `T` to be reference counted, the following two globally
defined methods must be defined (see [FastRef.h](FastRef.h)):


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


#### Use of Standalone Objects in ACTOR Functions

Special care needs to be taken when using using `Standalone` values in actor functions.
Consider the following example:

```
ACTOR Future<void> foo(StringRef param)
{
    //Do something
    return Void();
}

ACTOR Future<Void> bar()
{
    Standalone<StringRef> str("string");
    wait(foo(str));
    return Void();
}
```

Although it appears at first glance that `bar` keeps the `Arena` for `str` alive during the call to `foo`,
it will actually go out of scope in the class generated by the actor compiler. As a result, `param` in
`foo` will become invalid. To prevent this, either declare `param` to be of type
`Standalone<StringRef>` or make `str` a state variable.
