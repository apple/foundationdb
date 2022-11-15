# Swift in FoundationDB

## Build

The CMake build has been prepared with Swift interop in the following modules: 

- flow
- fdbrpc
- fdbclient
- fdbserver

The integration works "both ways", i.e. Swift can call into Flow/C++ code, as well as Flow/C++ can call into Swift.

Swift generates clang modules which can be consumed in C++. For example, the module `fdbserver_swift` contains all swift code in `fdbserver/`.

> Note: you can check, and add new files to the `_swift` targets by locating the command, e.g. `add_library(fdbserver_swft` in [fdbserver/CMakeLists.txt](fdbserver/CMakeLists.txt).

Then, you can then include the generated module in C++:

```cpp
// ... 
#include "SwiftModules/FDBServer"

#include "flow/actorcompiler.h" // This must be the last #include.
```

## Swift Basics

When in doubt about Swift syntax, refer to https://docs.swift.org/swift-book/ or reach out to the team, we're here to help.

## Swift → C++

Swift can import clang modules, and does so using the `import` statement, like so:

```swift
import Flow
import flow_swift
import FDBServer
import FDBClient
import fdbclient_swift
import FlowFutureSupport
import flow_swift_future
// ...
```

The module has to have dependencies set up properly in `CMake` as well, check `CMakeLists.txt` for examples.

### Futures and other templates

Swift's C++ interop cannot currently instantiate class templates in Swift, but can use specialized templates if they are declared so in C++. For example, in order to use Flow futures and promises in Swift, we currently need to type alias them on the C++ side, and then use the type alias name in Swift, like so:

```swift
// flow/include/flow/swift_future_support.h
using PromiseCInt = Promise<int>;
using FutureCInt = Future<int>;

using PromiseVoid = Promise<Void>;
using FutureVoid = Future<Void>;
```

To use them in Swift make sure to use the type-aliases:

```swift
public func getVersion(cxxState: MasterData, req: GetCommitVersionRequest, result promise: PromiseVoid) {
  // ... 
}
```

### Sendable

Sendable is Swift's mechanism to ensure compile time thread-safety. It does so by marking types known to be safe to send across task/actor/thread boundaries with `Sendable` (i.e. `struct Something: Sendable {}` and checks related to it).

In order to declare a C++ type as Sendable you can use the @Sendable attribute:

```cpp
#define SWIFT_SENDABLE                                               \
   __attribute__((swift_attr("@Sendable")))
```

which is used like this: 

```cpp
template <class T>
class SWIFT_SENDABLE Future {
   // ... 
};
```



### Importing types

Swift can import copyable C++ structs and classes as Swift value types. Non-copyable types with value semantics require special wrappers to make them available in Swift (see the "Non-copyable types" section).

Swift can also import some C++ types as reference types, if they're appropriately annotated. The sections "Bridging Singleton-like values with immortal reference types" and "Bridging reference types" describes how that can be done.

Swift will avoid importing "unsafe projections", which means a type which contains pointers to something "outside" the type, as they may end up pointing at unsafe memory.

#### Non-copyable types

Swift's C++ interop currently cannot import non-copyable C++ types. If you have a type that's non-copyable that you want to use as a value type in Swift  (i.e. it's typically used as its own type in C++, not through a pointer type), you most likely want to wrap it in a copyable type which is then made available to Swift. For types that have reference semantics (i.e. you always pass it around using a raw pointer or a custom smart pointer type in C++), see the "Bridging reference types" section.

For example, Flow's `Counter` type is non-copyable. We can make it available to 
Swift so that it can be used as a stored property in a Swift actor by creating a value
type wrapper for it in C++, that stores the counter in a shared pointer value:

```cpp
// A type with Swift value semantics for working with `Counter` types.
class CounterValue {
public:
    using Value = Counter::Value;

    CounterValue(std::string const& name, CounterCollection& collection);

    void operator+=(Value delta);
    void operator++();
    void clear();
private:
    std::shared_ptr<Counter> value;
};
```

We want to implement the required interface for this type that's needed from Swift.

#### Bridging Singleton-like values with immortal reference types

Certain C++ types act as singletons which have one value referenced throughout codebase. That value is expected to be alive throughout the lifetime of the program. Such types can be bridged to Swift using the `SWIFT_CXX_IMMORTAL_SINGLETON_TYPE` annotation. This annotation will instruct Swift to import such type as reference type that doesn't need any reference counting, i.e. it's assumed to be immortal.

For instance, the `INetwork` interface type:

```cpp
class SWIFT_CXX_IMMORTAL_SINGLETON_TYPE ServerKnobs : public KnobsImpl<ServerKnobs> {
public:
```

Gets bridged over to an immortal reference type in Swift:

```Swift
let knobs = getServerKnobs()     // knobs type is `ServerKnobs` in Swift, identical to `ServerKnobs *` C++ type.
knobs.MAX_VERSION_RATE_MODIFIER
```

#### Bridging reference types

Some C++ types have reference counting and referencial semantics, i.e. they're passed around using raw or smart pointers that point to an instance. That instance typically has its own reference count, that keeps track of when the instance should be released. Such types can be bridged over to Swift reference types, and Swift's automatic reference counting (ARC) will automatically retain and release them using their C++ reference counting implementation.

You can use the `SWIFT_CXX_REF` annotation for that. Right now `SWIFT_CXX_REF` does not work (due to https://github.com/apple/swift/issues/61620), so you have to make a custom annotation for each class you want to bridge with reference semantics to Swift. For example, the `MasterData` class receives the following annotation:

```cpp
#define SWIFT_CXX_REF_MASTERDATA   \
    __attribute__((swift_attr("import_as_ref")))   \
    __attribute__((swift_attr("retain:addrefMasterData")))   \
    __attribute__((swift_attr("release:delrefMasterData")))

struct SWIFT_CXX_REF_MASTERDATA MasterData : NonCopyable, ReferenceCounted<MasterData> {
```

This annotation then makes Swift's' `MasterData` type behave like C++/Flow's `Reference<MasterData>` type, i.e. it is automatically reference counted in Swift.

### Awaiting Flow concurrency types

Flow **Futures** can be awaited on in Swift, like this:

```swift
var f: FutureCInt = ...
await f.waitValue
```

to avoid name clashes with `value` it's currently called `waitValue` though we should probably rename this.

You can also await a next value of a stream:

```swift
var ps = PromiseStream<CInt>()
var fs: FutureStream<CInt> = ps.__getFutureUnsafe()

// ... 

let element = try? await fs.waitNext // suspends until value is sent into `ps`
```

It is also possible to use the `async for-loop` syntax on `FutureStream`s:

```swift
for try await num in fs {
  // ... 
}
```

This future will loop until an "end" is sent to the stream.

Sending an "end" element is currently done the same way as in Flow itself:

```swift
var i: CInt = 10
ps.send(&i)
ps.sendError(end_of_stream())
```

## C++ → Swift 

Calling Swift from C++ is relatively simple, you can write new Swift code and `@_expose(Cxx)` them, like this free function in Swift:

```swift
@_expose(Cxx)
public func swiftyTestRunner(p: PromiseVoid) { }
```

This can be called from C++ as expected:

```swift
fdbserver_swift::swiftyTestRunner(p);
```

### Exposing actors and async functions

Actors in Swift have strong isolation properties and cannot be accessed synchronously, as such, every method declared on an actor is implicitly `async` if called from the outside. For example:

```swift
@_expose(Cxx)
actor Example {
  func hi() -> String { "hi!" }
}
```

this `hi()` method is not imported into C++, so you'd get an error when trying to call it on an instance of `Example` from C++:

```
<<error not imported>>
```

This is because, calls "into" an actor are implicitly async, so the method is effectively async:

```swift
@_expose(Cxx)
actor Example {
  func hi() async -> CInt { 42 }
}
```

Since C++ does not understand Swift's async calling convention, we don't import such methods today. This is why today we implement a `nonisolated` wrapper method in order to bridge Flow/C++ and the Swift actor method, like this:

```swift
@_expose(Cxx)
actor Example {
  func hi() async -> CInt { 42 }

  nonisolated func hi(/*other params*/ result: PromiseCInt) {
    Task { // schedule a new Task
      var i = await self.hi() // inside that task, await the hi() method
      result.send(&i)
    }
  }
}
```

And since the `Example` was `_expose`-d the `nonisolated func hi(result:)` is as well, and that can be called from C++:

```swift
// C++
auto promise = Promise<int>();
actor.hi(promise);
wait(p.getFuture());
```

> Note: We hope to simplify this, so you could just `wait(actor.hi())`. And the nonisolated wrapper can also be implemented using a Swift macro once they land (Swift macros are work in progress, and operate on AST, so we're not too worried about using them -- i.e. they are not just textual macros).

## Swift tests

We have prepared a ver simple test suite runner since we cannot use the usual Swift's XCTest framework (maybe we could, didn't investigate yet). To run `swift` tests inside `fdbserver` run like this:

```bash
 FDBSWIFTTEST=1 bin/fdbserver -p AUTO
```

you can also `--filter-test future` etc.

## Limitations, TODOs

- We want to simplify the modules naming and exposing etc.
  - Currently we have `fdbserver` and `fdbserver_swift` and you might have to import both sometimes. 
    - Instead, we'd like to have `fdbserver_cxx` and `fdbserver_swift` and also have a module `fdbserver` which exports both of those, so when _most of the time_ working with this module, you'd just `import fdbserver` and only use the specific ones if really necessary.
- Simplify the boilerplate surrounding calling async functions
  - currently there is a dance on the C++ side to create a Promise and flow-`wait()`  on its future in order to come back from an exposed function etc. We can likely simplify this with either some macros, or something else so we don't have to write this repetetive pattern
  - Simplify the exposing of actor functions with a Swift macro `#expose(flow)` or something like that, that exposes an async function as a Promise/Future Flow compatible `nonisolated` function
