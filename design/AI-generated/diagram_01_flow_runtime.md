# Flow Runtime — Internal Architecture

```mermaid
graph TB
    subgraph ActorSystem["Actor System"]
        Coro["C++20 Coroutine\n(.cpp / .h source)"]
        Compiler["C++ Compiler\n(co_await / co_return)"]
        Frame["Coroutine Frame\n(parameters and locals)"]
        CoroPromise["CoroPromise / CoroActor\n(Flow integration)"]
    end

    subgraph AsyncPrimitives["Async Primitives"]
        Promise["Promise&lt;T&gt;"]
        Future["Future&lt;T&gt;"]
        SAV["SAV\n(SingleAssignmentVar)"]
        PS["PromiseStream&lt;T&gt;"]
        FS["FutureStream&lt;T&gt;"]
    end

    subgraph EventLoop["Event Loop (Net2)"]
        Scheduler["Priority Task Scheduler"]
        ASIO["Boost.ASIO\n(I/O multiplex)"]
        Timers["Timer Queue"]
        Yields["Yield Points"]
    end

    subgraph Memory["Memory Management"]
        Arena["Arena"]
        StringRef["StringRef / KeyRef\n(non-owning view)"]
        Standalone["Standalone&lt;T&gt;\n(owning)"]
        RefCounted["Reference&lt;T&gt;\n(intrusive refcount)"]
    end

    subgraph Diagnostics["Diagnostics"]
        Trace["TraceEvent\n(.detail() chains)"]
        DRandom["deterministicRandom()\n(seedable PRNG)"]
        CodeProbe["CODE_PROBE\n(coverage marks)"]
        Buggify["BUGGIFY\n(fault injection)"]
    end

    Coro --> Compiler --> Frame
    Frame --> CoroPromise --> SAV
    Promise --> SAV --> Future
    PS --> FS

    Future --> Scheduler
    Timers --> Scheduler
    Scheduler --> ASIO

    Arena --> StringRef
    Arena --> Standalone

    style ActorSystem fill:#e1f0ff,stroke:#4a90d9
    style AsyncPrimitives fill:#fff3e0,stroke:#f5a623
    style EventLoop fill:#e8f5e9,stroke:#4caf50
    style Memory fill:#fce4ec,stroke:#e91e63
    style Diagnostics fill:#f3e5f5,stroke:#9c27b0
```
## Future/Promise Lifecycle

```mermaid
sequenceDiagram
    participant Producer as Producer Actor
    participant SAV as SAV (SharedState)
    participant Consumer as Consumer Actor

    Producer->>SAV: Promise created (SAV allocated)
    Consumer->>SAV: Future obtained (refcount++)
    Consumer->>SAV: co_await pendingFuture — register callback
    Note over Consumer: Suspended
    Producer->>SAV: promise.send(value)
    SAV->>Consumer: Fire callback and resume coroutine with value
```
