####
Flow
####

Engineering challenges
======================

FoundationDB began with ambitious goals for both :doc:`high performance <performance>` per node and :doc:`scalability <scalability>`. We knew that to achieve these goals we would face serious engineering challenges while developing the FoundationDB core. We'd need efficient asynchronous communicating processes, the speed and I/O efficiency of C++, and extensive simulation to engineer for reliability and fault tolerance on large clusters.

Flow provides the asynchronous runtime for these processes. FoundationDB uses standard C++ coroutines with Flow futures, streams, and cooperative scheduling. The C++ compiler preserves each coroutine's execution state across suspension points, and Flow connects awaited operations to the event loop. The same runtime supports deterministic simulation of the system, including its physical interfaces and failure modes.

Flow supports three major engineering goals:

* high performance through native code,
* asynchronous concurrency through coroutines and message passing,
* simulation support for testing.

A first look
============

Coroutines receive asynchronous results through futures. When a coroutine needs a result, it can suspend without blocking other work on the event loop. This function waits for an integer, adds an offset, and returns the sum:

.. code-block:: cpp

    #include "flow/flow.h"

    Future<int> asyncAdd(Future<int> f, int offset) {
        int value = co_await f;
        co_return value + offset;
    }

A coroutine returning ``Future<T>`` starts immediately when called and runs until it completes or awaits an operation that is not ready. Its returned future represents the eventual result. Coroutine code lives in ordinary ``.cpp`` and ``.h`` files.

Flow features
=============

Promise<T> and Future<T>
------------------------

``Promise<T>`` and ``Future<T>`` connect an asynchronous sender and receiver. A promise can deliver one value of type ``T`` or an error. The future lets its holder observe that result. A future may have multiple holders.

These are local process handles. FoundationDB's RPC layer uses ``RequestStream<T>`` and ``ReplyPromise<T>`` for network requests and replies. A request can carry a reply promise to another process; sending the reply there makes the caller's future ready.

co_await and co_return
----------------------

``co_await`` waits for a future without blocking the event loop. Awaiting a ready future continues immediately; otherwise the coroutine suspends until it becomes ready. If the future contains an error, awaiting it throws that ``Error``.

A coroutine returning ``Future<T>`` completes with ``co_return value;``. For ``Future<Void>``, use ``co_return;`` to signal completion without a payload. Awaiting a ``Future<Void>`` does not produce a value to assign.

Local variables and ownership
-----------------------------

Local variables obey normal C++ scope rules and remain alive across suspension while their scope is active. Value parameters are stored in the coroutine frame. References, pointers, and views such as ``StringRef`` do not extend the lifetime of their referents or backing storage. Use owning types such as ``Reference<T>`` and ``Standalone<T>`` when the coroutine needs to retain an object or its bytes.

PromiseStream<T> and FutureStream<T>
------------------------------------

``PromiseStream<T>`` and ``FutureStream<T>`` represent a series of asynchronous messages. Awaiting a ``FutureStream<T>`` consumes its next value. If an item is already queued, execution continues without suspension:

.. code-block:: cpp

    Future<Void> forwardWithOffset(FutureStream<int> input,
                                   PromiseStream<int> output,
                                   int offset) {
        while (true) {
            int value = co_await input;
            output.send(value + offset);
        }
    }

Waiting for multiple inputs
---------------------------

``race()`` from ``flow/CoroUtils.h`` waits for the first ready input. It returns a ``std::variant`` whose index matches the winning argument. Inputs can be futures or streams; a winning stream consumes one item. If several inputs are already ready, the lowest argument index wins. Errors propagate from the winning input.

Losing inputs are detached from the race, not explicitly cancelled. They may still be cancelled if releasing the race drops their last future reference. Retain a future separately when its operation must continue after losing. A completed non-stream future stays ready, so replace it or remove it from a repeated race after handling it.

Example: A server interface
---------------------------

This local server maintains a count in response to asynchronous messages. It has one promise stream per request type and races those streams in a loop. A request for the current count carries a ``Promise<int>`` through which the server replies. A network interface uses the RPC types described above and also needs serialization.

.. code-block:: cpp

    #include "flow/CoroUtils.h"

    struct CountingServerInterface {
        PromiseStream<int> addCount;
        PromiseStream<int> subtractCount;
        PromiseStream<Promise<int>> getCount;
    };

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

The caller must keep the returned ``Future<Void>`` alive while the server is needed. The local ``count`` remains alive across each suspension.

Cancellation and lifetime pitfalls
==================================

By default, dropping the last reference to a pending coroutine's returned future cancels it. An explicit ``Future::cancel()`` also requests cancellation. A suspended coroutine resumes by throwing ``actor_cancelled`` from its await. Local objects are destroyed as their scopes unwind. Keep the future alive, or await it directly, when the work must continue.

Error and retry handlers must propagate ``actor_cancelled`` after any required synchronous cleanup. Do not silently swallow ``broken_promise`` or other errors unless the caller's contract explicitly handles them. C++ does not allow ``co_await`` inside a ``catch`` handler; save the error and await asynchronous recovery after leaving the handler.

Captures in a coroutine lambda belong to the lambda's closure, which may be destroyed while the coroutine is suspended. Prefer a named coroutine with explicit value parameters when the work can outlive the call that starts it.

See the `Flow tutorial <https://github.com/apple/foundationdb/blob/main/flow/README.md>`_, the `coroutine design guide <https://github.com/apple/foundationdb/blob/main/design/coroutines.md>`_, and the `runnable coroutine tutorial <https://github.com/apple/foundationdb/blob/main/documentation/coro_tutorial/tutorial.cpp>`_ for further examples.
