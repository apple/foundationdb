####
Flow
####

Engineering challenges
======================

FoundationDB began with ambitious goals for both :doc:`high performance <performance>` per node and :doc:`scalability <scalability>`. We knew that to achieve these goals we would face serious engineering challenges while developing the FoundationDB core. We'd need to implement efficient asynchronous communicating processes of the sort supported by `Erlang <http://en.wikipedia.org/wiki/Erlang_(programming_language)>`_ or the `Async library in .NET <http://msdn.microsoft.com/en-us/library/vstudio/hh191443.aspx>`_, but we'd also need the raw speed and I/O efficiency of C++. Finally, we'd need to perform extensive simulation to engineer for reliability and fault tolerance on large clusters.

To meet these challenges, we developed several new tools, the first of which is Flow, a new programming language that brings `actor-based concurrency <http://en.wikipedia.org/wiki/Actor_model>`_ to C++11. To add this capability, Flow introduces a number of new keywords and control-flow primitives for managing concurrency. Flow is implemented as a compiler which analyzes an asynchronous function (actor) and rewrites it as an object with many different sub-functions that use callbacks to avoid blocking (see `streamlinejs <https://github.com/Sage/streamlinejs>`_ for a similar concept using JavaScript). The Flow compiler's output is normal C++11 code, which is then compiled to a binary using traditional tools. Flow also provides input to our simulation tool, which conducts deterministic simulations of the entire system, including its physical interfaces and failure modes. In short, Flow allows efficient concurrency within C++ in a maintainable and extensible manner, achieving all three major engineering goals:

* high performance (by compiling to native code),
* actor-based concurrency (for high productivity development),
* simulation support (for testing).

A first look
============

Actors in Flow receive asynchronous messages from each other using a data type called a *future*. When an actor requires a data value to continue computation, it waits for it without blocking other actors. The following simple actor performs asynchronous addition. It takes a future integer and a normal integer as an offset, waits on the future integer, and returns the sum of the value and the offset:

.. code-block:: c

    ACTOR Future<int> asyncAdd(Future<int> f, int offset) {
      int value = wait( f );
      return value + offset;
    }

Flow features
=============

Flow's new keywords and control-flow primitives support the capability to pass messages asynchronously between components. Here's a brief overview.

Promise<T> and Future<T>
------------------------

The data types that connect asynchronous senders and receivers are ``Promise<T>`` and ``Future<T>`` for some C++ type ``T``. When a sender holds a ``Promise<T>``, it represents a promise to deliver a value of type ``T`` at some point in the future to the holder of the ``Future<T>``. Conversely, a receiver holding a ``Future<T>`` can asynchronously continue computation until the point at which it actually needs the ``T.``

Promises and futures can be used within a single process, but their real strength in a distributed system is that they can traverse the network. For example, one computer could create a promise/future pair, then send the promise to another computer over the network. The promise and future will still be connected, and when the promise is fulfilled by the remote computer, the original holder of the future will see the value appear.

wait()
------

At the point when a receiver holding a ``Future<T>`` needs the ``T`` to continue computation, it invokes the ``wait()`` statement with the ``Future<T>`` as its parameter. The ``wait()`` statement allows the calling actor to pause execution until the value of the future is set, returning a value of type ``T``. During the wait, other actors can continue execution, providing asynchronous concurrency within a single process.

ACTOR
-----

Only functions labeled with the ``ACTOR`` tag can call ``wait()``. Actors are the essential unit of asynchronous work and can be composed to create complex message-passing systems. By composing actors, futures can be chained together so that the result of one depends on the output of another.

An actor is declared as returning a ``Future<T>`` where ``T`` may be ``Void`` if the actor's return value is used only for signaling. Each actor is preprocessed into a C++11 class with internal callbacks and supporting functions.

State
-----

The ``state`` keyword is used to scope a variable so that it is visible across multiple ``wait()`` statements within an actor. The use of a ``state`` variable is illustrated in the example actor below.

PromiseStream<T>, FutureStream<T>
---------------------------------

When a component wants to work with a *stream* of asynchronous messages rather than a single message, it can use ``PromiseStream<T>`` and ``FutureStream<T>``. These constructs allow for two important features: multiplexing and reliable delivery of messages. They also play an important role in Flow design patterns. For example, many of the servers in FoundationDB expose their interfaces as a ``struct`` of promise streams—one for each request type.

waitNext()
----------

``waitNext()`` is the counterpart of ``wait()`` for streams. It pauses program execution and waits for the next value in a ``FutureStream``. If there is a value ready in the stream, execution continues without delay.

choose . . . when
-----------------

The ``choose`` and ``when`` constructs allow an actor to wait for multiple futures at once in a ordered and predictable way.

Example: A Server Interface
---------------------------

Below is a actor that runs on single server communicating over the network. Its functionality is to maintain a count in response to asynchronous messages from other actors. It supports an interface implemented with a loop containing a ``choose`` statement with a ``when`` for each request type. Each ``when`` uses ``waitNext()`` to asynchronously wait for the next request in the stream. The add and subtract interfaces modify the count itself, stored with a state variable. The get interface takes a ``Promise<int>`` instead of just an ``int`` to facilitate sending back the return message.

To write the equivalent code directly in C++, a developer would have to implement a complex set of callbacks with exception-handling, requiring far more engineering effort. Flow makes it much easier to implement this sort of asynchronous coordination, with no loss of performance:

.. code-block:: c

    ACTOR void serveCountingServerInterface(
               CountingServerInterface csi) {
        state int count = 0;
        while (1) {
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

Caveats
=======

Even though Flow code looks a lot like C++, it is not. It has different rules and the files are preprocessed. It is always important to keep this in mind when programming flow.

We still want to be able to use IDEs and modern editors (with language servers like cquery or clang-based completion engines like ycm). Because of this there is a header-file ``actorcompiler.h`` in flow which defines preprocessor definitions to make flow compile as normal C++ code. CMake even supports a special mode so that it doesn't preprocess flow files. This mode can be used by passing ``-DOPEN_FOR_IDE=ON`` to cmake. Additionally we generate a special ``compile_commands.json`` into the source-directory which will support opening the project in IDEs and editors that look for a compilation database.

Some preprocessor definitions will not fix all issues though. When programming Flow the following things have to be taken care of by the programmer:

- Local variables don't survive a call to ``wait``. So this would be legal Flow code, but NOT legal C++ code:

  .. code-block:: c

                ACTOR void foo {
                    int i = 0;
                    wait(someFuture);
                    int i = 2;
                    wait(someOtherFuture)
                }


  In order to make this not break IDE-support one can either rename the second occurrence of this variable or, if this is not desired as it might make the code unreadable, one can use scoping:


  .. code-block:: c

                ACTOR void foo {
                    {
                        int i = 0;
                        wait(someFuture);
                    }
                    {
                        int i = 2;
                        wait(someOtherFuture)
                    }
                }

- An ``ACTOR`` is compiled into a class internally. Which means that within an actor-function, ``this`` is a valid pointer to this class. But using them explicitly (or as described later implicitly) will break IDE support. One can use ``THIS`` and ``THIS_ADDR`` instead. But be careful as ``THIS`` will be of type ``nullptr_t`` in IDE-mode and of the actor-type in normal compilation mode.
- Lambdas and state variables are weird in a sense. After actor compilation, a state variable is a member of the compiled actor class. In IDE mode it is considered a normal local variable. This can result in some surprising side-effects. So the following code will only compile if the method ``Foo::bar`` is defined as ``const``:

  .. code-block:: c

                ACTOR foo() {
                    state Foo f;
                    foo([=]() { f.bar(); })
                }


  If it is not, one has to pass the member explicitly as a reference:

  .. code-block:: c

                ACTOR foo() {
                    state Foo f;
                    auto x = &f;
                    foo([x]() { x->bar(); })
                }

- state variables in Flow don't follow the normal scoping rules. So in Flow a state variable can be defined in an inner scope and later it can be used in the outer scope. In order to not break compilation in IDE-mode, always define state variables in the outermost scope they will be used.

