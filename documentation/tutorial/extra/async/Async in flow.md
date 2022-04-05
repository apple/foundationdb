# Asynchronous in `flow`

## Introduction

FoundationDB is based on an asynchronous infrastructure named `flow`. `flow` introduced a few keywords to C++, including `ACTOR` and`state`, together with some support functions like `wait` and `delay` for coroutines. In this article, the reader is expected to have basic understanding of asynchronous programming.

## Basics of `flow` `ACTOR` language

### Using `ACTOR` compiler

A C++ file must have `.actor.h` or `.actor.cpp` in order to trigger `actorcompiler.exe` transpile the code into standard C++ code, the generated file will have `.g.h` or `.g.cpp` extension.

The `ACTOR` file should have `#include "flow/actorcompiler.h"` as the **last** included file. If the file is a header file, at the end of the file it should have `#include "flow/unactorcompiler.h"` to prevent other include files being polluted by the macros defined in `flow/actorcompiler.h`.

Any functions that needs `ACTOR` support should put `ACTOR` before its definition, e.g.

```C++
ACTOR Future<Void> actorFunction();
```

The `ACTOR` should return a `Future` object.  [^footnote_actor_return_type] If it is intended to return `void`, then `Void` should be used; otherwise, if the value type is `T` ,then `Future<T>` should be the return type.

[^footnote_actor_return_type]: There are certain cases those `Future` is not used as the returning type, they are not discussed here.

### `wait` and `delay`

An `ACTOR` function must use at least one `wait` clause to implement async/await semantic. Calling `wait` will allow the current coroutine yields its execution to `g_network`, which is the event engine. It will be resumed when the condition in `wait` is fulfilled. One common condition is `delay`, which will be triggered after the given time has been elapsed.

```C++
ACTOR Future<Void> waitDelay() {
	std::cout << "Hello, " << std::flush;
	wait(delay(1));
	std::cout << "world!" << std::endl;

	return Void();
}
```

See `waitDelay.actor.cpp` for more details.

### `wait` for a `Future`

`delay` will return a `Void` object, thus no variable declaration is needed. In general, an `ACTOR` would return an object *asynchronously*. In this case, a variable should be used to receive the object. The variable *must* be declared together with the `wait` statement without the`Future` wrapper.

```c++
ACTOR Future<int> returnInt() {
    wait(delay(1));
    return 1;
}

ACTOR Future<Void> waitInt() {
    int intValue = wait(returnInt());
    std::cout << "Received " << intValue << std::endl;

    return Void();
}
```

 See `waitInt.actor.cpp` for more details.

### `state`  variable

As the `ACTOR` function will be transpiled into several complicated classes, the variable scope will be affected, and is different from standard C++ function variables. If a variable is defined as `state`, then it is accessible within the *whole* actor, no matter where it is defined. However, its value will be the default value before the first assignment. On the other hand, a variable that is defined without `state` will have limited scope, i.e. between its definition and the `wait` call after it.

```C++
ACTOR Future<Void> state_1() {
	// The variable marked as state will be accessible everywhere in the ACTOR
	state int stateValue = 0;

	// The variable without state decorator will have scope between the
	// declaration of itself and the first wait call. Thus it is possible to
	// redefine it after the wait call.
	int value = 0;

	std::cout << "The stateValue is " << stateValue << " while value is " << value << std::endl;

	wait(delay(1.0));

	int value = -1;
	++stateValue;
	std::cout << "The stateValue is " << stateValue << " while value is " << value << std::endl;

	return Void();
}

ACTOR Future<Void> state_2() {
	// Even the state variable is defined after
	std::cout << "State variable defined later can be accessed before its definition: stateVariable = " << stateVariable << std::endl;

	wait(delay(0.1));

	state int stateVariable = 10;

	return Void();
}
```

See `state.actor.cpp` for more details.

#### Variable declarations in control blocks

It is important to remember that, if `wait`, or other `ACTOR` commands are used in the control block, then the variables should be `state` decorated. The reason is that, the `ACTOR` compiler would compile the code into different functions inside a class. In this case, the variable scope needs to be class-wide, thus `state` is necessary. Such examples can be seen in the latter part of this article.

#### Add $\lambda$ functions to `ACTOR`

`ACTOR`s will be compiled into C++ classes; thus, a $\lambda$ function inside the `ACTOR` will also affected by the scoping of `ACTOR`s. Additionally, the variable capturing of the $\lambda$ function is usually unexpected. To allow $\lambda$ functions to access the `state` variables, it is necessary to explicitly let the $\lambda$ function capture `*this`. For example:

```c++
ACTOR Future<Void> lambdaCapturing() {
    // The variable marked as state will be accessible everywhere in the ACTOR
    state int stateValue = 0;
    // NOTE: it is necessary to capture `this` pointer in order to access
    // stateVariable!
    state std::function<void()> lambda1 = [this]() {
        std::cout << "stateValue = " << stateValue << std::endl;
    };

    int nonStateVariable = 10;
    auto lambda2 = [=]() -> void {
        std::cout << "nonStateVariable = " << nonStateVariable << std::endl;

        // It is not possible to access `stateVariable` since the lambda is
        // not caputring `this`
    };

    std::cout << "Before delay: " << std::endl;

    lambda1();
    lambda2();

    wait(delay(0.1));

    std::cout << std::endl << "After delay: " << std::endl;
    lambda1();
    // lambda2 is not accessible as it is out of scope

    return Void();
}
```

See `lambdaState.actor.cpp` for more details.

### `choose` and `when` clauses

When multiple events are waited, it is possible to use `choose` keyword together with `when` to handle different type of events. The event that responded first will be processed. The example below illustrated the schema.

```c++
ACTOR Future<int> returnValue(double delayTime) {
    wait(delay(delayTime));
    return 1;
}

ACTOR Future<Void> chooseWhen() {
    choose {
        when(wait(delay(1.0))) {
            std::cout << "Timeout!" << std::endl;
        }
        when(int val = wait(returnValue(2.0))) {
            std:: cout << "Received: " << val << std::endl;
        }
    }

    return Void();
}
```

By changing the `delayTime` parameter for `returnValue` to a number smaller than 1.0, one would see the `Timeout!` output instead of `Received: 1`.

See `chooseWhen.actor.cpp` for more details.

#### Parameters for `ACTOR`s

An `ACTOR` function can have parameters, like other functions. However, there are several pitfalls worthy to be mentioned:

1. `ACTOR` function parameters will *always* be `const`, i.e. the `const` qualifier will be added automatically during the transpile process. It is then illegal to add `const` qualifier to `ACTOR` function parameters by hand, or the C++ compiler would complain with `error: duplicate 'const'`.
2. On the other hand, `ACTOR` function parameters will *always* passed **by value** in transpiled code. That means, a local copy of the value is inevitable. To avoid unnecessary copies, consider pass the parameter by pointer instead.

### Assigning `ACTOR` to `Future` variables

It is possible to assign the `ACTOR` to `Future` variables, and then `wait` for the asynchronous result later. When the `ACTOR` is called without `wait`, it will execute until the code reaches the first `wait` inside the `ACTOR`, then it would yield until the event in the `wait` statement is triggered.

```C++
ACTOR Future<int> returnInt() {
    std::cout << "returnInt is triggered" << std::endl;
    wait(delay(1));
    std::cout << "returnInt is returning" << std::endl;
    return 1;
}

ACTOR Future<Void> waitDelay() {
    std::cout << "Assigning returnInt() call to a Future<int> variable" << std::endl;
    Future<int> intValueFuture = returnInt();

    std::cout << "Waiting for returnInt returns" << std::endl;
    int value = wait(intValueFuture);

    std::cout << "Received " << value << std::endl;

	return Void();
}
```

See `actorFuture.actor.cpp` for more details.

### `loop` during `choose`

`choose` `when` pair will only respond to the first event that is fired. If multiple events need to be responded, `loop` can be used. `Future` variable should be used and reassigned every time `when` is triggered in this example, otherwise the same event will be triggered only once.

```c++
ACTOR Future<int> echoValue(double delayTime, int value) {
    wait(delay(delayTime));
    return value;
}

ACTOR Future<Void> loopChooseWhen() {
    state int secondTicker = 0;
    state Future<int> echoValue1 = echoValue(0.5, 1);
    state Future<int> echoValue2 = echoValue(1.5, 2);
    state Future<Void> delayer = delay(1.0);

    loop choose {
        when(wait(delayer)) {
            std::cout << ++secondTicker << " second(s) has passed" << std::endl;
            if (secondTicker > 10) {
                break;
            }
            delayer = delay(1.0);
        }
        when(int val = wait(echoValue1)) {
            std::cout << "echoValue 1 Received: " << val << std::endl;
            echoValue1 = echoValue(0.5, 1);
        }
        when(int val = wait(echoValue2)) {
            std::cout << "echoValue 2 Received: " << val << std::endl;
            echoValue2 = echoValue(1.5, 2);
        }
    }

    return Void();
}
```

See `loopChooseWhen.actor.cpp` for more details.

#### Why explicit `Future` variables are required

When a `choose` is introduced, all `wait`ing `Future`s will be injected into a list of `wait`ing `ACTOR`s. If any of the events fired, *all* of the futures that are reacted/not reacted will be reset. Thus, it is necessary to use an additional `Future` variable to hold the variable in order to avoid the reset.

### Exceptions

`flow` infrastructure will only handle exceptions instantiated from `Error` class. It is *not* encouraged to directly call the constructor of `Error` class. Instead, call the errors defined in `flow/error_definitions.h`. In the header file, a macro `ERROR` is used to define the error, e.g.

```C++
ERROR( internal_error, 4100, "An internal error occurred" )
```

Error can be caught using standard C++ `catch` statement, e.g.

```C++
ACTOR Future<Void> throwException(double delayTime) {
    wait(delay(delayTime));
    throw internal_error();
    // The return is unreachable
    // return Void();
}

ACTOR Future<Void> catchException() {
    state int secondTicker = 0;
    state Future<Void> exception = throwException(2.5);
    state Future<Void> delayer = delay(1.0);

    loop {
        try {
            choose {
                when(wait(delayer)) {
                    std::cout << ++secondTicker << " second(s) has passed" << std::endl;
                    if (secondTicker > 10) {
                        break;
                    }
                    delayer = delay(1.0);
                }
                when(wait(exception)) {
                    std::cout << "This should not happen." << std::endl;
                }
            }
        } catch(Error& ex) {
            std::cout << "Caught exception: " << ex.code() << " -- " << ex.what() << std::endl;
            break;
        }
    }

    return Void();
}
```

See `exception.actor.cpp` for more details.

### `template`d  `ACTOR`s

`ACTOR`s can be `template`d for meta-programming.  To allow `ACTOR`s being transpiled properly, the keyword `ACTOR` must be placed before `template`, e.g.

```C++
ACTOR
template <typename T>
Future<T> coroutine();
```

An example can be found in `reportLatency.actor.cpp`.

### `ACTOR`s without being `wait`ed

`ACTOR`s are usually `wait`ed for its results. However, it is possible to start an `ACTOR` without a corresponding `wait` for it. In this case, the `ACTOR` might be cancelled when other `ACTOR`s finished.

```C++
ACTOR Future<Void> unwaitDelay() {
	state int rep = 0;

	loop {
		wait(delay(0.5));
		if (++rep == 10) break;
		std::cout << "unwaitDelay: " << rep << std::endl;
	}

	std::cout << "This should not happen as unwaitDelay should be cancelled after waitDelay finishes." << std::endl;

	return Void();
}

ACTOR Future<Void> waitDelay() {
	state int rep = 0;

	loop {
		wait(delay(0.5));
		if (++rep == 5) break;
		std::cout << "waitDelay: " << rep << std::endl;
	}

	std::cout << "waitDelay: completed." << std::endl;

	return Void();
}

int main() {
	platformInit();
	g_network = newNet2(TLSConfig(), false, true);

	auto _1 = unwaitDelay();	// NOTE: This actor is not being waited. It *must* be assigned to a variable
	auto _2 = stopAfter(waitForAll<Void>({waitDelay()}));
	g_network->run();

	return 0;
}
```

See `unwaitedActor.actor.cpp` for more details.

## `Promise` and `Future`

`Promise`s and `Future`s play central roles of an asynchronous system. `Promise`s are used to claim a resource, that might not be immediately available, can be `wait`ed through a `Future` object. 

A `Promise` can trigger multiple `Future`s that subscribes the `Promise`.  The following code serve as a  simple example of `Promise` :

```C++
const int VALUE = 42;
Promise<int> promise;

ACTOR Future<Void> waitForPromise(int id) {
    state int value = wait(promise.getFuture());

    std::cout << "waitForPromise id=" << id << " received " << value << std::endl;

	return Void();
}

ACTOR Future<Void> loopForPromise() {
    state Future<Void> delayer = delay(1.0);
    state Future<Void> future1 = waitForPromise(1);
    state Future<Void> future2 = waitForPromise(2);
    state int counter = 0;

    loop {
        choose {
            when(wait(delayer)) {
                ++counter;
                std::cout << counter << " second(s) has passed." << std::endl;
                if (counter == 5) {
                    break;
                }
                if (counter == 2) {
                    promise.send(3);
                }
                delayer = delay(1.0);
            }
            when(wait(future1)) {
                std::cout << "Future1 is done" << std::endl;
                // It is important to mute the future1 to be Never() so it will
                // not be triggered anymore in the loop; otherwise, since the
                // condition will be fulfilled in the 2th second, future1 will
                // *always* be triggered and other conditions will be ignored.
                future1 = Never();
            }
            when(wait(future2)) {
                std::cout << "Future2 is done" << std::endl;
                future2 = Never();
            }
        }
    }

    return Void();
}
```

The full example can be found in `promise.actor.cpp`

### `PromiseStream` and `FutureStream`

`Promise` and `Future` are good at passing single value between coroutines. To process multiple messages, a `PromiseStream` object will be useful.

```C++
PromiseStream<int> promise;

ACTOR Future<Void> producer() {
    state int counter = 0;
    state int i = 0;

    for (i = 0; i < 10; ++i) {
        promise.send(counter);
        ++counter;
        wait(delay(0.8));
    }

    return Void();
}

ACTOR Future<Void> loopForPromise() {
    state Future<Void> delayer = delay(1.0);
    state int counter = 0;

    loop {
        choose {
            when(wait(delayer)) {
                ++counter;
                std::cout << counter << " second(s) has passed." << std::endl;
                if (counter == 11) {
                    break;
                }
                delayer = delay(1.0);
            }
            when(int value = waitNext(promise.getFuture())) {
                std::cout << "Received " << value << std::endl;
            }
        }
    }

    return Void();
}
```

See `stream.actor.cpp` for more details.

## Generic `ACTOR`s in `flow/genericactors.actor.h`

Plenty of handful `ACTOR`s are defined in `flow/genericactors.actor.h` .

### `AsyncVar` and `AsyncTrigger`

`AsyncVar` and `AsyncTrigger`, defined in `flow/genericactors.actor.h`, are useful when monitoring value changes:

* `AsyncVar` will hold a variable. Change of the variable, or calling `trigger` method, would cause an event being triggered.
* `AsyncTrigger` will hold a `Void` object (note that `Void` , which is different from `void`, *is* an object) so the value is immutable, only `trigger` method is provided.

```C++
AsyncVar<int> integerTrigger(0);
AsyncTrigger terminateTrigger;

ACTOR Future<Void> loopFunc() {
    state int i = 0;

    for (i = 0; i < 10; ++i) {
        integerTrigger.set(integerTrigger.get() + 1);

        // Yield the control
        wait(delay(0));
    }

    terminateTrigger.trigger();
    return Void();
}

ACTOR Future<Void> asyncVarLoop() {
    loop choose {
        when(wait(integerTrigger.onChange())) {
            std::cout << "integerTrigger value: " << integerTrigger.get() << std::endl;
        }
        when(wait(terminateTrigger.onTrigger())) {
            std::cout << "terminateTrigger triggered!" << std::endl;
            break;
        }
    };

	return Void();
}
```

It is worthy to note again that in function `loopFunc`, since `wait` is called inside the `for` loop, the loop body will be transpiled into several different functions, one before the `wait`, and two after `wait` .  `i` must be decorated with `state` in this case. Also noting that `wait(delay(0))` will cause the current coroutine yield its control.

The full source code can be found in `asyncVar.actor.cpp`.

#### Interpreting the output of the program

The output of the program is somewhat anti-intuition:

```bash
integerTrigger value: 2
integerTrigger value: 3
integerTrigger value: 4
integerTrigger value: 5
integerTrigger value: 6
integerTrigger value: 7
integerTrigger value: 8
integerTrigger value: 9
integerTrigger value: 10
terminateTrigger triggered!
```

One might expect the following line of output is missing:

```bash
integerTrigger value: 1
```

However, this is actually the *expected* result. Recalling the fact that coroutines are *not* running in parallel. Instead, the running coroutine will not be stopped until it reaches `wait`s, which *yield*s the control to the event machine. With this in mind, revisiting the coroutines in this program:

```C++
	auto _ = stopAfter(waitForAll<Void>({loopFunc(), asyncVarLoop()}));
```

The function `loopFunc()` will run, increasing the `integerTrigger`'s value by 1, and yield the control. `asyncVarLoop`() is then started, and `loop` `wait`ing for `integerTrigger` and `terminateTrigger`. Since the event is triggered *before* the `wait`, it is then ignored by the `wait`. The first increment is thus not outputted.
