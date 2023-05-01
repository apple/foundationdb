# Swift in FoundationDB

## Building with Swift

Invoke cmake as follows in order to use clang (which is required for Swift):

```
cd
mkdir build && cd build 
cmake -G 'Ninja' -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_Swift_COMPILER=swiftc -DUSE_SWIFT -DCMAKE_Swift_COMPILER_EXTERNAL_TOOLCHAIN=/opt/rh/devtoolset-11/root/usr ../src/foundationdb/
```

We also pass the `-DUSE_SWIFT` flag to set the `SERVER_KNOBS->FLOW_USE_SWIFT` knob to `true` by default.

Build `fdbserver` or the entire project like usual

```
ninja fdbserver
```

Some functions may use the `FLOW_USE_SWIFT` knob to determine if they should invoke Swift or C++/Flow implementation of logic.
This is done for the purpose of making sure the ports are correct. Once we gained enough confidence we can remove
the C++ implementations.

### Simple Swift unit tests

Since we're not depending on Swift's testing framework, we have a simple alternative way to run a few simple unit tests.

```
FDBSWIFTTEST=1 ./bin/fdbserver -p AUTO
```

Otherwise, running the usual test suite of fdb should work as usual.

## Running Simulator test-case

A simple example that ends up invoking the Swift implementation of `getVersion`:

```
bin/fdbserver -r simulation --crash --logsize 1024MB -f ~/src/foundationdb/tests/fast/TxnStateStoreCycleTest.toml -s 447933818 -b off  | grep swift
```

Debug output (which won't print on `main` since debug prints are removed), but this shows hwo the execution flows:

```
[c++][sim2+net2] configured: swift_task_enqueueGlobal_hook
[c++][sim2.actor.cpp:2333](execTask) Run swift job: 0x7f18572d8800
[swift][tid:139742566915008][fdbserver_swift/masterserver.swift:174](getVersion(req:result:)) Calling swift getVersion impl in task!
[swift][fdbserver_swift/masterserver.swift:56](getVersion(req:))MasterDataActor handle request
[swift][flow_swift/future_support.swift:67](waitValue) future was ready, return immediately.
[swift][fdbserver_swift/masterserver.swift:139](getVersion(req:))MasterDataActor reply with version: 1
[c++][sim2.actor.cpp:952](delay) Enqueue SIMULATOR delay TASK: job=(nil) time=8.168702, task-time=8.168713
```


## Developer notes

### Adding new *.swift files

E.g. in flow we have swift files, don't forget to add new ones to CMake in `flow/CMakeLists.txt`:

```asm
add_library(flow_swift STATIC
    future_support.swift
    ...
)
```

### Importing reference types

When you get 

```
/root/src/foundationdb/swifttestapp/main.swift:13:29: error: cannot convert value of type 'UnsafeMutablePointer<FlowPromiseInt>?' (aka 'Optional<UnsafeMutablePointer<__CxxTemplateInst7PromiseIiE>>') to specified type 'FlowPromiseInt' (aka '__CxxTemplateInst7PromiseIiE')
    let p: FlowPromiseInt = makePromiseInt()
                            ^~~~~~~~~~~~~~~~
```

It means the type is not understood as reference type to Swift. You need to mark it as such, and also how to deal with 
its lifetime. Do this by adding one of the `SWIFT_CXX_...` attributes:

```c++ 
template <class T>
class UNSAFE_SWIFT_CXX_IMMORTAL_REF StrictFuture : public Future<T> {
```

This is just convenience for:
```
#define UNSAFE_SWIFT_CXX_IMMORTAL_REF
    __attribute__((swift_attr("import_as_ref"))) 
    __attribute__((swift_attr("retain:immortal"))) 
    __attribute__((swift_attr("release:immortal")))
```

refer to `swift.h` for more.
