# Swift in FoundationDB

## Running FDB with Swift `getVersion` impl

```
../src/foundationdb/tests/loopback_cluster/run_cluster.sh . 1 cat | grep "\[swift"
```

Will show the `MasterServerActor` implemented in Swift.

## Running Swift experiments

To build you have to currently:

```
cmake -G 'Ninja' -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_Swift_COMPILER=swiftc -D USE_CCACHE=ON ../src/foundationdb/
```

and the ninja invocation currently has to first build `fdbserver_swift` before `fdbserver` we're working to fix this though:

```
ninja fdbserver_swift fdbserver
```

Then you can run the binary with executing the "swift test" examples:

```
FDBSWIFTTEST=1 ./bin/fdbserver -p AUTO
```

This runs a bunch of "show it works" examples.

We're working towards executing a complete `getVersion` in `masterserver` in Swift along side the real implementation. 

## Running Simulator test-case

A simple example that ends up invoking the Swift implementation of `getVersion`:

```
bin/fdbserver -r simulation --crash --logsize 1024MB -f ~/src/foundationdb/tests/fast/TxnStateStoreCycleTest.toml -s 447933818 -b off  | grep swift
```

Results in:

```
[c++][sim2+net2] configured: swift_task_enqueueGlobal_hook
[swift][tid:139788310125504][fdbserver_swift/masterserver.swift:172](getVersion(req:result:)) Calling swift getVersion impl!
[c++][sim2][swift_sim2_hooks.cpp:82](sim2_enqueueGlobal_hook_impl) intercepted job enqueue: 0x7f22fd9da6c0 to g_network (0x7f22fe36a000)
[c++][sim2][sim2.actor.cpp:913](_swiftEnqueue) ready.push SWIFT JOB AS SIMULATOR TASK: job=0x7f22fd9da6c0
[swift][tid:139788310125504][fdbserver_swift/masterserver.swift:172](getVersion(req:result:)) Calling swift getVersion impl!
[c++][sim2][swift_sim2_hooks.cpp:82](sim2_enqueueGlobal_hook_impl) intercepted job enqueue: 0x7f22f068d7c0 to g_network (0x7f22fe36a000)
[c++][sim2][sim2.actor.cpp:913](_swiftEnqueue) ready.push SWIFT JOB AS SIMULATOR TASK: job=0x7f22f068d7c0
[swift][tid:139788310125504][fdbserver_swift/masterserver.swift:172](getVersion(req:result:)) Calling swift getVersion impl!
[c++][sim2][swift_sim2_hooks.cpp:82](sim2_enqueueGlobal_hook_impl) intercepted job enqueue: 0x7f22f068e300 to g_network (0x7f22fe36a000)
[c++][sim2][sim2.actor.cpp:913](_swiftEnqueue) ready.push SWIFT JOB AS SIMULATOR TASK: job=0x7f22f068e300
[swift][tid:139788310125504][fdbserver_swift/masterserver.swift:172](getVersion(req:result:)) Calling swift getVersion impl!
[c++][sim2][swift_sim2_hooks.cpp:82](sim2_enqueueGlobal_hook_impl) intercepted job enqueue: 0x7f22f068d400 to g_network (0x7f22fe36a000)
[c++][sim2][sim2.actor.cpp:913](_swiftEnqueue) ready.push SWIFT JOB AS SIMULATOR TASK: job=0x7f22f068d400
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
class SWIFT_CXX_REF_IMMORTAL StrictFuture : public Future<T> {
```

This is just convenience for:
```
#define SWIFT_CXX_REF_IMMORTAL 
    __attribute__((swift_attr("import_as_ref"))) 
    __attribute__((swift_attr("retain:immortal"))) 
    __attribute__((swift_attr("release:immortal")))
```

refer to `swift.h` for more.
