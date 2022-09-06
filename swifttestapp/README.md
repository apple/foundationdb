# Swift in FoundationDB

## Developer notes

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
