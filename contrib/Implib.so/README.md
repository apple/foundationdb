[![License](http://img.shields.io/:license-MIT-blue.svg)](https://github.com/yugr/Implib.so/blob/master/LICENSE.txt)
[![Build Status](https://github.com/yugr/Implib.so/actions/workflows/ci.yml/badge.svg)](https://github.com/yugr/Implib.so/actions)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/yugr/Implib.so.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/yugr/Implib.so/alerts/)
[![Codecov](https://codecov.io/gh/yugr/Implib.so/branch/master/graph/badge.svg)](https://codecov.io/gh/yugr/Implib.so)

# Motivation

In a nutshell, Implib.so is a simple equivalent of [Windows DLL import libraries](http://www.digitalmars.com/ctg/implib.html) for POSIX shared libraries.

On Linux/Android, if you link against shared library you normally use `-lxyz` compiler option which makes your application depend on `libxyz.so`. This would cause `libxyz.so` to be forcedly loaded at program startup (and its constructors to be executed) even if you never call any of its functions.

If you instead want to delay loading of `libxyz.so` (e.g. its unlikely to be used and you don't want to waste resources on it or [slow down startup time](https://lwn.net/Articles/341309/) or you want to select best platform-specific implementation at runtime), you can remove dependency from `LDFLAGS` and issue `dlopen` call manually. But this would cause `ld` to err because it won't be able to statically resolve symbols which are supposed to come from this shared library. At this point you have only two choices:
* emit normal calls to library functions and suppress link errors from `ld` via `-Wl,-z,nodefs`; this is undesired because you loose ability to detect link errors for other libraries statically
* load necessary function addresses at runtime via `dlsym` and call them via function pointers; this isn't very convenient because you have to keep track which symbols your program uses, properly cast function types and also somehow manage global function pointers

Implib.so provides an easy solution - link your program with a _wrapper_ which
* provides all necessary symbols to make linker happy
* loads wrapped library on first call to any of its functions
* redirects calls to library symbols

Generated wrapper code (often also called "shim" code or "shim" library) is analogous to Windows import libraries which achieve the same functionality for DLLs.

Implib.so can also be used to [reduce API provided by existing shared library](#reducing-external-interface-of-closed-source-library) or [rename it's exported symbols](#renaming-exported-interface-of-closed-source-library).

Implib.so was originally inspired by Stackoverflow question [Is there an elegant way to avoid dlsym when using dlopen in C?](https://stackoverflow.com/questions/45917816/is-there-an-elegant-way-to-avoid-dlsym-when-using-dlopen-in-c/47221180).

# Usage

A typical use-case would look like this:

```
$ implib-gen.py libxyz.so
```

This will generate code for host platform (presumably x86\_64). For other targets do

```
$ implib-gen.py --target $TARGET libxyz.so
```

where `TARGET` can be any of
  * x86\_64-linux-gnu, x86\_64-none-linux-android
  * i686-linux-gnu, i686-none-linux-android
  * arm-linux-gnueabi, armel-linux-gnueabi, armv7-none-linux-androideabi
  * arm-linux-gnueabihf (ARM hardfp ABI)
  * aarch64-linux-gnu, aarch64-none-linux-android
  * e2k-linux-gnu

Script generates two files: `libxyz.so.tramp.S` and `libxyz.so.init.cpp` which need to be linked to your application (instead of `-lxyz`):

```
$ gcc myfile1.c myfile2.c ... libxyz.so.tramp.S libxyz.so.init.cpp ... -ldl
```

Note that you need to link against libdl.so. On ARM in case your app is compiled to Thumb code (which e.g. Ubuntu's `arm-linux-gnueabihf-gcc` does by default) you'll also need to add `-mthumb-interwork`.

Application can then freely call functions from `libxyz.so` _without linking to it_. Library will be loaded (via `dlopen`) on first call to any of its functions. If you want to forcedly resolve all symbols (e.g. if you want to avoid delays further on) you can call `void libxyz_init_all()`.

Above command would perform a _lazy load_ i.e. load library on first call to one of it's symbols. 

If you do want to load library via `dlopen` but would prefer to call it yourself (e.g. with custom parameters or with modified library name), run script as

```
$ cat mycallback.c
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
#endif

// Callback that tries different library names
void *mycallback(const char *lib_name) {
  lib_name = lib_name;  // Please the compiler
  void *h;
  h = dlopen("libxyz.so", RTLD_LAZY);
  if (h)
    return h;
  h = dlopen("libxyz-stub.so", RTLD_LAZY);
  if (h)
    return h;
  fprintf(stderr, "dlopen failed: %s\n", dlerror());
  exit(1);
}

$ implib-gen.py --dlopen-callback=mycallback libxyz.so
```

(callback must have signature `void *(*)(const char *lib_name)` and return handle of loaded library).

# Wrapping vtables

By default the tool does not try to wrap vtables exported from the library. This can be enabled via `--vtables` flag:
```
$ implib-gen.py --vtables ...
```

# Reducing external interface of closed-source library

Sometimes you may want to reduce public interface of existing shared library (e.g. if it's a third-party lib which erroneously exports too many unrelated symbols).

To achieve this you can generate a wrapper with limited number of symbols and override the callback which loads the library to use `dlmopen` instead of `dlopen` (and thus does not pollute the global namespace):

```
$ cat mysymbols.txt
foo
bar
$ cat mycallback.c
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C"
#endif

// Dlopen callback that loads library to dedicated namespace
void *mycallback(const char *lib_name) {
  void *h = dlmopen(LM_ID_NEWLM, lib_name, RTLD_LAZY | RTLD_DEEPBIND);
  if (h)
    return h;
  fprintf(stderr, "dlmopen failed: %s\n", dlerror());
  exit(1);
}

$ implib-gen.py --dlopen-callback=mycallback --symbol-list=mysymbols.txt libxyz.so
$ ... # Link your app with libxyz.tramp.S, libxyz.init.cpp and mycallback.c
```

Similar approach can be used if you want to provide a common interface for several libraries with partially intersecting interfaces (see [this example](tests/multilib/run.sh) for more details).

# Renaming exported interface of closed-source library

Sometimes you may need to rename API of existing shared library to avoid name clashes.

To achieve this you can generate a wrapper with _renamed_ symbols which call to old, non-renamed symbols in original library loaded via `dlmopen` instead of `dlopen` (to avoid polluting global namespace):

```
$ cat mycallback.c
... Same as before ...
$ implib-gen.py --dlopen-callback=mycallback --symbol_prefix=MYPREFIX_ libxyz.so
$ ... # Link your app with libxyz.tramp.S, libxyz.init.cpp and mycallback.c
```

# Linker wrapper

Generation of wrappers may be automated via linker wrapper `scripts/ld`.
Adding it to `PATH` (in front of normal `ld`) would by default result
in all dynamic libs (besides system ones) to be replaced with wrappers.
Explicit list of libraries can be specified by exporting
`IMPLIBSO_LD_OPTIONS` environment variable:
```
export IMPLIBSO_LD_OPTIONS='--wrap-libs attr,acl'
```
For more details run with
```
export IMPLIBSO_LD_OPTIONS=--help
```

Atm linker wrapper is only meant for testing.

# Overhead

Implib.so overhead on a fast path boils down to
* predictable direct jump to wrapper
* predictable untaken direct branch to initialization code
* load from trampoline table
* predictable indirect jump to real function

This is very similar to normal shlib call:
* predictable direct jump to PLT stub
* load from GOT
* predictable indirect jump to real function

so it should have equivalent performance.

# Limitations

The tool does not transparently support all features of POSIX shared libraries. In particular
* it can not provide wrappers for data symbols (except C++ virtual/RTTI tables)
* it makes first call to wrapped functions asynch signal unsafe (as it will call `dlopen` and library constructors)
* it may change semantics if there are multiple definitions of same symbol in different loaded shared objects (runtime symbol interposition is considered a bad practice though)
* it may change semantics because shared library constructors are delayed until when library is loaded

The tool also lacks the following very important features:
* proper support for multi-threading
* symbol versions are not handled at all
* support OSX
(none should be hard to add so let me know if you need it).

Finally, Implib.so is only lightly tested and there are some minor TODOs in code.

# Related work

As mentioned in introduction import libraries are first class citizens on Windows platform:
* [Wikipedia on Windows Import Libraries](https://en.wikipedia.org/wiki/Dynamic-link_library#Import_libraries)
* [MSDN on Linker Support for Delay-Loaded DLLs](https://msdn.microsoft.com/en-us/library/151kt790.aspx)

Delay-loaded libraries were once present on OSX (via `-lazy_lXXX` and `-lazy_library` options).

Lazy loading is supported by Solaris shared libraries but was never implemented in Linux. There have been [some discussions](https://www.sourceware.org/ml/libc-help/2013-02/msg00017.html) in libc-alpha but no patches were posted.

Implib.so-like functionality is used in [OpenGL loading libraries](https://www.khronos.org/opengl/wiki/OpenGL_Loading_Library) e.g. [GLEW](http://glew.sourceforge.net/) via custom project-specific scripts.
