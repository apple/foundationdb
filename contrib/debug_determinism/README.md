Utilities for debugging unseed mismatches for foundationdb simulation tests.

99/100 times the source of the nondeterminism is use of uninitialized memory and
what you want to do is build with `-DUSE_VALGRIND=ON` and run simulations under
valgrind.

Common sources of nondeterminism and specialized tools to find them.
1. Use of uninitialized memory (use valgrind!)
1. Memory errors (use valgrind and/or asan)
1. Undefined behavior (use ubsan. You can also try _GLIBCXX_DEBUG)

If it's not any of these then now it's time to try this technique. Look for

1. Call to some kind of "get current time" function that's not in `INetwork`
1. Depending on the relative ordering of allocated memory. E.g. Using heap-allocated pointers as keys in a `std::map`.
1. Inspecting something about the current state of the system (e.g. free disk space)
1. Depending on iteration order of an unordered map

# Quickstart

Set these cmake flags

```
-DTRACE_PC_GUARD_INSTRUMENTATION_LIB=$BUILDDIR/lib/libdebug_determinism.a
```

and change `#define DEBUG_DETERMINISM 0` to `#define DEBUG_DETERMINISM 1` in
flow/Platform.h. This disables several known sources of nondeterminism that
don't affect unseeds.

For reasons I don't fully understand, it appears that sqlite exhibits some
nondeterminism if you don't add `#define SQLITE_OMIT_LOOKASIDE` to the top of
fdbserver/sqlite/sqlite3.amalgamation.c, so you probably want to do that too.

Now when you run an fdbserver simulation, it will write a file `out.bin` in the
current directory which contains the sequence of edges in the control flow graph
that were encountered during the simulation. If you rename `out.bin` to `in.bin`
and then re-run, the simulation will validate that the sequence of edges is the
same as the last run. If it's not, then the simulation will enter an infinite
loop at the first difference and print a message. Then you probably want to
attach gdb to the process and investigate from there.

You'll need to make sure you delete the `simfdb` folder before each run, because
otherwise you'll take a different codepath for deleting the `simfdb` folder at
the beginning of simulation.
