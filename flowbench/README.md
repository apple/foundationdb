Summary
=======

`flowbench` is an executable that can be used to microbenchmark parts of the FoundationDB code. The goal is to make it easy to test the performance of various sub-millisecond operations using `flow` and `fdbrpc`. Specifically, this tool can be used to:

- Test the performance effects of changes to the actor compiler or to the `flow` and `fdbrpc` libraries
- Test the performance of various uses of the `flow` and `fdbrpc` libraries
- Find areas for improvement in the `flow` and `fdbrpc` libraries
- Compare `flow`/`fdbrpc` primitives to alternatives provided by the standard library or other third-party libraries.

Usage
=====

- To build the `flowbench` executable, add `-DBUILD_FLOWBENCH=ON` to your cmake command.
- Then you can run `bin/flowbench --help` to see possible uses of `flowbench`.
- Running `bin/flowbench` directly will run all registered benchmarks, but you may want to limit your run to a subset of benchmarks. This can be done by running `bin/flowbench --benchmark_filter=<regex>`
- All benchmark names can be listed with `bin/flowbench --benchmark_list_tests`
- Example output:

```
$ bin/flowbench --benchmark_filter=bench_ref
2020-08-04 21:49:40
Running bin/flowbench
Run on (7 X 2904 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x7)
  L1 Instruction 32 KiB (x7)
  L2 Unified 256 KiB (x7)
  L3 Unified 12288 KiB (x1)
Load Average: 0.15, 0.15, 0.72
---------------------------------------------------------------------------------------------------------------
Benchmark                                                     Time             CPU   Iterations UserCounters...
---------------------------------------------------------------------------------------------------------------
bench_ref_create_and_destroy<RefType::RawPointer>          4.90 ns         4.90 ns    116822124 items_per_second=203.88M/s
bench_ref_create_and_destroy<RefType::UniquePointer>       4.94 ns         4.94 ns    141101924 items_per_second=202.555M/s
bench_ref_create_and_destroy<RefType::SharedPointer>       42.5 ns         42.5 ns     13802909 items_per_second=23.531M/s
bench_ref_create_and_destroy<RefType::FlowReference>       5.05 ns         5.05 ns    100000000 items_per_second=197.955M/s
bench_ref_copy<RefType::RawPointer>                        1.15 ns         1.15 ns    612121585 items_per_second=871.218M/s
bench_ref_copy<RefType::SharedPointer>                     10.0 ns         10.0 ns     67553102 items_per_second=99.8113M/s
bench_ref_copy<RefType::FlowReference>                     2.33 ns         2.33 ns    292317474 items_per_second=428.507M/s
```
- More detailed documentation can be found at https://github.com/google/benchmark

Existing Benchmarks
===================
- `bench_populate` measures the population of a vector of mutations
- `bench_ref` compares the performance of the `flow` `Reference` type to other pointer types
- `bench_iterate` measures iteration over a list of mutations
- `bench_stream` measures the performance of writing to and reading from a `PromiseStream`
- `bench_random` measures the performance of `DeterministicRandom`.
- `bench_timer` measures the perforamnce of FoundationDB timers.

Future use cases
================

- Benchmark the overhead of sending and receiving messages through `FlowTransport`
- Benchmark the performance of serializing/deserializing various types
