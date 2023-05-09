Summary
=======

With the `benchmark` role, `fdbserver` will execute microbenchmarks determined by the `--benchmark_filter` filter. The goal is to make it easy to test the performance of various sub-millisecond operations in FDB.

Usage
=====

- Running `bin/fdbserver -r benchmark` will run all registered benchmarks, but you may want to limit your run to a subset of benchmarks. This can be done by running `bin/fdbserver -r benchmark --benchmark_filter <regex>`
- Example output:

```
$ bin/fdbserver -r benchmark --benchmark_filter bench_ref
2023-05-07T19:47:00+00:00
Run on (128 X 3500 MHz CPU s)
CPU Caches:
  L1 Data 48 KiB (x64)
  L1 Instruction 32 KiB (x64)
  L2 Unified 1280 KiB (x64)
  L3 Unified 55296 KiB (x2)
Load Average: 0.26, 1.28, 9.22
-------------------------------------------------------------------------------------------------------------------------
Benchmark                                                               Time             CPU   Iterations UserCounters...
-------------------------------------------------------------------------------------------------------------------------
bench_ref_create_and_destroy<RefType::RawPointer>                    4.60 ns         4.60 ns    152280138 items_per_second=217.587M/s
bench_ref_create_and_destroy<RefType::UniquePointer>                 4.87 ns         4.87 ns    143870613 items_per_second=205.454M/s
bench_ref_create_and_destroy<RefType::SharedPointer>                 12.1 ns         12.1 ns     57693256 items_per_second=82.3919M/s
bench_ref_create_and_destroy<RefType::FlowReference>                 4.98 ns         4.98 ns    140142911 items_per_second=200.653M/s
bench_ref_create_and_destroy<RefType::FlowReferenceThreadSafe>       8.88 ns         8.88 ns     78724970 items_per_second=112.634M/s
bench_ref_copy<RefType::RawPointer>                                 0.286 ns        0.286 ns   1000000000 items_per_second=3.49255G/s
bench_ref_copy<RefType::SharedPointer>                               11.4 ns         11.4 ns     61197885 items_per_second=87.4362M/s
bench_ref_copy<RefType::FlowReference>                               4.13 ns         4.13 ns    167578390 items_per_second=242.328M/s
bench_ref_copy<RefType::FlowReferenceThreadSafe>                     11.4 ns         11.4 ns     61196408 items_per_second=87.4355M/s
```

- More detailed documentation on the benchmarking tool used can be found at https://github.com/google/benchmark.

Current Benchmarks
==================

Currently supported benchmarks are:

- `bench_serialize_deltas`
- `bench_sort_deltas`
- `bench_callback`
- `bench_encrypt`
- `bench_decrypt`
- `blob_cipher_encrypt`
- `blob_cipher_decrypt`
- `bench_hash`
- `bench_ionet2`
- `bench_add_idempotency_ids`
- `bench_iterate`
- `bench_memcmp`
- `bench_memcpy`
- `bench_check_metadata1`
- `bench_check_metadata2`
- `bench_net2`
- `bench_delay`
- `bench_populate`
- `bench_priorityMultiLock`
- `bench_random`
- `bench_ref`
- `bench_ddsketchUnsigned` 
- `bench_ddsketchInt`
- `bench_ddsketchDouble`
- `bench_ddsketchLatency`
- `bench_continuousSampleInt`
- `bench_continuousSampleLatency`
- `bench_latencyBands`
- `bench_histogramInt`
- `bench_histogramPct`
- `bench_histogramTime`
- `bench_stream`
- `bench_timer`
- `bench_timer_monotonic`
- `bench_vv_getdelta`
- `bench_serializable_traits_version` 
- `bench_dynamic_traits_version`
- `bench_zstd_decompress`
- `bench_zstd_decompress_stream`
