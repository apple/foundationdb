# AGENTS.md

This file provides guidance to AI coding agents working in this repository.

## Build

```bash
mkdir build && cd build
cmake -G Ninja <SOURCE_DIR>
ninja
# or: ninja fdbserver fdbcli fdbclient  (specific targets)
```

Key cmake options:
- `-DCMAKE_BUILD_TYPE=Debug` (default Release)
- `-DUSE_WERROR=ON` for development
- `-DOPEN_FOR_IDE=ON` to generate IDE project without building

macOS: add `-DBUILD_SWIFT_BINDING=OFF` if Swift issues arise.

Linux with Clang: `CC=clang CXX=clang++ cmake -DUSE_LD=LLD -DUSE_LIBCXX=1 -G Ninja ..`

## Testing

**Unit tests** use `TEST_CASE("/path/to/test")` macros (defined in `flow/include/flow/UnitTest.h`):
```bash
bin/fdbserver -r unittests                           # all unit tests
bin/fdbserver -r unittests -f "/flow/DNSCache"       # single test by prefix
```

**Simulation tests** use TOML workload definitions in `tests/fast/`, `tests/slow/`, etc.:
```bash
bin/fdbserver -r simulation -f tests/fast/CycleTest.toml
```

**Via ctest:**
```bash
ctest -L fast                    # all fast tests
ctest -R "StatusDuringOutage"    # by name pattern
```

Enable simulation tests in cmake: `-DENABLE_SIMULATION_TESTS=ON`

**Adding new tests:**
- For a new otherwise-unreferenced translation unit containing `TEST_CASE`, add a `forceLinkXxxTests()` stub and call it from `fdbserver/workloads/UnitTests.cpp`, or the linker can drop the tests.
- Register new `.toml` or `.txt` tests under `tests/` in `tests/CMakeLists.txt` via `add_fdb_test`. `configure_testing(... ERROR_ON_ADDITIONAL_FILES)` and `verify_testing()` should catch unregistered files during CMake configuration.

**Joshua**: bulk correctness/simulation testing runs on the internal Joshua cluster against optimized Release builds. Local `fdbserver -r simulation` runs are for iteration; large-scale shake-out happens on Joshua.

**Trace logs**: simulation and test runs emit `trace.*.xml` (or `.json`) files in the run directory. `TraceEvent(...)` is the canonical logging mechanism — search trace files by event name when debugging a simulation failure.

## Architecture

FoundationDB is a distributed ordered key-value store with strict serializability. The codebase is organized into ~12 subsystems. For background on subsystems before diving into code, the `design/` directory holds human-authored design docs and `design/AI-generated/` holds subsystem maps and per-subsystem diagrams (start with `design/AI-generated/foundationdb_subsystem_map.md`).

### Concurrency Model: Flow Actors and C++ Coroutines

FDB uses cooperative single-threaded concurrency. Code is written using either:

- **Flow actors** (`.actor.cpp` / `.actor.h` files): A custom preprocessor (`actorcompiler`) translates `ACTOR`, `state`, `wait()`, `choose/when` syntax into generated C++ state machines. The `#include "flow/actorcompiler.h"` must be the **last** include in actor files.
- **C++ coroutines**: Newer code uses `co_await` and `co_return` instead of `wait()` and actor-style `return`. Coroutines can appear in regular `.cpp` files and in `.actor.cpp` files that still contain actorcompiler input; actors and coroutines can be mixed. New code should use coroutines; see `design/coroutines.md`.

Key types: `Future<T>`, `Promise<T>`, `PromiseStream<T>`, `Reference<T>` (ref-counted pointer), `Optional<T>`, `ErrorOr<T>`, `Arena` (region-based allocation).

#### Common pitfalls

- `wait()` / `waitNext()` cannot appear inside ternary expressions, function arguments, or other sub-expressions. Assign to a `state` variable first, or use a small gating actor.
- C++ coroutines: `co_await` is not allowed inside a `catch` handler. Capture the error, exit the catch, then `co_await` outside.
- `ACTOR` functions declared in headers must not be defined inside an anonymous namespace, or call sites get ambiguous-overload errors.

### Core Subsystems

- **`flow/`** — Async runtime, event loop, tracing, deterministic random, arenas
- **`fdbrpc/`** — Endpoint-addressed RPC, peer management, failure monitor, Sim2 (deterministic simulation network)
- **`fdbclient/`** — Transaction API (`NativeAPI.actor.cpp`), read-your-writes (`ReadYourWrites.actor.cpp`), location cache, multi-version client
- **`fdbserver/`** — All server roles, organized by subdirectory:
  - `clustercontroller/` — Leader election, role recruitment, ServerDBInfo broadcasting
  - `coordinator/` — Paxos-based coordination state (generation registers)
  - `commitproxy/`, `grvproxy/`, `resolver/` — Transaction commit pipeline
  - `tlog/`, `logsystem/` — Durable mutation log (tag-partitioned)
  - `storageserver/` — Serves reads, applies mutations from TLogs
  - `kvstore/` — Pluggable storage engines (RocksDB, SQLite, memory/DiskQueue)
  - `datadistributor/` — Shard management, team building, data movement
  - `ratekeeper/` — Back-pressure and throttling
  - `workloads/` — Simulation test workloads

### Write Path

Client commit → GRV Proxy (assigns read version) → Commit Proxy (batches, sends to Resolver for conflict check, writes to TLogs) → TLogs (durable WAL) → Storage Servers (pull from TLogs, apply)

### Read Path

Client read → Storage Server (serves from MVCC versioned data or underlying storage engine)

### Recovery

When the transaction system fails, the Cluster Controller drives nine recovery phases (`READING_CSTATE` through `FULLY_RECOVERED`; `UNINITIALIZED` is state 0) defined in `fdbserver/core/include/fdbserver/core/RecoveryState.h`. Recovery reads coordinated state from coordinators, locks old TLogs, recruits the next transaction system, and recovers durable mutations from old log generations.

### Coordinator Consensus

Coordinators implement generation registers in `fdbserver/coordinator/Coordination.cpp`. `CoordinatedState` in `fdbserver/core/CoordinatedState.cpp` performs quorum reads and writes over them for recovery metadata. Coordinators separately participate in leader election, but transaction commits use the resolver/TLog pipeline instead.

### Simulation Testing

`fdbserver -r simulation` runs the entire cluster in a single process using Sim2, a deterministic simulated network. `BUGGIFY` macros inject faults (delays, failures, corruption). Tests are TOML files that compose workloads. This is FDB's primary testing strategy.

### Knobs

Runtime-tunable parameters are declared in `fdbserver/core/include/fdbserver/core/Knobs.h`, `fdbclient/include/fdbclient/Knobs.h`, and `flow/include/flow/Knobs.h` (UPPER_CASE names). Access them via `SERVER_KNOBS`, `CLIENT_KNOBS`, or `FLOW_KNOBS`. Defaults and buggification ranges live in `fdbserver/core/ServerKnobs.cpp`, `fdbclient/ClientKnobs.cpp`, and `flow/Knobs.cpp`.

## Compatibility

Before changing a serialized type that persists on disk, inspect its `serializer()` and adjacent versioning or downgrade comments. Do not reorder, remove, or change encoded fields without the appropriate version gate or migration and compatibility coverage.

## Naming Conventions

- **Variables, functions, methods**: camelCase (`smoothTotal`, `getWorkerForRole`)
- **Classes, structs, enums**: PascalCase (`StorageServer`, `RecoveryState`)
- **Knobs/constants**: UPPER_CASE (`MAX_BATCH_SIZE`, `VERSIONS_PER_SECOND`)
- **Globals**: `g_` prefix for important ones (`g_network`, `g_simulator`)

## Code Formatting

`clang-format` is used. Python code uses `black` and `flake8` (pre-commit hooks: `pip install pre-commit && pre-commit install`).

Edit `.actor.cpp` and `.actor.h` sources, not actorcompiler-generated output under the build directory.

## Branching

PRs target `main`. Release branches receive cherry-picks rather than direct PRs — don't open backport PRs without confirming first.

## Design Docs

When collaborating on a design doc, at some point reconcile the user's draft against `design/design-doc-template.md` — the template defines the section structure (motivation, design, alternatives, etc.) expected for FDB design docs. Suggest this reconciliation step before the doc is considered finished, not at the very start, so the draft has time to take shape first.
