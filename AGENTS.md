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
bin/fdbserver -r simulation -f tests/fast/Cycle.toml
```

**Via ctest:**
```bash
ctest -L fast                    # all fast tests
ctest -R "StatusDuringOutage"    # by name pattern
```

Enable simulation tests in cmake: `-DENABLE_SIMULATION_TESTS=ON`

**Adding new tests** (silent failure modes — the test compiles but never runs):
- New `.cpp` files containing `TEST_CASE` need a `forceLinkXxxTests()` stub, called from `fdbserver/workloads/UnitTests.cpp`, or the linker drops them.
- New simulation TOMLs under `tests/` must be registered in `tests/CMakeLists.txt` via `add_fdb_test`, otherwise `ctest` won't see them.

**Joshua**: bulk correctness/simulation testing runs on the internal Joshua cluster against optimized Release builds. Local `fdbserver -r simulation` runs are for iteration; large-scale shake-out happens on Joshua.

**Trace logs**: simulation and test runs emit `trace.*.xml` (or `.json`) files in the run directory. `TraceEvent(...)` is the canonical logging mechanism — search trace files by event name when debugging a simulation failure.

## Architecture

FoundationDB is a distributed ordered key-value store with strict serializability. The codebase is organized into ~12 subsystems. For background on subsystems before diving into code, the `design/` directory holds human-authored design docs and `design/AI-generated/` holds subsystem maps and per-subsystem diagrams (start with `design/AI-generated/foundationdb_subsystem_map.md`).

### Concurrency Model: Flow Actors and C++ Coroutines

FDB uses cooperative single-threaded concurrency. Code is written using either:

- **Flow actors** (`.actor.cpp` / `.actor.h` files): A custom preprocessor (`actorcompiler`) translates `ACTOR`, `state`, `wait()`, `choose/when` syntax into generated C++ state machines. The `#include "flow/actorcompiler.h"` must be the **last** include in actor files.
- **C++ coroutines** (regular `.cpp` files): Newer code uses `co_await`, `co_return`, `while(true)` instead of `wait()`, `return`, `loop`. Migration from actors to coroutines is ongoing.

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

When the transaction system fails, the Cluster Controller drives a 10-phase recovery state machine (`RecoveryState` 0-9 in `RecoveryState.h`) that reads coordinated state from coordinators, locks old TLogs, recruits a new transaction system, and replays uncommitted mutations.

### Coordinator Consensus

Coordinators implement single-decree Paxos via generation registers (`localGenerationReg()` in `Coordination.actor.cpp`). The `CoordinatedState` layer adds quorum voting. This is used only for ~1KB of cluster metadata (who is CC, log system config), never for transaction commits.

### Simulation Testing

`fdbserver -r simulation` runs the entire cluster in a single process using Sim2, a deterministic simulated network. `BUGGIFY` macros inject faults (delays, failures, corruption). Tests are TOML files that compose workloads. This is FDB's primary testing strategy.

### Knobs

Runtime-tunable parameters are in `ServerKnobs.h`/`ClientKnobs.h`/`FlowKnobs.h` (UPPER_CASE names). Accessed via `SERVER_KNOBS->KNOB_NAME`. Some are buggified in simulation. The matching `*Knobs.cpp` files contain `init(KNOB_NAME, value)` calls — these are the source of truth for default values and buggification ranges, complementing the `.h` declarations.

## Naming Conventions

- **Variables, functions, methods**: camelCase (`smoothTotal`, `getWorkerForRole`)
- **Classes, structs, enums**: PascalCase (`StorageServer`, `RecoveryState`)
- **Knobs/constants**: UPPER_CASE (`MAX_BATCH_SIZE`, `VERSIONS_PER_SECOND`)
- **Globals**: `g_` prefix for important ones (`g_network`, `g_simulator`)

## Code Formatting

`clang-format` is used. Python code uses `black` and `flake8` (pre-commit hooks: `pip install pre-commit && pre-commit install`).

## Branching

PRs target `main`. Release branches receive cherry-picks rather than direct PRs — don't open backport PRs without confirming first.

## Design Docs

When collaborating on a design doc, at some point reconcile the user's draft against `design/design-doc-template.md` — the template defines the section structure (motivation, design, alternatives, etc.) expected for FDB design docs. Suggest this reconciliation step before the doc is considered finished, not at the very start, so the draft has time to take shape first.
