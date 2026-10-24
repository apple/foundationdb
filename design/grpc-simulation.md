# gRPC in Deterministic Simulation

## Objective

Make FoundationDB's gRPC services testable under deterministic simulation, and make the gRPC that FDB ships the one it
tests. Today a gRPC service inside `fdbserver` runs only outside simulation, against whatever gRPC happens to be
installed in `/opt/grpc` on the build host. This design runs real gRPC on Sim2's simulated network and clock, pins and
patches the gRPC that FDB builds, and serves the worker `ControlService` in simulation with a workload that drives it
under faults.

## Background

FDB has two gRPC services in tree. `fdbctl::ControlService` (`fdbctl/protos/control_service.proto`) is roughly
`fdbcli` over gRPC: coordinators, status, workers, include, exclude and kill. Each worker started with a `:grpc` listen
address serves it. `FileTransferService` (`fdbrpc/protos/file_transfer.proto`) is a prototype with no caller outside
unit tests.

Neither had simulation coverage. `registerWorkerGrpcServices` returned `Never()` under simulation, and simulated
processes never initialised `FlowGrpc` at all. The reason was determinism: gRPC brings its own threads (timer manager,
executors, the sync server's thread pool), reads the real clock, seeds its random generators from the OS, and orders
some internal work by pointer value. Any of these makes a simulation seed irreproducible. Since FDB's correctness
testing is simulation, that left an operator-facing API (exclude, include, kill, coordinator changes) without the
testing that the rest of the server gets.

gRPC also came from outside the build. CMake searched `/opt/grpc` or `/opt/grpc_clang`. When gRPC was missing, it
silently turned `WITH_GRPC` off, while still packaging tests for gRPC workloads that then did not exist.

## Requirements

1. **Reproducible seeds.** A seed produces the same simulation (same `Unseed`) on any run of the same binary,
   regardless of ASLR, working directory, environment size, or what ran before in the process. This is the property
   Joshua's determinism check enforces on a sample of runs.
2. **Single-threaded under simulation.** No gRPC work runs off the simulation's run-loop thread.
3. **Faults.** gRPC must keep working, and report errors rather than hang or crash, while the simulator kills, reboots,
   clogs and partitions the processes it runs on. That includes processes killed with calls in flight.
4. **No production behaviour change.** Simulation hooks may exist in production binaries, but outside simulation every
   code path is gRPC's or abseil's own: OS entropy, real clocks, real threads.
5. **One gRPC.** FDB builds and links the gRPC version and patches it tests. A build either has that gRPC or has none
   (`WITH_GRPC=OFF`).
6. **Bounded build cost.** gRPC must not block configure, and its compile must parallelise with FDB's and go through
   the compiler launcher (sccache) like FDB's own code.
7. **Compatibility.** Persisted state touched along the way stays readable by the releases FDB supports downgrading to.

## Design Overview

- **`SimEventEngine`** (`fdbrpc/SimEventEngine.cpp`) implements gRPC's `EventEngine` interface on top of Flow:
  `INetworkConnections` for sockets, Flow `delay` for timers, the Flow run loop for closures. Under Sim2 those are the
  simulated network and clock, so gRPC runs inside the simulation like any Flow code.
- **Three gRPC patches** (`cmake/patches/grpc/`) remove the remaining sources of nondeterminism inside gRPC and abseil.
- **gRPC is built from source in FDB's build** (`cmake/CompileGrpc.cmake`), pinned to 1.83.0 with its bundled abseil,
  protobuf, re2 and c-ares.
- **`ControlService` becomes a callback service.** Simulated fdbd processes serve it when a test sets `grpcEnabled`,
  and the `GrpcControlService` workload exercises it.

## Detailed Design

### Event engine

gRPC 1.83 routes nearly all I/O, timers and closure execution through the `EventEngine` it is given. `SimEventEngine`
provides:

- **Endpoints and listeners.** These wrap Flow `IConnection`/`IListener`. Reads and writes complete through Flow
  futures; completions are always delivered asynchronously, because gRPC may start the next read or write from inside
  a callback.
- **Timers and closures (`Run`, `RunAfter`, `Cancel`).** These schedule Flow coroutines on the run loop.
- **DNS.** Numeric addresses only. A lookup answers inline when called from Flow code (gRPC calls it blocking while
  binding a port) and asynchronously when called from inside an engine closure (it holds locks there).
- **Clock.** `gpr_now_impl` is redirected to simulated time. gRPC refuses a monotonic clock below one second, so
  simulated time is offset by 10^6 s.
- **Timer threads off.** The legacy iomgr timer manager, which gRPC still initialises, runs unthreaded and is ticked
  from the run loop.
- **Logs.** abseil's log sink sends gRPC's logs to `TraceEvent("GrpcLog")` instead of stderr. Stderr output fails a
  simulation.
- **Seeds.** abseil's seed material comes from the simulation's `deterministicRandom()` through the hook added by
  patch 0003.

Two properties need care.

*Process ownership.* A Sim2 connection may only be touched from its own simulated process, but gRPC is one library per
OS process and runs any call's work wherever it happens to be. So:

- Each connection records its owning process: the listener's process for accepted connections, and for outgoing ones
  the process named by the channel argument `fdb.sim_process`.
- Work on a connection is sent to its owner.
- gRPC's own closures run on a home process that the simulator never kills. Running them on whichever process
  scheduled them would strand unrelated calls' timers whenever that process died.
- Server handlers that do Flow work hop explicitly to the process hosting the service (see below).

*Dead peers.* The simulator models a killed or rebooted peer as silence: reads block and writes never drain, much like
a host that disappears without sending a reset. gRPC detects this only through keepalive. The engine applies gRPC's
keepalive timeout as a TCP user timeout, as gRPC's POSIX engine does with `TCP_USER_TIMEOUT`, so a write to a vanished
peer fails instead of blocking forever. A channel without keepalive keeps using a dead connection until its calls'
deadlines expire. That is also what a real client would see after losing a host without a reset, and it is why clients
of these services should enable keepalive.

### Remaining nondeterminism in gRPC: the patches

With the engine in place, a sweep running each seed twice, with different working-directory lengths and environment
sizes, still diverged. The causes, each found by bisecting the first divergent engine operation:

1. **`0001`: queued-call sets ordered by pointer.** `ClientChannelFilter` keeps calls waiting for a resolver result or
   an LB pick in sets ordered by pointer value, so the order in which they are retried followed heap layout. The patch
   uses insertion-ordered sets (`src/core/util/insertion_ordered_set.h`).
2. **`0002`: streams cancelled in hash order.** When chttp2 tears down a transport or handles GOAWAY, it cancelled
   streams by iterating a hash map of stream pointers. The memory reclaimer picked its victim the same way. The patch
   cancels in stream-id order and reclaims the lowest id.
3. **`0003`: abseil seed material.** abseil seeds its random generators, used for example in reconnect backoff jitter,
   from the OS. The patch adds `absl::random_internal::SetSeedMaterialSource()`: when set, it supplies seed material
   instead of the OS. Simulation sets it; nothing else does, so production keeps abseil's own entropy path
   (requirement 4).

The patches are FDB's permanently. Upstream is slow to accept changes, and patches 0001 and 0002 only matter to a
deterministic engine. Upgrading gRPC means re-applying them and re-running the determinism sweeps.

### Building gRPC

`cmake/CompileGrpc.cmake`:

1. At configure time, `FetchContent` fetches gRPC v1.83.0 with only the submodules it needs (abseil-cpp, protobuf, re2,
   c-ares) and applies `cmake/patches/grpc/*.patch`, restoring the checkout first so a changed version or patch list
   re-patches cleanly. Hosts without GitHub access set `FETCHCONTENT_SOURCE_DIR_GRPC` to an already patched tree.
2. Adds the patched tree with `add_subdirectory(... EXCLUDE_FROM_ALL)` inside a function, so its settings stay local:
   - static libraries;
   - abseil, protobuf, re2 and c-ares built as modules;
   - zlib and OpenSSL taken from FDB's own `find_package` results;
   - FDB's warning options (including `-Werror`) dropped for gRPC's sources;
   - `CMP0077` and `CMP0126` set to NEW, so that bundled projects with old minimum CMake versions honour these
     settings rather than their cached defaults.
3. Defines `gRPC::grpc++` and `gRPC::grpc_cpp_plugin`, and includes protobuf's `protobuf_generate`, so existing
   `generate_grpc_protobuf` users are unchanged.

Only what FDB links gets compiled, and it compiles in parallel with FDB through any compiler launcher.

OpenSSL's include directory is passed to gRPC by its real path. On a Homebrew host it is otherwise `/opt/homebrew/include`,
which also holds any Homebrew-installed abseil and protobuf. Because gRPC puts the OpenSSL directory ahead of its
bundled ones, those would shadow the bundled headers.

The cost was measured on Linux (clang, libc++, LLD, Release, `-j6`, clean build, `ninja fdbserver`, one run each):

| | configure | build | stripped `fdbserver` |
|---|---|---|---|
| `WITH_GRPC=OFF` | 4 s | 1464 s | 84.7 MB |
| vendored gRPC | 30 s | 2021 s | 97.4 MB |

Repeat builds through sccache should recover most of the gRPC compile time; that has not been measured.

### ControlService in simulation

`ControlServiceImpl` was a sync gRPC service: each handler blocked a gRPC thread on
`onMainThread(...).getBlocking()`. It is now a callback service. A handler:

1. obtains the call's reactor;
2. starts its Flow work on the main thread (via `onMainThreadVoid` in production; under simulation it is already
   there and first moves to the process hosting the service);
3. finishes the reactor from an uncancellable coroutine when the work completes.

Request deadlines come from `raw_deadline()` and `gpr_now()`, which follow simulated time, instead of
`std::chrono::system_clock`.

With the test configuration `grpcEnabled = true`, `setupSimulatedSystem` installs the engine once. Each simulated fdbd
process then:

1. gets a listener on its first port plus `SIM_GRPC_PORT_OFFSET` (60000, above the ports the simulator assigns),
   through the new `ISimulator::addProcessListener`;
2. initialises `FlowGrpc` and runs `GrpcServer`.

The worker then registers `ControlService` as it does outside simulation. Under simulation, `GrpcServer` creates no
pool thread, and its blocking stop path asserts that it is not reached.

### Fixes found along the way

- **`GrpcServer::stopServerSyncInternal` returned early whenever a server existed.** Stopping was a no-op, and the
  `grpc::Server` destructor then blocked the main thread.
- **`GrpcServer::run` logged a SevError on `actor_cancelled`,** so every killed process failed a simulation. The
  `exclude` and `excludeStatus` handlers swallowed `actor_cancelled`.
- **`ProcessData::grpcAddress` was never persisted.** `workerListValue` encoded `ProcessData` at the 6.2 protocol
  version, while `ProcessData::serialize` writes `grpcAddress` only from 8.0, so `GetWorkers` never reported gRPC
  addresses. The worker list is now encoded at 8.0 (see Rollout).
- **Sim2 reported a spurious `LeakedConnection` SevError** when a peer closed a connection that its owner had already
  closed.

## Alternatives Considered

- **Stock gRPC with only the event engine.** Measured to diverge between runs of the same seed, from the three causes
  the patches address. This was confirmed against both gRPC 1.70.1 and 1.83.
- **A system or prebuilt gRPC.** This cannot guarantee the patches, and has no single version across build hosts.
- **Building gRPC at configure time** (a separate CMake build and install, then `find_package`). It worked, but
  blocked configure for about 12.5 minutes on a clean build directory, could not overlap with FDB's compile, bypassed
  sccache, and built every gRPC target. Replaced by the in-build approach above.
- **`ExternalProject` at build time,** as `cmake/awssdk.cmake` does. FDB would have to declare roughly 80 abseil
  libraries as imported targets by hand, and that list changes with every abseil release.
- **Overriding abseil's entropy functions at link time** (and, for gRPC 1.70, `clock_gettime`). This was the first
  approach. It changed production: the `clock_gettime` override sent every clock read in `fdbserver` through a system
  call instead of the vDSO, and the replacement seed mixing ran outside simulation too. Replaced by patch 0003.
- **Disabling gRPC in simulation** (the status quo). This leaves an operator-facing API untested under faults.

## Testing Considerations

- **`/fdbrpc/simgrpc/echo` and `/fdbrpc/simgrpc/extended`** (`fdbrpc_test --simulation`) run real gRPC servers and
  clients between simulated processes. Each seed chooses TLS (fixed test certificates; generated ones would take their
  validity from the wall clock), memory quota, keepalive, buggify and call gating. The tests cover unary and server
  streaming calls, several clients, graceful shutdown, and kill-and-restart of the server.
- **The `GrpcEcho` workload** (`tests/rare/GrpcEcho.toml`) runs echo servers on dedicated processes alongside a
  cluster, while killing, clogging, gracefully restarting and replacing them.
- **The `GrpcControlService` workload** (`tests/rare/GrpcControlService.toml`) drives the worker service through random
  workers, alongside Cycle, RandomClogging and Attrition with reboots. It calls GetStatus (and parses the JSON),
  GetWorkers (checking each worker's gRPC address), GetCoordinators and ExcludeStatus, and excludes then includes a
  storage process.
  - The toml sets `runFailureWorkloads = false`. Randomly added failure workloads can kill machines for good, and with
    a process excluded, the rest may be too few to recruit a transaction system; recovery then waits for an include
    that cannot commit.
  - The workload reconnects after a timed-out call, because the simulator does not reset connections to dead peers.
- **Determinism** was checked by running each seed twice, with different working directories and environment sizes,
  and comparing `Unseed` (fdbserver) or a fingerprint of the logged results (unit tests), on macOS and Linux.
- **Joshua results:**
  - Two 100,000-run ensembles of all tests had no failures. One had three runs killed externally with no output; the
    other passed 100,000 of 100,000. They included 293 runs of the `restarting/to_*` downgrade tests.
  - A 100,000-run ensemble of GrpcControlService alone passed 100,000 of 100,000.

## Observability/Supportability Considerations

- gRPC's own log messages appear as `GrpcLog` trace events at a severity mapped from abseil's.
- `ControlService` is an operator API, and its clients should enable keepalive (see Dead peers). gRPC servers reject
  frequent client pings by default, so a deployment that recommends keepalive needs a server ping policy to match.
- `GetWorkers` returns an empty list, with status OK, while a new cluster controller rebuilds the worker list. Tools
  should treat that as transient.

## Rollout/Migration Considerations

- **Building.** Builds need either network access to GitHub at configure time or `FETCHCONTENT_SOURCE_DIR_GRPC`
  pointing at a patched v1.83.0 source tree with the four submodules. `WITH_GRPC=OFF` remains available, and the gRPC
  tests are registered only when gRPC is built.
- **Worker list encoding.** `FDB_PV_WORKER_LIST_VALUE` moves from the 6.2 version to 8.0 (`0x0FDB00B080000000`).
  - 8.0 decodes the gRPC address.
  - release-7.4 accepts the 8.0 tag (its limit is `0x0FDB00B081000000`) and ignores the trailing field, because its
    `ProcessData` reads only locality, class and address.
  - Each new cluster controller clears and rewrites the worker list, so values in an older format do not persist.
  - Tagging with a newer version than 8.0 would break downgrades to 8.0.
  - The change is safe to backport to 8.0.x.
- **gRPC upgrades.** Moving to a new gRPC version means re-applying the patches, re-running the determinism sweeps,
  and a Joshua ensemble of the gRPC tests.

### Known limitations

- **Memory-quota hangs.** With a small gRPC memory quota (256 KiB in `echo`, 1 MiB in `extended`), about 2 of 30 seeds
  hang, with the server answering GOAWAY "Buffers full". The tests avoid those quotas.
- **Leaks on simulated reboot.** Each reboot leaks a `FlowGrpc`, `GrpcServer` and `grpc::Server`, because destroying a
  `grpc::Server` blocks.
- **Blocking stop paths.** The `GrpcServer` `*Sync` stop paths block the main thread, so they deadlock while a callback
  request is in flight.
- **FileTransferService is not covered.** It is still a sync service that reads files with `std::ifstream`, bypassing
  the simulated file system.
