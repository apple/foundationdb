# Running mako_storage_bench.sh on a RAM disk (okteto dev pods)

`mako_storage_bench.sh` is designed to make the **storage-server CPU** the
bottleneck so it can detect storage-engine CPU regressions. That only holds if
the cluster's data directory is on fast local storage. On an okteto dev pod it
is **not**: the pod's filesystem is a container overlay backed by **network
attached EBS**, so the benchmark is bound by EBS fsync/IO latency, not storage
CPU. Symptom: throughput plateaus low (≈1.5k TPS on a fresh m7a pod) while the
storage process sits well under 85% CPU, and commit latency is tens of ms.

The fix is to put the cluster's data on a **tmpfs (RAM-backed) directory** so
commits no longer pay disk latency and the storage process becomes CPU-bound.
The script does this for you: when `/mnt/ram` exists and is writable it uses it
automatically (and adds the tmpfs knob described below), so once the dev pod has
a `/mnt/ram`, a plain `contrib/mako_storage_bench.sh build_output4` runs
CPU-bound with no extra flags. The only real work is giving the pod that RAM
directory, which on okteto takes a little care.

## TL;DR

```bash
# one-time pod setup (details below): give the dev pod a /mnt/ram tmpfs + headroom.
# then just run it -- the script auto-detects /mnt/ram and adds the tmpfs knob:
contrib/mako_storage_bench.sh build_output4
```

## What the script does automatically (and how to override)

- **`WORKDIR`** — the cluster data dir and mako output live under `WORKDIR`. The
  script defaults it to `/mnt/ram/mako_storage_bench` when `/mnt/ram` is a
  writable directory, otherwise `$PWD/mako_storage_bench`. Override by exporting
  `WORKDIR` (e.g. point it off the RAM disk to force the EBS data dir).
- **The tmpfs knob** — fdbserver opens its data files with `O_DIRECT` kernel AIO
  by default, and **tmpfs rejects `O_DIRECT`**, so without a fallback the cluster
  never starts. When `WORKDIR` is on a tmpfs the script automatically adds
  `--knob_disable_posix_kernel_aio=1` (flow then uses the EIO buffered path that
  tmpfs accepts). Pass extra knobs via `KNOBS='...'`; the script appends the
  tmpfs knob for you if you didn't include it.
- **Bounded duration** (`SECONDS_RUN`/`WARMUP_SECONDS`) — optional; see *Sizing*.

## Storage engines: redwood (default) vs rocksdb on tmpfs

The default engine is **redwood** (`ssd-redwood-1`); the script's automatic
tmpfs knob is all it needs, so no `KNOBS` are required.

The **rocksdb** engine (`... build_output4 rocksdb`) needs **two more knobs**.
RocksDB does its own file I/O, independent of flow, and on `ssd-rocksdb-v1` it
opens the DB with `O_DIRECT` by default (`ROCKSDB_USE_DIRECT_READS` and
`ROCKSDB_USE_DIRECT_IO_FLUSH_COMPACTION` both default `true` in ServerKnobs).
tmpfs rejects `O_DIRECT`, so the storage server fails at `Open`
(`RocksDBError: "Invalid argument: Direct I/O is not supported by the specified
DB."`) and **crash-loops** — and because that happens before any data dir is
created, the bench **hangs at `configure new`** (the script's `BUILD_TIMEOUT`
only guards the mako build, not cluster startup, so it will not self-abort).

The automatic tmpfs knob does not affect RocksDB; you must disable RocksDB's
direct I/O separately. These are runtime knobs — **no recompile**. The script
still auto-adds the flow knob, so you only supply the two `rocksdb_*` knobs:

```bash
KNOBS='--knob_rocksdb_use_direct_reads=false --knob_rocksdb_use_direct_io_flush_compaction=false' \
    contrib/mako_storage_bench.sh build_output4 rocksdb
```

The two `rocksdb_*` knobs are harmless to redwood, so the same `KNOBS` also
works for a `... build_output4 redwood rocksdb` run.

Observations from a full 300s/600s run on a 24Gi tmpfs (m7a pod): with the
knobs, rocksdb runs storage-CPU-bound just like redwood (~4.6k vs ~4.8k TPS) and
its peak footprint was **smaller** (~3.3 GB vs redwood's ~5.5 GB) — i.e. rocksdb
did **not** blow out the ramdisk. Expect higher per-GET latency and a higher
conflict rate than redwood, though.

## You cannot just `mount` a tmpfs inside the pod

The dev container has no `CAP_SYS_ADMIN`, so `mount -t tmpfs ...` fails with
*permission denied*. `/dev/shm` exists but is capped at 64 MB. The large tmpfs
mounts you may see (`/var/okteto/secret`, etc.) are read-only. So the RAM
directory has to come from the pod spec as a memory-backed `emptyDir`.

## okteto topology gotcha: patch the BASE deployment, not the clone

`okteto up` does **not** run your deployment directly. For an okteto manifest
with `name: <dev>` it keeps two deployments:

- **`<dev>`** — the base deployment, scaled to `0/0`. This is the source of
  truth okteto clones from.
- **`<dev>-okteto`** — the live clone okteto generates and **actively
  reconciles**. It runs your pod.

okteto **strips volumes it doesn't own from the clone**, so a
`kubectl patch deployment <dev>-okteto ...` adding a volume silently disappears.
But okteto **preserves the base deployment's volumes** when it clones (that is
how `efs-ccache`, the fdb cluster file, etc. reach the dev pod). So the volume
must be added to the **base** `<dev>` deployment; `okteto up` then carries it
into the pod.

(Worked example below uses the deployment name `gglass-dev`; substitute your
okteto manifest's `name:`.)

## Setup

### 1. Raise the dev container memory limit (okteto.yml)

A memory-backed `emptyDir`'s contents are **charged to the container's memory
cgroup**, i.e. they count against the `dev` container's memory *limit* (not the
node's free RAM). So the tmpfs contents **plus** all fdbserver/mako RSS must fit
under the limit. Raise it in `okteto.yml`:

```yaml
resources:
  requests:
    cpu: "4000m"
    memory: "24Gi"     # was 16Gi
  limits:
    cpu: "8000m"
    memory: "48Gi"     # was 32Gi -- room for a 24Gi tmpfs + fdbserver/mako RSS
```

This part *does* persist in `okteto.yml` across `okteto up`.

### 2. Add the tmpfs volume to the base deployment

`okteto.yml` (this manifest format) cannot declare an `emptyDir`, so apply it as
a strategic-merge patch on the **base** deployment. Save as `fdb-ramdisk.yaml`:

```yaml
spec:
  template:
    spec:
      volumes:
        - name: ramdisk
          emptyDir:
            medium: Memory
            sizeLimit: 24Gi
      containers:
        - name: dev
          volumeMounts:
            - name: ramdisk
              mountPath: /mnt/ram
```

Apply it to the base (it is at 0 replicas, so this disturbs nothing running):

```bash
kubectl patch deployment gglass-dev --patch-file fdb-ramdisk.yaml
```

> The patch edits the live base deployment. Whatever provisions `gglass-dev`
> (your dev-pod infra/manifest) will overwrite it on a re-apply. For a permanent
> setup, add the same `volumes`/`volumeMounts` blocks to that source manifest.

### 3. Re-create the pod

```bash
okteto up
```

okteto clones the (now patched) base into `<dev>-okteto`, so the new pod has
`/mnt/ram` and the 48Gi limit.

### 4. Verify

```bash
kubectl exec <pod> -c dev -- df -hT /mnt/ram        # want: tmpfs, 24G
kubectl exec <pod> -c dev -- sh -c 'touch /mnt/ram/.w && rm /mnt/ram/.w && echo ok'
```

## Sizing the tmpfs

The on-disk footprint grows with **total inserts = TPS x wall-time** (the
default `g18ui` workload inserts one new key per transaction). RAM-backing
raises TPS, so the same wall-clock writes *more* than on EBS. As a reference, an
EBS run that managed ~1.5k TPS for 600 s wrote ~4 GB; the redwood file is the
bulk and runs roughly ~2 KB per insert including COW/extent overhead.

Two facts make sizing easy:

- A tmpfs `sizeLimit` is a **hard cap** (writes past it get ENOSPC) **and** its
  usage counts against the container memory limit (over the limit ⇒ OOMKill).
- Memory is only consumed for bytes actually written, not reserved up front.

Recommendations:

- The full 300s/600s default fits comfortably on a 24Gi tmpfs for redwood
  (~5.5 GB peak). To bound the footprint for a faster engine or a longer run,
  shorten it with `SECONDS_RUN=120 WARMUP_SECONDS=60` (a 2-minute measured
  window is plenty once CPU-bound).
- `sizeLimit: 24Gi` + `limits.memory: 48Gi` comfortably covers that. For very
  high TPS or long runs, raise both (e.g. 40Gi tmpfs / 64Gi limit) and confirm
  the node has the RAM.

## Disabling / reverting

- Remove the volume from the base and re-clone:

  ```bash
  kubectl patch deployment gglass-dev --type=json \
    -p '[{"op":"remove","path":"/spec/template/spec/volumes/-"}]'
  okteto up
  ```

  (or re-apply your base `gglass-dev` manifest).

- Restore the `okteto.yml` memory limits if you want the original ceiling.
- The script now **prefers `/mnt/ram` automatically** when it is present, so a
  plain run uses the RAM disk. To force the EBS data dir without removing the
  volume, export `WORKDIR=$PWD/mako_storage_bench` (any non-tmpfs path) for that
  run.

## Other gotchas

- **You will lose `build_output4`.** It lives on the ephemeral container overlay,
  and both the memory-limit change and the volume patch force a pod rollout.
  Rebuild with `run-ccmk4` (or your build wrapper) afterward — the ccache is on a
  persistent EFS volume, so a warm rebuild is minutes, not a cold build.
- **`mako` links `libfdb_c.so` and is built with `SKIP_BUILD_RPATH`.**
  `mako_storage_bench.sh` already sets `LD_LIBRARY_PATH=<build>/lib` (and
  `LD_BIND_NOW=1`) so mako loads the matching client. Unrelated to the RAM disk,
  but it is why mako runs from the build tree at all.
- **mako aborts in `_dl_fini` at exit** (a libfdb_c-as-shared-lib teardown issue)
  after writing all its output. The script judges success by the completed
  report and disables core dumps (`ulimit -c 0`), so this is harmless — do not
  be alarmed by the abort message.
