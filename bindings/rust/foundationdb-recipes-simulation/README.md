# FoundationDB recipes simulation

This crate runs deterministic FoundationDB simulator workloads for recipes.
Its leader-election workload exercises the poll-based `LeaderElection` recipe
composed with `RankedRegister` fencing.

## Running

```bash
./scripts/run_leader_election_simulation.sh
./scripts/run_leader_election_simulation.sh 50
```

The script builds the release workload and runs the one canonical polling
configuration. Its argument is the total number of runs, so `50` means exactly
50 runs. Each run prints a generated 32-bit seed and writes to an isolated
trace directory. Passing directories are deleted; a failing directory is kept
and the script prints an exact reproduction command. To reproduce a seed
directly:

```bash
fdbserver -r simulation \
  -f foundationdb-recipes-simulation/test_poll_leader_election.toml \
  -b on --trace-format json -L target/traces --logsize 1GiB --seed <SEED>
```

## CI campaigns

CI runs 1 worker x 10 seeds for ordinary pull requests and pushes to `main`.
Changes to the leader-election or RankedRegister recipes, simulation code, or
the runner use 4 workers x 50 seeds. The nightly and manually dispatched
campaigns, plus pull requests labelled `leader-election-correctness`, use 4
workers x 200 seeds. Failed runs retain their trace directory as a workflow
artifact.

## Leader-election workload

One serializable durable key contains a monotonic revision, optional owner, and
persisted lease duration. Revisions never decrease. Clients use heterogeneous
configured durations, but followers wait only the duration persisted with their
exact unchanged observation.

Each client deterministically selects one swarm profile from the workload RNG:
standard (40 operations, 2-second lease base), contention (160 operations,
1-second lease base), or suspicion (80 operations, 3-second lease base).
`operationCount` and `suspicionSecs` remain optional workload overrides for
focused runs; the canonical configuration leaves both unset so clients use the
swarm defaults. Setup traces and metrics report the selected profile and the
effective operation count and lease base. The `swarm_profile` metric maps
standard, contention, and suspicion to 0, 1, and 2 respectively.

Each `db.run` attempt obtains `attempt_started_at` from simulated monotonic time
as its first action, then sets `AutomaticIdempotency` and calls `poll`. After a
successful outer run, follower observations are adopted with a fresh simulated
time. Failed poll and resign paths discard local state and rotate to a fresh
caller incarnation. Time is never persisted or used to order commits.

Clients autonomously select a weighted mix of normal polls and adversarial
resigns, observers, stale-token actions, pauses, duration changes, delayed
post-commit local-state adoption, and local incarnation loss. A deterministic
swarm bitset enables optional operation families. The delayed-adoption family
selects sub-lease, exact-lease, and over-lease simulated delays from the
workload RNG before `db.run`. It delays only a new or reset follower
observation, using its persisted lease duration, after a successful run and
before calling `PollResult::into_next_state`. Every leader poll co-commits
`RankedRegister::read(rank)`, protected `write(rank, payload)`, and its
operation log entry. Leadership alone is not authority.

The completion phase biases execution toward boundary, stale-token,
duration-reset, fencing, and delayed-adoption coverage that a short autonomous
run might otherwise miss. Delayed-adoption metrics report successful delays in
each of the three duration classes. Replay requires an over-lease delayed
observation followed by a poll before that observation's adoption-based expiry.

## Commit-order oracle

Every simulated poll, resign, observer, and stale write is co-logged in its
transaction. Log keys begin with an incomplete versionstamp, so the final range
is ordered by actual commit order. Entries include durable prior/current state,
caller local input, attempt time, configured duration, transition, fencing
write evidence, and payload identity.

During `check`, client 0 reads the entire operation log, public election state,
and ranked-register write rank and value in one transaction at one read version.
It replays from revision zero, no owner, and no protected value. The oracle
enforces commit order, unique logical operations, exact local-state adoption,
and public snapshot agreement. It tracks observed coverage for:

- before, exact, and after persisted-duration expiry, including foreign takeover;
- stale renewal, stale renewal after rank advance, exact resignation, delayed
  stale resignation, and stale resignation after takeover;
- follower reset after a renewal changes persisted duration;
- rejected rank-zero and post-advance stale writes; and
- a same-transaction fenced protected write for every leader poll.
