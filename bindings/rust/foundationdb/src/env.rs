// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Pluggable sources for the effects that must stay deterministic under
//! FoundationDB simulation: time and randomness.
//!
//! # Why control flow must not read ambient time or randomness
//!
//! The deterministic simulator virtualizes both: it decides what time it is and
//! which pseudo-random choices are made, which is precisely what lets it replay
//! a failing run from a seed. Code that calls `Instant::now()` or draws from a
//! thread-local generator steps outside that control, so its behavior changes
//! from run to run and a bug found in simulation cannot be reproduced.
//!
//! The way out is to take time and randomness as dependencies instead of
//! reaching for them: [`Clock`] and [`Rng`], bundled in an [`Environment`].
//! Production code uses [`WallClock`] and [`SeededRng`], which do the obvious
//! thing. Simulation workloads pass implementations backed by the simulator,
//! provided by the `foundationdb-simulation` crate.
//!
//! Ambient wall-clock time remains fine for purely observational output, metrics
//! durations or log timestamps that nothing branches on.
//!
//! # Measure with monotonic, persist with wall
//!
//! [`Clock`] offers two readings and they are not interchangeable.
//! [`Clock::monotonic`] never goes backwards but counts from an epoch private to
//! that one instance, so it is the right and only choice for durations,
//! deadlines, budgets and backoff. [`Clock::wall`] is a real timestamp, counted
//! from the UNIX epoch, so it is the right choice for anything written to the
//! database or compared across processes, at the cost of being able to jump when
//! the machine clock is adjusted.
//!
//! Never persist a monotonic reading and never compare two of them coming from
//! different [`Clock`] instances: the values are meaningless outside the
//! instance that produced them.
//!
//! # Drawing a lot of randomness
//!
//! [`Rng`] is deliberately minimal, one `u64` per call through a shared
//! reference. A component that needs a large volume of random data, or
//! distributions this trait does not offer, should draw a single `u64` and use
//! it to seed its own local generator (`rand_chacha::ChaCha8Rng::seed_from_u64`
//! for instance). The run stays reproducible, since the seed came from the
//! environment, and the heavy sampling stays local and fast.
//!
//! # Non-goals
//!
//! This module supplies *values*, never control over execution: there is no
//! sleep and no spawn capability here. Under simulation the caller owns the
//! schedule: it advances simulated time and re-invokes the deterministic unit of
//! work. An asynchronous `Timer` capability is planned surface and is
//! deliberately deferred, so do not work around its absence by sleeping on the
//! ambient runtime inside code that must be simulable.
//!
//! # Known limitation
//!
//! Under simulation, wall time is derived from simulated time and therefore
//! never jumps. Bugs that only appear when the machine clock is stepped by NTP,
//! a leap second or an operator, cannot be exercised in simulation: exercise
//! those with a purpose-built [`Clock`] in a unit test instead.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Where time comes from.
///
/// See the [module documentation](self) for why time is a dependency rather
/// than something to reach for, and for the rule that decides between the two
/// readings: measure with [`monotonic`](Self::monotonic), persist with
/// [`wall`](Self::wall).
pub trait Clock: fmt::Debug + Send + Sync {
    /// A monotonic reading, for measuring how much time has passed.
    ///
    /// Successive readings never decrease, which makes this the reading to use
    /// for elapsed times, deadlines, budgets and backoff.
    ///
    /// <div class="warning">
    ///
    /// The epoch is arbitrary and **private to this instance**. Never persist a
    /// monotonic reading, and never compare readings taken from two different
    /// `Clock` instances: only the difference between two readings of the *same*
    /// instance carries meaning.
    ///
    /// </div>
    fn monotonic(&self) -> Duration;

    /// A wall-clock reading, time elapsed since the UNIX epoch.
    ///
    /// This is a real timestamp, comparable across processes and machines, so it
    /// is the reading to persist in the database or put in a message. In
    /// production it can jump forwards or backwards when the machine clock is
    /// adjusted (NTP, a leap second, an operator), so never measure a duration
    /// with it. Under simulation it is deterministic.
    fn wall(&self) -> Duration;
}

/// Where pseudo-random values come from.
///
/// See the [module documentation](self) for why randomness is a dependency
/// rather than something to reach for, and for the pattern to follow when a lot
/// of randomness is needed.
///
/// <div class="warning">
///
/// Implementations are **not** cryptographically secure and must not be used to
/// derive secrets, tokens or keys.
///
/// </div>
pub trait Rng: fmt::Debug + Send + Sync {
    /// The next value of the sequence, uniform over the whole `u64` range.
    ///
    /// This takes `&self` so that a generator can be shared: an implementation
    /// advances its state through interior mutability.
    fn next_u64(&self) -> u64;

    /// The next value of the sequence, uniform over the whole `u32` range.
    ///
    /// The default implementation truncates [`next_u64`](Self::next_u64).
    fn next_u32(&self) -> u32 {
        self.next_u64() as u32
    }
}

/// The [`Clock`] of production code, reading the clocks of the machine.
///
/// [`monotonic`](Clock::monotonic) counts from the moment the instance was
/// built, so the first reading of a fresh `WallClock` is close to zero.
#[derive(Debug, Clone)]
pub struct WallClock {
    anchor: Instant,
}

impl WallClock {
    /// Builds a clock whose monotonic epoch is now.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug"))]
    pub fn new() -> Self {
        Self {
            anchor: Instant::now(),
        }
    }
}

impl Default for WallClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for WallClock {
    fn monotonic(&self) -> Duration {
        self.anchor.elapsed()
    }

    fn wall(&self) -> Duration {
        // A system clock set before 1970 is the only way this fails, and no
        // caller can do anything useful with an error here.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
    }
}

/// The [`Rng`] of production code: SplitMix64, seeded once.
///
/// [`new`](Self::new) takes the seed, which makes the whole sequence
/// reproducible: the same seed always yields the same values in the same order.
/// [`Default`] takes its seed from the entropy the standard library uses to seed
/// `HashMap`, which is best effort, unpredictable in practice but neither
/// reproducible nor guaranteed by anything.
///
/// <div class="warning">
///
/// SplitMix64 is a fast statistical generator, **not** a cryptographic one.
///
/// </div>
#[derive(Debug)]
pub struct SeededRng {
    state: AtomicU64,
}

/// The golden-ratio increment of SplitMix64.
const SPLITMIX_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

impl SeededRng {
    /// Builds a generator producing the sequence of `seed`, reproducibly.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug"))]
    pub fn new(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }
}

impl Default for SeededRng {
    fn default() -> Self {
        Self::new(RandomState::new().build_hasher().finish())
    }
}

impl Rng for SeededRng {
    fn next_u64(&self) -> u64 {
        // fetch_add hands back the state this call consumed and leaves the next
        // one in place, so concurrent callers each finalize a distinct value.
        let z = self.state.fetch_add(SPLITMIX_GAMMA, Ordering::Relaxed);
        let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The effects a layer needs but must not reach for: time and randomness.
///
/// Cloning is cheap, both sources sit behind an `Arc`, so an `Environment` is
/// meant to be passed by value into the layers and recipes that need it and kept
/// alongside the [`Database`](crate::Database).
///
/// # Example
///
/// ```
/// # use foundationdb::env::Environment;
/// // Production: machine clock, generator seeded from entropy.
/// let env = Environment::default();
///
/// // A reproducible run: the same seed replays the same sequence.
/// let env = Environment::with_seed(42);
/// let first = env.rng().next_u64();
/// assert_eq!(Environment::with_seed(42).rng().next_u64(), first);
/// ```
#[derive(Clone, Debug)]
pub struct Environment {
    clock: Arc<dyn Clock>,
    rng: Arc<dyn Rng>,
}

impl Environment {
    /// Bundles a clock and a generator.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(clock, rng))
    )]
    pub fn new(clock: Arc<dyn Clock>, rng: Arc<dyn Rng>) -> Self {
        Self { clock, rng }
    }

    /// The machine clock, with a generator replaying the sequence of `seed`.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug"))]
    pub fn with_seed(seed: u64) -> Self {
        Self::new(Arc::new(WallClock::new()), Arc::new(SeededRng::new(seed)))
    }

    /// The clock of this environment.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// The pseudo-random generator of this environment.
    pub fn rng(&self) -> &Arc<dyn Rng> {
        &self.rng
    }
}

impl Default for Environment {
    fn default() -> Self {
        Self::new(Arc::new(WallClock::new()), Arc::new(SeededRng::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn take(rng: &SeededRng, count: usize) -> Vec<u64> {
        (0..count).map(|_| rng.next_u64()).collect()
    }

    #[test]
    fn the_same_seed_produces_the_same_sequence() {
        let expected = take(&SeededRng::new(42), 16);

        assert_eq!(take(&SeededRng::new(42), 16), expected);
        assert!(
            expected.windows(2).any(|pair| pair[0] != pair[1]),
            "the sequence is constant: {expected:?}"
        );
    }

    #[test]
    fn different_seeds_produce_different_sequences() {
        assert_ne!(take(&SeededRng::new(42), 16), take(&SeededRng::new(43), 16));
    }

    #[test]
    fn next_u32_truncates_next_u64() {
        let wide = SeededRng::new(7);
        let narrow = SeededRng::new(7);

        for _ in 0..16 {
            assert_eq!(narrow.next_u32(), wide.next_u64() as u32);
        }
    }

    #[test]
    fn wall_clock_monotonic_never_goes_backwards() {
        let clock = WallClock::new();

        let mut previous = clock.monotonic();
        for _ in 0..1_000 {
            let now = clock.monotonic();
            assert!(now >= previous, "{now:?} is before {previous:?}");
            previous = now;
        }
    }

    #[test]
    fn wall_clock_wall_is_a_real_timestamp() {
        // Any machine running this test has a clock set past 2020.
        const YEAR_2020: Duration = Duration::from_secs(1_577_836_800);

        assert!(WallClock::new().wall() > YEAR_2020);
    }

    #[test]
    fn the_default_environment_is_usable() {
        let env = Environment::default();

        assert!(env.clock().monotonic() < Duration::from_secs(1));
        let drawn: Vec<u64> = (0..4).map(|_| env.rng().next_u64()).collect();
        assert!(drawn.windows(2).any(|pair| pair[0] != pair[1]));

        // Cloning shares the very same sources.
        let clone = env.clone();
        assert!(Arc::ptr_eq(clone.clock(), env.clock()));
        assert!(Arc::ptr_eq(clone.rng(), env.rng()));
    }

    #[test]
    fn a_seeded_environment_replays_its_sequence() {
        let expected: Vec<u64> = (0..8)
            .map(|_| Environment::with_seed(99).rng().next_u64())
            .collect();

        // Every fresh environment restarts the same sequence.
        assert!(expected.iter().all(|value| *value == expected[0]));

        let env = Environment::with_seed(99);
        assert_eq!(env.rng().next_u64(), expected[0]);
        assert_ne!(
            env.rng().next_u64(),
            expected[0],
            "the sequence must advance"
        );
    }

    #[test]
    fn debug_output_names_the_implementations() {
        assert!(format!("{:?}", WallClock::new()).contains("WallClock"));
        assert!(format!("{:?}", SeededRng::new(1)).contains("SeededRng"));

        let debug = format!("{:?}", Environment::with_seed(1));
        assert!(debug.contains("Environment"), "{debug}");
        assert!(debug.contains("WallClock"), "{debug}");
        assert!(debug.contains("SeededRng"), "{debug}");
    }
}
