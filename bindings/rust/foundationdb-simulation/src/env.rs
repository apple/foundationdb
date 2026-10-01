//! Simulator-backed implementations of the [`foundationdb::env`] capabilities.
//!
//! A workload that reads the machine clock or draws from an ambient generator
//! stops being reproducible, which defeats the point of running it in the
//! simulator. [`SimClock`] and [`SimRng`] plug the simulated time and the
//! deterministic generator of fdbserver into the [`Clock`] and [`Rng`] traits, so
//! layer code written against an [`Environment`] replays identically from a
//! seed.
//!
//! Build one from the [`WorkloadContext`] handed to your workload:
//!
//! ```ignore
//! impl RustWorkload for MyWorkload {
//!     fn new(name: String, context: WorkloadContext) -> Self {
//!         Self {
//!             env: context.environment(),
//!             context,
//!         }
//!     }
//! }
//! ```
//!
//! # Lifetime and threads
//!
//! Handles share the workload's guarded context. Calls panic if the workload has
//! been released or if they run outside the simulator thread that created it.
//! An [`Environment`] may be cloned or moved, but accessing its clock or RNG on
//! another thread is rejected before entering fdbserver.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use foundationdb::env::{Clock, Environment, Rng};

use crate::bindings::WorkloadContext;

/// The [`Clock`] of a simulated workload, reading fdbserver's simulated time.
///
/// [`monotonic`](Clock::monotonic) is the simulated clock, which starts at zero
/// and only moves when the simulator decides to advance it, so a run replays
/// identically. [`wall`](Clock::wall) is that same reading: simulated wall time
/// counts from the UNIX epoch at simulation start, second zero being the epoch.
///
/// Simulated wall time is therefore deterministic and, unlike a real machine
/// clock, never jumps: a workload cannot exercise clock-skew handling this way.
///
/// See the [module documentation](self) for how long an instance stays valid.
pub struct SimClock {
    context: WorkloadContext,
}

impl SimClock {
    /// Reads simulated time from `context`.
    pub fn new(context: &WorkloadContext) -> Self {
        Self {
            context: context.clone(),
        }
    }
}

impl fmt::Debug for SimClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SimClock")
    }
}

impl Clock for SimClock {
    fn monotonic(&self) -> Duration {
        // The simulated clock starts at zero and never goes backwards, but it is
        // an f64 crossing the FFI boundary: clamp rather than panic.
        Duration::from_secs_f64(self.context.now().max(0.0))
    }

    /// Simulated wall time counts from the UNIX epoch at simulation start, so
    /// second zero is the epoch itself: only differences and orderings carry
    /// meaning. It never jumps, so clock-skew handling cannot be exercised here.
    fn wall(&self) -> Duration {
        self.monotonic()
    }
}

/// The [`Rng`] of a simulated workload, drawing from fdbserver's deterministic
/// generator.
///
/// Every draw advances the simulator's own generator, so the values are part of
/// the run's reproducible state: the same seed replays the same sequence, and
/// drawing more or fewer values than a previous run changes what every later
/// consumer sees.
///
/// A component that needs a large volume of randomness should draw one value
/// here and seed a local generator with it, the pattern described in
/// [`foundationdb::env`]. That keeps the simulator's sequence short and stable.
///
/// See the [module documentation](self) for how long an instance stays valid.
pub struct SimRng {
    context: WorkloadContext,
}

impl SimRng {
    /// Draws from the deterministic generator of `context`.
    pub fn new(context: &WorkloadContext) -> Self {
        Self {
            context: context.clone(),
        }
    }
}

impl fmt::Debug for SimRng {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SimRng")
    }
}

impl Rng for SimRng {
    /// Composes two 32-bit draws, so one call consumes two values of the
    /// simulator's sequence.
    fn next_u64(&self) -> u64 {
        let high = self.context.rnd();
        let low = self.context.rnd();
        (u64::from(high) << 32) | u64::from(low)
    }

    /// One draw of the simulator's sequence, taken directly from the generator.
    fn next_u32(&self) -> u32 {
        self.context.rnd()
    }
}

impl WorkloadContext {
    /// The deterministic [`Environment`] of this workload: simulated time and
    /// the simulator's generator.
    ///
    /// Pass it to the layers and recipes the workload exercises so that they
    /// read time and randomness from the simulator instead of from the machine.
    ///
    /// Calls on the resulting clock and RNG panic after this workload is
    /// released or outside its simulator thread.
    pub fn environment(&self) -> Environment {
        Environment::new(Arc::new(SimClock::new(self)), Arc::new(SimRng::new(self)))
    }
}
