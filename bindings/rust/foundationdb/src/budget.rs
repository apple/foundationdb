// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Per-attempt usage accounting and the client-side budget.
//!
//! <div class="warning">
//!
//! The budget is a **client-side feature of this Rust binding**, not a native
//! FoundationDB limit. Nothing here is enforced by the cluster: the binding
//! counts the bytes and calls that go through [`Transaction`](crate::Transaction)
//! and compares them with the configured limits when you ask it to, with
//! [`Transaction::check_client_budget`](crate::Transaction::check_client_budget).
//! The numbers are therefore an **estimate**, checked *between* operations:
//! a single operation can overshoot a limit, and reads that are still in flight
//! are not counted yet.
//!
//! It is fully decoupled from
//! [`TransactionOption::Timeout`](crate::options::TransactionOption::Timeout) and
//! [`TransactionOption::SizeLimit`](crate::options::TransactionOption::SizeLimit),
//! which are enforced by the C client. Use those to bound what the database
//! does; use the budget to bound what your own code does before it reaches
//! them.
//!
//! </div>
//!
//! # Per-attempt semantics
//!
//! Accounting is always on and scoped to a single *transaction attempt*: usage
//! is reset whenever the transaction restarts (`on_error`, `reset`), while the
//! configured limits survive and apply to the new attempt. A retried
//! transaction therefore gets a fresh time and byte allowance, exactly like it
//! gets a fresh read version.
//!
//! # Determinism and simulation
//!
//! The byte and call counters are deterministic: they depend only on the
//! operations your code issues. [`ClientBudget::time_limit`] is not, unless you
//! say where time comes from: give the budget a [`Clock`] with
//! [`ClientBudget::with_clock`] and every elapsed time of the attempt is
//! measured with it. Under FoundationDB's deterministic simulator, pass a clock
//! backed by simulated time so that a run replays identically.
//!
//! Without one the [`WallClock`] is used, which is the right default outside
//! simulation but makes time budgets non-reproducible: do not rely on them in a
//! simulated workload. See [`crate::env`] for the whole picture.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::env::{Clock, WallClock};
use crate::metrics::MetricKey;

/// Client-side limits applied to a single transaction attempt.
///
/// Each field is optional, `None` meaning "no limit". A default `ClientBudget`
/// limits nothing.
///
/// See the [module documentation](self): these limits are computed by the
/// binding from what it observes, not enforced by FoundationDB.
///
/// # Example
///
/// ```
/// # use foundationdb::*;
/// # use std::time::Duration;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let db = Database::default()?;
/// let trx = db.create_trx()?;
/// trx.set_client_budget(ClientBudget {
///     time_limit: Some(Duration::from_secs(2)),
///     max_bytes_read: Some(10 * 1024 * 1024),
///     ..ClientBudget::default()
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct ClientBudget {
    /// Maximum time an attempt may spend, measured from the start of the attempt
    /// with the [`clock`](Self::clock) of this budget.
    pub time_limit: Option<Duration>,
    /// Maximum number of bytes read by an attempt, keys and values summed.
    pub max_bytes_read: Option<u64>,
    /// Maximum number of bytes written by an attempt, keys, values and mutation
    /// parameters summed.
    pub max_bytes_written: Option<u64>,
    /// Where [`time_limit`](Self::time_limit) reads time from, `None` meaning
    /// the [`WallClock`].
    ///
    /// Set it with [`with_clock`](Self::with_clock) to keep time budgets
    /// deterministic, see the [module documentation](self).
    pub clock: Option<Arc<dyn Clock>>,
}

impl ClientBudget {
    /// Measures the attempts with `clock` instead of the [`WallClock`].
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, clock))
    )]
    pub fn with_clock(self, clock: impl Clock + 'static) -> Self {
        Self {
            clock: Some(Arc::new(clock)),
            ..self
        }
    }

    /// Checks `usage` against these limits, returning the first exceeded one.
    pub(crate) fn check(&self, usage: &AttemptUsage) -> Result<(), BudgetExceeded> {
        if let Some(limit) = self.time_limit {
            let used = usage.elapsed().as_millis() as u64;
            let limit = limit.as_millis() as u64;
            if used > limit {
                return Err(BudgetExceeded {
                    kind: BudgetKind::Time,
                    used,
                    limit,
                });
            }
        }

        if let Some(limit) = self.max_bytes_read {
            let used = usage.bytes_read();
            if used > limit {
                return Err(BudgetExceeded {
                    kind: BudgetKind::BytesRead,
                    used,
                    limit,
                });
            }
        }

        if let Some(limit) = self.max_bytes_written {
            let used = usage.bytes_written();
            if used > limit {
                return Err(BudgetExceeded {
                    kind: BudgetKind::BytesWritten,
                    used,
                    limit,
                });
            }
        }

        Ok(())
    }
}

/// Which [`ClientBudget`] limit was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetKind {
    /// [`ClientBudget::time_limit`], in milliseconds.
    Time,
    /// [`ClientBudget::max_bytes_read`], in bytes.
    BytesRead,
    /// [`ClientBudget::max_bytes_written`], in bytes.
    BytesWritten,
}

impl fmt::Display for BudgetKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BudgetKind::Time => write!(f, "time"),
            BudgetKind::BytesRead => write!(f, "bytes read"),
            BudgetKind::BytesWritten => write!(f, "bytes written"),
        }
    }
}

/// A [`ClientBudget`] limit was exceeded by the current transaction attempt.
///
/// `used` and `limit` are expressed in milliseconds for [`BudgetKind::Time`],
/// in bytes otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetExceeded {
    /// The limit that was exceeded.
    pub kind: BudgetKind,
    /// What the attempt used, milliseconds for [`BudgetKind::Time`], bytes
    /// otherwise.
    pub used: u64,
    /// The configured limit, in the same unit as `used`.
    pub limit: u64,
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let unit = match self.kind {
            BudgetKind::Time => "ms",
            BudgetKind::BytesRead | BudgetKind::BytesWritten => "bytes",
        };
        write!(
            f,
            "client budget exceeded ({}): used {} {}, limit {} {}. This is a client-side estimate of this binding, not a FoundationDB limit",
            self.kind, self.used, unit, self.limit, unit
        )
    }
}

impl std::error::Error for BudgetExceeded {}

/// Usage accounted for a single transaction attempt.
///
/// Counters are incremented by [`Transaction`](crate::Transaction) as
/// operations are issued (writes) or resolved (reads). They are always on: no
/// instrumentation is required to get them.
///
/// A new instance is created for every attempt, see the
/// [module documentation](self).
///
/// The application metrics recorded with
/// [`Transaction::set_custom_metric`](crate::Transaction::set_custom_metric) are
/// stored here too, and are therefore per-attempt as well. Their map is
/// allocated on first use, so a transaction that never records one pays
/// nothing. Without a metrics consumer collecting them (typically
/// [`Database::instrumented_run`](crate::Database::instrumented_run)), they are
/// simply dropped with the generation.
#[derive(Debug)]
pub struct AttemptUsage {
    /// Where the elapsed time of this attempt is measured.
    clock: Arc<dyn Clock>,
    /// The monotonic reading taken when the attempt started.
    started_at: Duration,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    keys_values_fetched: AtomicU64,
    call_get: AtomicU64,
    call_get_range: AtomicU64,
    call_set: AtomicU64,
    call_clear: AtomicU64,
    call_clear_range: AtomicU64,
    call_atomic_op: AtomicU64,
    /// Application metrics of this attempt, allocated on first use.
    custom: Mutex<Option<HashMap<MetricKey, u64>>>,
}

impl Default for AttemptUsage {
    fn default() -> Self {
        Self::new()
    }
}

impl AttemptUsage {
    /// Starts a new, empty accounting generation, measured with the
    /// [`WallClock`].
    pub fn new() -> Self {
        Self::with_clock(None)
    }

    /// Starts a new, empty accounting generation measured with `clock`, or with
    /// the [`WallClock`] when it is `None`.
    pub(crate) fn with_clock(clock: Option<Arc<dyn Clock>>) -> Self {
        let clock = clock.unwrap_or_else(|| Arc::new(WallClock::new()));
        Self {
            started_at: clock.monotonic(),
            clock,
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            keys_values_fetched: AtomicU64::new(0),
            call_get: AtomicU64::new(0),
            call_get_range: AtomicU64::new(0),
            call_set: AtomicU64::new(0),
            call_clear: AtomicU64::new(0),
            call_clear_range: AtomicU64::new(0),
            call_atomic_op: AtomicU64::new(0),
            custom: Mutex::new(None),
        }
    }

    /// Time elapsed since the attempt started, measured with the clock the
    /// attempt was stamped with.
    ///
    /// A clock that goes backwards yields zero rather than panicking.
    pub fn elapsed(&self) -> Duration {
        self.clock.monotonic().saturating_sub(self.started_at)
    }

    /// Bytes read so far, keys and values summed.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Bytes written so far, keys, values and mutation parameters summed.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Takes a consistent-enough copy of every counter.
    ///
    /// Counters are read one after the other without a lock, so a snapshot
    /// taken while operations are completing may mix values from slightly
    /// different instants.
    pub fn snapshot(&self) -> UsageSnapshot {
        UsageSnapshot {
            elapsed: self.elapsed(),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            keys_values_fetched: self.keys_values_fetched.load(Ordering::Relaxed),
            call_get: self.call_get.load(Ordering::Relaxed),
            call_get_range: self.call_get_range.load(Ordering::Relaxed),
            call_set: self.call_set.load(Ordering::Relaxed),
            call_clear: self.call_clear.load(Ordering::Relaxed),
            call_clear_range: self.call_clear_range.load(Ordering::Relaxed),
            call_atomic_op: self.call_atomic_op.load(Ordering::Relaxed),
        }
    }

    /// Records a resolved `get` or `get_key`.
    pub(crate) fn record_get(&self, bytes: u64, keys_values: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.keys_values_fetched
            .fetch_add(keys_values, Ordering::Relaxed);
        self.call_get.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a resolved `get_range` batch.
    pub(crate) fn record_get_range(&self, bytes: u64, keys_values: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
        self.keys_values_fetched
            .fetch_add(keys_values, Ordering::Relaxed);
        self.call_get_range.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a `set`.
    pub(crate) fn record_set(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.call_set.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a `clear`.
    pub(crate) fn record_clear(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.call_clear.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a `clear_range`.
    pub(crate) fn record_clear_range(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.call_clear_range.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an `atomic_op`.
    pub(crate) fn record_atomic_op(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.call_atomic_op.fetch_add(1, Ordering::Relaxed);
    }

    /// Sets an application metric of this attempt, replacing any previous value.
    pub(crate) fn set_custom(&self, key: MetricKey, value: u64) {
        self.with_custom(|custom| {
            custom.insert(key, value);
        });
    }

    /// Adds `amount` to an application metric of this attempt, starting from
    /// zero if it was not recorded yet.
    pub(crate) fn increment_custom(&self, key: MetricKey, amount: u64) {
        self.with_custom(|custom| {
            *custom.entry(key).or_insert(0) += amount;
        });
    }

    /// The application metrics recorded during this attempt.
    pub fn custom_metrics(&self) -> HashMap<MetricKey, u64> {
        self.custom
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .unwrap_or_default()
    }

    fn with_custom<R>(&self, f: impl FnOnce(&mut HashMap<MetricKey, u64>) -> R) -> R {
        let mut custom = self
            .custom
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(custom.get_or_insert_with(HashMap::new))
    }
}

/// A copy of the counters of a single transaction attempt, taken by
/// [`AttemptUsage::snapshot`] or
/// [`Transaction::attempt_usage`](crate::Transaction::attempt_usage).
///
/// Every byte count is a client-side estimate, see the
/// [module documentation](self).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageSnapshot {
    /// Time elapsed since the attempt started.
    pub elapsed: Duration,
    /// Bytes read: for each resolved read, the key(s) requested plus the
    /// key-values returned.
    pub bytes_read: u64,
    /// Bytes written: keys, values and mutation parameters handed to the
    /// client. For `clear_range` only the two boundary keys are counted, **not**
    /// the volume of data the range deletes.
    pub bytes_written: u64,
    /// Number of key-values returned by resolved reads.
    pub keys_values_fetched: u64,
    /// Number of `get` and `get_key` calls resolved.
    pub call_get: u64,
    /// Number of `get_range` batches resolved. A single `get_ranges` stream
    /// counts once per underlying FoundationDB batch, not once per stream.
    pub call_get_range: u64,
    /// Number of `set` calls.
    pub call_set: u64,
    /// Number of `clear` calls.
    pub call_clear: u64,
    /// Number of `clear_range` calls.
    pub call_clear_range: u64,
    /// Number of `atomic_op` calls.
    pub call_atomic_op: u64,
}

/// The accounting generation currently active on a transaction.
///
/// Operations clone the `Arc` out of the slot when they are **issued** and
/// record into that clone when they complete. Starting a new attempt swaps the
/// slot with a fresh [`AttemptUsage`] instead of zeroing the current one, so a
/// read issued during attempt N that completes during attempt N+1 records into
/// the counters of attempt N, which nobody reads anymore, and cannot pollute
/// N+1.
#[derive(Debug, Default)]
pub(crate) struct UsageSlot(Mutex<Arc<AttemptUsage>>);

impl UsageSlot {
    /// The generation to record into for an operation issued now.
    pub(crate) fn current(&self) -> Arc<AttemptUsage> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Starts a fresh generation measured with `clock` (the [`WallClock`] when
    /// `None`), leaving the previous one to the in-flight operations still
    /// holding it.
    pub(crate) fn begin(&self, clock: Option<Arc<dyn Clock>>) {
        let mut slot = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *slot = Arc::new(AttemptUsage::with_clock(clock));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A clock the test moves by hand, in milliseconds.
    #[derive(Debug, Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl FakeClock {
        fn set_millis(&self, millis: u64) {
            self.0.store(millis, Ordering::Relaxed);
        }
    }

    impl Clock for FakeClock {
        fn monotonic(&self) -> Duration {
            Duration::from_millis(self.0.load(Ordering::Relaxed))
        }

        fn wall(&self) -> Duration {
            self.monotonic()
        }
    }

    #[test]
    fn budget_without_limits_never_fails() {
        let usage = AttemptUsage::new();
        usage.record_get(1_000_000, 10);
        usage.record_set(1_000_000);

        assert!(ClientBudget::default().check(&usage).is_ok());
    }

    #[test]
    fn budget_allows_usage_up_to_the_limit() {
        let budget = ClientBudget {
            max_bytes_read: Some(10),
            max_bytes_written: Some(10),
            ..ClientBudget::default()
        };

        let usage = AttemptUsage::new();
        usage.record_get(10, 1);
        usage.record_set(10);

        assert!(budget.check(&usage).is_ok());
    }

    #[test]
    fn budget_reports_exceeded_bytes_read() {
        let budget = ClientBudget {
            max_bytes_read: Some(10),
            ..ClientBudget::default()
        };

        let usage = AttemptUsage::new();
        usage.record_get(7, 1);
        assert!(budget.check(&usage).is_ok());
        usage.record_get_range(5, 2);

        let err = budget.check(&usage).unwrap_err();
        assert_eq!(err.kind, BudgetKind::BytesRead);
        assert_eq!(err.used, 12);
        assert_eq!(err.limit, 10);
        assert!(err.to_string().contains("client-side estimate"));
    }

    #[test]
    fn budget_reports_exceeded_bytes_written() {
        let budget = ClientBudget {
            max_bytes_written: Some(4),
            ..ClientBudget::default()
        };

        let usage = AttemptUsage::new();
        usage.record_set(2);
        usage.record_clear(1);
        usage.record_clear_range(1);
        assert!(budget.check(&usage).is_ok());
        usage.record_atomic_op(1);

        let err = budget.check(&usage).unwrap_err();
        assert_eq!(err.kind, BudgetKind::BytesWritten);
        assert_eq!(err.used, 5);
        assert_eq!(err.limit, 4);
    }

    #[test]
    fn budget_reports_exceeded_time() {
        let clock = FakeClock::default();
        clock.set_millis(1_000);

        let budget = ClientBudget {
            time_limit: Some(Duration::from_millis(20)),
            ..ClientBudget::default()
        }
        .with_clock(clock.clone());

        let usage = AttemptUsage::with_clock(budget.clock.clone());
        clock.set_millis(1_020);
        assert!(budget.check(&usage).is_ok(), "the limit itself is allowed");

        clock.set_millis(1_035);

        let err = budget.check(&usage).unwrap_err();
        assert_eq!(err.kind, BudgetKind::Time);
        assert_eq!(err.used, 35);
        assert_eq!(err.limit, 20);
        assert!(err.to_string().contains("client-side estimate"));
    }

    #[test]
    fn elapsed_follows_a_custom_clock() {
        let clock = FakeClock::default();
        clock.set_millis(500);

        let usage = AttemptUsage::with_clock(Some(Arc::new(clock.clone())));
        assert_eq!(usage.elapsed(), Duration::ZERO);

        clock.set_millis(700);
        assert_eq!(usage.elapsed(), Duration::from_millis(200));
        assert_eq!(usage.snapshot().elapsed, Duration::from_millis(200));
    }

    #[test]
    fn a_clock_going_backwards_saturates_to_zero() {
        let clock = FakeClock::default();
        clock.set_millis(1_000);

        let usage = AttemptUsage::with_clock(Some(Arc::new(clock.clone())));
        clock.set_millis(10);

        assert_eq!(usage.elapsed(), Duration::ZERO);
    }

    #[test]
    fn a_new_generation_is_stamped_with_the_given_clock() {
        let clock = FakeClock::default();
        clock.set_millis(100);

        let slot = UsageSlot::default();
        let previous = slot.current();

        slot.begin(Some(Arc::new(clock.clone())));
        let current = slot.current();
        clock.set_millis(160);

        assert_eq!(current.elapsed(), Duration::from_millis(60));
        // The previous generation keeps the wall clock it was stamped with.
        assert!(previous.elapsed() < Duration::from_millis(60));
    }

    #[test]
    fn snapshot_counts_every_operation() {
        let usage = AttemptUsage::new();
        usage.record_get(3, 1);
        usage.record_get_range(7, 2);
        usage.record_set(4);
        usage.record_clear(5);
        usage.record_clear_range(6);
        usage.record_atomic_op(7);

        let snapshot = usage.snapshot();
        assert_eq!(snapshot.bytes_read, 10);
        assert_eq!(snapshot.bytes_written, 22);
        assert_eq!(snapshot.keys_values_fetched, 3);
        assert_eq!(snapshot.call_get, 1);
        assert_eq!(snapshot.call_get_range, 1);
        assert_eq!(snapshot.call_set, 1);
        assert_eq!(snapshot.call_clear, 1);
        assert_eq!(snapshot.call_clear_range, 1);
        assert_eq!(snapshot.call_atomic_op, 1);
    }

    #[test]
    fn custom_metrics_are_scoped_to_the_generation() {
        let slot = UsageSlot::default();
        let key = MetricKey::new("documents", &[("kind", "user")]);

        assert!(slot.current().custom_metrics().is_empty());

        slot.current().set_custom(key.clone(), 2);
        slot.current().increment_custom(key.clone(), 3);
        assert_eq!(slot.current().custom_metrics().get(&key), Some(&5));

        let previous = slot.current();
        slot.begin(None);

        assert!(slot.current().custom_metrics().is_empty());
        assert_eq!(previous.custom_metrics().get(&key), Some(&5));
    }

    #[test]
    fn a_new_generation_starts_empty() {
        let slot = UsageSlot::default();
        slot.current().record_set(42);
        assert_eq!(slot.current().snapshot().bytes_written, 42);

        slot.begin(None);

        let snapshot = slot.current().snapshot();
        assert_eq!(
            UsageSnapshot {
                elapsed: Duration::ZERO,
                ..snapshot
            },
            UsageSnapshot::default()
        );
    }

    #[test]
    fn a_stale_generation_does_not_pollute_the_next_one() {
        let slot = UsageSlot::default();

        // An operation issued during the first attempt, still in flight.
        let in_flight = slot.current();

        slot.begin(None);
        slot.current().record_get(10, 1);

        // The stale operation completes and records into its own generation.
        in_flight.record_get(999, 99);

        let snapshot = slot.current().snapshot();
        assert_eq!(snapshot.bytes_read, 10);
        assert_eq!(snapshot.keys_values_fetched, 1);
        assert_eq!(snapshot.call_get, 1);
        assert_eq!(in_flight.snapshot().bytes_read, 999);
    }
}
