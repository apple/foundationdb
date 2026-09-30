//! Per-attempt metrics collected by [`Database::instrumented_run`](crate::Database::instrumented_run).
//!
//! The report is a list of [`AttemptMetrics`], one per transaction attempt, in
//! order: a retried transaction keeps everything it did in its earlier
//! attempts. Operation counters come from the always-on
//! [`UsageSnapshot`] of the attempt, so
//! instrumentation only adds the timings, the outcome and the aggregates on
//! top of what the binding already counts.

use crate::budget::{AttemptUsage, UsageSnapshot};
use crate::error::FdbError;
use crate::transaction::ConflictingKeyRange;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Label key-value pairs for metrics
pub type Labels = Vec<(String, String)>;

/// Unique key for a metric: name + labels
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct MetricKey {
    pub name: String,
    pub labels: Labels,
}

impl MetricKey {
    /// Create a new MetricKey
    ///
    /// # Arguments
    /// * `name` - The name of the metric
    /// * `labels` - Key-value pairs for labeling the metric
    ///
    /// # Returns
    /// * `MetricKey` - A new MetricKey instance
    pub fn new(name: &str, labels: &[(&str, &str)]) -> Self {
        // Convert labels to owned strings
        let mut sorted_labels: Labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        // Sort labels by key to ensure consistent ordering
        sorted_labels.sort_by(|a, b| a.0.cmp(&b.0));

        Self {
            name: name.to_string(),
            labels: sorted_labels,
        }
    }
}

/// How a transaction attempt ended.
///
/// Marked non exhaustive: finer-grained failure reporting is expected to add
/// variants.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AttemptOutcome {
    /// The attempt committed. For a run, only the last attempt can be in this
    /// state.
    Committed,
    /// The attempt was retried, `cause` being the error handed to `on_error`.
    /// A closure error asking for a retry without a native error underneath
    /// surfaces here as code 1020 (`not_committed`).
    Retried { cause: FdbError },
    /// The attempt ended the run without committing: the error was not
    /// retryable, or the retries were exhausted.
    Failed,
}

/// The conflicting key ranges of a transaction attempt.
///
/// They are only read after a commit conflict, and only carry ranges when
/// [`TransactionOption::ReportConflictingKeys`](crate::options::TransactionOption::ReportConflictingKeys)
/// was set on the transaction.
#[derive(Debug, Clone, Default)]
pub enum ConflictKeys {
    /// The attempt did not fail on a commit conflict, so nothing was read.
    #[default]
    NotRequested,
    /// The special keyspace was read. The list is empty when
    /// `ReportConflictingKeys` was not enabled.
    Available(Vec<ConflictingKeyRange>),
    /// The special keyspace read failed.
    ReadFailed(FdbError),
}

impl ConflictKeys {
    /// The conflicting ranges, empty unless they were read successfully.
    pub fn ranges(&self) -> &[ConflictingKeyRange] {
        match self {
            ConflictKeys::Available(ranges) => ranges,
            ConflictKeys::NotRequested | ConflictKeys::ReadFailed(_) => &[],
        }
    }
}

/// Everything recorded about a single transaction attempt.
///
/// One is pushed to [`MetricsReport::attempts`] when the attempt ends, so an
/// attempt that was retried keeps its own counters, timings and custom metrics
/// instead of being overwritten by the next one.
#[derive(Debug, Clone)]
pub struct AttemptMetrics {
    /// Position of the attempt in the run, starting at 0.
    pub index: usize,
    /// Operation counters and bytes of the attempt, from the always-on
    /// accounting. See [`crate::budget`] for how precise they are.
    pub usage: UsageSnapshot,
    /// Application metrics recorded during the attempt with
    /// [`Transaction::set_custom_metric`](crate::Transaction::set_custom_metric).
    pub custom_metrics: HashMap<MetricKey, u64>,
    /// Wall clock of the attempt, measured from its start to the end of its
    /// work: the retry backoff performed by `on_error` is **not** included.
    pub duration: Option<Duration>,
    /// Time spent in `commit`, `None` if the attempt never reached it.
    pub commit_duration: Option<Duration>,
    /// Time spent waiting on `get_read_version`, `None` if it was never called.
    /// The first completion of the attempt is kept.
    pub grv_duration: Option<Duration>,
    /// Time spent in `on_error`, backoff included. `None` if `on_error` did not
    /// run, which is the case for the last attempt of a run.
    pub on_error_duration: Option<Duration>,
    /// How the attempt ended.
    pub outcome: AttemptOutcome,
    /// Conflicting key ranges, only read after a commit conflict.
    pub conflicting_keys: ConflictKeys,
    /// Read version of the attempt, recorded when
    /// [`Transaction::get_read_version`](crate::Transaction::get_read_version)
    /// is called. The binding never fetches it on its own.
    pub read_version: Option<i64>,
}

/// Transaction-level information that spans attempts
#[derive(Debug, Default, Clone)]
pub struct TransactionInfo {
    /// Number of retries performed, that is `attempts.len() - 1` for a
    /// completed run
    pub retries: u64,
    /// Number of retries caused by commit conflicts (`not_committed`, error 1020)
    pub conflict_count: u64,
    /// Last read version observed, see [`AttemptMetrics::read_version`]
    pub read_version: Option<i64>,
    /// Commit version of the committed attempt
    pub commit_version: Option<i64>,
}

/// The metrics of a whole `instrumented_run`: the detail of every attempt plus
/// the aggregates that only make sense for the run as a whole.
#[derive(Debug, Clone, Default)]
pub struct MetricsReport {
    /// One entry per transaction attempt, in order. Nothing is lost across
    /// retries.
    pub attempts: Vec<AttemptMetrics>,
    /// Wall clock of the whole run, every attempt and backoff included.
    pub total_duration: Option<Duration>,
    /// Information that spans attempts.
    pub transaction: TransactionInfo,
}

impl MetricsReport {
    /// Sums the usage of every attempt, retries included.
    ///
    /// `elapsed` is the sum of the attempt durations, which is shorter than
    /// [`total_duration`](Self::total_duration): the retry backoffs are not
    /// part of any attempt.
    pub fn total_usage(&self) -> UsageSnapshot {
        self.attempts
            .iter()
            .fold(UsageSnapshot::default(), |mut total, attempt| {
                let usage = &attempt.usage;
                total.elapsed += usage.elapsed;
                total.bytes_read += usage.bytes_read;
                total.bytes_written += usage.bytes_written;
                total.keys_values_fetched += usage.keys_values_fetched;
                total.call_get += usage.call_get;
                total.call_get_range += usage.call_get_range;
                total.call_set += usage.call_set;
                total.call_clear += usage.call_clear;
                total.call_clear_range += usage.call_clear_range;
                total.call_atomic_op += usage.call_atomic_op;
                total
            })
    }

    /// The attempt that ended the run, `None` if no attempt was ever started.
    pub fn last_attempt(&self) -> Option<&AttemptMetrics> {
        self.attempts.last()
    }
}

/// The attempt currently being recorded.
///
/// Holds the accounting generation of the transaction, so that the attempt can
/// be pushed to the report even after the transaction itself is gone (a
/// non-retryable `on_error` consumes it), and the pieces that are known before
/// the attempt ends.
#[derive(Debug, Default)]
struct OpenAttempt {
    usage: Option<Arc<AttemptUsage>>,
    duration: Option<Duration>,
    commit_duration: Option<Duration>,
    grv_duration: Option<Duration>,
    conflicting_keys: ConflictKeys,
    read_version: Option<i64>,
}

/// Collects the metrics of a transaction, attempt after attempt.
///
/// This is the handle shared between the transaction, the runner hooks and the
/// caller: cloning it clones the handle, not the data. The recording methods
/// are called by the binding as the transaction progresses, the report is read
/// back with [`get_metrics_data`](Self::get_metrics_data).
#[derive(Debug, Clone, Default)]
pub struct TransactionMetrics {
    /// The report being built.
    pub metrics: Arc<Mutex<MetricsReport>>,
    /// The attempt being recorded, pushed to the report when it ends.
    open: Arc<Mutex<OpenAttempt>>,
}

impl TransactionMetrics {
    /// Create a new instance of TransactionMetrics
    pub fn new() -> Self {
        Self::default()
    }

    fn report(&self) -> std::sync::MutexGuard<'_, MetricsReport> {
        self.metrics
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn open(&self) -> std::sync::MutexGuard<'_, OpenAttempt> {
        self.open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Starts recording a new attempt, on the accounting generation the
    /// transaction just entered.
    ///
    /// Anything recorded on a previous attempt that was not ended with
    /// [`finish_attempt`](Self::finish_attempt) is dropped: the transaction
    /// restarted without reaching a conclusion, typically because
    /// [`Transaction::set_client_budget`](crate::Transaction::set_client_budget)
    /// or [`Transaction::reset`](crate::Transaction::reset) was called.
    pub(crate) fn begin_attempt(&self, usage: Arc<AttemptUsage>) {
        let mut open = self.open();

        #[cfg(feature = "trace")]
        if let Some(dropped) = open.usage.as_ref() {
            // `elapsed` always moves, so the counters are what tells whether
            // the attempt being dropped did anything worth reporting. Some
            // activity is recorded straight on `OpenAttempt` instead of
            // `AttemptUsage` (read version, conflicting keys, commit
            // duration, grv duration), so it has to be checked too.
            let snapshot = dropped.snapshot();
            let idle = snapshot
                == UsageSnapshot {
                    elapsed: snapshot.elapsed,
                    ..UsageSnapshot::default()
                }
                && dropped.custom_metrics().is_empty()
                && open.read_version.is_none()
                && open.duration.is_none()
                && open.commit_duration.is_none()
                && open.grv_duration.is_none()
                && matches!(open.conflicting_keys, ConflictKeys::NotRequested);

            if !idle {
                tracing::warn!(
                    "a new attempt was opened while the previous one was still open, its usage and custom metrics are dropped (set_client_budget or reset called mid-attempt)"
                );
            }
        }

        *open = OpenAttempt {
            usage: Some(usage),
            ..OpenAttempt::default()
        };
    }

    /// Freezes the duration of the attempt at the current elapsed time, so that
    /// what happens next (retry backoff, error handling) is not counted as part
    /// of it. Later calls do not move it.
    pub(crate) fn mark_attempt_end(&self) {
        let mut open = self.open();
        if open.duration.is_none() {
            open.duration = open.usage.as_ref().map(|usage| usage.elapsed());
        }
    }

    /// Records the time spent in `commit`, whatever its result, and ends the
    /// attempt's work.
    pub(crate) fn record_commit(&self, duration: Duration) {
        let mut open = self.open();
        open.commit_duration = Some(duration);
        if open.duration.is_none() {
            open.duration = open.usage.as_ref().map(|usage| usage.elapsed());
        }
    }

    /// Records the time spent waiting on `get_read_version`, whatever its
    /// result. Later calls of the same attempt are ignored.
    pub(crate) fn record_grv(&self, duration: Duration) {
        let mut open = self.open();
        if open.grv_duration.is_none() {
            open.grv_duration = Some(duration);
        }
    }

    /// Ends the current attempt and pushes it to the report.
    ///
    /// Does nothing when no attempt is being recorded, which makes it safe to
    /// call on the exit paths of a runner that may already have ended it.
    pub fn finish_attempt(&self, outcome: AttemptOutcome) {
        let (usage, open) = {
            let mut open = self.open();
            match open.usage.take() {
                // `usage` was taken above, so this leaves the scratch empty.
                Some(usage) => (usage, std::mem::take(&mut *open)),
                None => return,
            }
        };

        let retried = matches!(outcome, AttemptOutcome::Retried { .. });

        let mut report = self.report();
        let index = report.attempts.len();
        report.attempts.push(AttemptMetrics {
            index,
            usage: usage.snapshot(),
            custom_metrics: usage.custom_metrics(),
            duration: open.duration.or_else(|| Some(usage.elapsed())),
            commit_duration: open.commit_duration,
            grv_duration: open.grv_duration,
            on_error_duration: None,
            outcome,
            conflicting_keys: open.conflicting_keys,
            read_version: open.read_version,
        });

        if retried {
            report.transaction.retries += 1;
        }
    }

    /// Records the time spent in `on_error` for the attempt that just ended.
    ///
    /// The runner measures it after `on_error` returned, hence after the
    /// attempt was pushed: it lands on the last attempt of the report.
    pub fn set_on_error_duration(&self, duration: Duration) {
        if let Some(attempt) = self.report().attempts.last_mut() {
            attempt.on_error_duration = Some(duration);
        }
    }

    /// Records the total duration of the run.
    pub fn set_total_duration(&self, duration: Duration) {
        self.report().total_duration = Some(duration);
    }

    /// Records the read version of the current attempt.
    pub fn set_read_version(&self, version: i64) {
        self.open().read_version = Some(version);
        self.report().transaction.read_version = Some(version);
    }

    /// Records the commit version of the transaction.
    pub fn set_commit_version(&self, version: i64) {
        self.report().transaction.commit_version = Some(version);
    }

    /// Records the conflicting key ranges of the current attempt.
    pub fn set_conflicting_keys(&self, keys: ConflictKeys) {
        self.open().conflicting_keys = keys;
    }

    /// Increment the conflict counter
    pub fn increment_conflict_count(&self) {
        self.report().transaction.conflict_count += 1;
    }

    /// Get the number of retries
    pub fn get_retries(&self) -> u64 {
        self.report().transaction.retries
    }

    /// Returns a clone of the report built so far.
    pub fn get_metrics_data(&self) -> MetricsReport {
        self.report().clone()
    }

    /// Returns a clone of the transaction information.
    pub fn get_transaction_info(&self) -> TransactionInfo {
        self.report().transaction.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_metric_key_equality() {
        // Test that different combinations of the same labels are considered equal
        let key1 = MetricKey::new("counter", &[("region", "us-west"), ("service", "api")]);
        let key2 = MetricKey::new("counter", &[("service", "api"), ("region", "us-west")]);

        // Same labels in different order should be considered equal
        assert_eq!(key1, key2);

        // Different label values should produce different keys
        let key3 = MetricKey::new("counter", &[("region", "us-east"), ("service", "api")]);
        assert_ne!(key1, key3);

        // Different label keys should produce different keys
        let key4 = MetricKey::new("counter", &[("zone", "us-west"), ("service", "api")]);
        assert_ne!(key1, key4);

        // Different metric names should produce different keys
        let key5 = MetricKey::new("timer", &[("region", "us-west"), ("service", "api")]);
        assert_ne!(key1, key5);
    }

    #[test]
    fn test_metric_key_in_hashmap() {
        let mut metrics = HashMap::new();

        // Insert metrics with different label combinations
        metrics.insert(
            MetricKey::new("counter", &[("region", "us-west"), ("service", "api")]),
            100,
        );
        metrics.insert(
            MetricKey::new("counter", &[("region", "us-east"), ("service", "api")]),
            200,
        );
        metrics.insert(
            MetricKey::new("timer", &[("region", "us-west"), ("service", "api")]),
            300,
        );

        // Verify we can retrieve metrics with the same label combinations
        assert_eq!(
            metrics.get(&MetricKey::new(
                "counter",
                &[("region", "us-west"), ("service", "api")]
            )),
            Some(&100)
        );

        // Verify we can retrieve metrics with labels in different order
        assert_eq!(
            metrics.get(&MetricKey::new(
                "counter",
                &[("service", "api"), ("region", "us-west")]
            )),
            Some(&100)
        );

        // Verify different label values produce different keys
        assert_eq!(
            metrics.get(&MetricKey::new(
                "counter",
                &[("region", "us-east"), ("service", "api")]
            )),
            Some(&200)
        );

        // Verify different metric names produce different keys
        assert_eq!(
            metrics.get(&MetricKey::new(
                "timer",
                &[("region", "us-west"), ("service", "api")]
            )),
            Some(&300)
        );
    }

    #[test]
    fn test_metric_key_label_order_independence() {
        // Create a HashSet to verify uniqueness
        let mut unique_keys = HashSet::new();

        // Add keys with the same labels in different orders
        unique_keys.insert(MetricKey::new(
            "counter",
            &[("a", "1"), ("b", "2"), ("c", "3")],
        ));
        unique_keys.insert(MetricKey::new(
            "counter",
            &[("a", "1"), ("c", "3"), ("b", "2")],
        ));
        unique_keys.insert(MetricKey::new(
            "counter",
            &[("b", "2"), ("a", "1"), ("c", "3")],
        ));
        unique_keys.insert(MetricKey::new(
            "counter",
            &[("b", "2"), ("c", "3"), ("a", "1")],
        ));
        unique_keys.insert(MetricKey::new(
            "counter",
            &[("c", "3"), ("a", "1"), ("b", "2")],
        ));
        unique_keys.insert(MetricKey::new(
            "counter",
            &[("c", "3"), ("b", "2"), ("a", "1")],
        ));

        // All permutations should be considered the same key
        assert_eq!(unique_keys.len(), 1);
    }

    /// Every attempt keeps its own counters and custom metrics, and the report
    /// aggregates them.
    #[test]
    fn attempts_are_recorded_one_by_one() {
        let metrics = TransactionMetrics::new();
        let key = MetricKey::new("documents", &[]);

        // Nothing to push before an attempt is opened.
        metrics.finish_attempt(AttemptOutcome::Failed);
        assert!(metrics.get_metrics_data().attempts.is_empty());

        let first = Arc::new(AttemptUsage::new());
        metrics.begin_attempt(first.clone());
        first.record_set(10);
        first.increment_custom(key.clone(), 1);
        metrics.record_commit(Duration::from_millis(3));
        metrics.record_grv(Duration::from_millis(5));
        metrics.record_grv(Duration::from_millis(1)); // first sample wins
        metrics.finish_attempt(AttemptOutcome::Retried {
            cause: FdbError::from_code(1020),
        });
        metrics.set_on_error_duration(Duration::from_millis(7));

        let second = Arc::new(AttemptUsage::new());
        metrics.begin_attempt(second.clone());
        second.record_set(4);
        second.increment_custom(key.clone(), 2);
        metrics.finish_attempt(AttemptOutcome::Committed);

        // A second end is a no-op: an attempt is pushed exactly once.
        metrics.finish_attempt(AttemptOutcome::Failed);

        let report = metrics.get_metrics_data();
        assert_eq!(report.attempts.len(), 2);
        assert_eq!(report.transaction.retries, 1);

        let first = &report.attempts[0];
        assert_eq!(first.index, 0);
        assert_eq!(first.usage.bytes_written, 10);
        assert_eq!(first.custom_metrics.get(&key), Some(&1));
        assert_eq!(first.commit_duration, Some(Duration::from_millis(3)));
        assert_eq!(first.grv_duration, Some(Duration::from_millis(5)));
        assert_eq!(first.on_error_duration, Some(Duration::from_millis(7)));
        assert!(matches!(
            first.outcome,
            AttemptOutcome::Retried { cause } if cause.code() == 1020
        ));

        let second = &report.attempts[1];
        assert_eq!(second.index, 1);
        assert_eq!(second.usage.bytes_written, 4);
        assert_eq!(second.custom_metrics.get(&key), Some(&2));
        assert!(second.commit_duration.is_none());
        assert!(second.grv_duration.is_none());
        assert!(matches!(second.outcome, AttemptOutcome::Committed));

        assert_eq!(report.total_usage().bytes_written, 14);
        assert_eq!(report.total_usage().call_set, 2);
    }

    /// An attempt whose only activity was `set_read_version` (no counter
    /// touched, no custom metric) must not be silently dropped: it should be
    /// discarded the same way a busy attempt is when a new one starts before
    /// `finish_attempt` was called, and the crate must keep working normally
    /// afterwards. This covers the state side of the `idle` heuristic in
    /// `begin_attempt`; the `#[cfg(feature = "trace")]` warning itself is not
    /// asserted here as the crate has no test pattern capturing `tracing`
    /// output content (see `tests/trace.rs`, which only checks the writer
    /// doesn't panic).
    #[test]
    fn dropped_attempt_with_only_read_version_is_handled() {
        let metrics = TransactionMetrics::new();

        let first = Arc::new(AttemptUsage::new());
        metrics.begin_attempt(first.clone());
        metrics.set_read_version(42);

        // A new attempt starts (e.g. `set_client_budget`/`reset`) without the
        // first one ever reaching `finish_attempt`: it must be dropped
        // cleanly, no panic.
        let second = Arc::new(AttemptUsage::new());
        metrics.begin_attempt(second.clone());
        second.record_set(1);
        metrics.finish_attempt(AttemptOutcome::Committed);

        let report = metrics.get_metrics_data();
        assert_eq!(report.attempts.len(), 1);
        let only = &report.attempts[0];
        assert_eq!(only.usage.bytes_written, 1);
        assert!(matches!(only.outcome, AttemptOutcome::Committed));

        // The read version set on the dropped attempt is still reflected at
        // the transaction level.
        assert_eq!(report.transaction.read_version, Some(42));
    }
}
