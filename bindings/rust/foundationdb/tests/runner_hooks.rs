use foundationdb::*;
#[allow(unused_imports)]
use foundationdb_macros::cfg_api_versions;
#[allow(unused_imports)]
use std::sync::Arc;
#[allow(unused_imports)]
use std::sync::atomic::{AtomicU64, Ordering};

mod common;

/// Happy path: instrumented_run completes with metrics, no conflicts.
#[tokio::test]
async fn test_happy_path_instrumented() -> FdbResult<()> {
    let db = common::database().await?;

    let (result, metrics) = db
        .instrumented_run(|trx, _| async move {
            trx.set(b"test_runner_hooks_happy", b"value");
            Ok::<_, FdbBindingError>(42u64)
        })
        .await
        .expect("transaction should succeed");

    assert_eq!(result, 42);
    assert_eq!(metrics.transaction.retries, 0);
    assert_eq!(metrics.attempts.len(), 1);
    assert!(metrics.attempts[0].conflicting_keys.ranges().is_empty());

    Ok(())
}

/// Conflict path via instrumented_run: force a conflict and verify the
/// conflicting keys of the conflicted attempt are populated when
/// ReportConflictingKeys is enabled.
///
/// ReportConflictingKeys (option 712) was added in FDB 6.3.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn test_conflict_reports_in_metrics() -> FdbResult<()> {
    let db = common::database().await?;
    let attempt = Arc::new(AtomicU64::new(0));

    let (_, metrics) = db
        .instrumented_run(|trx, _| {
            let attempt = attempt.clone();
            async move {
                let current = attempt.fetch_add(1, Ordering::SeqCst);

                // Enable conflict reporting
                trx.set_option(options::TransactionOption::ReportConflictingKeys)?;

                // Read a key to establish read conflict range
                let _ = trx.get(b"test_conflict_metrics_key", false).await?;

                if current == 0 {
                    // On first attempt, write the same key from another transaction
                    let db2 = Database::new_compat(None).await?;
                    let other_trx = db2.create_trx()?;
                    other_trx.set(b"test_conflict_metrics_key", b"other_value");
                    other_trx
                        .commit()
                        .await
                        .map_err(|e| FdbBindingError::NonRetryableFdbError(FdbError::from(e)))?;
                }

                trx.set(b"test_conflict_metrics_key", b"my_value");
                Ok::<_, FdbBindingError>(())
            }
        })
        .await
        .expect("transaction should eventually succeed");

    // Should have retried at least once
    assert!(metrics.transaction.retries >= 1);
    // Should have recorded at least one conflict
    assert!(metrics.transaction.conflict_count >= 1);
    // Conflicting keys should be populated on the attempt that conflicted
    assert!(
        metrics
            .attempts
            .iter()
            .any(|attempt| !attempt.conflicting_keys.ranges().is_empty()),
        "expected conflicting keys to be reported"
    );

    Ok(())
}

/// Direct API: use Transaction::conflicting_keys() on a TransactionCommitError.
///
/// ReportConflictingKeys (option 712) was added in FDB 6.3.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn test_conflict_keys_direct_api() -> FdbResult<()> {
    let db = common::database().await?;

    // Transaction A: read, then try to commit after B writes the same key
    let trx_a = db.create_trx()?;
    trx_a.set_option(options::TransactionOption::ReportConflictingKeys)?;
    let _ = trx_a.get(b"test_conflict_direct_key", false).await?;

    // Transaction B: write the same key and commit
    let trx_b = db.create_trx()?;
    trx_b.set(b"test_conflict_direct_key", b"b_value");
    trx_b.commit().await.expect("trx B should commit");

    // Transaction A: write something and try to commit — should conflict
    trx_a.set(b"test_conflict_direct_key", b"a_value");
    let commit_result = trx_a.commit().await;

    match commit_result {
        Ok(_committed) => {
            // It's possible (though unlikely) that A commits successfully
            // if the read version window doesn't overlap. That's fine.
        }
        Err(commit_error) => {
            // Read conflicting keys before on_error resets the transaction
            let conflicting = commit_error.conflicting_keys().await?;
            for range in &conflicting {
                eprintln!(
                    "conflicting range: begin={:?} end={:?}",
                    String::from_utf8_lossy(range.begin()),
                    String::from_utf8_lossy(range.end()),
                );
            }
            assert!(
                !conflicting.is_empty(),
                "expected conflicting keys on commit error"
            );

            // Verify the conflicting range includes our key
            let key: &[u8] = b"test_conflict_direct_key";
            let has_our_key = conflicting
                .iter()
                .any(|range| key >= range.begin() && key < range.end());
            assert!(has_our_key, "conflicting range should contain our key");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Hook composition, ordering and lifecycle
// ---------------------------------------------------------------------------

use foundationdb::runner::{AttemptFailure, MetricsHooks, RetryPolicy, RunnerHooks};
use std::fmt;
use std::sync::Mutex;

/// A layer error with an app-level retry condition and a fatal domain error,
/// the two cases a [`RetryPolicy`] is expected to arbitrate.
#[derive(Debug)]
enum HookTestError {
    /// Carries a retryable FdbError, so the runner proposes `Fdb`.
    Fdb(FdbError),
    Binding(FdbBindingError),
    /// No FdbError anywhere in the chain: the runner proposes `Fatal`.
    Domain,
}

impl fmt::Display for HookTestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for HookTestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fdb(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Domain => None,
        }
    }
}

impl From<FdbError> for HookTestError {
    fn from(e: FdbError) -> Self {
        Self::Fdb(e)
    }
}

impl From<FdbBindingError> for HookTestError {
    fn from(e: FdbBindingError) -> Self {
        Self::Binding(e)
    }
}

impl RetryableError for HookTestError {}

/// Records every callback it receives, tagged with the name of the hook, so a
/// tuple of them shows both the lifecycle order and the order inside the tuple.
struct RecordingHooks {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
}

impl RecordingHooks {
    fn new(name: &'static str, events: &Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            name,
            events: events.clone(),
        }
    }

    fn record(&self, event: &str) {
        self.events
            .lock()
            .expect("events mutex")
            .push(format!("{}:{event}", self.name));
    }
}

impl RunnerHooks for RecordingHooks {
    fn on_attempt_start(&self, _trx: &Transaction, attempt: usize) {
        self.record(&format!("attempt_start:{attempt}"));
    }

    async fn before_commit(&self, _trx: &Transaction, attempt: usize) -> FdbResult<()> {
        self.record(&format!("before_commit:{attempt}"));
        Ok(())
    }

    fn on_hook_error(&self, err: &FdbError, attempt: usize) {
        self.record(&format!("hook_error:{}:{attempt}", err.code()));
    }

    fn on_commit_success(&self, _committed: &TransactionCommitted, _ms: u64, attempt: usize) {
        self.record(&format!("commit_success:{attempt}"));
    }

    async fn on_commit_error(&self, err: &TransactionCommitError, attempt: usize) -> FdbResult<()> {
        self.record(&format!("commit_error:{}:{attempt}", err.code()));
        Ok(())
    }

    fn on_closure_error(&self, err: &FdbError, attempt: usize) {
        self.record(&format!("closure_error:{}:{attempt}", err.code()));
    }

    fn on_error_duration(&self, _ms: u64, attempt: usize) {
        self.record(&format!("error_duration:{attempt}"));
    }

    fn on_retry(&self, attempt: usize) {
        self.record(&format!("retry:{attempt}"));
    }

    fn on_complete(&self) {
        self.record("complete");
    }
}

/// Hooks whose `before_commit` always fails, to check that the runner reports
/// it and commits anyway.
struct FailingBeforeCommit {
    events: Arc<Mutex<Vec<String>>>,
}

impl RunnerHooks for FailingBeforeCommit {
    async fn before_commit(&self, _trx: &Transaction, _attempt: usize) -> FdbResult<()> {
        self.events
            .lock()
            .expect("events mutex")
            .push("before_commit".to_string());
        // transaction_timed_out, an error the transaction itself never hit.
        Err(FdbError::from_code(1031))
    }

    fn on_hook_error(&self, err: &FdbError, attempt: usize) {
        self.events
            .lock()
            .expect("events mutex")
            .push(format!("hook_error:{}:{attempt}", err.code()));
    }
}

/// Both hooks of a tuple fire, left to right, and the lifecycle order of a run
/// that retries once is stable.
#[tokio::test]
async fn tuple_hooks_fire_left_to_right_in_lifecycle_order() {
    let db = common::database().await.expect("failed to open database");
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = (
        RecordingHooks::new("a", &events),
        RecordingHooks::new("b", &events),
    );
    let attempts = AtomicU64::new(0);
    let attempts_ref = &attempts;

    let result: Result<(), HookTestError> = db
        .run_with_hooks(&hooks, |trx, _| async move {
            if attempts_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(HookTestError::Fdb(FdbError::from_code(1020)));
            }
            trx.set(b"runner_hooks_tuple", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "run should succeed on the second attempt");
    let events = events.lock().expect("events mutex").clone();
    assert_eq!(
        events,
        vec![
            "a:attempt_start:0",
            "b:attempt_start:0",
            "a:closure_error:1020:0",
            "b:closure_error:1020:0",
            "a:error_duration:0",
            "b:error_duration:0",
            "a:retry:0",
            "b:retry:0",
            "a:attempt_start:1",
            "b:attempt_start:1",
            "a:before_commit:1",
            "b:before_commit:1",
            "a:commit_success:1",
            "b:commit_success:1",
            "a:complete",
            "b:complete",
        ],
    );
}

/// `Option<H>`: `Some` behaves like the hooks themselves, `None` is a no-op.
#[tokio::test]
async fn option_hooks_are_a_noop_when_none() {
    let db = common::database().await.expect("failed to open database");

    let events = Arc::new(Mutex::new(Vec::new()));
    let some = Some(RecordingHooks::new("some", &events));
    db.run_with_hooks(&some, |trx, _| async move {
        trx.set(b"runner_hooks_option_some", b"ok");
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("run should succeed");

    assert_eq!(
        events.lock().expect("events mutex").clone(),
        vec![
            "some:attempt_start:0",
            "some:before_commit:0",
            "some:commit_success:0",
            "some:complete",
        ],
    );

    let none: Option<RecordingHooks> = None;
    db.run_with_hooks(&none, |trx, _| async move {
        trx.set(b"runner_hooks_option_none", b"ok");
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("run should succeed without hooks");
}

/// A `before_commit` that fails is reported through `on_hook_error`, and the
/// transaction is committed all the same.
#[tokio::test]
async fn before_commit_error_is_reported_but_does_not_abort_the_commit() {
    let db = common::database().await.expect("failed to open database");
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = FailingBeforeCommit {
        events: events.clone(),
    };

    db.run_with_hooks(&hooks, |trx, _| async move {
        trx.set(b"runner_hooks_before_commit", b"committed");
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("the failing hook must not abort the run");

    assert_eq!(
        events.lock().expect("events mutex").clone(),
        vec!["before_commit", "hook_error:1031:0"],
    );

    let value = db
        .run(|trx, _| async move {
            Ok::<_, FdbBindingError>(
                trx.get(b"runner_hooks_before_commit", false)
                    .await?
                    .map(|slice| slice.to_vec()),
            )
        })
        .await
        .expect("read back");
    assert_eq!(value.as_deref(), Some(b"committed".as_ref()));
}

/// `on_complete` fires exactly once on the fatal path, and the original error
/// is returned.
#[tokio::test]
async fn on_complete_fires_once_on_the_fatal_path() {
    let db = common::database().await.expect("failed to open database");
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = RecordingHooks::new("h", &events);

    let result: Result<(), HookTestError> = db
        .run_with_hooks(&hooks, |_trx, _| async move { Err(HookTestError::Domain) })
        .await;

    assert!(matches!(result, Err(HookTestError::Domain)));
    assert_eq!(
        events.lock().expect("events mutex").clone(),
        vec!["h:attempt_start:0", "h:complete"],
    );
}

/// `on_complete` fires exactly once when the C API retry budget is exhausted.
#[tokio::test]
async fn on_complete_fires_once_when_retries_are_exhausted() {
    let db = common::database().await.expect("failed to open database");
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = RecordingHooks::new("h", &events);

    let result: Result<(), HookTestError> = db
        .run_with_hooks(&hooks, |trx, _| async move {
            trx.set_option(options::TransactionOption::RetryLimit(1))?;
            Err(HookTestError::Fdb(FdbError::from_code(1020)))
        })
        .await;

    assert!(matches!(result, Err(HookTestError::Fdb(_))));
    let events = events.lock().expect("events mutex").clone();
    assert_eq!(
        events.iter().filter(|event| *event == "h:complete").count(),
        1,
        "on_complete must fire exactly once, got {events:?}"
    );
    assert_eq!(
        events.last().map(String::as_str),
        Some("h:complete"),
        "on_complete must be the last event, got {events:?}"
    );
    // RetryLimit(1): one retry, so two attempts.
    assert!(events.contains(&"h:attempt_start:1".to_string()));
    assert!(!events.contains(&"h:attempt_start:2".to_string()));
}

// ---------------------------------------------------------------------------
// Retry policies
// ---------------------------------------------------------------------------

/// Stops the run once `max` attempts have been made, whatever the error.
struct MaxAttempts {
    max: usize,
}

impl RetryPolicy<HookTestError> for MaxAttempts {
    fn decide(
        &self,
        _failure: AttemptFailure<'_, HookTestError>,
        proposed: RetryDecision,
        attempt: usize,
    ) -> RetryDecision {
        if attempt + 1 >= self.max {
            RetryDecision::Fatal
        } else {
            proposed
        }
    }
}

/// Retries an app-level error the runner would consider fatal.
struct RetryDomainErrors;

impl RetryPolicy<HookTestError> for RetryDomainErrors {
    fn decide(
        &self,
        failure: AttemptFailure<'_, HookTestError>,
        proposed: RetryDecision,
        _attempt: usize,
    ) -> RetryDecision {
        match failure {
            AttemptFailure::Closure(HookTestError::Domain) => RetryDecision::Retry,
            _ => proposed,
        }
    }
}

/// A policy capping the attempts ends the run with the original closure error,
/// before the C API retry budget has anything to say.
#[tokio::test]
async fn retry_policy_can_cap_the_number_of_attempts() {
    let db = common::database().await.expect("failed to open database");
    let attempts = AtomicU64::new(0);
    let attempts_ref = &attempts;
    let policy = MaxAttempts { max: 3 };

    let result: Result<(), HookTestError> = db
        .runner()
        .retry_policy(&policy)
        .run(|_trx, _| async move {
            attempts_ref.fetch_add(1, Ordering::SeqCst);
            // Retryable on its own: only the policy can stop this run.
            Err(HookTestError::Fdb(FdbError::from_code(1020)))
        })
        .await;

    assert!(
        matches!(result, Err(HookTestError::Fdb(e)) if e.code() == 1020),
        "the original closure error must be returned, got {result:?}"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

/// A policy can retry an error the runner classifies as fatal: it goes through
/// `on_error` with code 1020, like a `RetryDecision::Retry` from the error
/// itself would.
#[tokio::test]
async fn retry_policy_can_retry_an_otherwise_fatal_error() {
    let db = common::database().await.expect("failed to open database");
    let attempts = AtomicU64::new(0);
    let attempts_ref = &attempts;
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = RecordingHooks::new("h", &events);

    let result: Result<(), HookTestError> = db
        .runner()
        .hooks(&hooks)
        .retry_policy(&RetryDomainErrors)
        .run(|trx, _| async move {
            if attempts_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(HookTestError::Domain);
            }
            trx.set(b"runner_hooks_policy_retry", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "the policy must force a retry: {result:?}");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let events = events.lock().expect("events mutex").clone();
    assert!(
        events.contains(&"h:closure_error:1020:0".to_string()),
        "the fatal error must be routed through on_error(1020), got {events:?}"
    );
}

// ---------------------------------------------------------------------------
// MetricsHooks stacked on user hooks
// ---------------------------------------------------------------------------

/// The wiring case: metrics hooks stacked on a user hook through the plain
/// `run_with_hooks` produce the same complete report as `instrumented_run`.
#[tokio::test]
async fn metrics_hooks_stacked_on_user_hooks_produce_a_full_report() {
    let db = common::database().await.expect("failed to open database");
    let events = Arc::new(Mutex::new(Vec::new()));
    let metrics = TransactionMetrics::new();
    let hooks = (
        MetricsHooks::new(&metrics),
        RecordingHooks::new("user", &events),
    );
    let attempts = AtomicU64::new(0);
    let attempts_ref = &attempts;

    let result: Result<(), HookTestError> = db
        .run_with_hooks(&hooks, |trx, _| async move {
            let first = attempts_ref.fetch_add(1, Ordering::SeqCst) == 0;
            trx.set(b"runner_hooks_metrics_stacked", b"value");
            trx.set_custom_metric("stacked", 1, &[]);
            let _ = trx.get(b"runner_hooks_metrics_stacked", false).await?;
            if first {
                return Err(HookTestError::Fdb(FdbError::from_code(1020)));
            }
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "run should succeed: {result:?}");

    let report = metrics.get_metrics_data();
    assert_eq!(report.transaction.retries, 1);
    assert_eq!(
        report.attempts.len(),
        report.transaction.retries as usize + 1,
        "attempts are pushed exactly once per attempt: {report:#?}"
    );
    assert!(report.total_duration.is_some());
    assert!(report.transaction.commit_version.is_some());

    let first = &report.attempts[0];
    assert_eq!(first.index, 0);
    assert!(matches!(first.outcome, AttemptOutcome::Retried { .. }));
    assert!(first.on_error_duration.is_some());
    assert_eq!(first.usage.call_set, 1);
    assert_eq!(first.usage.call_get, 1);
    assert!(first.usage.bytes_written > 0);
    assert_eq!(
        first
            .custom_metrics
            .get(&metrics::MetricKey::new("stacked", &[])),
        Some(&1),
    );

    let last = report.attempts.last().expect("a last attempt");
    assert!(matches!(last.outcome, AttemptOutcome::Committed));
    assert!(last.commit_duration.is_some());
    assert_eq!(last.usage.call_set, 1);

    // The user hook saw the same run.
    let events = events.lock().expect("events mutex").clone();
    assert_eq!(
        events.first().map(String::as_str),
        Some("user:attempt_start:0")
    );
    assert_eq!(events.last().map(String::as_str), Some("user:complete"));
}

/// A reference to hooks is itself hooks, and delegates to what it points at.
#[tokio::test]
async fn reference_hooks_delegate_to_the_hooks_they_point_at() {
    let db = common::database().await.expect("failed to open database");
    let events = Arc::new(Mutex::new(Vec::new()));
    let hooks = RecordingHooks::new("ref", &events);
    let by_reference = &hooks;

    db.run_with_hooks(&by_reference, |trx, _| async move {
        trx.set(b"runner_hooks_reference", b"ok");
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("run should succeed");

    assert_eq!(
        events.lock().expect("events mutex").clone(),
        vec![
            "ref:attempt_start:0",
            "ref:before_commit:0",
            "ref:commit_success:0",
            "ref:complete",
        ],
    );
}
