#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use std::{
    cell::RefCell,
    env, fmt,
    sync::{Once, atomic::AtomicU64},
};

use foundationdb_simulation::{Severity, WorkloadContext};
use tracing_core::{
    Dispatch, Event, Level, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Registry,
    filter::{LevelFilter, LevelParseError},
    layer::{Context, Layer, SubscriberExt},
    registry::LookupSpan,
};

// The context bound to the current simulation thread. `WorkloadContext` is
// `!Send`/`!Sync` (it wraps raw fdbserver pointers), so it never leaves the
// thread it was installed on: events fired on any other thread find an empty
// slot and are dropped. The slot also carries the install generation so an
// older `TracingGuard` cannot clear a binding that a newer `install` replaced.
thread_local! {
    static SIM_CONTEXT: RefCell<Option<(WorkloadContext, u64)>> = const { RefCell::new(None) };
}

/// Monotonic install counter used to tell competing bindings apart.
static GENERATION: AtomicU64 = AtomicU64::new(0);
/// Guards the one-shot global subscriber registration.
static INIT: Once = Once::new();
/// Environment variable controlling the maximum level forwarded to fdbserver.
const LEVEL_ENV: &str = "FDB_SIM_TRACING_LEVEL";

/// Installs the global tracing subscriber (first call only, idempotent) and
/// binds tracing events emitted on the current simulation thread to
/// `context`'s trace log. Call from your workload factory
/// (`SingleRustWorkload::new` / `RustWorkloadFactory::create`) and store the
/// returned guard in your workload struct: when the guard drops (workload
/// teardown), the binding is cleared, so no event can ever reach a freed
/// context.
///
/// The subscriber is a [`tracing_subscriber::Registry`] with a forwarding
/// [`tracing_subscriber::Layer`]. By default, it forwards `INFO` and above;
/// set `FDB_SIM_TRACING_LEVEL` to `debug` or `trace` before the first call to
/// override the level. Span context (names and fields) is captured and attached
/// to each forwarded event.
///
/// If another global tracing subscriber is already installed, this prints a
/// warning to stderr and forwarding stays disabled (events reach the
/// pre-existing subscriber instead).
// Not `#[instrument]`: the crate depends only on `tracing-core` /
// `tracing-subscriber`, neither of which provides the attribute macro
// (`#[instrument]` lives in `tracing`), and on the first call the subscriber
// does not exist yet, so there would be nothing to record.
#[must_use = "dropping the guard immediately unbinds the context, so tracing \
              events would no longer be forwarded"]
pub fn install(context: &WorkloadContext) -> TracingGuard {
    let generation = GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    SIM_CONTEXT.with(|slot| {
        *slot.borrow_mut() = Some((context.clone(), generation));
    });
    INIT.call_once(|| {
        let subscriber = Registry::default()
            .with(SimLayer)
            .with(configured_level_filter());
        if tracing_core::dispatcher::set_global_default(Dispatch::new(subscriber)).is_err() {
            eprintln!(
                "foundationdb-simulation-tracing: a global tracing subscriber is already \
                 installed; simulator trace forwarding is disabled and events will reach the \
                 pre-existing subscriber instead"
            );
        }
    });
    TracingGuard { generation }
}

/// Reads the forwarding level once, while the global subscriber is installed.
fn configured_level_filter() -> LevelFilter {
    match env::var(LEVEL_ENV) {
        Ok(value) => match parse_level_filter(Some(&value)) {
            Ok(level) => level,
            Err(error) => {
                eprintln!(
                    "foundationdb-simulation-tracing: invalid {LEVEL_ENV}={value:?} ({error}); \
                     defaulting to info"
                );
                LevelFilter::INFO
            }
        },
        Err(env::VarError::NotPresent) => LevelFilter::INFO,
        Err(env::VarError::NotUnicode(value)) => {
            eprintln!(
                "foundationdb-simulation-tracing: invalid non-Unicode {LEVEL_ENV}={value:?}; \
                 defaulting to info"
            );
            LevelFilter::INFO
        }
    }
}

fn parse_level_filter(value: Option<&str>) -> Result<LevelFilter, LevelParseError> {
    value.unwrap_or("info").parse()
}

/// Clears the thread-local context slot on drop, but only if the slot still
/// points at the context this guard was created for (a newer [`install`]
/// wins). Keep it alive for as long as tracing events should be forwarded,
/// typically by storing it in your workload struct.
pub struct TracingGuard {
    generation: u64,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        SIM_CONTEXT.with(|slot| {
            let mut slot = slot.borrow_mut();
            if matches!(&*slot, Some((_, generation)) if *generation == self.generation) {
                *slot = None;
            }
        });
    }
}

/// Fields captured for a span, stored in its registry extensions so events
/// fired inside the span can attach them. Keys and values are already
/// NUL-sanitized.
struct SpanFields(Vec<(String, String)>);

/// `tracing_subscriber::Layer` that captures span fields into the registry and
/// forwards every event to the thread-local simulation context. It holds no
/// `WorkloadContext` (the registry owns all span state), so it stays
/// `Send + Sync` as `Dispatch` requires.
struct SimLayer;

impl<S> Layer<S> for SimLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist for its id");
        let mut visitor = DetailVisitor::default();
        attrs.record(&mut visitor);
        span.extensions_mut()
            .insert(SpanFields(visitor.into_all_fields()));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist for its id");
        let mut extensions = span.extensions_mut();
        if let Some(fields) = extensions.get_mut::<SpanFields>() {
            let mut visitor = DetailVisitor::default();
            values.record(&mut visitor);
            for (key, value) in visitor.into_all_fields() {
                if let Some(existing) = fields.0.iter_mut().find(|(k, _)| *k == key) {
                    existing.1 = value;
                } else {
                    fields.0.push((key, value));
                }
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        SIM_CONTEXT.with(|slot| {
            let slot = slot.borrow();
            let Some((context, _)) = slot.as_ref() else {
                return;
            };
            let details = event_details(event, &ctx);
            context.trace(
                severity(event.metadata().level()),
                "RustTracingEvent",
                &details,
            );
        });
    }
}

/// Builds the `(key, value)` details for an event: the `Target`/`File`/`Line`/
/// `Message` meta keys, then the event's own fields, then span context (`Span`,
/// `SpanPath`, and ancestor span fields from root to leaf). A span field is
/// skipped when its key is already present, so an event field always wins over
/// a span field of the same name.
fn event_details<S>(event: &Event<'_>, ctx: &Context<'_, S>) -> Vec<(String, String)>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let metadata = event.metadata();
    let mut visitor = DetailVisitor::default();
    event.record(&mut visitor);

    let mut details: Vec<(String, String)> = Vec::with_capacity(visitor.fields.len() + 6);
    details.push(("Target".to_owned(), sanitize(metadata.target())));
    if let Some(file) = metadata.file() {
        details.push(("File".to_owned(), sanitize(file)));
    }
    if let Some(line) = metadata.line() {
        details.push(("Line".to_owned(), line.to_string()));
    }
    if let Some(message) = visitor.message {
        details.push(("Message".to_owned(), message));
    }
    details.extend(visitor.fields);

    if let Some(scope) = ctx.event_scope(event) {
        // `from_root` yields the ancestors from the outermost span to the
        // innermost, which is the order we want for both `Span` (the leaf) and
        // `SpanPath` (root to leaf).
        let spans: Vec<_> = scope.from_root().collect();
        if let Some(leaf) = spans.last() {
            details.push(("Span".to_owned(), sanitize(leaf.name())));
        }
        let path = spans
            .iter()
            .map(|span| span.name())
            .collect::<Vec<_>>()
            .join("/");
        details.push(("SpanPath".to_owned(), sanitize(&path)));

        // Append span fields from leaf to root, with skip-if-present, so a
        // deeper span's field overrides a shallower one. Combined with event
        // fields being added first, the precedence is event > innermost span >
        // ... > root span.
        for span in spans.iter().rev() {
            let extensions = span.extensions();
            if let Some(fields) = extensions.get::<SpanFields>() {
                for (key, value) in &fields.0 {
                    if !details.iter().any(|(existing, _)| existing == key) {
                        details.push((key.clone(), value.clone()));
                    }
                }
            }
        }
    }

    details
}

/// Maps a `tracing` level to an FDB [`Severity`]. `ERROR` maps to
/// [`Severity::WarnAlways`] rather than [`Severity::Error`] on purpose:
/// `Severity::Error` flags the whole simulation as failed, and a dependency's
/// `tracing::error!` must not kill a run. Failing a simulation stays an
/// explicit `context.trace(Severity::Error, ...)`.
fn severity(level: &Level) -> Severity {
    if *level == Level::ERROR {
        Severity::WarnAlways
    } else if *level == Level::WARN {
        Severity::Warn
    } else if *level == Level::INFO {
        Severity::Info
    } else {
        // TRACE and DEBUG
        Severity::Debug
    }
}

/// Strips NUL bytes so values survive the `CString` boundary in
/// `WorkloadContext::trace` (a NUL would otherwise panic across `extern "C"`).
fn sanitize(value: &str) -> String {
    if value.contains('\0') {
        value.chars().filter(|&c| c != '\0').collect()
    } else {
        value.to_owned()
    }
}

/// Collects fields into `(key, value)` strings, pulling the field literally
/// named `message` out separately (used for the event `Message` detail). Every
/// key and value has NUL bytes stripped before it is stored.
#[derive(Default)]
struct DetailVisitor {
    message: Option<String>,
    fields: Vec<(String, String)>,
}

impl DetailVisitor {
    fn push(&mut self, field: &Field, value: String) {
        let value = sanitize(&value);
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.fields.push((sanitize(field.name()), value));
        }
    }

    /// Returns every captured field, folding a captured `message` back in as a
    /// regular `message` field. Used for spans, where the `message` field (if
    /// any) is just another field to carry.
    fn into_all_fields(mut self) -> Vec<(String, String)> {
        if let Some(message) = self.message {
            self.fields.insert(0, ("message".to_owned(), message));
        }
        self.fields
    }
}

impl Visit for DetailVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, value.to_owned());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field, value.to_string());
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.push(field, value.to_string());
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.push(field, value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use foundationdb_simulation::WorkloadContext;
    use foundationdb_simulation::internals::FDBWorkloadContext;

    use super::*;

    /// A zeroed context is enough for guard tests: we only store it in the slot
    /// and drop it, never calling any method that would dereference a pointer.
    fn dummy_context() -> WorkloadContext {
        WorkloadContext::new(unsafe { std::mem::zeroed::<FDBWorkloadContext>() })
    }

    fn slot_generation() -> Option<u64> {
        SIM_CONTEXT.with(|slot| slot.borrow().as_ref().map(|(_, generation)| *generation))
    }

    /// Shared slot the capture layer writes the forwarded details into.
    type CapturedSlot = Arc<Mutex<Option<Vec<(String, String)>>>>;

    /// Test layer that records the details `event_details` would forward, so we
    /// can assert on them without a live `WorkloadContext`.
    struct CaptureLayer(CapturedSlot);

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
            *self.0.lock().unwrap() = Some(event_details(event, &ctx));
        }
    }

    /// Runs `emit` under a `Registry` stacked with `SimLayer` (so span fields
    /// are captured) and a `CaptureLayer` (so we can read the forwarded
    /// details), returning the details of the single emitted event.
    fn capture(emit: impl FnOnce()) -> Vec<(String, String)> {
        capture_with_filter(LevelFilter::INFO, emit).unwrap()
    }

    fn capture_with_filter(
        level_filter: LevelFilter,
        emit: impl FnOnce(),
    ) -> Option<Vec<(String, String)>> {
        let captured = Arc::new(Mutex::new(None));
        let subscriber = Registry::default()
            .with(SimLayer)
            .with(CaptureLayer(captured.clone()))
            .with(level_filter);
        tracing_core::dispatcher::with_default(&Dispatch::new(subscriber), emit);
        captured.lock().unwrap().take()
    }

    fn has(details: &[(String, String)], key: &str, value: &str) -> bool {
        details.iter().any(|(k, v)| k == key && v == value)
    }

    #[test]
    fn severity_mapping_never_errors() {
        assert!(matches!(severity(&Level::TRACE), Severity::Debug));
        assert!(matches!(severity(&Level::DEBUG), Severity::Debug));
        assert!(matches!(severity(&Level::INFO), Severity::Info));
        assert!(matches!(severity(&Level::WARN), Severity::Warn));
        assert!(matches!(severity(&Level::ERROR), Severity::WarnAlways));
    }

    #[test]
    fn debug_events_are_dropped_by_default_and_enabled_by_override() {
        let default = parse_level_filter(None).unwrap();
        assert!(capture_with_filter(default, || tracing::debug!("filtered")).is_none());

        let debug = parse_level_filter(Some("debug")).unwrap();
        let details = capture_with_filter(debug, || tracing::debug!("forwarded")).unwrap();
        assert!(has(&details, "Message", "forwarded"));
    }

    #[test]
    fn debug_spans_are_dropped_by_default() {
        let details = capture(|| {
            let span = tracing::debug_span!("filtered_span", client = 7);
            let _entered = span.enter();
            tracing::info!("visible event");
        });

        assert!(!details.iter().any(|(key, _)| key == "Span"));
        assert!(!has(&details, "client", "7"));
    }

    #[test]
    fn sanitize_strips_nul_bytes() {
        assert_eq!(sanitize("a\0b\0c"), "abc");
        assert_eq!(sanitize("clean"), "clean");
        assert_eq!(sanitize("\0"), "");
    }

    #[test]
    fn event_splits_message_from_fields() {
        let details = capture(|| tracing::info!(client = 3, "hello world"));
        assert!(has(&details, "Message", "hello world"));
        assert!(has(&details, "client", "3"));
    }

    #[test]
    fn event_treats_explicit_message_field_as_message() {
        let details = capture(|| tracing::info!(message = "explicit", other = 1));
        assert!(has(&details, "Message", "explicit"));
        assert!(has(&details, "other", "1"));
        assert!(!details.iter().any(|(key, _)| key == "message"));
    }

    #[test]
    fn event_strips_nul_from_keys_and_values() {
        let details = capture(|| tracing::info!(dirty = "a\0b", "mes\0sage"));
        assert!(has(&details, "Message", "message"));
        assert!(has(&details, "dirty", "ab"));
    }

    #[test]
    fn event_in_span_carries_span_context() {
        let details = capture(|| {
            let span = tracing::info_span!("demo_span", client = 7);
            let _entered = span.enter();
            tracing::info!("inside the span");
        });
        assert!(has(&details, "Span", "demo_span"));
        assert!(has(&details, "SpanPath", "demo_span"));
        assert!(has(&details, "client", "7"));
        assert!(has(&details, "Message", "inside the span"));
    }

    #[test]
    fn nested_spans_produce_full_path() {
        let details = capture(|| {
            let outer = tracing::info_span!("outer", a = 1);
            let _outer = outer.enter();
            let inner = tracing::info_span!("inner", b = 2);
            let _inner = inner.enter();
            tracing::info!("deep event");
        });
        assert!(has(&details, "Span", "inner"));
        assert!(has(&details, "SpanPath", "outer/inner"));
        assert!(has(&details, "a", "1"));
        assert!(has(&details, "b", "2"));
    }

    #[test]
    fn event_field_wins_over_span_field() {
        let details = capture(|| {
            let span = tracing::info_span!("collision", shared = "from_span");
            let _entered = span.enter();
            tracing::info!(shared = "from_event", "msg");
        });
        let shared: Vec<_> = details.iter().filter(|(key, _)| key == "shared").collect();
        assert_eq!(shared.len(), 1, "span field must be skipped on collision");
        assert_eq!(shared[0].1, "from_event");
    }

    #[test]
    fn innermost_span_field_wins_over_outer() {
        let details = capture(|| {
            let outer = tracing::info_span!("outer", shared = "from_outer");
            let _outer = outer.enter();
            let inner = tracing::info_span!("inner", shared = "from_inner");
            let _inner = inner.enter();
            tracing::info!("event without a shared field");
        });
        let shared: Vec<_> = details.iter().filter(|(key, _)| key == "shared").collect();
        assert_eq!(
            shared.len(),
            1,
            "outer span field must be skipped on collision"
        );
        assert_eq!(shared[0].1, "from_inner");
    }

    #[test]
    fn guard_clears_slot_on_drop() {
        let context = dummy_context();
        {
            let _guard = install(&context);
            assert!(slot_generation().is_some());
        }
        assert_eq!(slot_generation(), None);
    }

    #[test]
    fn newer_install_survives_older_guard_drop() {
        let context = dummy_context();
        let guard_a = install(&context);
        let gen_a = slot_generation();
        let guard_b = install(&context);
        let gen_b = slot_generation();
        assert_ne!(gen_a, gen_b);

        // The older guard must leave the newer binding untouched.
        drop(guard_a);
        assert_eq!(slot_generation(), gen_b);

        // The newer guard owns the current binding and clears it.
        drop(guard_b);
        assert_eq!(slot_generation(), None);
    }
}
