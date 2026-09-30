use foundationdb::directory::DirectoryError;
use foundationdb::tuple::hca::HcaError;
use foundationdb::{FdbBindingError, FdbError, RetryDecision, RetryableError};
use std::fmt;

#[test]
// This test is here because I'm always creating infinite recursion on Display and Debug impl 🤦
// Exhibit A: https://github.com/foundationdb-rs/foundationdb-rs/pull/83
// Exhibit B: https://github.com/foundationdb-rs/foundationdb-rs/issues/93
fn test_debug_display_trait() {
    let error = FdbBindingError::ReferenceToTransactionKept;
    println!("{error}");
    println!("{error:?}");
}

/// A typical layer error wrapping an FdbError as its source, like a
/// thiserror enum with `#[source]`/`#[from]` would.
#[derive(Debug)]
struct LayerError {
    source: FdbError,
}

impl fmt::Display for LayerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "layer error: {}", self.source)
    }
}

impl std::error::Error for LayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[test]
fn get_fdb_error_walks_source_chain() {
    let layer_err = LayerError {
        source: FdbError::from_code(1020),
    };
    let err = FdbBindingError::new_custom_error(Box::new(layer_err));
    assert_eq!(err.get_fdb_error().map(|e| e.code()), Some(1020));
}

#[test]
fn get_fdb_error_finds_nested_directory_hca() {
    let err = FdbBindingError::from(DirectoryError::HcaError(HcaError::FdbError(
        FdbError::from_code(1020),
    )));
    assert_eq!(err.get_fdb_error().map(|e| e.code()), Some(1020));
}

#[test]
fn get_fdb_error_boxed_binding_error() {
    let inner = FdbBindingError::from(FdbError::from_code(1020));
    let err = FdbBindingError::new_custom_error(Box::new(inner));
    assert_eq!(err.get_fdb_error().map(|e| e.code()), Some(1020));
}

/// A layer error enum implementing RetryableError with the default
/// source-walk classification.
#[derive(Debug)]
enum TypedLayerError {
    Fdb(FdbError),
    Binding(FdbBindingError),
    Invalid,
}

impl fmt::Display for TypedLayerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TypedLayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fdb(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::Invalid => None,
        }
    }
}

impl From<FdbError> for TypedLayerError {
    fn from(e: FdbError) -> Self {
        Self::Fdb(e)
    }
}

impl From<FdbBindingError> for TypedLayerError {
    fn from(e: FdbBindingError) -> Self {
        Self::Binding(e)
    }
}

impl RetryableError for TypedLayerError {}

#[test]
fn retry_decision_default_walk() {
    let wrapped = TypedLayerError::Fdb(FdbError::from_code(1020));
    match wrapped.retry_decision() {
        RetryDecision::Fdb(e) => assert_eq!(e.code(), 1020),
        other => panic!("expected Fdb decision, got {other:?}"),
    }

    let nested = TypedLayerError::Binding(FdbBindingError::from(FdbError::from_code(1020)));
    match nested.retry_decision() {
        RetryDecision::Fdb(e) => assert_eq!(e.code(), 1020),
        other => panic!("expected Fdb decision, got {other:?}"),
    }

    match TypedLayerError::Invalid.retry_decision() {
        RetryDecision::Fatal => {}
        other => panic!("expected Fatal decision, got {other:?}"),
    }
}

#[test]
fn stringified_error_has_no_fdb_error() {
    // Stringifying an error destroys the source chain: there is no FdbError
    // to recover from a String, so such errors are never retried.
    let layer_err = LayerError {
        source: FdbError::from_code(1020),
    };
    let err = FdbBindingError::CustomError(layer_err.to_string().into());
    assert!(err.get_fdb_error().is_none());
}
