use std::collections::HashSet;

use foundationdb::{Database, FdbError, KeySelector, RangeOption};
use futures_util::TryStreamExt;

// Helper function to insert some initial data: "a" through "z"
async fn setup_db(db: &Database) -> Result<(), FdbError> {
    let transaction_result: Result<(), FdbError> = db
        .run(|trx, _maybe_committed| async move {
            trx.clear_range(b"", b"\\xFF"); // Clear all existing keys
            let keys_to_insert = vec![
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
                "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
            ];
            for key_str in keys_to_insert {
                let key_bytes = key_str.as_bytes();
                trx.set(key_bytes, key_bytes); // Using key as value for simplicity
            }
            Ok(())
        })
        .await
        .map_err(|binding_err: foundationdb::FdbBindingError| {
            eprintln!("FoundationDB Binding Error during setup_db: {binding_err:?}");
            FdbError::from_code(2004) // client_invalid_operation
        });

    transaction_result
}

/**
 * Helper function to run a range query test using KeySelectors.
 * It fetches a range of keys and asserts them against the expected keys.
 */
async fn run_range_query_test<'a>(
    db: &'a Database,
    options: RangeOption<'a>,
    expected_keys: Vec<&str>,
) -> Result<(), FdbError> {
    let query_result: Result<HashSet<String>, FdbError> = db
        .run(move |trx, _maybe_committed| {
            let options = options.clone(); // Clone options to move into async block
            async move {
                let mut results: HashSet<String> = HashSet::new();
                let mut elements = trx.get_ranges_keyvalues(options, false);
                while let Ok(Some(element)) = elements.try_next().await {
                    // Collect keys for comparison
                    results.insert(String::from_utf8_lossy(element.key()).to_string());
                }
                Ok(results)
            }
        })
        .await
        .map_err(|binding_err: foundationdb::FdbBindingError| {
            eprintln!("FoundationDB Binding Error during run_range_query_test: {binding_err:?}");
            FdbError::from_code(2004) // client_invalid_operation
        });

    let actual_keys_set = query_result?;

    let expected = HashSet::from_iter(expected_keys.iter().map(|s| s.to_string()));

    let difference = actual_keys_set.difference(&expected).collect::<Vec<_>>();
    if !difference.is_empty() {
        eprintln!("Expected keys: {expected_keys:?}");
        eprintln!("Actual keys: {actual_keys_set:?}");
        eprintln!("Difference: {difference:?}");
        assert!(
            difference.is_empty(),
            "Expected {} keys, but got {}",
            expected_keys.len(),
            difference.len()
        );
    }

    Ok(())
}

async fn key_selector_example(db: &Database) -> Result<(), FdbError> {
    setup_db(db).await?;

    // Selectors: first_greater_or_equal(b"a") to first_greater_than(b"h")
    // Keys: a, b, c, d, ..., z
    // Begin: first_greater_or_equal(b"a") -> resolves to "a"
    // End: first_greater_than(b"h") -> resolves to "i"
    // Range is ["a", "i") -> "a", "b", "c", "d", "e", "f", "g", "h"
    println!(
        "Running: FGOE(\"a\") to FGT(\"h\") -> expecting ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']"
    );
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_or_equal(b"a"),
            end: KeySelector::first_greater_than(b"h"),
            ..Default::default()
        },
        vec!["a", "b", "c", "d", "e", "f", "g", "h"],
    )
    .await?;

    // Original example: Get range ["b", "d") which means keys "b", "c"
    // Keys: a, b, c, d, ..., z
    // Begin: first_greater_or_equal(b"b") -> resolves to "b"
    // End: first_greater_than(b"c") -> resolves to "d"
    // Range is ["b", "d") -> "b", "c"
    println!("Running: FGOE(\"b\") to FGT(\"c\") -> expecting ['b', 'c']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_or_equal(b"b"),
            end: KeySelector::first_greater_than(b"c"), // resolves to "d"
            ..Default::default()
        },
        vec!["b", "c"],
    )
    .await?;

    // Selectors: first_greater_or_equal (begin) and first_greater_or_equal (end)
    // Begin: KeySelector::first_greater_or_equal(b"c") -> "c"
    // End: KeySelector::first_greater_or_equal(b"f") -> "f"
    // Range: ["c", "f") -> "c", "d", "e"
    println!("Running: FGOE(\"c\") to FGOE(\"f\") -> expecting ['c', 'd', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_or_equal(b"c"),
            end: KeySelector::first_greater_or_equal(b"f"),
            ..Default::default()
        },
        vec!["c", "d", "e"],
    )
    .await?;

    // Selectors: first_greater_than (begin) and last_less_than (end)
    // Begin: KeySelector::first_greater_than(b"b") -> "c"
    // End: KeySelector::last_less_than(b"f") -> "e" (key is "e", range is exclusive at end)
    // Range: ["c", "e") -> "c", "d"
    println!("Running: FGT(\"b\") to LLT(\"f\") -> expecting ['c', 'd']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_than(b"b"),
            end: KeySelector::last_less_than(b"f"),
            ..Default::default()
        },
        vec!["c", "d"],
    )
    .await?;

    // Selectors: KeySelector::new for both begin and end, with various offsets
    // Begin: KeySelector::new(b"a", true, 2) -> first_greater_or_equal("a") + 2 -> "c"
    // End: KeySelector::new(b"g", false, 0) -> first_greater_than("g") + 0 -> "h"
    // Range: ["c", "h") -> "c", "d", "e", "f", "g"
    println!("Running: new(a,T,2) to new(g,F,0) -> expecting ['c', 'd', 'e', 'f', 'g']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::new(std::borrow::Cow::Borrowed(b"a"), true, 2),
            end: KeySelector::new(std::borrow::Cow::Borrowed(b"g"), false, 0),
            ..Default::default()
        },
        vec!["c", "d", "e", "f", "g"],
    )
    .await?;

    // Selectors: KeySelector::new(b"a", true, 0) and KeySelector::new(b"g", false, -2)
    // Begin: KeySelector::new(b"a", true, 0) -> FGOE("a") + 0 -> "a"
    // End: KeySelector::new(b"g", false, -2) -> FGT("g") - 2 -> "h" - 2 -> "d"
    // Range: ["a", "d") -> "a", "b", "c"
    println!("Running: new(a,T,0) to new(g,F,-2) -> expecting ['a', 'b', 'c']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::new(std::borrow::Cow::Borrowed(b"a"), true, 0),
            end: KeySelector::new(std::borrow::Cow::Borrowed(b"g"), false, -2),
            ..Default::default()
        },
        vec!["a", "b", "c"],
    )
    .await?;

    // Selectors resolve to the same key, resulting in an empty range
    // Begin: KeySelector::first_greater_or_equal(b"d") -> "d"
    // End: KeySelector::first_greater_than(b"c") -> "d" (equivalent to first_greater_or_equal(b"d"))
    // Range: ["d", "d") -> empty
    println!("Running: FGOE(\"d\") to FGT(\"c\") -> expecting []");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_or_equal(b"d"),
            end: KeySelector::first_greater_than(b"c"),
            ..Default::default()
        },
        vec![],
    )
    .await?;

    // Selectors: last_less_or_equal (begin) and first_greater_than (end)
    // Begin: KeySelector::last_less_or_equal(b"c") -> "c"
    // End: KeySelector::first_greater_than(b"f") -> "g"
    // Range: ["c", "g") -> "c", "d", "e", "f"
    println!("Running: LLOE(\"c\") to FGT(\"f\") -> expecting ['c', 'd', 'e', 'f']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_or_equal(b"c"),
            end: KeySelector::first_greater_than(b"f"),
            ..Default::default()
        },
        vec!["c", "d", "e", "f"],
    )
    .await?;

    // Selectors: last_less_than (begin) and last_less_than (end)
    // Begin: KeySelector::last_less_than(b"c") -> "b"
    // End: KeySelector::last_less_than(b"f") -> "e"
    // Range: ["b", "e") -> "b", "c", "d"
    println!("Running: LLT(\"c\") to LLT(\"f\") -> expecting ['b', 'c', 'd']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_than(b"c"),
            end: KeySelector::last_less_than(b"f"),
            ..Default::default()
        },
        vec!["b", "c", "d"],
    )
    .await?;

    // Selectors: last_less_than (begin) and last_less_or_equal (end)
    // Begin: KeySelector::last_less_than(b"c") -> "b"
    // End: KeySelector::last_less_or_equal(b"f") -> "f"
    // Range: ["b", "f") -> "b", "c", "d", "e"
    println!("Running: LLT(\"c\") to LLOE(\"f\") -> expecting ['b', 'c', 'd', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_than(b"c"),
            end: KeySelector::last_less_or_equal(b"f"),
            ..Default::default()
        },
        vec!["b", "c", "d", "e"],
    )
    .await?;

    // Selectors: last_less_than (begin) and first_greater_than (end)
    // Begin: KeySelector::last_less_than(b"c") -> "b"
    // End: KeySelector::first_greater_than(b"f") -> "g"
    // Range: ["b", "g") -> "b", "c", "d", "e", "f"
    println!("Running: LLT(\"c\") to FGT(\"f\") -> expecting ['b', 'c', 'd', 'e', 'f']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_than(b"c"),
            end: KeySelector::first_greater_than(b"f"),
            ..Default::default()
        },
        vec!["b", "c", "d", "e", "f"],
    )
    .await?;

    // Selectors: last_less_than (begin) and first_greater_or_equal (end)
    // Begin: KeySelector::last_less_than(b"c") -> "b"
    // End: KeySelector::first_greater_or_equal(b"f") -> "f"
    // Range: ["b", "f") -> "b", "c", "d", "e"
    println!("Running: LLT(\"c\") to FGOE(\"f\") -> expecting ['b', 'c', 'd', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_than(b"c"),
            end: KeySelector::first_greater_or_equal(b"f"),
            ..Default::default()
        },
        vec!["b", "c", "d", "e"],
    )
    .await?;

    // Selectors: last_less_or_equal (begin) and last_less_than (end)
    // Begin: KeySelector::last_less_or_equal(b"c") -> "c"
    // End: KeySelector::last_less_than(b"f") -> "e"
    // Range: ["c", "e") -> "c", "d"
    println!("Running: LLOE(\"c\") to LLT(\"f\") -> expecting ['c', 'd']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_or_equal(b"c"),
            end: KeySelector::last_less_than(b"f"),
            ..Default::default()
        },
        vec!["c", "d"],
    )
    .await?;

    // Selectors: last_less_or_equal (begin) and last_less_or_equal (end)
    // Begin: KeySelector::last_less_or_equal(b"c") -> "c"
    // End: KeySelector::last_less_or_equal(b"f") -> "f"
    // Range: ["c", "f") -> "c", "d", "e"
    println!("Running: LLOE(\"c\") to LLOE(\"f\") -> expecting ['c', 'd', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_or_equal(b"c"),
            end: KeySelector::last_less_or_equal(b"f"),
            ..Default::default()
        },
        vec!["c", "d", "e"],
    )
    .await?;

    // Selectors: last_less_or_equal (begin) and first_greater_or_equal (end)
    // Begin: KeySelector::last_less_or_equal(b"c") -> "c"
    // End: KeySelector::first_greater_or_equal(b"f") -> "f"
    // Range: ["c", "f") -> "c", "d", "e"
    println!("Running: LLOE(\"c\") to FGOE(\"f\") -> expecting ['c', 'd', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::last_less_or_equal(b"c"),
            end: KeySelector::first_greater_or_equal(b"f"),
            ..Default::default()
        },
        vec!["c", "d", "e"],
    )
    .await?;

    // Selectors: first_greater_than (begin) and last_less_or_equal (end)
    // Begin: KeySelector::first_greater_than(b"c") -> "d"
    // End: KeySelector::last_less_or_equal(b"f") -> "f"
    // Range: ["d", "f") -> "d", "e"
    println!("Running: FGT(\"c\") to LLOE(\"f\") -> expecting ['d', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_than(b"c"),
            end: KeySelector::last_less_or_equal(b"f"),
            ..Default::default()
        },
        vec!["d", "e"],
    )
    .await?;

    // Selectors: first_greater_than (begin) and first_greater_than (end)
    // Begin: KeySelector::first_greater_than(b"c") -> "d"
    // End: KeySelector::first_greater_than(b"f") -> "g"
    // Range: ["d", "g") -> "d", "e", "f"
    println!("Running: FGT(\"c\") to FGT(\"f\") -> expecting ['d', 'e', 'f']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_than(b"c"),
            end: KeySelector::first_greater_than(b"f"),
            ..Default::default()
        },
        vec!["d", "e", "f"],
    )
    .await?;

    // Selectors: first_greater_than (begin) and first_greater_or_equal (end)
    // Begin: KeySelector::first_greater_than(b"c") -> "d"
    // End: KeySelector::first_greater_or_equal(b"f") -> "f"
    // Range: ["d", "f") -> "d", "e"
    println!("Running: FGT(\"c\") to FGOE(\"f\") -> expecting ['d', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_than(b"c"),
            end: KeySelector::first_greater_or_equal(b"f"),
            ..Default::default()
        },
        vec!["d", "e"],
    )
    .await?;

    // Selectors: first_greater_or_equal (begin) and last_less_than (end)
    // Begin: KeySelector::first_greater_or_equal(b"c") -> "c"
    // End: KeySelector::last_less_than(b"f") -> "e"
    // Range: ["c", "e") -> "c", "d"
    println!("Running: FGOE(\"c\") to LLT(\"f\") -> expecting ['c', 'd']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_or_equal(b"c"),
            end: KeySelector::last_less_than(b"f"),
            ..Default::default()
        },
        vec!["c", "d"],
    )
    .await?;

    // Selectors: first_greater_or_equal (begin) and last_less_or_equal (end)
    // Begin: KeySelector::first_greater_or_equal(b"c") -> "c"
    // End: KeySelector::last_less_or_equal(b"f") -> "f"
    // Range: ["c", "f") -> "c", "d", "e"
    println!("Running: FGOE(\"c\") to LLOE(\"f\") -> expecting ['c', 'd', 'e']");
    run_range_query_test(
        db,
        RangeOption {
            begin: KeySelector::first_greater_or_equal(b"c"),
            end: KeySelector::last_less_or_equal(b"f"),
            ..Default::default()
        },
        vec!["c", "d", "e"],
    )
    .await?;

    println!("All key selector examples ran successfully!");
    Ok(())
}

#[tokio::main]
async fn main() {
    foundationdb::boot().expect("failed to initialize FoundationDB");
    // The network is stopped and joined automatically at process exit, which is
    // fine for tests and short-lived tools like this example. In a production
    // application, prefer a clean teardown: the network thread is the event loop
    // driving every transaction and you may still have on-going operations at
    // exit time. Finish or cancel your work, drop the Database handles, then
    // call `foundationdb::api::stop_network()` yourself (terminal: any
    // FoundationDB use afterwards fails with error 2025).

    match Database::default() {
        Ok(db) => {
            if let Err(e) = key_selector_example(&db).await {
                eprintln!("Error in key_selector_example: {e:?}");
                // Consider panic! or std::process::exit(1) for CI environments
            }
        }
        Err(e) => {
            eprintln!("Failed to open FDB database: {e:?}");
            // Consider panic! or std::process::exit(1)
        }
    }
}
