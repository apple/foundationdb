//! # FoundationDB Recipes
//!
//! This module provides high-level distributed system recipes for FoundationDB,
//! similar to Apache Curator for ZooKeeper. These recipes implement common
//! distributed system patterns and primitives on top of FoundationDB's
//! transactional key-value store.
//!
//! ## Available Recipes
//!
//! - **Leader Election**: Distributed leader election mechanism that allows
//!   multiple processes to coordinate and elect a single leader. This is useful
//!   for implementing primary/secondary architectures, distributed task coordination,
//!   and ensuring single-writer patterns in distributed systems.
//!
//! ## Usage
//!
//! Each recipe is behind its own feature flag to keep the core library lightweight.
//! Enable the recipes you need in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! foundationdb = { version = "*", features = ["recipes-leader-election"] }
//! ```
//!
//! Or enable all recipes at once:
//!
//! ```toml
//! [dependencies]
//! foundationdb = { version = "*", features = ["recipes"] }
//! ```

/// Leader election recipe for distributed consensus
#[cfg(feature = "recipes-leader-election")]
pub mod leader_election;

/// Ranked register recipe for Paxos-style ballot fencing
#[cfg(feature = "recipes-ranked-register")]
pub mod ranked_register;
