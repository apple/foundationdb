//! Simulation workloads for FoundationDB recipes
//!
//! This crate provides simulation workloads to test FoundationDB recipes
//! under chaos conditions including network partitions, process failures,
//! and cluster reconfigurations.

mod leader_election;

pub use leader_election::LeaderElectionWorkload;

use foundationdb_simulation::register_workload;

register_workload!(LeaderElectionWorkload);
