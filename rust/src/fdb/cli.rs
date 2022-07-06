use clap::{arg, command, value_parser, Arg, Command, Parser, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[clap(rename_all = "snake_case")]
pub enum FdbRole {
    Storage,
    Transaction,
    Resolution,
    GrvProxy,
    CommitProxy,
    Master,
    Test,
    Unset,
    Stateless,
    Log,
    Router,
    ClusterController,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// Public address, specified as `IP_ADDRESS:PORT' or (not implemented yet) `auto:PORT'.
    #[clap(short, long, value_parser, value_name = "ADDRESS")]
    pub public_address: SocketAddr,
    /// Listen address, specified as `IP_ADDRESS:PORT' (defaults to public address).
    #[clap(short, long, value_parser, value_name = "ADDRESS")]
    pub listen_address: Option<SocketAddr>,
    /// The path of a file containing the connection string for the
    /// FoundationDB cluster. The default is first the value of the
    /// FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',
    /// then `/etc/foundationdb/fdb.cluster'.
    #[clap(short = 'C', long, value_parser, value_name = "CONNFILE")]
    pub cluster_file: Option<PathBuf>,
    /// Machine class.  TODO: Only "master" is implemented.
    #[clap(short = 'c', long, value_enum, value_name = "CLASS")]
    pub class: Option<FdbRole>,
}
