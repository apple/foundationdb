use clap::Parser;
use foundationdb::endpoints::ping_request;
use foundationdb::flow::{uid::WLTOKEN, Result, cluster_file::ClusterFile};
use foundationdb::services::{ConnectionKeeper, LoopbackHandler};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    println!("{:?}", cli);

    let cluster_file = cli.cluster_file.unwrap_or("fdb.cluster".parse()?);
    let cluster_file = match std::fs::read_to_string(&cluster_file) {
        Ok(cluster_file) => cluster_file,
        Err(err) => { return Err(format!("Could not read {:?}: {:?}", cluster_file, err).into()); }
    };
    let cluster_file : ClusterFile = match cluster_file.parse() {
        Ok(cluster_file) => cluster_file,
        Err(err) => { return Err(format!("Could not parse cluster file: {:?}", err).into()); }
    };

    let loopback_handler = LoopbackHandler::new()?;
    loopback_handler
        .register_well_known_endpoint(WLTOKEN::PingPacket, Box::new(ping_request::Ping::new()));

    // Register worker role here
    // loopback_handler.register_well_known_endpoint(WLTOKEN::Worker???, ...:new(cluster_file));

    match cli.class {
        Some(cli::FdbRole::Master) | None => { // None => default, which is currently just the GRV master.
        }
        Some(class) => {
            return Err(format!("Unimplemented process class: {:?}", class).into());
        }
    }

    let pool = ConnectionKeeper::new(Some(cli.public_address), loopback_handler);
    pool.listen(cli.listen_address).await?;
    println!("Goodbye, cruel world!");
    Ok(())
}
