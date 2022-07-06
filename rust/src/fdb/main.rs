use clap::Parser;
use foundationdb::endpoints::ping_request;
use foundationdb::flow::{uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionKeeper, LoopbackHandler};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    println!("{:?}", cli);

    let loopback_handler = LoopbackHandler::new()?;
    loopback_handler
        .register_well_known_endpoint(WLTOKEN::PingPacket, Box::new(ping_request::Ping::new()));

    let pool = ConnectionKeeper::new(Some(cli.public_address), loopback_handler);
    pool.listen(cli.listen_address).await?;
    println!("Goodbye, cruel world!");
    Ok(())
}
