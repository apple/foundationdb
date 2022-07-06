use foundationdb::endpoints::{network_test, ping_request};
use foundationdb::flow::{uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionKeeper, LoopbackHandler};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<()> {
    let loopback_handler = LoopbackHandler::new()?;
    loopback_handler
        .register_well_known_endpoint(WLTOKEN::PingPacket, Box::new(ping_request::Ping::new()));
    loopback_handler.register_well_known_endpoint(
        WLTOKEN::ReservedForTesting,
        Box::new(network_test::NetworkTest::new()),
    );
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let pool = ConnectionKeeper::new(Some(listen_addr), loopback_handler);
    pool.listen().await?;
    println!("Goodbye, cruel world!");
    Ok(())
}
