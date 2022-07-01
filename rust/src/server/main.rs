use foundationdb::endpoints::{network_test, ping_request};
use foundationdb::flow::{uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionHandler, LoopbackHandler, RequestRouter};
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
    let request_router = RequestRouter::new(loopback_handler);
    let listen_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let mut rx = ConnectionHandler::new_listener(listen_addr, request_router.clone()).await?;
    println!("Listening.");
    while let Some(connection_handler) = rx.recv().await {
        println!("New connection: {:?}", connection_handler.peer);
        request_router
            .remote_endpoints
            .insert(connection_handler.peer, connection_handler);
    }
    println!("Goodbye, cruel world!");
    Ok(())
}
