use foundationdb::endpoints::{network_test, ping_request};
use foundationdb::flow::{uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionHandler, LoopbackHandler, RequestRouter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<()> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let loopback_handler = LoopbackHandler::new()?;
    loopback_handler.register_well_known_endpoint(WLTOKEN::PingPacket, ping_request::handler);
    loopback_handler
        .register_well_known_endpoint(WLTOKEN::ReservedForTesting, network_test::handler);
    let request_router = RequestRouter::new(loopback_handler);
    let svc = ConnectionHandler::new_outgoing_connection(saddr, request_router.clone()).await?;
    request_router.remote_endpoints.insert(saddr, svc);
    ping_request::ping(saddr, request_router.as_ref()).await?;
    println!("got ping response from {:?}", saddr);
    network_test::network_test(saddr, request_router.as_ref(), 100, 100).await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
