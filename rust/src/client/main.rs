use foundationdb::fdbserver::connection_handler::ConnectionHandler;
use foundationdb::flow::Result;
use foundationdb::services::{network_test, ping_request};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<()> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let svc = ConnectionHandler::new_outgoing_connection(saddr).await?;
    ping_request::ping(&svc).await?;
    println!("got ping response from {:?}", svc.peer);
    network_test::network_test(&svc, 100, 100).await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
