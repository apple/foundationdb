use foundationdb::fdbserver::connection_handler::ConnectionHandler;
use foundationdb::flow::Result;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<()> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let svc = ConnectionHandler::new_outgoing_connection(saddr).await?;
    svc.ping().await?;


    println!("got ping response from {:?}", svc.peer);
    println!("Goodbye, cruel world!");

    Ok(())
}
