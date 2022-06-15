use foundationdb::flow;
use foundationdb::services;
use foundationdb::fdbserver;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> flow::Result<()> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let conn = TcpStream::connect(saddr).await?;

    let (svc, response_rx) = services::Svc::new();
    tokio::spawn(fdbserver::connection_handler::connection_handler(
        svc.clone(),
        response_rx,
        None,
        conn,
        saddr,
    ));
    svc.ping().await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
