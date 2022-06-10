use foundationdb::flow;
use foundationdb::flow::connection;
use foundationdb::fdbserver;
use foundationdb::services;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;


#[tokio::main]
async fn main() -> flow::Result<()> {

    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789); 
    let conn = TcpStream::connect(saddr).await?;

    services::spawn_connection_handler(services::Svc{}, None, conn, saddr).await?;

    println!("Goodbye, cruel world!");

    Ok(())
}
