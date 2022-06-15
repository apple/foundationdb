use foundationdb::fdbserver::connection_handler::ConnectionHandler;
use foundationdb::flow;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

#[tokio::main]
async fn main() -> flow::Result<()> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let conn = TcpStream::connect(saddr).await?;
    let limit_connections = Arc::new(Semaphore::new(1));
    let permit = limit_connections.clone().acquire_owned().await?;

    let svc = ConnectionHandler::new((conn, saddr), permit).await?;
    svc.ping().await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
