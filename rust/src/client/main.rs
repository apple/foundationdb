use foundationdb::flow;
use foundationdb::flow::uid::{UID, WLTOKEN};
use foundationdb::services;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> flow::Result<()> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6789);
    let conn = TcpStream::connect(saddr).await?;

    let (svc, response_rx) = services::Svc::new();
    tokio::spawn(services::connection_handler(
        svc.clone(),
        response_rx,
        None,
        conn,
        saddr,
    ));

    let token = UID::well_known_token(WLTOKEN::UnauthorizedEndpoint);
    let payload = vec![1; 33];
    let offset = 0;
    let frame = flow::frame::Frame::new(token, payload, offset);
    svc.response_tx.send(frame).await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    println!("Goodbye, cruel world!");

    Ok(())
}
