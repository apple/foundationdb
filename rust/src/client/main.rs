use foundationdb::flow;
use foundationdb::flow::uid::{UID, WLTOKEN};
use foundationdb::services;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;
use tokio::sync::oneshot;

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
    let uid = UID::random_token();
    let frame = flow::frame::Frame::new(token, payload, offset);
    let (tx, rx) = oneshot::channel();
    svc.in_flight_requests.insert(uid, tx);

    svc.response_tx.send(frame).await?;

    println!("waiting for response!");
    let response_frame = rx.await?;
    println!("reponse frame: {:?}", response_frame);
    println!("Goodbye, cruel world!");

    Ok(())
}
