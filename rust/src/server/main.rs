use foundationdb::endpoints::{network_test, ping_request};
use foundationdb::flow::{uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionHandler, LoopbackHandler, RequestRouter};

#[tokio::main]
async fn main() -> Result<()> {
    let loopback_handler = LoopbackHandler::new()?;
    loopback_handler.register_well_known_endpoint(WLTOKEN::PingPacket, ping_request::handler);
    loopback_handler
        .register_well_known_endpoint(WLTOKEN::ReservedForTesting, network_test::handler);
    let request_router = RequestRouter::new(loopback_handler);
    let mut rx =
        ConnectionHandler::new_listener(&format!("127.0.0.1:{}", 6789), request_router.clone())
            .await?;
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
