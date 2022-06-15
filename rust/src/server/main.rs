use fdbserver::connection_handler::ConnectionHandler;
use foundationdb::fdbserver;
use foundationdb::flow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut rx = ConnectionHandler::new_listener(&format!("127.0.0.1:{}", 6789)).await?;
    println!("Listening.");
    while let Some(connection_handler) = rx.recv().await {
        println!("New connection: {:?}", connection_handler.addr);
    }
    println!("Goodbye, cruel world!");
    Ok(())
}
