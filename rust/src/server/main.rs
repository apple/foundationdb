use foundationdb::flow::Result;
use foundationdb::fdbserver;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let bind = TcpListener::bind(&format!("127.0.0.1:{}", 6789)).await?;

    fdbserver::connection_handler::listen(bind).await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
