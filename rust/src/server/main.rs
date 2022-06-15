use foundationdb::fdbserver;
use foundationdb::flow::Result;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    println!("binding");
    let bind = TcpListener::bind(&format!("127.0.0.1:{}", 6789)).await?;

    println!("listening");
    fdbserver::connection_handler::listen(bind).await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
