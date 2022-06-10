use foundationdb::flow;
use foundationdb::services;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> flow::Result<()> {
    let bind = TcpListener::bind(&format!("127.0.0.1:{}", 6789)).await?;

    services::listen(bind, services::Svc{}).await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
