mod fdbserver;
mod flow;
mod handlers;

#[tokio::main]
async fn main() -> flow::Result<()> {
    fdbserver::grv_master::foo();
    handlers::hello().await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
