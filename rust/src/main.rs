mod fdbserver;
mod flow;
mod services;

#[tokio::main]
async fn main() -> flow::Result<()> {
    fdbserver::grv_master::foo();
    // services::hello().await?;
    services::hello_tower().await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
