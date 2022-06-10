mod fdbserver;
mod flow;
mod services;

#[tokio::main]
async fn main() -> flow::Result<()> {
    fdbserver::grv_master::foo();
    services::hello_tower(services::Svc{}).await?;
    println!("Goodbye, cruel world!");

    Ok(())
}
