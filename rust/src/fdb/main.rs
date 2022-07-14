use clap::Parser;
use foundationdb::endpoints::{ping_request, protocol_info};
use foundationdb::flow::{cluster_file::ClusterFile, uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionKeeper, LoopbackHandler};

mod cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    println!("{:?}", cli);

    let cluster_file = cli.cluster_file.unwrap_or("fdb.cluster".parse()?);
    let cluster_file = match std::fs::read_to_string(&cluster_file) {
        Ok(cluster_file) => cluster_file,
        Err(err) => {
            return Err(format!("Could not read {:?}: {:?}", cluster_file, err).into());
        }
    };
    let cluster_file: ClusterFile = match cluster_file.parse() {
        Ok(cluster_file) => cluster_file,
        Err(err) => {
            return Err(format!("Could not parse cluster file: {:?}", err).into());
        }
    };

    let loopback_handler = LoopbackHandler::new()?;
    loopback_handler
        .register_well_known_endpoint(WLTOKEN::PingPacket, Box::new(ping_request::Ping::new()));
    loopback_handler.register_well_known_endpoint(
        WLTOKEN::ProtocolInfo,
        Box::new(protocol_info::ProtocolInfo::new()),
    );
    let pool = ConnectionKeeper::new(Some(cli.public_address), loopback_handler);

    // --------- Open socket for incoming connections
    {
        let p = pool.clone();
        tokio::spawn(async move { p.listen(cli.listen_address).await.unwrap() });
    }

    // --------- Send some messages to well-known endpoints (to prove we can)

    for server in &cluster_file.hosts {
        println!("Sending protocol info to {:?}", server);
        match server {
            foundationdb::flow::cluster_file::ClusterHost::IPAddr(socket_addr, false) => {
                println!(
                    "Got: {:x?}",
                    protocol_info::protocol_info(*socket_addr, &pool).await
                );
            }
            x => {
                println!("Connnection to {:?} not supported yet.", x);
            }
        }
    }

    // --------- Register with the cluster

    println!(
        "Connecting to FDB cluster {}:{}",
        cluster_file.description, cluster_file.id
    );

    {
        let pool = pool.clone();
        // TODO: Store fdb.cluster somewhere where it can be updated; MonitorLeader
        //       sometimes gets a new version of it from the cluster.
        let cluster_file = cluster_file.clone();

        use foundationdb::endpoints::get_leader::MonitorLeader;
        let (mut monitor, mut cluster_controller_full_interface) = MonitorLeader::new(pool, cluster_file);
        tokio::spawn(async move {
            loop {
                let res = cluster_controller_full_interface.register_worker_worker().await;
                println!("register_worker returned {:?}.  Respwaning.", res);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        tokio::spawn(async move {
            loop {
                let res = monitor.monitor_leader().await;
                println!("monitor_leader returned {:?}.  Respwaning.", res);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    tokio::time::sleep(std::time::Duration::from_secs(1000000)).await;
    println!("Goodbye, cruel world!");
    Ok(())
}
