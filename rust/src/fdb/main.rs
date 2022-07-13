use clap::Parser;
use foundationdb::endpoints::{ping_request, protocol_info};
use foundationdb::flow::{cluster_file::ClusterFile, uid::UID, uid::WLTOKEN, Result};
use foundationdb::services::{ConnectionKeeper, LoopbackHandler};

use std::sync::Arc;

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
    {
        let p = pool.clone();
        tokio::spawn(async move { p.listen(cli.listen_address).await.unwrap() });
    }

    {
        // TODO: Put this somehwere where it can be updated...
        let cluster_file = cluster_file.clone();
        let known_leader = UID::default();
        let svc = pool.clone();
        let serialized_info = Vec::new();
        tokio::spawn(async move {
            use foundationdb::endpoints::get_leader::MonitorLeader;
            let mut monitor = MonitorLeader {
                cluster_file,
                known_leader,
                svc,
                serialized_info,
            };
            loop {
                let res = monitor.monitor_leader().await;
                println!("monitor_leader returns {:?}.  Respwaning.", res);
            }
        });
    }

    println!(
        "Attempting to connect to FDB cluster id={} description={}",
        cluster_file.description, cluster_file.id
    );

    for server in cluster_file.hosts {
        println!("Sending protocol info to {:?}", server);
        match server {
            foundationdb::flow::cluster_file::ClusterHost::IPAddr(socket_addr, false) => {
                println!(
                    "Got: {:x?}",
                    protocol_info::protocol_info(socket_addr, &pool).await
                );
            }
            x => {
                println!("Connnection to {:?} not supported yet.", x);
            }
        }
    }
    tokio::time::sleep(std::time::Duration::from_secs(1000000)).await;
    println!("Goodbye, cruel world!");
    Ok(())
}
