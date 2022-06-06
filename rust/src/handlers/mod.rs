use crate::flow::connection;
use crate::flow::file_identifier;
use crate::flow::uid;
use crate::flow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

pub mod network_test;
pub mod ping_request;

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    // TODO: Shutdown?
}

const MAX_CONNECTIONS: usize = 250;

async fn handle_connection(
    file_identifier_table: &file_identifier::FileIdentifierNames,
    mut conn: connection::Connection,
) -> Result<()> {
    conn.send_connect_packet().await?;
    println!("sent ConnectPacket");
    loop {
        match conn.read_frame().await? {
            None => {
                println!("clean shutdown!");
                break;
            }
            Some(frame) => {
                if frame.payload.len() < 8 {
                    println!("Frame is too short! {:x?}", frame);
                    continue;
                }

                let file_identifier = frame.peek_file_identifier()?;
                let parsed_file_identifier = file_identifier_table.from_id(file_identifier)?;

                match frame.token.get_well_known_endpoint() {
                    Some(uid::WLTOKEN::PingPacket) => {
                        super::handlers::ping_request::handle(
                            &mut conn,
                            parsed_file_identifier,
                            frame,
                        )
                        .await?;
                    }
                    Some(uid::WLTOKEN::NetworkTest) => {
                        super::handlers::network_test::handle(
                            &mut conn,
                            parsed_file_identifier,
                            frame,
                        )
                        .await?;
                    }
                    Some(uid::WLTOKEN::EndpointNotFound) => {
                        println!("EndpointNotFound payload: {:x?}", frame);
                        println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
                    }
                    Some(uid::WLTOKEN::AuthTenant) => {
                        println!("AuthTenant payload: {:x?}", frame);
                        println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
                    }
                    Some(uid::WLTOKEN::UnauthorizedEndpoint) => {
                        println!("UnauthorizedEndpoint payload: {:x?}", frame);
                        println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
                    }
                    None => {
                        println!("Message not destined for well-known endpoint: {:x?}", frame);
                        println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
                    }
                }
            }
        }
    }
    Ok(())
}

pub async fn hello() -> Result<()> {
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", 6789)).await?;
    let server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
    };

    let file_identifier_table = file_identifier::FileIdentifierNames::new()?;

    println!("listening");

    loop {
        server.limit_connections.acquire().await.unwrap().forget();
        let socket = server.listener.accept().await?;
        println!("got socket from {}", socket.1);
        let conn = connection::Connection::new(socket.0);
        match handle_connection(&file_identifier_table, conn).await {
            Ok(()) => {
                println!("Clean connection shutdown!");
            }
            Err(e) => {
                println!("Unclean connnection shutdown: {:?}", e);
            }
        }
    }
}
