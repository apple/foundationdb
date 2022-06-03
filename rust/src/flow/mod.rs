// Implementation of the flow network protocol.  See flow_transport.md for more information.
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

// TODO
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub mod connection;
pub mod file_identifier;
mod file_identifier_table;
pub mod frame;
pub mod uid;


struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    // TODO: Shutdown?
}

const MAX_CONNECTIONS: usize = 250;

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
        let mut conn = connection::Connection::new(socket.0);
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
                            super::handlers::ping_request::handle(&mut conn, parsed_file_identifier, frame).await?;
                        }
                        Some(uid::WLTOKEN::NetworkTest) => {
                            super::handlers::network_test::handle(&mut conn, parsed_file_identifier, frame).await?;
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
                },
            }
        }
    }
}

// #[test]
// fn test_uid() -> Result<()> {
//     let s = "0123456789abcdeffedcba9876543210";
//     let uid = uid::UID::from_string(s)?;
//     let uid_s = uid.to_string();
//     assert_eq!(uid_s, s);
//     let uid2 = uid::UID::from_string(&uid_s)?;
//     assert_eq!(uid, uid2);
//     assert_eq!(uid.to_u128(), 0x0123456789abcdeffedcba9876543210);
//     Ok(())
// }
