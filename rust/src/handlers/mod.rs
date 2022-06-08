use crate::flow::connection;
use crate::flow::file_identifier;
use crate::flow::frame;
use crate::flow::uid;
use crate::flow::Result;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

pub mod network_test;
pub mod ping_request;

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    limit_requests: Arc<Semaphore>,
    // TODO: Shutdown?
}

const MAX_CONNECTIONS: usize = 250;
const MAX_REQUESTS: usize = MAX_CONNECTIONS * 2;

async fn handle_frame(
    frame: frame::Frame,
    parsed_file_identifier: file_identifier::ParsedFileIdentifier,
    response_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    frame.validate()?;

    let response_frame = match frame.token.get_well_known_endpoint() {
        Some(uid::WLTOKEN::PingPacket) => {
            super::handlers::ping_request::handle(parsed_file_identifier, frame).await?
        }
        Some(uid::WLTOKEN::NetworkTest) => {
            super::handlers::network_test::handle(parsed_file_identifier, frame).await?
        }
        Some(uid::WLTOKEN::EndpointNotFound) => {
            println!("EndpointNotFound payload: {:x?}", frame);
            println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
            None
        }
        Some(uid::WLTOKEN::AuthTenant) => {
            println!("AuthTenant payload: {:x?}", frame);
            println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
            None
        }
        Some(uid::WLTOKEN::UnauthorizedEndpoint) => {
            println!("UnauthorizedEndpoint payload: {:x?}", frame);
            println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
            None
        }
        None => {
            println!("Message not destined for well-known endpoint: {:x?}", frame);
            println!("{:x?} {:04x?}", frame.token, parsed_file_identifier);
            None
        }
    };
    match response_frame {
        Some(response_frame) => response_tx.send(response_frame.as_bytes()).await?,
        None => (),
    };
    Ok(())
}

async fn handle_connection<C: 'static + AsyncRead + AsyncWrite + Unpin + Send>(
    file_identifier_table: &file_identifier::FileIdentifierNames,
    limit_requests: Arc<Semaphore>,
    conn: C,
) -> Result<()> {
    let (mut reader, mut writer) = connection::new(conn);
    writer.send_connect_packet().await?;
    println!("sent ConnectPacket");
    // Bound the number of backlogged messages to a given remote endpoint.  This is
    // set to the process-wide MAX_REQUESTS / 10 so that a few backpressuring receivers
    // can't consume all the request slots for this process.
    let (response_tx, mut response_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(MAX_REQUESTS / 10);
    tokio::spawn(async move {
        while let Some(frame) = response_rx.recv().await {
            writer.write_frame_bytes(&frame).await.unwrap(); //XXX unwrap!
            loop {
                match response_rx.try_recv() {
                    Ok(frame) => {
                        writer.write_frame_bytes(&frame).await.unwrap();
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        writer.flush().await.unwrap();
                        break;
                    }
                    Err(e) => {
                        println!("Unexpected error! {:?}", e);
                        return;
                    }
                }
            }
        }
    });
    loop {
        let response_tx = response_tx.clone();
        let limit_requests = limit_requests.clone();

        match reader.read_frame().await? {
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
                limit_requests.acquire().await.unwrap().forget();
                tokio::spawn(async move {
                    match handle_frame(frame, parsed_file_identifier, response_tx).await {
                        Ok(()) => (),
                        Err(e) => println!("Error: {:?}", e),
                    };
                    limit_requests.add_permits(1);
                });
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
        limit_requests: Arc::new(Semaphore::new(MAX_REQUESTS)),
    };

    println!("listening");

    loop {
        let permit = server
            .limit_connections
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let socket = server.listener.accept().await?;
        println!("got socket from {}", socket.1);
        let limit_requests = server.limit_requests.clone();
        tokio::spawn(async move {
            let file_identifier_table = file_identifier::FileIdentifierNames::new()?;
            match handle_connection(&file_identifier_table, limit_requests, socket.0).await {
                Ok(()) => {
                    println!("Clean connection shutdown!");
                }
                Err(e) => {
                    println!("Unclean connnection shutdown: {:?}", e);
                }
            }
            drop(permit);
            Ok::<(), crate::flow::Error>(())
        });
    }
}
