use crate::services::{FlowMessage, Svc};
use crate::flow::{Result, connection};
use tokio::io::{AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::net::{TcpListener, TcpStream};
use tower::Service;
use std::ops::Deref;

use std::net::SocketAddr;
use std::sync::Arc;

const MAX_CONNECTIONS: usize = 250;
const MAX_REQUESTS: usize = MAX_CONNECTIONS * 2;

async fn sender<C: 'static + AsyncWrite + Unpin + Send>(
    mut response_rx: tokio::sync::mpsc::Receiver<FlowMessage>,
    mut writer: connection::ConnectionWriter<C>,
) -> Result<()> {
    while let Some(message) = response_rx.recv().await {
        writer.write_frame(message.frame()).await?;
        loop {
            match response_rx.try_recv() {
                Ok(message) => {
                    writer.write_frame(message.frame()).await.unwrap();
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    writer.flush().await.unwrap();
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

fn spawn_sender<C: 'static + AsyncWrite + Unpin + Send>(
    response_rx: mpsc::Receiver<FlowMessage>,
    writer: connection::ConnectionWriter<C>,
) {
    tokio::spawn(async move {
        match sender(response_rx, writer).await {
            Ok(_) => {}
            Err(e) => {
                println!("Unexpected error from sender! {:?}", e);
            }
        }
        // TODO: Connection teardown logic?
    });
}

pub async fn connection_handler(
    svc: Arc<Svc>,
    response_rx: mpsc::Receiver<FlowMessage>,
    permit: Option<OwnedSemaphorePermit>,
    stream: TcpStream,
    addr: SocketAddr,
) -> Result<()> {
    let (mut reader, writer) = match connection::new(stream).await {
        Err(e) => {
            println!("{}: {:?}", addr, e);
            match permit {
                Some(permit) => drop(permit),
                None => (),
            };
            return Err(e);
        }
        Ok((reader, writer, connect_packet)) => {
            // TODO: Check protocol compatibility, create object w/ enough info to allow request routing
            println!("{}: {:x?}", addr, connect_packet);
            (reader, writer)
        }
    };

    spawn_sender(response_rx, writer);

    let mut svc = tower::limit::concurrency::ConcurrencyLimit::new(svc.deref(), MAX_REQUESTS);
    loop {
        match reader.read_frame().await? {
            None => {
                println!("clean shutdown!");
                break;
            }
            Some(frame) => {
                let request = FlowMessage::new(frame)?;
                let response_tx = svc.get_ref().response_tx.clone();
                // poll_ready and call must be invoked atomically
                // we could read this before reading the next frame to prevent the next, throttled request from consuming
                // TCP buffers.  However, keeping one extra frame around (above the limit) is unlikely to matter in terms
                // of memory usage, but it helps interleave network + processing time.
                futures_util::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
                let fut = svc.call(request);
                tokio::spawn(async move {
                    // the real work happens in await, anyway
                    let response = fut.await.unwrap();
                    match response {
                        Some(response) => response_tx.send(response).await.unwrap(),
                        None => (),
                    };
                });
            }
        }
    }
    match permit {
        Some(permit) => drop(permit),
        None => (),
    };
    Ok(())
}
pub async fn listen(bind: TcpListener) -> Result<()> {
    let limit_connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));

    loop {
        let permit = limit_connections.clone().acquire_owned().await?;
        let socket = bind.accept().await?;
        let (svc, response_rx) = Svc::new();
        let svc = Arc::new(svc);
        tokio::spawn(connection_handler(
            svc,
            response_rx,
            Some(permit),
            socket.0,
            socket.1,
        ));
    }
}
