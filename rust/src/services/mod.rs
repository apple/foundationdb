use crate::flow::connection;
use crate::flow::file_identifier;
use crate::flow::frame;
use crate::flow::frame::Frame;
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::Result;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::Service;

pub mod network_test;
pub mod ping_request;

pub struct FlowRequest {
    frame: frame::Frame,
    file_identifier: file_identifier::FileIdentifier,
}

pub struct FlowResponse {
    frame: frame::Frame,
}

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    limit_requests: Arc<Semaphore>,
    // TODO: Shutdown?
}

const MAX_CONNECTIONS: usize = 250;
const MAX_REQUESTS: usize = MAX_CONNECTIONS * 2;

#[derive(Clone, Debug)]
pub struct Svc {
    pub response_tx: mpsc::Sender<Frame>,
    pub in_flight_requests: Arc<dashmap::DashMap<UID, oneshot::Sender<Frame>>>,
}

impl Svc {
    pub fn new() -> (Self, mpsc::Receiver<Frame>) {
        // Bound the number of backlogged messages to a given remote endpoint.  This is
        // set to the process-wide MAX_REQUESTS / 10 so that a few backpressuring receivers
        // can't consume all the request slots for this process.
        let (response_tx, response_rx) = tokio::sync::mpsc::channel::<Frame>(MAX_REQUESTS / 10);
        (
            Svc {
                response_tx,
                in_flight_requests: Arc::new(dashmap::DashMap::new()),
            },
            response_rx,
        )
    }
}

fn handle_req<'a, 'b>(
    in_flight_requests: &dashmap::DashMap<UID, oneshot::Sender<Frame>>,
    request: FlowRequest,
) -> Result<
    std::pin::Pin<Box<dyn Send + futures_util::Future<Output = Result<Option<FlowResponse>>>>>,
> {
    // TODO: Put this back in the async path?
    request.frame.validate()?;
    let wltoken = request.frame.token.get_well_known_endpoint();
    let response_handler = match wltoken {
        None => in_flight_requests.remove(&request.frame.token),
        Some(_) => None,
    };

    if let Some((_uid, tx)) = response_handler {
        match tx.send(request.frame) {
            Ok(()) => (),
            Err(frame) => {
                println!("Dropping duplicate response for token {:?}", frame.token)
            }
        };
        return Ok(Box::pin(async move { Ok(None) } )); // XXX inefficient
    }
    // TODO: remove the closure?  Many of these paths just await a future.  May as well pin those instead.  Also, response_tx.send() is sync...
    Ok(Box::pin(async move {
        Ok(match request.frame.token.get_well_known_endpoint() {
            Some(WLTOKEN::PingPacket) => ping_request::handle(request).await?,
            Some(WLTOKEN::ReservedForTesting) => network_test::handle(request).await?,
            Some(wltoken) => {
                let fit = file_identifier::FileIdentifierNames::new()?;
                println!(
                    "Got unhandled request for well-known enpoint {:?}: {:x?} {:04x?} {:04x?}",
                    wltoken,
                    request.frame.token,
                    &request.file_identifier,
                    fit.from_id(&request.file_identifier)
                );
                None
            }
            None => {
                let fit = file_identifier::FileIdentifierNames::new()?;
                println!(
                    "Message not destined for well-known endpoint and not a known respsonse: {:x?}",
                    request.frame
                );

                println!(
                    "{:x?} {:04x?} {:04x?}",
                    request.frame.token,
                    request.file_identifier,
                    fit.from_id(&request.file_identifier)
                );
                None
            }
        })
    }))
}

impl Service<FlowRequest> for Svc {
    // type Future: Future<Output = std::result::Result<Self::Response, Self::Error>>;
    type Response = Option<FlowResponse>;
    type Error = super::flow::Error;
    type Future = std::pin::Pin<Box<dyn 'static + Send + Future<Output = Result<Self::Response>>>>; // XXX get rid of box!

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: FlowRequest) -> Self::Future {
        match handle_req(&self.in_flight_requests, req) {
            Ok(fut) => fut,
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}

#[allow(unused_variables)]
async fn handle_frame(
    frame: frame::Frame,
    file_identifier: file_identifier::FileIdentifier,
    response_tx: tokio::sync::mpsc::Sender<Frame>,
) -> Result<()> {
    let request = FlowRequest {
        frame,
        file_identifier,
    };

    // match handle_req(request).await? {
    //     Some(response) => response_tx.send(response.frame).await?,
    //     None => (),
    // };
    Ok(())
}

async fn sender<C: 'static + AsyncWrite + Unpin + Send>(
    mut response_rx: tokio::sync::mpsc::Receiver<Frame>,
    mut writer: connection::ConnectionWriter<C>,
) -> Result<()> {
    while let Some(frame) = response_rx.recv().await {
        writer.write_frame(frame).await.unwrap(); //XXX unwrap!
        loop {
            match response_rx.try_recv() {
                Ok(frame) => {
                    writer.write_frame(frame).await.unwrap();
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
    response_rx: tokio::sync::mpsc::Receiver<Frame>,
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
    svc: Svc,
    response_rx: mpsc::Receiver<Frame>,
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

    let mut svc = tower::limit::concurrency::ConcurrencyLimit::new(svc, MAX_REQUESTS);
    loop {
        match reader.read_frame().await? {
            None => {
                println!("clean shutdown!");
                break;
            }
            Some(frame) => {
                if frame.payload().len() < 8 {
                    println!("Frame is too short! {:x?}", frame);
                    continue;
                }
                let file_identifier = frame.peek_file_identifier()?;
                let request = FlowRequest {
                    frame,
                    file_identifier,
                };
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
                        Some(response) => response_tx.send(response.frame).await.unwrap(),
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
        tokio::spawn(connection_handler(
            svc,
            response_rx,
            Some(permit),
            socket.0,
            socket.1,
        ));
    }
}

async fn handle_connection<C: 'static + AsyncRead + AsyncWrite + Unpin + Send>(
    limit_requests: Arc<Semaphore>,
    conn: C,
) -> Result<()> {
    let (mut reader, writer, _conn_packet) = connection::new(conn).await?;

    // Bound the number of backlogged messages to a given remote endpoint.  This is
    // set to the process-wide MAX_REQUESTS / 10 so that a few backpressuring receivers
    // can't consume all the request slots for this process.
    let (response_tx, response_rx) = tokio::sync::mpsc::channel::<Frame>(MAX_REQUESTS / 10);
    spawn_sender(response_rx, writer);

    loop {
        let response_tx = response_tx.clone();
        let limit_requests = limit_requests.clone();

        match reader.read_frame().await? {
            None => {
                println!("clean shutdown!");
                break;
            }
            Some(frame) => {
                if frame.payload().len() < 8 {
                    println!("Frame is too short! {:x?}", frame);
                    continue;
                }
                let file_identifier = frame.peek_file_identifier()?;
                limit_requests.acquire().await.unwrap().forget();
                tokio::spawn(async move {
                    match handle_frame(frame, file_identifier, response_tx).await {
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

#[allow(dead_code)]
pub async fn hello() -> Result<()> {
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", 6789)).await?;
    let server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        limit_requests: Arc::new(Semaphore::new(MAX_REQUESTS)),
    };

    println!("listening");

    loop {
        let permit = server.limit_connections.clone().acquire_owned().await?;
        // .unwrap();
        let socket = server.listener.accept().await?;
        println!("got socket from {}", socket.1);
        let limit_requests = server.limit_requests.clone();
        tokio::spawn(async move {
            match handle_connection(limit_requests, socket.0).await {
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
