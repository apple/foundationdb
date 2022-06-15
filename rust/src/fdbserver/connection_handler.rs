use crate::flow::{connection, file_identifier::FileIdentifierNames, uid::UID, Error, Result};
use crate::services::{network_test, ping_request, route_well_known_request, FlowMessage};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tower::Service;

use std::future::Future;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::task::{Context, Poll};

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

async fn receiver<C>(
    connection_handler: Arc<ConnectionHandler>,
    mut reader: connection::ConnectionReader<C>,
    permit: OwnedSemaphorePermit,
) -> Result<()>
where
    C: 'static + AsyncRead + Unpin + Send,
{
    let mut svc =
        tower::limit::concurrency::ConcurrencyLimit::new(connection_handler.deref(), MAX_REQUESTS);
    while let Some(frame) = reader.read_frame().await? {
        let request = FlowMessage::new(connection_handler.peer, None,  frame)?;
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
    println!("clean shutdown!");
    drop(permit);
    Ok(())
}

fn spawn_receiver<C>(
    connection_handler: Arc<ConnectionHandler>,
    reader: connection::ConnectionReader<C>,
    permit: OwnedSemaphorePermit,
) where
    C: 'static + AsyncRead + Unpin + Send,
{
    tokio::spawn(receiver(connection_handler, reader, permit));
}
pub async fn connection_handler(
    svc: Arc<ConnectionHandler>,
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
                let request = FlowMessage::new(svc.get_ref().peer, None, frame)?;
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

#[derive(Debug)]
pub struct ConnectionHandler {
    pub peer: SocketAddr,
    pub fit: FileIdentifierNames,
    pub response_tx: mpsc::Sender<FlowMessage>,
    pub in_flight_requests: dashmap::DashMap<UID, oneshot::Sender<FlowMessage>>,
}

impl ConnectionHandler {
    async fn new(
        socket: (TcpStream, SocketAddr),
        permit: OwnedSemaphorePermit,
    ) -> Result<Arc<Self>> {
        let (stream, peer) = socket;
        // Bound the number of backlogged messages to a given remote endpoint.  This is
        // set to the process-wide MAX_REQUESTS / 10 so that a few backpressuring receivers
        // can't consume all the request slots for this process.
        // TODO: Add flow knobs or something so that multiple packages can share configuration values.
        let (response_tx, response_rx) = tokio::sync::mpsc::channel::<FlowMessage>(32);
        let connection_handler = ConnectionHandler {
            peer,
            fit: FileIdentifierNames::new().unwrap(),
            response_tx,
            in_flight_requests: dashmap::DashMap::new(),
        };
        let connection_handler = Arc::new(connection_handler);
        let (reader, writer, connect_packet) = connection::new(stream).await?;
        // TODO: Check protocol compatibility, create object w/ enough info to allow request routing
        println!("{} {:x?}", peer, connect_packet);

        spawn_sender(response_rx, writer);
        spawn_receiver(connection_handler.clone(), reader, permit);
        Ok(connection_handler)
    }

    pub async fn new_outgoing_connection(saddr: SocketAddr) -> Result<Arc<ConnectionHandler>> {
        let conn = TcpStream::connect(saddr).await?;
        let limit_connections = Arc::new(Semaphore::new(1));
        let permit = limit_connections.clone().acquire_owned().await?;
        ConnectionHandler::new((conn, saddr), permit).await
    }

    async fn listener(
        bind: TcpListener,
        limit_connections: Arc<Semaphore>,
        tx: mpsc::Sender<Arc<ConnectionHandler>>,
    ) -> Result<()> {
        loop {
            let permit = limit_connections.clone().acquire_owned().await?;
            let socket = bind.accept().await?;
            tx.send(ConnectionHandler::new(socket, permit).await?)
                .await?; // Send will return error if the Receiver has been close()'ed.
        }
    }

    pub async fn new_listener(addr: &str) -> Result<mpsc::Receiver<Arc<ConnectionHandler>>> {
        let bind = TcpListener::bind(addr).await?;
        let limit_connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(Self::listener(bind, limit_connections, tx));
        Ok(rx)
    }

    pub async fn rpc(&self, req : FlowMessage) -> Result<FlowMessage> {
        match req.completion() {
            Some(uid) => {
                let (tx, rx) = oneshot::channel();
                self.in_flight_requests.insert(uid, tx);
                self.response_tx.send(req).await?;
                println!("waiting for response!");
                Ok(rx.await?)
            },
            None => Err("attempt to dispatch RPC without a completion token".into()),
        }
    }

    pub async fn ping(&self) -> Result<()> {
        let req = ping_request::serialize_request(self.peer)?;
        let response_frame = self.rpc(req).await?;
        ping_request::deserialize_response(response_frame.frame())
    }

    fn dispatch_response(&self, tx: oneshot::Sender<FlowMessage>, request: FlowMessage) {
        match tx.send(request) {
            Ok(()) => (),
            Err(message) => {
                println!(
                    "Dropping duplicate response for token {:?}",
                    message.frame().token
                )
            }
        }
    }

    fn dbg_unkown_msg(&self, request: FlowMessage) {
        let file_identifier = request.file_identifier();
        let frame = request.frame();
        println!(
            "Message not destined for well-known endpoint and not a known respsonse: {:x?}",
            frame
        );

        println!(
            "{:?} {:04x?} {:04x?}",
            frame.token,
            file_identifier,
            self.fit.from_id(&file_identifier)
        );
    }

    fn handle_req<'a, 'b>(
        &self, // in_flight_requests: &dashmap::DashMap<UID, oneshot::Sender<Frame>>,
        request: FlowMessage,
    ) -> Result<
        Option<
            std::pin::Pin<
                Box<dyn Send + futures_util::Future<Output = Result<Option<FlowMessage>>>>,
            >,
        >,
    > {
        request.validate()?;
        let wltoken = request.token().get_well_known_endpoint();
        Ok(match wltoken {
            None => match self.in_flight_requests.remove(&request.token()) {
                Some((_uid, tx)) => {
                    self.dispatch_response(tx, request);
                    None
                }
                None => {
                    self.dbg_unkown_msg(request);
                    None
                }
            },
            Some(wltoken) => Some(Box::pin(route_well_known_request(self.peer, wltoken, request))),
        })
    }
}

impl Service<FlowMessage> for &ConnectionHandler {
    type Response = Option<FlowMessage>;
    type Error = Error;
    type Future = std::pin::Pin<Box<dyn 'static + Send + Future<Output = Result<Self::Response>>>>; // XXX get rid of box!

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: FlowMessage) -> Self::Future {
        match self.handle_req(req) {
            Ok(Some(fut)) => fut,
            Ok(None) => Box::pin(async move { Ok(None) }),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}
