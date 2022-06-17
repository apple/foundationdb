use crate::flow::{
    connection, file_identifier::FileIdentifierNames, Error, FlowFuture, FlowMessage, Result,
};
use crate::services::LoopbackHandler;
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

/// Takes FlowMessages from multiple threads and writes them to a ConnectionWriter in a single-threaded way
async fn sender<C: 'static + AsyncWrite + Unpin + Send>(
    mut response_rx: tokio::sync::mpsc::Receiver<FlowMessage>,
    mut writer: connection::ConnectionWriter<C>,
) -> Result<()> {
    while let Some(message) = response_rx.recv().await {
        writer.write_frame(message.frame).await?;
        loop {
            match response_rx.try_recv() {
                Ok(message) => {
                    writer.write_frame(message.frame).await.unwrap();
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

/// Takes FlowMessages from a single-threaded connection reader, and runs them in parallel by spawning concurrent tasks.
async fn receiver<C>(
    svc: Arc<ConnectionHandler>,
    mut reader: connection::ConnectionReader<C>,
) -> Result<()>
where
    C: 'static + AsyncRead + Unpin + Send,
{
    let mut svc = tower::limit::concurrency::ConcurrencyLimit::new(svc.deref(), MAX_REQUESTS);
    while let Some(frame) = reader.read_frame().await? {
        let request = FlowMessage::new_remote(svc.get_ref().peer, None, frame)?;
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
    Ok(())
}

fn spawn_receiver<C>(
    connection_handler: Arc<ConnectionHandler>,
    reader: connection::ConnectionReader<C>,
    permit: OwnedSemaphorePermit,
) where
    C: 'static + AsyncRead + Unpin + Send,
{
    tokio::spawn(async move {
        match receiver(connection_handler, reader).await {
            Ok(_) => {
                println!("clean shutdown!");
            }
            Err(e) => {
                println!("Unexpected error from receiver! {:?}", e)
            }
        }
        drop(permit);
    });
}

fn spawn_sender<C>(
    response_rx: mpsc::Receiver<FlowMessage>,
    writer: connection::ConnectionWriter<C>,
) where
    C: 'static + AsyncWrite + Unpin + Send,
{
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

pub struct ConnectionHandler {
    pub peer: SocketAddr,
    pub fit: FileIdentifierNames,
    pub response_tx: mpsc::Sender<FlowMessage>,
    pub loopback_handler: Arc<LoopbackHandler>,
}

impl std::fmt::Debug for ConnectionHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("ConnectionHandler")
            .field("peer", &self.peer)
            .finish()
    }
}

impl ConnectionHandler {
    async fn new(
        socket: (TcpStream, SocketAddr),
        permit: OwnedSemaphorePermit,
        loopback_handler: Arc<LoopbackHandler>,
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
            loopback_handler,
        };
        let (reader, writer, connect_packet) = connection::new(stream).await?;
        // TODO: Check protocol compatibility, create object w/ enough info to allow request routing
        println!("{} {:x?}", peer, connect_packet);
        let connection_handler = Arc::new(connection_handler);
        spawn_sender(response_rx, writer);
        spawn_receiver(connection_handler.clone(), reader, permit);
        Ok(connection_handler)
    }

    pub async fn new_outgoing_connection(
        saddr: SocketAddr,
        loopback_handler: Arc<LoopbackHandler>,
    ) -> Result<Arc<ConnectionHandler>> {
        let conn = TcpStream::connect(saddr).await?;
        let limit_connections = Arc::new(Semaphore::new(1));
        let permit = limit_connections.clone().acquire_owned().await?;
        ConnectionHandler::new((conn, saddr), permit, loopback_handler).await
    }

    async fn listener(
        bind: TcpListener,
        limit_connections: Arc<Semaphore>,
        tx: mpsc::Sender<Arc<ConnectionHandler>>,
        loopback_handler: Arc<LoopbackHandler>,
    ) -> Result<()> {
        loop {
            let permit = limit_connections.clone().acquire_owned().await?;
            let socket = bind.accept().await?;
            tx.send(ConnectionHandler::new(socket, permit, loopback_handler.clone()).await?)
                .await?; // Send will return error if the Receiver has been close()'ed.
        }
    }

    pub async fn new_listener(
        addr: &str,
        loopback_handler: Arc<LoopbackHandler>,
    ) -> Result<mpsc::Receiver<Arc<ConnectionHandler>>> {
        let bind = TcpListener::bind(addr).await?;
        let limit_connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(Self::listener(
            bind,
            limit_connections,
            tx,
            loopback_handler,
        ));
        Ok(rx)
    }

    pub async fn rpc(&self, req: FlowMessage) -> Result<FlowMessage> {
        match req.completion() {
            Some(uid) => {
                let (tx, rx) = oneshot::channel();
                self.loopback_handler.in_flight_requests.insert(uid, tx);
                self.response_tx.send(req).await?;
                Ok(rx.await?)
            }
            None => Err("attempt to dispatch RPC without a completion token".into()),
        }
    }

    async fn handle(
        handler: Arc<LoopbackHandler>,
        request: FlowMessage,
    ) -> Result<Option<FlowMessage>> {
        futures_util::future::poll_fn(|cx| handler.as_ref().poll_ready(cx)).await?;
        handler.as_ref().call(request).await
    }
    fn handle_req(&self, request: FlowMessage) -> Result<Option<FlowFuture>> {
        request.validate()?;
        // This futures / async pattern will allow the router service to route requests to appropriate services.
        Ok(Some(Box::pin(Self::handle(
            self.loopback_handler.clone(),
            request,
        ))))
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
