use crate::flow::{FlowMessage, FlowFuture, Error, Peer, Result};
use crate::services::{ConnectionHandler, LoopbackHandler, RequestRouter};
use tower::Service;
use std::task::{Context, Poll};
use std::net::SocketAddr;
use pin_project::pin_project;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Reconnect<S> {
    inner: S,
}

#[pin_project]
pub struct ConnectionKeeperFuture<F> {
    #[pin]
    inner: F,
    #[pin]
    reconnect: Option<tokio::time::Sleep>, // TODO
}

pub struct ConnectionKeeper {
    listen_addr: Option<SocketAddr>,
    request_router: Arc<RequestRouter>,
}

impl ConnectionKeeper {
    pub fn new(listen_addr: Option<SocketAddr>, loopback_handler: Arc<LoopbackHandler>) -> ConnectionKeeper {
        ConnectionKeeper {
            listen_addr,
            request_router: RequestRouter::new(loopback_handler)
        }
    }
    async fn ensure_endpoint(request_router: &Arc<RequestRouter>, listen_addr: Option<SocketAddr>, saddr: &SocketAddr) -> Result<()> {
        if !request_router.remote_endpoints.contains_key(saddr) {
            let svc = ConnectionHandler::new_outgoing_connection(listen_addr, *saddr, request_router.clone()).await?;
            request_router.remote_endpoints.insert(*saddr, svc);
            // todo: remove on connection error.
        }
        Ok(())
    }
    pub async fn rpc(&self, req: FlowMessage) -> Result<FlowMessage> {
        match &req.flow.dst {
            Peer::Remote(saddr) => Self::ensure_endpoint(&self.request_router, self.listen_addr, saddr).await?,
            _ => (),
        };
        self.request_router.rpc(req).await
    }
}

impl <F, Response> futures_util::Future for ConnectionKeeperFuture<F> 
    where F: futures_util::Future<Output = Result<Response>>{
    type Output = F::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context::<'_>) -> std::task::Poll<<Self as futures_util::Future>::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(Err(e)) => {
                println!("need to reconnect!");
                Poll::Ready(Err(e))
            }
            poll => poll
        }
    }
}

impl Service<FlowMessage> for ConnectionKeeper {
    type Response = <&'static RequestRouter as Service<FlowMessage>>::Response;
    type Error = <&'static RequestRouter as Service<FlowMessage>>::Error;
    type Future = FlowFuture;
    // type Future = ConnectionKeeperFuture<<&'static RequestRouter as Service<FlowMessage>>::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        (self.request_router.as_ref()).poll_ready(cx)
    }

    fn call(&mut self, req: FlowMessage) -> Self::Future {
        let request_router = self.request_router.clone();
        let listen_addr = self.listen_addr;
        Box::pin(async move {
            let dst = &req.flow.dst;
            match dst {
                Peer::Local(_) => {},
                Peer::Remote(saddr) => {
                    Self::ensure_endpoint(&request_router, listen_addr, saddr).await?;
                }
            };
            request_router.as_ref().call(req).await
        })
        // ConnectionKeeperFuture {
        //     inner: (&self.request_router).call(req),
        //     reconnect: None,
        // }
    }
}
