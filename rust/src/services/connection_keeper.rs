use crate::flow::{FlowFuture, FlowMessage, Peer, Result};
use crate::services::{ConnectionHandler, LoopbackHandler, RequestRouter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;

pub struct ConnectionKeeper {
    public_addr: Option<SocketAddr>,
    request_router: Arc<RequestRouter>,
}

impl ConnectionKeeper {
    pub fn new(
        public_addr: Option<SocketAddr>,
        loopback_handler: Arc<LoopbackHandler>,
    ) -> Arc<ConnectionKeeper> {
        Arc::new(ConnectionKeeper {
            public_addr,
            request_router: RequestRouter::new(loopback_handler),
        })
    }
    pub async fn listen(&self, listen_addr: Option<SocketAddr>) -> Result<()> {
        match listen_addr.or_else(|| self.public_addr) {
            Some(listen_addr) => {
                let mut rx =
                    ConnectionHandler::new_listener(listen_addr, self.request_router.clone())
                        .await?;
                println!("Listening.");
                while let Some(connection_handler) = rx.recv().await {
                    println!("New connection: {:?}", connection_handler.peer);
                    self.request_router
                        .remote_endpoints
                        .insert(connection_handler.peer, connection_handler);
                }
                Ok(())
            }
            None => Err("No address to listen on!".into()),
        }
    }
    async fn ensure_endpoint(
        request_router: &Arc<RequestRouter>,
        public_addr: Option<SocketAddr>,
        saddr: &SocketAddr,
    ) -> Result<()> {
        if !request_router.remote_endpoints.contains_key(saddr) {
            let svc = ConnectionHandler::new_outgoing_connection(
                public_addr,
                *saddr,
                request_router.clone(),
            )
            .await?;
            request_router.remote_endpoints.insert(*saddr, svc);
            // todo: remove on connection error.
        }
        Ok(())
    }
    pub async fn rpc(&self, req: FlowMessage) -> Result<FlowMessage> {
        match &req.flow.dst {
            Peer::Remote(saddr) => {
                Self::ensure_endpoint(&self.request_router, self.public_addr, saddr).await?
            }
            _ => (),
        };
        self.request_router.rpc(req).await
    }
}

impl Service<FlowMessage> for ConnectionKeeper {
    type Response = <&'static RequestRouter as Service<FlowMessage>>::Response;
    type Error = <&'static RequestRouter as Service<FlowMessage>>::Error;
    type Future = FlowFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        (self.request_router.as_ref()).poll_ready(cx)
    }

    fn call(&mut self, req: FlowMessage) -> Self::Future {
        let request_router = self.request_router.clone();
        let public_addr = self.public_addr;
        // TODO: Remove box + pin
        Box::pin(async move {
            let dst = &req.flow.dst;
            match dst {
                Peer::Local(_) => {}
                Peer::Remote(saddr) => {
                    Self::ensure_endpoint(&request_router, public_addr, saddr).await?;
                }
            };
            request_router.as_ref().call(req).await
        })
    }
}
