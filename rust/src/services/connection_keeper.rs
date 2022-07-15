use crate::endpoints::ping_request;
use crate::flow::{FlowFuture, FlowMessage, Peer, Result};
use crate::services::{ConnectionHandler, LoopbackHandler, RequestRouter};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;

pub struct ConnectionKeeper {
    pub public_addr: Option<SocketAddr>,
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
    fn add_connection(self: &Arc<Self>, connection_handler: Arc<ConnectionHandler>) {
        self.request_router
            .remote_endpoints
            .insert(connection_handler.peer, connection_handler.clone());
        println!("Registered connection to {:?}", connection_handler.peer);
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match ping_request::ping(connection_handler.peer, &this).await {
                    Ok(_) => {}
                    Err(err) => {
                        println!(
                            "Ping failed; closing connection {:?}: {:?}",
                            connection_handler.peer, err
                        );
                        this.request_router
                            .remote_endpoints
                            .remove(&connection_handler.peer);
                        // TODO: Close file handle
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        });
    }
    pub async fn listen(self: &Arc<Self>, listen_addr: Option<SocketAddr>) -> Result<()> {
        match listen_addr.or_else(|| self.public_addr) {
            Some(listen_addr) => {
                let mut rx =
                    ConnectionHandler::new_listener(listen_addr, self.request_router.clone())
                        .await?;
                println!("Listening.");
                while let Some(connection_handler) = rx.recv().await {
                    println!("New connection: {:?}", connection_handler.peer);
                    self.add_connection(connection_handler);
                    // self.request_router
                    //     .remote_endpoints
                    //     .insert(connection_handler.peer, connection_handler);
                }
                Ok(())
            }
            None => Err("No address to listen on!".into()),
        }
    }
    async fn ensure_endpoint(
        self: &Arc<Self>,
        public_addr: Option<SocketAddr>,
        saddr: &SocketAddr,
    ) -> Result<()> {
        if !self.request_router.remote_endpoints.contains_key(saddr) {
            let svc = ConnectionHandler::new_outgoing_connection(
                public_addr,
                *saddr,
                self.request_router.clone(),
            )
            .await?;
            self.add_connection(svc);
        }
        Ok(())
    }
    pub fn loopback_handler<'a>(self: &'a Arc<Self>) -> &'a Arc<LoopbackHandler> {
        &self.request_router.local_endpoint
    }
    pub async fn rpc(self: &Arc<Self>, req: FlowMessage) -> Result<FlowMessage> {
        match &req.flow.dst {
            Peer::Remote(saddr) => self.ensure_endpoint(self.public_addr, saddr).await?,
            _ => (),
        };
        self.request_router.rpc(req).await
    }
}

impl Service<FlowMessage> for Arc<ConnectionKeeper> {
    type Response = <&'static RequestRouter as Service<FlowMessage>>::Response;
    type Error = <&'static RequestRouter as Service<FlowMessage>>::Error;
    type Future = FlowFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        (self.request_router.as_ref()).poll_ready(cx)
    }

    fn call(&mut self, req: FlowMessage) -> Self::Future {
        let this = self.clone();
        let public_addr = self.public_addr;
        // TODO: Remove box + pin
        Box::pin(async move {
            let dst = &req.flow.dst;
            match dst {
                Peer::Local(_) => {}
                Peer::Remote(saddr) => {
                    this.ensure_endpoint(public_addr, saddr).await?;
                }
            };
            this.request_router.as_ref().call(req).await
        })
    }
}
