use crate::flow::{Error, FlowFuture, FlowMessage, Peer, Result};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;
use tower::Service;

// TODO: These should be generic parameters.
use super::ConnectionHandler;
use super::LoopbackHandler;

pub struct RequestRouter {
    pub remote_endpoints: DashMap<SocketAddr, Arc<ConnectionHandler>>,
    pub local_endpoint: Arc<LoopbackHandler>,
}

impl RequestRouter {
    pub fn new(local_endpoint: Arc<LoopbackHandler>) -> Arc<RequestRouter> {
        Arc::new(RequestRouter {
            local_endpoint,
            remote_endpoints: DashMap::new(),
        })
    }
    async fn handle_local(
        handler: Arc<LoopbackHandler>,
        request: FlowMessage,
    ) -> Result<Option<FlowMessage>> {
        futures_util::future::poll_fn(|cx| handler.as_ref().poll_ready(cx)).await?;
        handler.as_ref().call(request).await
    }
    async fn handle_remote(
        handler: Arc<ConnectionHandler>,
        request: FlowMessage,
    ) -> Result<Option<FlowMessage>> {
        futures_util::future::poll_fn(|cx| handler.as_ref().poll_ready(cx)).await?;
        handler.as_ref().call(request).await
    }
    fn handle_req(&self, request: FlowMessage) -> Result<Option<FlowFuture>> {
        request.validate()?;
        match &request.flow.dst {
            Peer::Local(_) => {
                // TODO: check completion?
                Ok(Some(Box::pin(Self::handle_local(
                    self.local_endpoint.clone(),
                    request,
                ))))
            }
            Peer::Remote(addr) => {
                let remote = match self.remote_endpoints.get(addr) {
                    Some(remote) => remote.clone(),
                    None => {
                        return Err(format!("Could not send to unknown endpoint: {}", addr).into())
                    }
                };
                Ok(Some(Box::pin(Self::handle_remote(remote, request))))
            }
        }
    }
    // pub async fn is_routable(&self, req: FlowMessage) -> bool {
    //     match &req.flow.dst {
    //         Peer::Local(_) => true,
    //         Peer::Remote(socket_addr) => {
    //             self.remote_endpoints.contains_key(socket_addr)
    //         }
    //     }
    // }
    pub async fn rpc(&self, req: FlowMessage) -> Result<FlowMessage> {
        match &req.flow.src {
            Peer::Local(Some(uid)) => {
                let (tx, rx) = oneshot::channel();
                self.local_endpoint
                    .in_flight_requests
                    .insert(uid.clone(), tx);
                let res = match self.handle_req(req)? {
                    Some(fut) => fut.await?,
                    None => None,
                };
                Ok(rx.await?)
            }
            src => Err(format!(
                "attempt to dispatch RPC without a completion token. src: {:?}",
                src
            )
            .into()),
        }
    }
}

impl Service<FlowMessage> for &RequestRouter {
    type Response = Option<FlowMessage>;
    type Error = Error;
    type Future = FlowFuture;

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
