use dashmap::DashMap;
use tower::Service;
use crate::flow::{Result, Error, FlowMessage, FlowFuture, Peer};
use std::net::SocketAddr;
use std::task::{Context, Poll};
use std::sync::Arc;

// TODO: These should be generic parameters.
use super::LoopbackHandler;
use super::ConnectionHandler;

pub struct RequestRouter {
    pub remote_endpoints: DashMap<SocketAddr, Arc<ConnectionHandler>>,
    pub local_endpoint: Arc<LoopbackHandler>,
}

impl RequestRouter
{
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
        match request.peer {
            Peer::Local(_) => {
                // TODO: check completion?
                Ok(Some(Box::pin(Self::handle_local(
                    self.local_endpoint.clone(),
                    request,
                ))))
            },
            Peer::Remote(addr, _) => { 
                let remote = self.remote_endpoints.get(&addr).unwrap().clone();
                Ok(Some(Box::pin(Self::handle_remote(
                    remote,
                    request,
                ))))
            }
        }
    }

}

impl Service<FlowMessage> for &RequestRouter
{
    type Response=Option<FlowMessage>;
    type Error=Error;
    type Future=FlowFuture;

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