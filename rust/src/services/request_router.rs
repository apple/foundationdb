use crate::flow::{Error, FlowMessage, FlowResponse, Peer, Result};
use dashmap::DashMap;
use pin_project::pin_project;
use std::mem::take;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;
use tower::Service;

// TODO: These should be generic parameters.
use super::connection_handler::RemoteFuture;
use super::loopback_handler::LoopbackFuture;
use super::ConnectionHandler;
use super::LoopbackHandler;

pub struct RequestRouter {
    pub remote_endpoints: DashMap<SocketAddr, Arc<ConnectionHandler>>,
    pub local_endpoint: Arc<LoopbackHandler>,
}

// This statically dispatches between one of two types of nested futures,
// depending on whether this request is going over the network or being
// handled locally.
#[pin_project]
pub struct RequestRouterFuture<Request, Response, LocalHandler, RemoteHandler> {
    #[pin]
    handler: Handler<Response, LocalHandler, RemoteHandler>,
    // Option because it will have been consumed once Handler is in a *Fut state.
    request: Option<Request>,
}

// Future state machine state
#[pin_project(project = HandlerProj)]
enum Handler<Response, LocalHandler, RemoteHandler> {
    Result(Result<Response>),
    LocalPoll(Arc<LocalHandler>),
    LocalFut(#[pin] LoopbackFuture),
    RemotePoll(Arc<RemoteHandler>),
    RemoteFut(#[pin] RemoteFuture),
}

// TODO: Remove hardcoding of the generic parameters.
impl std::future::Future
    for RequestRouterFuture<FlowMessage, FlowResponse, LoopbackHandler, ConnectionHandler>
{
    type Output = Result<FlowResponse>;

    // Implements state machine for this future.  The big match statement either returns a Poll value,
    // or it evaluates to the next state to be stored in this.handler.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            // .project() consumes self; as_mut makes a copy of the pin so that we can
            // call set() on original pin and then consume it again the next time around the loop.
            let new_handler_state = match this.handler.as_mut().project() {
                // Synchronous completion paths (no handler to delegate to)
                HandlerProj::Result(Ok(None)) => {
                    return Poll::Ready(Ok(None));
                }
                HandlerProj::Result(Err(e)) => {
                    return Poll::Ready(Err(format!("{:?}", e).into()));
                }
                HandlerProj::Result(Ok(Some(s))) => {
                    return Poll::Ready(Err(format!(
                        "Can't have non-None response on sync path: {:?}",
                        s
                    )
                    .into()));
                }

                // Common case starts here
                HandlerProj::LocalPoll(loopback_handler) => {
                    match loopback_handler.as_ref().poll_ready(cx) {
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                        Poll::Ready(_) => {
                            // update self handler to contain a future
                            let request = take(this.request);
                            let fut = loopback_handler.as_ref().call(request.unwrap());
                            Handler::LocalFut(fut)
                        }
                    }
                }
                HandlerProj::LocalFut(local_future) => {
                    return local_future.poll(cx);
                }

                // Copy-paste of above because, except we're delegating to a different
                // handler with a different future type.
                HandlerProj::RemotePoll(remote_handler) => {
                    match remote_handler.as_ref().poll_ready(cx) {
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                        Poll::Ready(_) => {
                            let request = take(this.request);
                            let fut = remote_handler.as_ref().call(request.unwrap());
                            Handler::RemoteFut(fut)
                        }
                    }
                }
                HandlerProj::RemoteFut(remote_future) => {
                    return remote_future.poll(cx);
                }
            };
            this.handler.set(new_handler_state);
        }
    }
}

impl RequestRouter {
    pub fn new(local_endpoint: Arc<LoopbackHandler>) -> Arc<RequestRouter> {
        Arc::new(RequestRouter {
            local_endpoint,
            remote_endpoints: DashMap::new(),
        })
    }
    fn handle_req(
        &self,
        request: FlowMessage,
    ) -> Result<
        Option<RequestRouterFuture<FlowMessage, FlowResponse, LoopbackHandler, ConnectionHandler>>,
    > {
        match &request.flow.dst {
            Peer::Local(_uid) => Ok(Some(RequestRouterFuture {
                handler: Handler::LocalPoll(self.local_endpoint.clone()),
                request: Some(request),
            })),
            Peer::Remote(addr) => {
                let handler = match self.remote_endpoints.get(addr) {
                    Some(remote) => remote.clone(),
                    None => {
                        return Err(format!("Could not send to unknown endpoint: {}", addr).into())
                    }
                };
                Ok(Some(RequestRouterFuture {
                    handler: Handler::RemotePoll(handler),
                    request: Some(request),
                }))
            }
        }
    }
    pub async fn rpc(&self, req: FlowMessage) -> Result<FlowMessage> {
        match &req.flow.src {
            Peer::Local(Some(uid)) => match &req.flow.dst {
                Peer::Local(_) => match self.handle_req(req)? {
                    Some(fut) => match fut.await? {
                        Some(res) => Ok(res),
                        None => Err("Missing response from local RPC handler!".into()),
                    },
                    None => Err("Missing response from local RPC handler!".into()),
                },
                Peer::Remote(_) => {
                    let (tx, rx) = oneshot::channel();
                    self.local_endpoint
                        .in_flight_requests
                        .insert(uid.clone(), tx);
                    let res = match self.handle_req(req)? {
                        Some(fut) => fut.await?,
                        None => None,
                    };
                    match res {
                        Some(res) => Err(format!(
                            "Unexpected local result generated when sending RPC: {:?}",
                            res
                        )
                        .into()),
                        None => Ok(rx.await?),
                    }
                }
            },
            Peer::Local(None) => Err("attempt to dispatch RPC without a completion token".into()),
            Peer::Remote(src) => Err(format!(
                "attempt to dispatch RPC from another process. src: {:?}",
                src
            )
            .into()),
        }
    }
}

impl Service<FlowMessage> for &RequestRouter {
    type Response = Option<FlowMessage>;
    type Error = Error;
    type Future =
        RequestRouterFuture<FlowMessage, Self::Response, LoopbackHandler, ConnectionHandler>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: FlowMessage) -> Self::Future {
        match self.handle_req(req) {
            Ok(Some(fut)) => fut,
            Ok(None) => RequestRouterFuture {
                handler: Handler::Result(Ok(None)),
                request: None,
            },
            Err(e) => RequestRouterFuture {
                handler: Handler::Result(Err(e)),
                request: None,
            },
        }
    }
}
