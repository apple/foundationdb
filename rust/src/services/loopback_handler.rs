use crate::flow::{
    file_identifier::FileIdentifierNames, uid::UID, uid::WLTOKEN, Error, FlowFn, FlowFuture,
    FlowHandler, FlowMessage, FlowResponse, Result,
};

use tokio::sync::oneshot;
use tower::Service;

use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[pin_project]
pub struct LoopbackFuture {
    // TODO: Make this an Option<FlowFuture> to avoid allocations on sync paths.
    #[pin]
    inner: FlowFuture,
}

impl Future for LoopbackFuture {
    type Output = Result<FlowResponse>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

pub struct LoopbackHandler {
    pub fit: FileIdentifierNames,
    // TODO: Handle to top-level FlowMessage router, so that this service can send messages to other endpoints.
    // Should this be a Service<FlowMessage> instead?
    // pub out_tx: mpsc::Sender<FlowMessage>,
    pub in_flight_requests: dashmap::DashMap<UID, oneshot::Sender<FlowMessage>>,
    // pub well_known_endpoints: dashmap::DashMap<WLTOKEN, Box<FlowFn>>,
    pub well_known_endpoints: dashmap::DashMap<WLTOKEN, Box<dyn FlowHandler>>,
}

impl LoopbackHandler {
    pub fn new(/*out_tx: mpsc::Sender<FlowMessage>*/) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            fit: FileIdentifierNames::new().unwrap(),
            in_flight_requests: dashmap::DashMap::new(),
            well_known_endpoints: dashmap::DashMap::new(),
        }))
    }

    pub fn register_well_known_endpoint(&self, wltoken: WLTOKEN, endpoint: Box<dyn FlowHandler>)
    // where
    // F: 'static + Send + Sync + Fn(FlowMessage) -> FlowFuture,
    {
        self.well_known_endpoints.insert(wltoken, endpoint);
    }

    fn handle_req<'a>(&'a self, request: FlowMessage) -> Result<Option<FlowFuture>> {
        match request.token().get_well_known_endpoint() {
            Some(wltoken) => match self.well_known_endpoints.get(&wltoken) {
                // Some(well_known_endpoint) => Ok(Some((**well_known_endpoint)(request))),
                Some(well_known_endpoint) => Ok(Some(well_known_endpoint.handle(request))),
                None => {
                    Err(format!("Unhandled request for well-known endpoint {:?}", wltoken,).into())
                }
            },
            None => match self.in_flight_requests.remove(&request.token()) {
                Some((_uid, tx)) => match tx.send(request) {
                    Ok(()) => Ok(None),
                    Err(message) => Err(format!(
                        "Dropping duplicate response for token {:?}",
                        message.frame.token
                    )
                    .into()),
                },
                None => {
                    let file_identifier = request.file_identifier();
                    let frame = request.frame;
                    Err(format!("Message not destined for well-known endpoint and not a known response: {:?} {:04x?} {:04x?}",
                        frame.token,
                        file_identifier,
                        self.fit.from_id(&file_identifier)).into())
                }
            },
        }
    }
}

impl Service<FlowMessage> for &LoopbackHandler {
    type Response = Option<FlowMessage>;
    type Error = Error;
    type Future = LoopbackFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: FlowMessage) -> LoopbackFuture {
        LoopbackFuture {
            inner: match self.handle_req(req) {
                Ok(Some(inner)) => inner,
                Ok(None) => Box::pin(async move { Ok(None) }),
                Err(e) => Box::pin(async move { Err(e) }),
            },
        }
    }
}
