use crate::flow::{
    file_identifier::FileIdentifierNames, uid::UID, uid::WLTOKEN, Error, FlowFn, FlowFuture,
    FlowMessage, Result,
};

use tokio::sync::oneshot;
use tower::Service;

use std::sync::Arc;
use std::task::{Context, Poll};

pub struct LoopbackHandler {
    pub fit: FileIdentifierNames,
    // TODO: Handle to top-level FlowMessage router, so that this service can send messages to other endpoints.
    // Should this be a Service<FlowMessage> instead?
    // pub out_tx: mpsc::Sender<FlowMessage>,
    pub in_flight_requests: dashmap::DashMap<UID, oneshot::Sender<FlowMessage>>,
    pub well_known_endpoints: dashmap::DashMap<WLTOKEN, Box<FlowFn>>,
}

impl LoopbackHandler {
    pub fn new(/*out_tx: mpsc::Sender<FlowMessage>*/) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            fit: FileIdentifierNames::new().unwrap(),
            in_flight_requests: dashmap::DashMap::new(),
            well_known_endpoints: dashmap::DashMap::new(),
        }))
    }

    pub fn register_well_known_endpoint<F>(&self, wltoken: WLTOKEN, f: F)
    where
        F: 'static + Send + Sync + Fn(FlowMessage) -> FlowFuture,
    {
        self.well_known_endpoints.insert(wltoken, Box::new(f));
    }

    fn handle_req(&self, request: FlowMessage) -> Result<Option<FlowFuture>> {
        request.validate()?;

        match request.token().get_well_known_endpoint() {
            Some(wltoken) => match self.well_known_endpoints.get(&wltoken) {
                Some(well_known_endpoint) => Ok(Some((**well_known_endpoint)(request))),
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
                    ).into()),
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
