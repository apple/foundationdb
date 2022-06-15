use crate::flow::{file_identifier, Frame};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::Result;

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tower::Service;

pub mod network_test;
pub mod ping_request;

pub struct FlowRequest {
    pub frame: Frame,
    pub file_identifier: file_identifier::FileIdentifier,
}

pub struct FlowResponse {
    pub frame: Frame,
}

#[derive(Clone, Debug)]
pub struct Svc {
    pub fit: Arc<file_identifier::FileIdentifierNames>,
    pub response_tx: mpsc::Sender<Frame>,
    pub in_flight_requests: Arc<dashmap::DashMap<UID, oneshot::Sender<Frame>>>,
}

impl Svc {
    pub fn new() -> (Self, mpsc::Receiver<Frame>) {
        // Bound the number of backlogged messages to a given remote endpoint.  This is
        // set to the process-wide MAX_REQUESTS / 10 so that a few backpressuring receivers
        // can't consume all the request slots for this process.
        // TODO: Add flow knobs or something so that multiple packages can share configuration values.
        let (response_tx, response_rx) = tokio::sync::mpsc::channel::<Frame>(32);
        (
            Svc {
                fit: Arc::new(file_identifier::FileIdentifierNames::new().unwrap()),
                response_tx,
                in_flight_requests: Arc::new(dashmap::DashMap::new()),
            },
            response_rx,
        )
    }

    pub async fn ping(&self) -> Result<()> {
        let uid = UID::random_token();
        let frame = ping_request::serialize_request(&uid)?;
        let (tx, rx) = oneshot::channel();
        self.in_flight_requests.insert(uid, tx);
    
        self.response_tx.send(frame).await?;
    
        println!("waiting for response!");
        let response_frame = rx.await?;
        println!("ping reponse frame: {:?}", response_frame);
        ping_request::deserialize_response(response_frame)?;
        Ok(())
    }

    fn dispatch_response(&self, tx: oneshot::Sender<Frame>, request: FlowRequest) {
        match tx.send(request.frame) {
            Ok(()) => (),
            Err(frame) => {
                println!("Dropping duplicate response for token {:?}", frame.token)
            }
        }
    }

    fn dbg_unkown_msg(&self, request: FlowRequest) {
        println!(
            "Message not destined for well-known endpoint and not a known respsonse: {:x?}",
            request.frame
        );

        println!(
            "{:x?} {:04x?} {:04x?}",
            request.frame.token,
            request.file_identifier,
            self.fit.from_id(&request.file_identifier)
        );
    }

    fn handle_req<'a, 'b>(
        &self, // in_flight_requests: &dashmap::DashMap<UID, oneshot::Sender<Frame>>,
        request: FlowRequest,
    ) -> Result<
        Option<std::pin::Pin<Box<dyn Send + futures_util::Future<Output = Result<Option<FlowResponse>>>>>>,
    > {
        request.frame.validate()?;
        let wltoken = request.frame.token.get_well_known_endpoint();
        Ok(match wltoken {
            None => match self.in_flight_requests.remove(&request.frame.token) {
                Some((_uid, tx)) => {
                    self.dispatch_response(tx, request);
                    None
                },
                None => {
                    self.dbg_unkown_msg(request);
                    None
                }
            }
            Some(WLTOKEN::PingPacket) => Some(Box::pin(ping_request::handle(request))),
            Some(WLTOKEN::ReservedForTesting) => Some(Box::pin(network_test::handle(request))),
            Some(wltoken) => {
                let fit = file_identifier::FileIdentifierNames::new()?;
                println!(
                    "Got unhandled request for well-known enpoint {:?}: {:x?} {:04x?} {:04x?}",
                    wltoken,
                    request.frame.token,
                    &request.file_identifier,
                    fit.from_id(&request.file_identifier)
                );
                None
            }
        })
    }
}


impl Service<FlowRequest> for Svc {
    type Response = Option<FlowResponse>;
    type Error = super::flow::Error;
    type Future = std::pin::Pin<Box<dyn 'static + Send + Future<Output = Result<Self::Response>>>>; // XXX get rid of box!

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: FlowRequest) -> Self::Future {
        match self.handle_req(req) {
            Ok(Some(fut)) => fut,
            Ok(None) => Box::pin(async move { Ok(None) }),
            Err(e) => Box::pin(async move { Err(e) }),
        }
    }
}
