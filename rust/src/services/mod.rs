use crate::flow::{file_identifier::FileIdentifier, file_identifier::FileIdentifierNames, Frame};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::Result;

use std::future::Future;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};
use tower::Service;

pub mod network_test;
pub mod ping_request;

#[derive(Debug)]
pub struct FlowMessage {
    frame: Frame,
}

impl FlowMessage {
    pub fn new(frame: Frame) -> Result<Self> {
        frame.peek_file_identifier()?;
        Ok(Self{ frame })
    }
    pub fn file_identifier(&self) -> FileIdentifier {
        self.frame.peek_file_identifier().unwrap()
    }
    pub fn frame(self) -> Frame {
        self.frame
    }
}

#[derive(Debug)]
pub struct Svc {
    pub fit: FileIdentifierNames,
    pub response_tx: mpsc::Sender<FlowMessage>,
    pub in_flight_requests: dashmap::DashMap<UID, oneshot::Sender<FlowMessage>>,
}

impl Svc {
    pub fn new() -> (Self, mpsc::Receiver<FlowMessage>) {
        // Bound the number of backlogged messages to a given remote endpoint.  This is
        // set to the process-wide MAX_REQUESTS / 10 so that a few backpressuring receivers
        // can't consume all the request slots for this process.
        // TODO: Add flow knobs or something so that multiple packages can share configuration values.
        let (response_tx, response_rx) = tokio::sync::mpsc::channel::<FlowMessage>(32);
        (
            Svc {
                fit: FileIdentifierNames::new().unwrap(),
                response_tx,
                in_flight_requests: dashmap::DashMap::new(),
            },
            response_rx,
        )
    }

    pub async fn ping(&self) -> Result<()> {
        let uid = UID::random_token();
        let frame = ping_request::serialize_request(&uid)?;
        let (tx, rx) = oneshot::channel();
        self.in_flight_requests.insert(uid, tx);
    
        self.response_tx.send(FlowMessage::new(frame)?).await?;
    
        println!("waiting for response!");
        let response_frame = rx.await?;
        println!("ping reponse frame: {:?}", response_frame);
        ping_request::deserialize_response(response_frame.frame())?;
        Ok(())
    }

    fn dispatch_response(&self, tx: oneshot::Sender<FlowMessage>, request: FlowMessage) {
        match tx.send(request) {
            Ok(()) => (),
            Err(message) => {
                println!("Dropping duplicate response for token {:?}", message.frame().token)
            }
        }
    }

    fn dbg_unkown_msg(&self, request: FlowMessage) {
        println!(
            "Message not destined for well-known endpoint and not a known respsonse: {:x?}",
            request.frame
        );

        let file_identifier = request.file_identifier();
        println!("{:?} {:04x?} {:04x?}", request.frame.token, file_identifier, self.fit.from_id(&file_identifier));
    }

    fn handle_req<'a, 'b>(
        &self, // in_flight_requests: &dashmap::DashMap<UID, oneshot::Sender<Frame>>,
        request: FlowMessage,
    ) -> Result<
        Option<std::pin::Pin<Box<dyn Send + futures_util::Future<Output = Result<Option<FlowMessage>>>>>>,
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
                println!(
                    "Got unhandled request for well-known enpoint {:?}",
                    wltoken,
                );
                self.dbg_unkown_msg(request);
                None
            }
        })
    }
}


impl Service<FlowMessage> for &Svc {
    type Response = Option<FlowMessage>;
    type Error = super::flow::Error;
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
