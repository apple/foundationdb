use crate::flow::file_identifier::FileIdentifier;
use crate::flow::uid::UID;
use crate::flow::{Frame, Result};
use std::clone::Clone;
use std::future::Future;
use std::net::SocketAddr;

pub mod network_test;
pub mod ping_request;

pub type FlowResponse = Option<FlowMessage>;
// XXX get rid of pin?
pub type FlowFuture =
    std::pin::Pin<Box<dyn 'static + Send + Future<Output = Result<FlowResponse>>>>;
pub type FlowFn = dyn Send + Sync + Fn(FlowMessage) -> FlowFuture;

#[derive(Debug)]
pub struct FlowMessage {
    peer: SocketAddr,
    completion: Option<UID>,
    frame: Frame,
}

impl FlowMessage {
    pub fn new(peer: SocketAddr, completion: Option<UID>, frame: Frame) -> Result<Self> {
        frame.peek_file_identifier()?;
        Ok(Self {
            peer,
            completion,
            frame,
        })
    }
    pub fn file_identifier(&self) -> FileIdentifier {
        self.frame.peek_file_identifier().unwrap()
    }
    pub fn frame(self) -> Frame {
        self.frame
    }
    pub fn completion(&self) -> Option<UID> {
        self.completion.clone()
    }
    pub fn validate(&self) -> Result<()> {
        self.frame.validate()
    }
    pub fn token(&self) -> UID {
        self.frame.token.clone()
    }
}
