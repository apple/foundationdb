use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::Result;
use crate::flow::{file_identifier::FileIdentifier, Frame};
use std::clone::Clone;
use std::net::SocketAddr;

pub mod network_test;
pub mod ping_request;

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

pub async fn route_well_known_request(
    wltoken: WLTOKEN,
    request: FlowMessage,
) -> Result<Option<FlowMessage>> {
    match wltoken {
        WLTOKEN::PingPacket => ping_request::handle(request).await,
        WLTOKEN::ReservedForTesting => network_test::handle(request).await,
        wltoken => Err(format!(
            "Got unhandled request for well-known endpoint {:?}",
            wltoken,
        )
        .into()),
    }
}
