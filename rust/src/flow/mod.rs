pub mod connection;
mod connection_handler;
pub mod file_identifier;
mod loopback_handler;

mod file_identifier_table;
mod frame;
pub mod uid;

// Implementation of the flow network protocol.  See flow_transport.md for more information.
// TODO
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Frame = frame::Frame;

use file_identifier::FileIdentifier;
use std::clone::Clone;
use std::future::Future;
use std::net::SocketAddr;
use uid::UID;

use tokio::sync::oneshot;

pub use connection_handler::ConnectionHandler;
pub use loopback_handler::LoopbackHandler;

pub type FlowResponse = Option<FlowMessage>;
// XXX get rid of pin?
pub type FlowFuture =
    std::pin::Pin<Box<dyn 'static + Send + Sync + Future<Output = Result<FlowResponse>>>>;
pub type FlowFn = dyn Send + Sync + Fn(FlowMessage) -> FlowFuture;

#[derive(Debug)]
pub enum Peer {
    Remote(SocketAddr, Option<UID>),
    Local(Option<(UID, oneshot::Receiver<FlowMessage>)>),
}

#[derive(Debug)]
pub struct FlowMessage {
    pub peer: Peer,
    pub frame: Frame,
}

impl FlowMessage {
    pub fn new_remote(peer: SocketAddr, completion: Option<UID>, frame: Frame) -> Result<Self> {
        frame.peek_file_identifier()?;
        Ok(Self {
            peer: Peer::Remote(peer, completion),
            frame,
        })
    }
    pub fn new_local(
        completion: Option<(UID, oneshot::Receiver<FlowMessage>)>,
        frame: Frame,
    ) -> Result<Self> {
        frame.peek_file_identifier()?;
        Ok(Self {
            peer: Peer::Local(completion),
            frame,
        })
    }
    pub fn new_response(peer: Peer, frame: Frame) -> Result<Self> {
        use Peer::*;
        frame.peek_file_identifier()?;
        Ok(Self {
            peer: match peer {
                Remote(peer, _completion) => Remote(peer, None),
                Local(_completion) => Local(None),
            },
            frame,
        })
    }
    pub fn file_identifier(&self) -> FileIdentifier {
        self.frame.peek_file_identifier().unwrap()
    }
    pub fn completion(&self) -> Option<UID> {
        match &self.peer {
            Peer::Remote(_, uid) => uid.clone(),
            Peer::Local(Some((uid, _))) => Some(uid.clone()),
            Peer::Local(None) => None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.frame.validate()
    }
    pub fn token(&self) -> UID {
        self.frame.token.clone()
    }
}

// #[test]
// fn test_uid() -> Result<()> {
//     let s = "0123456789abcdeffedcba9876543210";
//     let uid = uid::UID::from_string(s)?;
//     let uid_s = uid.to_string();
//     assert_eq!(uid_s, s);
//     let uid2 = uid::UID::from_string(&uid_s)?;
//     assert_eq!(uid, uid2);
//     assert_eq!(uid.to_u128(), 0x0123456789abcdeffedcba9876543210);
//     Ok(())
// }
