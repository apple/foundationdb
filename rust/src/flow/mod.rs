pub mod cluster_file;
pub mod connection;
pub mod file_identifier;
mod file_identifier_table;
mod frame;
pub mod uid;

// Implementation of the flow network protocol.  See flow_transport.md for more information.
// TODO
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;
pub type Frame = frame::Frame;

use file_identifier::FileIdentifier;
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4, Ipv4Addr};
use uid::UID;

pub type FlowResponse = Option<FlowMessage>;
// XXX get rid of pin?
pub type FlowFuture =
    std::pin::Pin<Box<dyn 'static + Send + Sync + Future<Output = Result<FlowResponse>>>>;

#[derive(Debug)]
pub enum Peer {
    Remote(SocketAddr),
    Local(Option<UID>),
}

impl From<crate::common_generated::NetworkAddress<'_>> for Peer {
    fn from(network_address: crate::common_generated::NetworkAddress) -> Peer {
        let ip : [u8; 4] = network_address.ip().unwrap().ip4().unwrap().ip().to_be_bytes();
        let port : u16= network_address.port();
        let socket_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(ip[0],ip[1],ip[2],ip[3]), port));
        Peer::Remote(socket_addr)
    }
}

#[derive(Debug)]
pub struct Endpoint {
    peer: Peer,
    secondary_address: Option<Peer>,
    token: UID,
}

impl From<crate::common_generated::Endpoint<'_>> for Endpoint {
    fn from(endpoint: crate::common_generated::Endpoint) -> Self {
        let addresses = endpoint.addresses().unwrap();
        let peer : Peer = addresses.address().unwrap().into();
        use crate::common_generated::OptionalNetworkAddress;
        let secondary_address : Option<Peer> = match addresses.secondary_address_type() {
            OptionalNetworkAddress::NetworkAddress => {
                Some(addresses.secondary_address_as_network_address().unwrap().into())
            },
            OptionalNetworkAddress::Void | _ => {
                None
            },
        };
        let token = endpoint.token().unwrap().into();
        Endpoint {
            peer, secondary_address, token
        }
    }
}

#[derive(Debug)]
pub struct Flow {
    pub src: Peer,
    pub dst: Peer,
}

#[derive(Debug)]
pub struct FlowMessage {
    pub flow: Flow,
    pub frame: Frame,
}

impl FlowMessage {
    pub fn new(flow: Flow, frame: Frame) -> Result<Self> {
        frame.peek_file_identifier()?;
        Ok(Self { flow, frame })
    }
    pub fn new_response(flow: Flow, frame: Frame) -> Result<Self> {
        frame.peek_file_identifier()?;
        Ok(Self {
            flow: Flow {
                // SIC; we're revesering the direction of the flow
                // because this is a response.
                src: flow.dst,
                dst: flow.src,
            },
            frame,
        })
    }
    pub fn file_identifier(&self) -> FileIdentifier {
        self.frame.peek_file_identifier().unwrap()
    }
    pub fn validate(&self) -> Result<()> {
        self.frame.validate()
    }
    pub fn token(&self) -> UID {
        self.frame.token.clone()
    }
}

pub trait FlowHandler: Send + Sync {
    fn handle(&'_ self, msg: FlowMessage) -> FlowFuture;
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
