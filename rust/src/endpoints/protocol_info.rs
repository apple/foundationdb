#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/ProtocolInfoRequest_generated.rs"]
mod protocol_info_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/ProtocolInfoReply_generated.rs"]
mod protocol_info_reply;

use crate::flow::file_identifier::{FileIdentifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::{Flow, FlowFuture, FlowHandler, FlowMessage, Frame, Peer, Result};
use crate::services::ConnectionKeeper;
use std::net::SocketAddr;
use std::sync::Arc;

use protocol_info_reply::{ProtocolInfoReply, ProtocolInfoReplyArgs};
use protocol_info_request::{ProtocolInfoRequest, ProtocolInfoRequestArgs};

const PROTOCOL_INFO_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 13261233,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("ProtocolInfoRequest"),
};

const PROTOCOL_INFO_REPLY_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 7784298,
    inner_wrapper: IdentifierType::ErrorOr,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("ProtocolInfoReply"),
};

fn serialize_request(peer: SocketAddr) -> Result<FlowMessage> {
    use protocol_info_request::{FakeRoot, FakeRootArgs};

    let builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let (flow, reply_promise, mut builder) = super::create_rpc_headers(builder, peer);

    let protocol_info_request = Some(ProtocolInfoRequest::create(
        &mut builder,
        &ProtocolInfoRequestArgs { reply_promise },
    ));

    let fake_root = FakeRoot::create(
        &mut builder,
        &FakeRootArgs {
            request: protocol_info_request,
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let wltoken = UID::well_known_token(WLTOKEN::ProtocolInfo);
    super::finalize_request(builder, flow, wltoken, PROTOCOL_INFO_REQUEST_FILE_IDENTIFIER)
}

fn serialize_reply(token: UID) -> Result<Frame> {
    use protocol_info_reply::{FakeRoot, FakeRootArgs, ProtocolVersion};
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let protocol_version = ProtocolVersion::new(0xfdb00b072000000);
    let version = Some(&protocol_version);
    let protocol_info_reply =
        ProtocolInfoReply::create(&mut builder, &ProtocolInfoReplyArgs { version });
    // let ensure_table = EnsureTable::create(&mut builder, &EnsureTableArgs { reply: Some(protocol_info_reply) });
    let fake_root = FakeRoot::create(
        &mut builder,
        &FakeRootArgs {
            error_or_type: protocol_info_reply::ErrorOr::ProtocolInfoReply,
            error_or: Some(protocol_info_reply.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let (mut payload, offset) = builder.collapse();
    FileIdentifier::new(PROTOCOL_INFO_REPLY_FILE_IDENTIFIER.file_identifier)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload[offset..])?;
    Ok(Frame::new(token, payload, offset))
}

fn deserialize_request(buf: &[u8]) -> Result<ProtocolInfoRequest> {
    let fake_root = protocol_info_request::root_as_fake_root(buf)?;
    let request = fake_root.request().unwrap();
    Ok(request)
}
fn deserialize_reply(buf: &[u8]) -> Result<Result<ProtocolInfoReply>> {
    let fake_root = protocol_info_reply::root_as_fake_root(buf)?;
    if fake_root.error_or_type() == protocol_info_reply::ErrorOr::Error {
        Ok(Err(format!(
            "protocol info returned an error: {:?}",
            fake_root.error_or_as_error()
        )
        .into()))
    } else {
        let reply = fake_root.error_or_as_protocol_info_reply().unwrap();
        Ok(Ok(reply))
    }
}
pub struct ProtocolInfo {}

impl ProtocolInfo {
    pub fn new() -> Self {
        Self {}
    }
}

async fn handle(request: FlowMessage) -> Result<Option<FlowMessage>> {
    request
        .file_identifier()
        .ensure_expected(PROTOCOL_INFO_REQUEST_FILE_IDENTIFIER)?;
    deserialize_request(request.frame.payload())?;
    let fake_root = protocol_info_request::root_as_fake_root(request.frame.payload())?;
    let protocol_info_request = fake_root.request().unwrap();
    let reply_promise = protocol_info_request.reply_promise().unwrap();
    let uid = reply_promise.uid().unwrap();
    let uid: UID = uid.into();
    let frame = serialize_reply(uid)?;
    Ok(Some(FlowMessage::new_response(request.flow, frame)?))
}

impl FlowHandler for ProtocolInfo {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(handle(msg))
    }
}

pub async fn protocol_info(peer: SocketAddr, svc: &Arc<ConnectionKeeper>) -> Result<u64> {
    let req = serialize_request(peer)?;
    let response_frame = svc.rpc(req).await?;
    let reply = deserialize_reply(response_frame.frame.payload())?;
    Ok(reply?.version().unwrap().version())
}

#[test]
#[allow(non_snake_case)]
fn protocol_info_flatbuffers() -> Result<()> {
    let buffer_19ProtocolInfoRequest = vec![
        0x14, 0x00, 0x00, 0x00, 0xb1, 0x59, 0xca, 0x00, 0x06, 0x00, 0x08, 0x00, 0x04, 0x00, 0x06,
        0x00, 0x14, 0x00, 0x04, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x14, 0x00,
        0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x1c, 0x2b, 0xe6, 0x1b, 0xc7,
        0xf6, 0x1c, 0x64, 0x1d, 0x00, 0x00, 0x00, 0x40, 0xc7, 0x5f, 0x0e,
    ];
    // let buffer_17ProtocolInfoReply = vec![0x14, 0x00, 0x00, 0x00, 0x6a, 0xc7, 0x76, 0x00, 0x06, 0x00, 0x0c, 0x00, 0x04, 0x00, 0x06, 0x00, 0x08, 0x00, 0x04, 0x00, 0x06, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x72, 0xb0, 0x00, 0xdb, 0x0f];
    let buffer_7ErrorOrI17ProtocolInfoReplyE = vec![
        0x20, 0x00, 0x00, 0x00, 0x6a, 0xc7, 0x76, 0x02, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x0c,
        0x00, 0x04, 0x00, 0x06, 0x00, 0x06, 0x00, 0x04, 0x00, 0x08, 0x00, 0x09, 0x00, 0x08, 0x00,
        0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x20,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x72, 0xb0, 0x00, 0xdb, 0x0f,
    ];

    println!("{:x?}", deserialize_request(&buffer_19ProtocolInfoRequest)?);
    println!(
        "{:x?}",
        deserialize_reply(&buffer_7ErrorOrI17ProtocolInfoReplyE)?
    );

    Ok(())
}
