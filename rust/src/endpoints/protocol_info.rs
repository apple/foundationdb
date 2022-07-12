#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/ProtocolInfoRequest_generated.rs"]
mod protocol_info_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/ProtocolInfoReply_generated.rs"]
mod protocol_info_reply;

use crate::flow::file_identifier::{FileIdentifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::{FlowFuture, FlowHandler, FlowMessage, Frame, Result};
use crate::services::ConnectionKeeper;
use std::net::SocketAddr;
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;

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

thread_local! {
    static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

thread_local! {
    static REPLY_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

fn serialize_request(
    builder: &mut FlatBufferBuilder<'static>,
    peer: SocketAddr,
) -> Result<FlowMessage> {
    use protocol_info_request::{FakeRoot, FakeRootArgs};

    let (flow, reply_promise) = super::create_request_headers(builder, peer);

    let request = Some(ProtocolInfoRequest::create(
        builder,
        &ProtocolInfoRequestArgs { reply_promise },
    ));

    let fake_root = FakeRoot::create(builder, &FakeRootArgs { request });

    builder.finish(fake_root, Some("myfi"));

    super::finalize_request(
        builder,
        flow,
        UID::well_known_token(WLTOKEN::ProtocolInfo),
        PROTOCOL_INFO_REQUEST_FILE_IDENTIFIER,
    )
}

fn serialize_reply(builder: &mut FlatBufferBuilder<'static>,
                    token: UID) -> Result<Frame> {
    use protocol_info_reply::{FakeRoot, FakeRootArgs, ProtocolVersion};

    let protocol_version = ProtocolVersion::new(0xfdb00b072000000);
    let version = Some(&protocol_version);
    let protocol_info_reply =
        ProtocolInfoReply::create(builder, &ProtocolInfoReplyArgs { version });
    let fake_root = FakeRoot::create(
        builder,
        &FakeRootArgs {
            error_or_type: protocol_info_reply::ErrorOr::ProtocolInfoReply,
            error_or: Some(protocol_info_reply.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let mut payload : Vec<u8> = builder.finished_data().into();
    FileIdentifier::new(PROTOCOL_INFO_REPLY_FILE_IDENTIFIER.file_identifier)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload)?;
    Ok(Frame::new(token, payload, 0))
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
    let frame = REPLY_BUILDER.with(|builder| { serialize_reply(&mut builder.borrow_mut(), uid) })?;
    Ok(Some(FlowMessage::new_response(request.flow, frame)?))
}

impl FlowHandler for ProtocolInfo {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(handle(msg))
    }
}

pub async fn protocol_info(peer: SocketAddr, svc: &Arc<ConnectionKeeper>) -> Result<u64> {
    let req = REQUEST_BUILDER.with(|builder| serialize_request(&mut builder.borrow_mut(), peer))?;
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
