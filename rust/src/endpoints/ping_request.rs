#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/PingRequest_generated.rs"]
mod ping_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/ErrorOrVoid_generated.rs"]
mod void;

use crate::flow::file_identifier::{IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::{FlowFuture, FlowHandler, FlowMessage, Frame, Result};
use crate::services::ConnectionKeeper;

use flatbuffers::FlatBufferBuilder;

use std::net::SocketAddr;
use std::sync::Arc;

const PING_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x47d2c7,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("PingRequest"),
};

const PING_REPLY_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x1ead4a,
    inner_wrapper: IdentifierType::ErrorOr,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("PingReply"),
};

thread_local! {
    static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

thread_local! {
    static REPLY_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

pub fn serialize_request(
    builder: &mut FlatBufferBuilder<'static>,
    peer: SocketAddr,
) -> Result<FlowMessage> {
    use ping_request::{FakeRoot, FakeRootArgs, PingRequest, PingRequestArgs};

    let (flow, reply_promise) = super::create_request_headers(builder, peer);
    let ping_request = Some(PingRequest::create(
        builder,
        &PingRequestArgs { reply_promise },
    ));
    let fake_root = FakeRoot::create(builder, &FakeRootArgs { ping_request });
    builder.finish(fake_root, Some("myfi"));
    super::finalize_request(
        builder,
        flow,
        UID::well_known_token(WLTOKEN::PingPacket),
        PING_REQUEST_FILE_IDENTIFIER,
    )
}

fn serialize_reply(builder: &mut FlatBufferBuilder<'static>, token: UID) -> Result<Frame> {
    use crate::common_generated::{Void, VoidArgs};
    let void = Void::create(builder, &VoidArgs {});
    let ensure_table =
        void::EnsureTable::create(builder, &void::EnsureTableArgs { void: Some(void) });
    let fake_root = void::FakeRoot::create(
        builder,
        &void::FakeRootArgs {
            error_or_type: void::ErrorOr::EnsureTable,
            error_or: Some(ensure_table.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    super::finalize_reply(builder, token, PING_REPLY_FILE_IDENTIFIER)
}

pub fn deserialize_reply(frame: Frame) -> Result<()> {
    let fake_root = void::root_as_fake_root(frame.payload())?;
    if fake_root.error_or_type() == void::ErrorOr::Error {
        Err(format!("ping returned error: {:?}", fake_root.error_or()).into())
    } else {
        Ok(())
    }
}

async fn handle(request: FlowMessage) -> Result<Option<FlowMessage>> {
    request
        .file_identifier()
        .ensure_expected(PING_REQUEST_FILE_IDENTIFIER)?;
    let fake_root = ping_request::root_as_fake_root(request.frame.payload())?;
    let ping_request = fake_root.ping_request().unwrap();
    let reply_promise = ping_request.reply_promise().unwrap();

    let uid = reply_promise.uid().unwrap();
    let uid: UID = uid.into();

    let frame = REPLY_BUILDER.with(|builder| serialize_reply(&mut builder.borrow_mut(), uid))?;
    Ok(Some(FlowMessage::new_response(request.flow, frame)?))
}

pub struct Ping {}

impl Ping {
    pub fn new() -> Self {
        Self {}
    }
}

impl FlowHandler for Ping {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(handle(msg))
    }
}

pub async fn ping(peer: SocketAddr, svc: &Arc<ConnectionKeeper>) -> Result<()> {
    let req = REQUEST_BUILDER.with(|builder| serialize_request(&mut builder.borrow_mut(), peer))?;
    let response_frame = svc.rpc(req).await?;
    deserialize_reply(response_frame.frame)
}
