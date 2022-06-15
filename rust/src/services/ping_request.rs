// #[allow(non_snake_case)]
#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/PingRequest_generated.rs"]
mod ping_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/Void_generated.rs"]
mod void;

use super::FlowMessage;
use crate::flow::file_identifier::{FileIdentifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::Frame;
use crate::flow::Result;

use std::net::SocketAddr;

const PING_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x47d2c7,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("PingRequest"),
};

const PING_RESPONSE_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x1ead4a,
    inner_wrapper: IdentifierType::ErrorOr,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("PingResponse"),
};

pub fn serialize_request(peer: SocketAddr) -> Result<FlowMessage> {
    use ping_request::{
        FakeRoot, FakeRootArgs, PingRequest, PingRequestArgs, ReplyPromise, ReplyPromiseArgs,
    };

    let completion = UID::random_token();
    let wltoken = UID::well_known_token(WLTOKEN::PingPacket);

    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let response_token = ping_request::UID::new(completion.uid[0], completion.uid[1]);
    let uid = Some(&response_token);
    let reply_promise = Some(ReplyPromise::create(
        &mut builder,
        &ReplyPromiseArgs { uid },
    ));
    let ping_request = Some(PingRequest::create(
        &mut builder,
        &PingRequestArgs { reply_promise },
    ));
    let fake_root = FakeRoot::create(&mut builder, &FakeRootArgs { ping_request });
    builder.finish(fake_root, Some("myfi"));
    let (mut payload, offset) = builder.collapse();
    FileIdentifier::new(PING_REQUEST_FILE_IDENTIFIER.file_identifier)?
        .rewrite_flatbuf(&mut payload[offset..])?;
    Ok(FlowMessage{ peer, completion: Some(completion.clone()), frame: Frame::new(wltoken, payload, offset) })
}

fn serialize_response(token: UID) -> Result<Frame> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let void = void::Void::create(&mut builder, &void::VoidArgs {});
    let ensure_table =
        void::EnsureTable::create(&mut builder, &void::EnsureTableArgs { void: Some(void) });
    let fake_root = void::FakeRoot::create(
        &mut builder,
        &void::FakeRootArgs {
            error_or_type: void::ErrorOr::EnsureTable,
            error_or: Some(ensure_table.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let (mut payload, offset) = builder.collapse();
    // See also: flow/README.md ### Flatbuffers/ObjectSerializer
    FileIdentifier::new(PING_RESPONSE_FILE_IDENTIFIER.file_identifier)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload[offset..])?;
    Ok(Frame::new(token, payload, offset))
}

pub fn deserialize_response(frame: Frame) -> Result<()> {
    let fake_root = void::root_as_fake_root(frame.payload())?;
    if fake_root.error_or_type() == void::ErrorOr::Error {
        Err(format!("ping returned error: {:?}", fake_root.error_or()).into())
    } else {
        println!("ping response: {:?}", fake_root);
        Ok(())
    }
}

pub async fn handle(peer: SocketAddr, request: FlowMessage) -> Result<Option<FlowMessage>> {
    request
        .file_identifier()
        .ensure_expected(PING_REQUEST_FILE_IDENTIFIER)?;
    let fake_root = ping_request::root_as_fake_root(request.frame.payload())?;
    let reply_promise = fake_root.ping_request().unwrap().reply_promise().unwrap();

    let uid = reply_promise.uid().unwrap();
    let uid = UID {
        uid: [uid.first(), uid.second()],
    };

    let frame = serialize_response(uid)?;
    Ok(Some(FlowMessage { peer, completion: None, frame }))
}
