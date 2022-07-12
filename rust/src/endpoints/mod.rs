pub mod get_leader;
pub mod network_test;
pub mod ping_request;
pub mod protocol_info;

use crate::flow::file_identifier::{FileIdentifier, ParsedFileIdentifier};
use crate::flow::{uid::UID, Flow, FlowMessage, Frame, Peer, Result};

fn create_request_headers(
    builder: &mut flatbuffers::FlatBufferBuilder<'static>,
    peer: std::net::SocketAddr,
) -> (
    Flow,
    Option<flatbuffers::WIPOffset<crate::common_generated::ReplyPromise<'static>>>,
) {
    use crate::common_generated::{ReplyPromise, ReplyPromiseArgs};
    let completion = UID::random_token();
    let response_token: crate::common_generated::UID = (&completion).into();
    let uid = Some(&response_token);
    let reply_promise = Some(ReplyPromise::create(builder, &ReplyPromiseArgs { uid }));
    (
        Flow {
            dst: Peer::Remote(peer),
            src: Peer::Local(Some(completion)),
        },
        reply_promise,
    )
}

fn finalize_request(
    builder: &mut flatbuffers::FlatBufferBuilder<'static>,
    flow: Flow,
    token: UID,
    file_identifier: ParsedFileIdentifier,
) -> Result<FlowMessage> {
    let mut payload: Vec<u8> = builder.finished_data().into();
    FileIdentifier::new(file_identifier.file_identifier)?.rewrite_flatbuf(&mut payload)?;
    FlowMessage::new(flow, Frame::new(token, payload, 0))
}

fn finalize_reply(
    builder: &mut flatbuffers::FlatBufferBuilder<'static>,
    token: UID,
    file_identifier: ParsedFileIdentifier,
) -> Result<Frame> {
    let mut payload: Vec<u8> = builder.finished_data().into();
    builder.reset();
    FileIdentifier::new(file_identifier.file_identifier)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload)?;
    Ok(Frame::new(token, payload, 0))
}