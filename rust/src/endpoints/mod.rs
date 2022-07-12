pub mod get_leader;
pub mod network_test;
pub mod ping_request;
pub mod protocol_info;

use crate::flow::{Frame, Flow, FlowMessage, Peer, Result, uid::UID};
use crate::flow::file_identifier::{FileIdentifier, ParsedFileIdentifier};

fn create_rpc_headers(mut builder: flatbuffers::FlatBufferBuilder, peer: std::net::SocketAddr) -> (Flow, Option<flatbuffers::WIPOffset<crate::common_generated::ReplyPromise>>, flatbuffers::FlatBufferBuilder) {
    use crate::common_generated::{ReplyPromise, ReplyPromiseArgs};
    let completion = UID::random_token();
    let response_token: crate::common_generated::UID = (&completion).into();
    let uid = Some(&response_token);
    let reply_promise = Some(ReplyPromise::create(
        &mut builder,
        &ReplyPromiseArgs { uid },
    ));
    (Flow {
        dst: Peer::Remote(peer),
        src: Peer::Local(Some(completion))
    }, reply_promise, builder)
}

fn finalize_request(builder: flatbuffers::FlatBufferBuilder, flow: Flow, token: UID, file_identifier: ParsedFileIdentifier) -> Result<FlowMessage> {
    let (mut payload, offset) = builder.collapse();
    FileIdentifier::new(file_identifier.file_identifier)?
        .rewrite_flatbuf(&mut payload[offset..])?;
    FlowMessage::new(flow, Frame::new(token, payload, offset))
}