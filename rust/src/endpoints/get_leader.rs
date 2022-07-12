#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/GetLeaderRequest_generated.rs"]
mod get_leader_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/GetLeaderReply_generated.rs"]
mod get_leader_reply;

use crate::flow::file_identifier::{FileIdentifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::{Flow, FlowFuture, FlowHandler, FlowMessage, Frame, Peer, Result};
use crate::services::ConnectionKeeper;
use std::net::SocketAddr;

use get_leader_reply::{LeaderInfo, LeaderInfoArgs};
use get_leader_request::{GetLeaderRequest, GetLeaderRequestArgs};

const GET_LEADER_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 214727,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("GetLeaderRequest"),
};

const GET_LEADER_REPLY_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 8338794, // AKA LeaderInfo...
    inner_wrapper: IdentifierType::ErrorOr,
    outer_wrapper: IdentifierType::Optional,
    file_identifier_name: Some("LeaderInfo"),
};

// fn serialize_request(peer: SocketAddr, cluster_key: &str, known_leader: UID) -> Result<FlowMessage> {
//     // key, known_leader, reply_promise, directly in fake_root
//     use crate::common_generated::{ReplyPromise, ReplyPromiseArgs};
//     use get_leader_request::{FakeRoot, FakeRootArgs};

//     let completion = UID::random_token();
//     let wltoken = UID::well_known_token(WLTOKEN::ClientLeaderRegGetLeader);

//     let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
//     let response_token: crate::common_generatedUID = (&completion).into();
//     let uid = Some(&response_token);
//     let reply_promise = Some(ReplyPromise::create())

// }

fn deserialize_request(buf: &[u8]) -> Result<GetLeaderRequest> {
    let fake_root = get_leader_request::root_as_fake_root(buf)?;
    let request = fake_root.request().unwrap();
    Ok(request)
}

fn deserialize_reply(buf: &[u8]) -> Result<Option<Result<LeaderInfo>>> {
    let fake_root = get_leader_reply::root_as_fake_root(buf)?;
    match fake_root.error_or_type() {
        get_leader_reply::ErrorOr::Error => Ok(Some(Err(format!(
            "get leader returned an error: {:?}",
            fake_root.error_or_as_error()
        )
        .into()))),
        get_leader_reply::ErrorOr::LeaderInfo => {
            Ok(Some(Ok(fake_root.error_or_as_leader_info().unwrap())))
        }
        get_leader_reply::ErrorOr::NONE | _ => Ok(None),
    }
}

#[test]
#[allow(non_snake_case)]
fn test_leader_info() -> Result<()> {
    let buffer_16GetLeaderRequest = vec![
        0x24, 0x00, 0x00, 0x00, 0xc7, 0x46, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
        0x00, 0x08, 0x00, 0x04, 0x00, 0x0a, 0x00, 0x1c, 0x00, 0x14, 0x00, 0x04, 0x00, 0x18, 0x00,
        0x06, 0x00, 0x14, 0x00, 0x04, 0x00, 0x16, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x18,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x2e, 0x00, 0x00, 0x00, 0x9c, 0xed, 0xf4, 0xc0, 0x26, 0xee, 0x0b, 0x68, 0x1d, 0x00,
        0x00, 0x00, 0x1e, 0x0e, 0xe3, 0xd1, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x74,
        0x65, 0x73, 0x74, 0x4b, 0x65, 0x79, 0x00,
    ];
    let buffer_7ErrorOrI10LeaderInfoE = vec![
        0x28, 0x00, 0x00, 0x00, 0x6a, 0x3d, 0x7f, 0x02, 0x00, 0x00, 0x06, 0x00, 0x08, 0x00, 0x04,
        0x00, 0x06, 0x00, 0x06, 0x00, 0x04, 0x00, 0x08, 0x00, 0x09, 0x00, 0x08, 0x00, 0x04, 0x00,
        0x0a, 0x00, 0x19, 0x00, 0x04, 0x00, 0x14, 0x00, 0x18, 0x00, 0x12, 0x00, 0x00, 0x00, 0x08,
        0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0xc9, 0x44, 0x74, 0x36,
        0xd1, 0x47, 0xaa, 0x47, 0x35, 0x7f, 0x4d, 0x85, 0x96, 0x3d, 0x1d, 0xfa, 0x08, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x58, 0x58, 0x58, 0x58, 0x59, 0x59,
        0x59, 0x59, 0x5a, 0x5a, 0x5a, 0x5a,
    ];

    println!("{:x?}", deserialize_request(&buffer_16GetLeaderRequest)?);
    println!("{:x?}", deserialize_reply(&buffer_7ErrorOrI10LeaderInfoE)?);

    Ok(())
}
