#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/GetLeaderRequest_generated.rs"]
mod get_leader_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/GetLeaderReply_generated.rs"]
mod get_leader_reply;

use crate::flow::file_identifier::{peek_file_identifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::{cluster_file::ClusterFile, FlowMessage, Frame, Result};
use crate::services::ConnectionKeeper;

use flatbuffers::FlatBufferBuilder;

use std::net::SocketAddr;
use std::sync::Arc;

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
    inner_wrapper: IdentifierType::Optional,
    outer_wrapper: IdentifierType::ErrorOr,
    file_identifier_name: Some("LeaderInfo"),
};

const CLUSTER_CONTROLLER_CLIENT_INTERFACE_FILE_IDENTIFIER: ParsedFileIdentifier =
    ParsedFileIdentifier {
        file_identifier: 14997695,
        inner_wrapper: IdentifierType::None,
        outer_wrapper: IdentifierType::None,
        file_identifier_name: Some("ClusterControllerClientInterface"),
    };

thread_local! {
    static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

thread_local! {
    static REPLY_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

// TODO: Is known_leader optional?
fn serialize_request(
    builder: &mut FlatBufferBuilder<'static>,
    peer: SocketAddr,
    cluster_key: &str,
    known_leader: &UID,
) -> Result<FlowMessage> {
    // key, known_leader, reply_promise, directly in fake_root
    use get_leader_request::{FakeRoot, FakeRootArgs};

    let (flow, reply_promise) = super::create_request_headers(builder, peer);
    let key = Some(builder.create_vector(cluster_key.as_bytes()));

    let known_leader_val: crate::common_generated::UID = known_leader.into();

    let known_leader = Some(&known_leader_val);

    let request = Some(GetLeaderRequest::create(
        builder,
        &GetLeaderRequestArgs {
            key,
            known_leader,
            reply_promise,
        },
    ));
    let fake_root = FakeRoot::create(builder, &FakeRootArgs { request });
    builder.finish(fake_root, Some("myfi"));
    super::finalize_request(
        builder,
        flow,
        UID::well_known_token(WLTOKEN::ClientLeaderRegGetLeader),
        GET_LEADER_REQUEST_FILE_IDENTIFIER,
    )
}

#[allow(dead_code)]
fn serialize_reply(
    builder: &mut FlatBufferBuilder<'static>,
    token: UID,
    change_id: UID,
    forward: bool,
    serialized_info: &[u8],
) -> Result<Frame> {
    use get_leader_reply::{
        EnsureTable, EnsureTableArgs, ErrorOr, FakeRoot, FakeRootArgs, Optional,
    };
    // change_id, forward, serailized_info

    let change_id: crate::common_generated::UID = change_id.into();
    let change_id = Some(&change_id);

    let serialized_info = Some(builder.create_vector(serialized_info));

    let leader_info = LeaderInfo::create(
        builder,
        &LeaderInfoArgs {
            serialized_info,
            change_id,
            forward,
        },
    );

    // let optional = Some(Optional::create(
    //     builder,
    //     &OptionalArgs {
    //         leader_info,
    //     }
    // ));

    let ensure_table = EnsureTable::create(
        builder,
        &EnsureTableArgs {
            optional_type: Optional::LeaderInfo,
            optional: Some(leader_info.as_union_value()),
        },
    );

    let fake_root = FakeRoot::create(
        builder,
        &FakeRootArgs {
            error_or_type: ErrorOr::EnsureTable,
            error_or: Some(ensure_table.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    super::finalize_reply(builder, token, GET_LEADER_REPLY_FILE_IDENTIFIER)
}

#[allow(dead_code)]
fn deserialize_request(buf: &[u8]) -> Result<GetLeaderRequest> {
    let fake_root = get_leader_request::root_as_fake_root(buf)?;
    let request = fake_root.request().unwrap();
    Ok(request)
}

fn deserialize_reply(buf: &[u8]) -> Result<Option<LeaderInfo>> {
    let fake_root = get_leader_reply::root_as_fake_root(buf)?;
    match fake_root.error_or_type() {
        get_leader_reply::ErrorOr::EnsureTable => {
            let ensure_table = fake_root.error_or_as_ensure_table().unwrap();
            match ensure_table.optional_type() {
                get_leader_reply::Optional::LeaderInfo => {
                    Ok(Some(ensure_table.optional_as_leader_info().unwrap()))
                }
                get_leader_reply::Optional::Void => Ok(None),
                x => Err(format!("Unknown optional_type: {:?}", x).into()),
            }
        }
        get_leader_reply::ErrorOr::Error => Err(format!(
            "get leader returned an error: {:?}",
            fake_root.error_or_as_error()
        )
        .into()),
        x => Err(format!("Unknown error_or_type(): {:?}", x).into()),
    }
}

fn deserialize_network_address(buf: &[u8]) -> Result<()> {
    //crate::common_generated::NetworkAddress> {
    let file_identifier = peek_file_identifier(buf)?;
    file_identifier.ensure_expected(ParsedFileIdentifier {
        file_identifier: 14155727,
        inner_wrapper: IdentifierType::None,
        outer_wrapper: IdentifierType::None,
        file_identifier_name: Some("NetworkAddress"),
    })?;
    crate::flow::Frame::reverse_engineer_flatbuffer(buf)?;
    // let network_address = crate::common_generated::root_as_network_address_fake_root(buf)?;
    // println!("{:?}", network_address.network_address().unwrap());
    Ok(())
}
fn deserialize_endpoint(buf: &[u8]) -> Result<()> {
    let file_identifier = peek_file_identifier(buf)?;
    println!("File identifier: {:?}", file_identifier);
    file_identifier.ensure_expected(ParsedFileIdentifier {
        file_identifier: 10618805,
        inner_wrapper: IdentifierType::None,
        outer_wrapper: IdentifierType::None,
        file_identifier_name: Some("Endpoint"),
    })?;

    let endpoint = crate::common_generated::root_as_endpoint_fake_root(buf)?;
    println!("{:x?}", endpoint);

    Ok(())
}
fn deserialize_cluster_controller_full_interface(buf: &[u8]) -> Result<()> {
    Ok(())
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
    let buffer_7ErrorOrI11EnsureTableI8OptionalI10LeaderInfoEEE = vec![
        0x2c, 0x00, 0x00, 0x00, 0x6a, 0x3d, 0x7f, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
        0x00, 0x08, 0x00, 0x04, 0x00, 0x06, 0x00, 0x06, 0x00, 0x04, 0x00, 0x08, 0x00, 0x09, 0x00,
        0x08, 0x00, 0x04, 0x00, 0x0a, 0x00, 0x19, 0x00, 0x04, 0x00, 0x14, 0x00, 0x18, 0x00, 0x12,
        0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x1e, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x8e, 0x5e, 0xd5,
        0x0a, 0x08, 0x80, 0x0e, 0xb7, 0xe5, 0x75, 0xa8, 0xe7, 0x57, 0x20, 0x34, 0xbf, 0x08, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x58, 0x58, 0x58, 0x58, 0x59,
        0x59, 0x59, 0x59, 0x5a, 0x5a, 0x5a, 0x5a,
    ];

    let buffer_14NetworkAddress = vec![
        0x28, 0x00, 0x00, 0x00, 0xcf, 0xff, 0xd7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
        0x00, 0x08, 0x00, 0x04, 0x00, 0x08, 0x00, 0x09, 0x00, 0x08, 0x00, 0x04, 0x00, 0x0c, 0x00,
        0x0d, 0x00, 0x04, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x0c, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x04,
        0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0xef, 0xbe, 0xad, 0xde,
    ];

    let buffer_8Endpoint = vec![
        0x34, 0x00, 0x00, 0x00, 0xb5, 0x07, 0xa2, 0x00, 0x0a, 0x00, 0x0d, 0x00, 0x04, 0x00, 0x0c,
        0x00, 0x08, 0x00, 0x06, 0x00, 0x08, 0x00, 0x04, 0x00, 0x08, 0x00, 0x09, 0x00, 0x08, 0x00,
        0x04, 0x00, 0x0c, 0x00, 0x0d, 0x00, 0x04, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x0c, 0x00, 0x08,
        0x00, 0x18, 0x00, 0x14, 0x00, 0x04, 0x00, 0x22, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x10, 0x00, 0x00, 0x00, 0x6d, 0x62, 0xc6, 0xd7, 0x4a, 0xd8, 0xb4, 0xe1, 0x1d, 0x00, 0x00,
        0x00, 0x98, 0xb3, 0xc9, 0xe5, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50, 0x00,
        0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x48,
        0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x60, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad,
        0xde,
    ];

    let buffer_30ClusterControllerFullInterface = vec![
        0x6c, 0x00, 0x00, 0x00, 0xbf, 0xd8, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x0d,
        0x00, 0x04, 0x00, 0x0c, 0x00, 0x08, 0x00, 0x06, 0x00, 0x08, 0x00, 0x04, 0x00, 0x08, 0x00,
        0x09, 0x00, 0x08, 0x00, 0x04, 0x00, 0x0c, 0x00, 0x0d, 0x00, 0x04, 0x00, 0x08, 0x00, 0x0a,
        0x00, 0x0c, 0x00, 0x16, 0x00, 0x28, 0x00, 0x04, 0x00, 0x08, 0x00, 0x0c, 0x00, 0x10, 0x00,
        0x14, 0x00, 0x18, 0x00, 0x1c, 0x00, 0x20, 0x00, 0x24, 0x00, 0x1e, 0x00, 0x38, 0x00, 0x04,
        0x00, 0x08, 0x00, 0x0c, 0x00, 0x10, 0x00, 0x14, 0x00, 0x18, 0x00, 0x1c, 0x00, 0x20, 0x00,
        0x24, 0x00, 0x28, 0x00, 0x2c, 0x00, 0x30, 0x00, 0x34, 0x00, 0x08, 0x00, 0x18, 0x00, 0x14,
        0x00, 0x04, 0x00, 0x56, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2e, 0x00, 0x00, 0x00,
        0xf4, 0x03, 0x00, 0x00, 0xa0, 0x03, 0x00, 0x00, 0x4c, 0x03, 0x00, 0x00, 0xf8, 0x02, 0x00,
        0x00, 0xa4, 0x02, 0x00, 0x00, 0x50, 0x02, 0x00, 0x00, 0xfc, 0x01, 0x00, 0x00, 0xa8, 0x01,
        0x00, 0x00, 0x54, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0xac, 0x00, 0x00, 0x00, 0x58,
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x96, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x50, 0x00, 0x00, 0x00, 0x47, 0xce, 0x20, 0xf6, 0x88, 0x15, 0x1e, 0x11, 0x31, 0x00, 0x00,
        0x00, 0x31, 0xe1, 0x56, 0xd7, 0x04, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb8, 0x00, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xe6, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0xa0, 0x00, 0x00, 0x00, 0x91, 0xb8, 0x7c, 0xfc, 0x7c, 0xe9,
        0x94, 0xc9, 0x30, 0x00, 0x00, 0x00, 0x58, 0x8a, 0xdf, 0x30, 0x04, 0x00, 0x00, 0x00, 0x10,
        0x01, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x08, 0x01, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x20, 0x01, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe,
        0xad, 0xde, 0x36, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xf0, 0x00, 0x00, 0x00, 0xc9,
        0x4b, 0xda, 0x8e, 0x8c, 0x44, 0xe9, 0xf9, 0x2f, 0x00, 0x00, 0x00, 0x10, 0x18, 0xf9, 0xdd,
        0x04, 0x00, 0x00, 0x00, 0x60, 0x01, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x58, 0x01, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x86, 0x01, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x40, 0x01, 0x00, 0x00, 0x0b, 0xa5, 0x52, 0x23, 0x4f, 0xba, 0xd0, 0x5e, 0x2e, 0x00, 0x00,
        0x00, 0xd3, 0x56, 0xc3, 0x16, 0x04, 0x00, 0x00, 0x00, 0xb0, 0x01, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xa8, 0x01, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x01, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xd6, 0x01, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00, 0x63, 0xf8, 0x6b, 0xe0, 0xde, 0x3d,
        0x9f, 0xec, 0x2d, 0x00, 0x00, 0x00, 0xc9, 0x56, 0xa5, 0x78, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xf8, 0x01, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x10, 0x02, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe,
        0xad, 0xde, 0x26, 0x02, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xe0, 0x01, 0x00, 0x00, 0x4d,
        0x15, 0x0c, 0x29, 0x97, 0x93, 0x70, 0x9e, 0x2c, 0x00, 0x00, 0x00, 0xb1, 0x64, 0x64, 0x20,
        0x04, 0x00, 0x00, 0x00, 0x50, 0x02, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x48, 0x02, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x60, 0x02, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x76, 0x02, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x30, 0x02, 0x00, 0x00, 0x0f, 0xe8, 0x14, 0xa4, 0xca, 0x7f, 0xc8, 0xab, 0x2b, 0x00, 0x00,
        0x00, 0x09, 0xe0, 0xe5, 0x52, 0x04, 0x00, 0x00, 0x00, 0xa0, 0x02, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x98, 0x02, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x02, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xc6, 0x02, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x80, 0x02, 0x00, 0x00, 0x75, 0x84, 0x09, 0x63, 0xf9, 0xdb,
        0xb6, 0x7e, 0x2a, 0x00, 0x00, 0x00, 0x40, 0xfc, 0x7c, 0xee, 0x04, 0x00, 0x00, 0x00, 0xf0,
        0x02, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xe8, 0x02, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x03, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe,
        0xad, 0xde, 0x16, 0x03, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xd0, 0x02, 0x00, 0x00, 0xe3,
        0x31, 0xd1, 0xa3, 0x40, 0x43, 0x57, 0xa7, 0x29, 0x00, 0x00, 0x00, 0xd3, 0x62, 0xd9, 0x63,
        0x04, 0x00, 0x00, 0x00, 0x40, 0x03, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x03, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x50, 0x03, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x66, 0x03, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x20, 0x03, 0x00, 0x00, 0x7b, 0x49, 0xde, 0xae, 0xb4, 0xf1, 0x36, 0x77, 0x28, 0x00, 0x00,
        0x00, 0xb6, 0x9d, 0x40, 0x96, 0x04, 0x00, 0x00, 0x00, 0x90, 0x03, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x88, 0x03, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xa0, 0x03, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xb6, 0x03, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x70, 0x03, 0x00, 0x00, 0xe9, 0xc3, 0x4e, 0xea, 0x22, 0x5a,
        0x7a, 0xbb, 0x27, 0x00, 0x00, 0x00, 0xb4, 0x7a, 0xf3, 0xe5, 0x04, 0x00, 0x00, 0x00, 0xe0,
        0x03, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xd8, 0x03, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0xf0, 0x03, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe,
        0xad, 0xde, 0x06, 0x04, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0xc0, 0x03, 0x00, 0x00, 0xb5,
        0x75, 0xff, 0x3e, 0xbf, 0x0c, 0x72, 0x19, 0x26, 0x00, 0x00, 0x00, 0x76, 0x87, 0x0a, 0x63,
        0x04, 0x00, 0x00, 0x00, 0x30, 0x04, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x04, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x04, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x3c, 0x04, 0x00, 0x00, 0xa4, 0x02, 0x00, 0x00,
        0x50, 0x02, 0x00, 0x00, 0xfc, 0x01, 0x00, 0x00, 0xa8, 0x01, 0x00, 0x00, 0x54, 0x01, 0x00,
        0x00, 0x00, 0x01, 0x00, 0x00, 0xac, 0x00, 0x00, 0x00, 0x58, 0x00, 0x00, 0x00, 0x04, 0x00,
        0x00, 0x00, 0x7e, 0x04, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x38, 0x04, 0x00, 0x00, 0x67,
        0x28, 0xe1, 0x3c, 0x6b, 0xe4, 0x33, 0xd1, 0x25, 0x00, 0x00, 0x00, 0x67, 0x39, 0x7d, 0xbd,
        0x04, 0x00, 0x00, 0x00, 0xa8, 0x04, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0xa0, 0x04, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb8, 0x04, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xce, 0x04, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x88, 0x04, 0x00, 0x00, 0xf7, 0x9a, 0x42, 0xd3, 0x36, 0xd7, 0xb6, 0x86, 0x24, 0x00, 0x00,
        0x00, 0xd6, 0x69, 0xfa, 0x95, 0x04, 0x00, 0x00, 0x00, 0xf8, 0x04, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x04, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x05, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x1e, 0x05, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0xd8, 0x04, 0x00, 0x00, 0x23, 0x6e, 0x2a, 0x51, 0x7e, 0xfc,
        0xcd, 0xfb, 0x23, 0x00, 0x00, 0x00, 0xcb, 0x10, 0xa6, 0x0f, 0x04, 0x00, 0x00, 0x00, 0x48,
        0x05, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x40, 0x05, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x58, 0x05, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe,
        0xad, 0xde, 0x6e, 0x05, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x28, 0x05, 0x00, 0x00, 0x3f,
        0xc4, 0xa0, 0x58, 0x44, 0x9c, 0xd3, 0x4f, 0x22, 0x00, 0x00, 0x00, 0xb9, 0x55, 0xe8, 0xb0,
        0x04, 0x00, 0x00, 0x00, 0x98, 0x05, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x90, 0x05, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xa8, 0x05, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xbe, 0x05, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x78, 0x05, 0x00, 0x00, 0x09, 0xa4, 0x5f, 0xa7, 0x07, 0x23, 0xda, 0x7b, 0x21, 0x00, 0x00,
        0x00, 0x67, 0xd1, 0x4a, 0x4c, 0x04, 0x00, 0x00, 0x00, 0xe8, 0x05, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe0, 0x05, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x05, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x0e, 0x06, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0xc8, 0x05, 0x00, 0x00, 0xdb, 0xc1, 0x93, 0x31, 0x2a, 0xbc,
        0x86, 0x5a, 0x20, 0x00, 0x00, 0x00, 0x8c, 0xc3, 0xe3, 0x2a, 0x04, 0x00, 0x00, 0x00, 0x38,
        0x06, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x30, 0x06, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x48, 0x06, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe,
        0xad, 0xde, 0x5e, 0x06, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x18, 0x06, 0x00, 0x00, 0xb9,
        0xba, 0x6a, 0x95, 0xb3, 0xa8, 0x45, 0xb1, 0x1f, 0x00, 0x00, 0x00, 0x77, 0x1d, 0xf5, 0x4d,
        0x04, 0x00, 0x00, 0x00, 0x88, 0x06, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x06, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x98, 0x06, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xae, 0x06, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
        0x68, 0x06, 0x00, 0x00, 0xff, 0x95, 0x4c, 0x4c, 0x1f, 0xab, 0x31, 0xed, 0x1e, 0x00, 0x00,
        0x00, 0xad, 0x01, 0x93, 0x29, 0x04, 0x00, 0x00, 0x00, 0xd8, 0x06, 0x00, 0x00, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd0, 0x06, 0x00, 0x00, 0x0c,
        0x00, 0x00, 0x00, 0xfe, 0xca, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0xe8, 0x06, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0xfe, 0x06, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0xb8, 0x06, 0x00, 0x00, 0xeb, 0x76, 0xc2, 0x8d, 0x52, 0xda,
        0xd5, 0x5d, 0x1d, 0x00, 0x00, 0x00, 0x8c, 0xa9, 0xa4, 0xd6, 0x08, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x2c, 0x07, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x24, 0x07, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0xfe, 0xca, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x07, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0xef, 0xbe, 0xad, 0xde,
    ];
    println!("{:x?}", deserialize_request(&buffer_16GetLeaderRequest)?);
    println!(
        "{:x?}",
        deserialize_reply(&buffer_7ErrorOrI11EnsureTableI8OptionalI10LeaderInfoEEE)?
    );
    println!(
        "{:x?}",
        deserialize_network_address(&buffer_14NetworkAddress)?
    );
    println!("{:x?}", deserialize_endpoint(&buffer_8Endpoint)?);

    println!(
        "{:x?}",
        deserialize_cluster_controller_full_interface(&buffer_30ClusterControllerFullInterface)?
    );
    Ok(())
}

pub struct MonitorLeader {
    pub svc: Arc<ConnectionKeeper>,
    pub cluster_file: ClusterFile,
    pub known_leader: UID,
    pub serialized_info: Vec<u8>,
}

impl MonitorLeader {
    pub async fn monitor_leader(self: &mut Self) -> Result<()> {
        use crate::flow::cluster_file::ClusterHost;
        loop {
            // the rand crate has something more convenient; bit it's cryptographically secure, which is overkill.
            let host = &self.cluster_file.hosts[fastrand::usize(0..self.cluster_file.hosts.len())];
            let peer = match host {
                ClusterHost::IPAddr(peer, false) => peer,
                x => {
                    return Err(format!(
                        "Unsupported connection type: {:?}; stopping monitor_leader",
                        x
                    )
                    .into());
                }
            };
            // Note:  FDB uses the cluster description and the id when deciding whether you're talking to the
            //        right cluster or not.
            let cluster_key = format!("{}:{}", self.cluster_file.description, self.cluster_file.id);
            let request = REQUEST_BUILDER.with(|builder| {
                serialize_request(
                    &mut builder.borrow_mut(),
                    *peer,
                    &cluster_key,
                    &self.known_leader,
                )
            })?;
            let reply = self.svc.rpc(request).await?;
            reply
                .file_identifier()
                .ensure_expected(GET_LEADER_REPLY_FILE_IDENTIFIER)?;
            match deserialize_reply(reply.frame.payload())? {
                Some(reply) => {
                    self.known_leader = reply.change_id().unwrap().into();
                    println!("New leader change_id: {:x?}", self.known_leader);
                    if reply.forward() {
                        println!("Got new cluster file:");
                        let cluster_file =
                            String::from_utf8(reply.serialized_info().unwrap().into())?;
                        println!("{}", cluster_file);
                        self.cluster_file = cluster_file.parse()?;
                    } else {
                        // TODO: serialized_info contains an instance of ClusterContollerFullInterface.

                        // It was serialized with IncludeVersion() in the C++ code, which means the first
                        // 64-bits are an FDB version number.
                        let serialized_info = reply.serialized_info().unwrap();
                        let version = u64::from_le_bytes(serialized_info[0..8].try_into()?);
                        println!("Protocol version: {:x}", version);

                        let serialized_info = &serialized_info[8..];
                        crate::flow::file_identifier::peek_file_identifier(serialized_info)?
                            .ensure_expected(CLUSTER_CONTROLLER_CLIENT_INTERFACE_FILE_IDENTIFIER)?;
                        // println!("File identifier: {:?}", file_identifier);
                        // let parsed_file_identifier = crate::flow::file_identifier::FileIdentifierNames::new()?.from_id(&file_identifier);
                        // println!("Parsed file identifier: {:?}", parsed_file_identifier);
                        // self.serialized_info = reply.serialized_info().unwrap().to_vec();

                        // but it has been serialized in some format other than flatbuffers. (C packed
                        // struct, maybe?)
                    }
                }
                x => {
                    return Err(
                        format!("Expected response from GetLeader; got {:?} instead.", x).into(),
                    );
                }
            }
        }
    }
}
