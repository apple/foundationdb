// #[allow(non_snake_case)]
#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/PingRequest_generated.rs"]
mod ping_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/ErrorOrVoid_generated.rs"]
mod error_or_void;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/NetworkTestRequest_generated.rs"]
mod network_test_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/NetworkTestResponse_generated.rs"]
mod network_test_response;

use crate::flow::file_identifier::{FileIdentifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::frame::Frame;
use crate::flow::uid::UID;
use crate::flow::Result;

const NETWORK_TEST_REQUEST_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x3f4551,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("NetworkTestRequest"),
};

fn serialize_error_or_network_test_response(token: UID, response_len: u32) -> Result<Frame> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(usize::min(
        1024 + (response_len as usize),
        flatbuffers::FLATBUFFERS_MAX_BUFFER_SIZE,
    ));
    let payload = vec!['.' as u8; response_len.try_into()?];
    let payload = builder.create_vector(&payload[..]);
    let network_test_response = network_test_response::NetworkTestResponse::create(
        &mut builder,
        &network_test_response::NetworkTestResponseArgs {
            payload: Some(payload),
        },
    );
    let ensure_table = network_test_response::EnsureTable::create(
        &mut builder,
        &network_test_response::EnsureTableArgs {
            network_test_response: Some(network_test_response),
        },
    );
    let fake_root = network_test_response::FakeRoot::create(
        &mut builder,
        &network_test_response::FakeRootArgs {
            error_or_type: network_test_response::ErrorOr::EnsureTable,
            error_or: Some(ensure_table.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let mut payload = builder.finished_data().to_vec();
    FileIdentifier::new(14465374)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload)?;
    // println!("reply: {:x?}", builder.finished_data());
    Ok(Frame::new_reply(token, payload))
}

pub async fn handle(
    parsed_file_identifier: ParsedFileIdentifier,
    frame: Frame,
) -> Result<Option<Frame>> {
    if parsed_file_identifier != NETWORK_TEST_REQUEST_IDENTIFIER {
        return Err(format!(
            "Expected NetworkTestRequest.  Got {:?}",
            parsed_file_identifier
        )
        .into());
    }
    // println!("frame: {:?}", frame.payload);
    let fake_root = network_test_request::root_as_fake_root(&frame.payload[..])?;
    let network_test_request = fake_root.network_test_request().unwrap();
    // println!("Got: {:?}", network_test_request);
    let reply_promise = network_test_request.reply_promise().unwrap();

    //   tokio::time::sleep(core::time::Duration::from_millis(1)).await;

    let uid = reply_promise.uid().unwrap();
    let uid = UID {
        uid: [uid.first(), uid.second()],
    };

    let frame = serialize_error_or_network_test_response(
        uid,
        network_test_request.reply_size().try_into()?,
    )?;
    Ok(Some(frame))
}
