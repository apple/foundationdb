// #[allow(non_snake_case)]
#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/PingRequest_generated.rs"]
mod ping_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/Void_generated.rs"]
mod void;

use crate::flow::Result;
use crate::flow::connection::Connection;
use crate::flow::frame::Frame;
use crate::flow::uid::UID;
use crate::flow::file_identifier::{ParsedFileIdentifier, FileIdentifier, IdentifierType};

const PING_FILE_IDENTIFIER : ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x47d2c7,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("PingRequest"),
};

fn serialize_response(token: UID) -> Result<Frame> {
    let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(1024);
    let void = void::Void::create(&mut builder, &void::VoidArgs {});
    let ensure_table = void::EnsureTable::create(
        &mut builder,
        &void::EnsureTableArgs { void: Some(void) },
    );
    let fake_root = void::FakeRoot::create(
        &mut builder,
        &void::FakeRootArgs {
            error_or_type: void::ErrorOr::EnsureTable,
            error_or: Some(ensure_table.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let mut payload = builder.finished_data().to_vec();
    // See also: flow/README.md ### Flatbuffers/ObjectSerializer
    FileIdentifier::new(0x1ead4a)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload)?;
    // println!("reply: {:x?}", builder.finished_data());
    Ok(Frame { token, payload })
 }

pub async fn handle(conn: &mut Connection, parsed_file_identifier: ParsedFileIdentifier, frame: Frame) -> Result<()> {
    if parsed_file_identifier != PING_FILE_IDENTIFIER {
        return Err(format!("Expected PingRequest.  Got {:?}", parsed_file_identifier).into())
    }
    let fake_root = ping_request::root_as_fake_root(&frame.payload[..])?;
    let reply_promise = fake_root.ping_request().unwrap().reply_promise().unwrap();

    let uid = reply_promise.uid().unwrap();
    let uid = UID {
        uid: [uid.first(), uid.second()],
    };

    let frame = serialize_response(uid)?;
    conn.write_frame(frame).await
}