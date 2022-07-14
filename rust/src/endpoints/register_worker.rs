#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/RegisterWorkerRequest_generated.rs"]
mod register_worker_request;

// #[allow(dead_code, unused_imports)]
// #[path = "../../target/flatbuffers/RegisterWorkerReply_generated.rs"]
// mod register_worker_reply;

use crate::flow::{Endpoint, FlowMessage, Result, file_identifier::{IdentifierType, ParsedFileIdentifier}};
use crate::services::ConnectionKeeper;

use flatbuffers::FlatBufferBuilder;

use std::sync::Arc;

const REGISTER_WORKER_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 14332605,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("RegisterWorkerRequest"),
};

const REGISTER_WORKER_RESPONSE_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 16475696,
    inner_wrapper: IdentifierType::Optional,
    outer_wrapper: IdentifierType::ErrorOr,
    file_identifier_name: Some("RegisterWorkerReply"),
};

thread_local! {
    static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

thread_local! {
    static REPLY_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

fn serialize_request(builder: &mut FlatBufferBuilder<'static>, endpoint: Endpoint) -> Result<FlowMessage> {
    // use register_worker_request::{FakeRoot, FakeRootArgs, RegisterWorkerRequest, RegisterWorkerRequestArgs};
    unimplemented!();
}

pub async fn register_worker(svc: &Arc<ConnectionKeeper>, endpoint: Endpoint) -> Result<()> {
    let req = REQUEST_BUILDER.with(|builder| serialize_request(&mut builder.borrow_mut(), endpoint))?;
    let res = svc.rpc(req).await?;

    Ok(())
}