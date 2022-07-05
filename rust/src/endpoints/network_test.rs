#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/NetworkTestRequest_generated.rs"]
mod network_test_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/NetworkTestResponse_generated.rs"]
mod network_test_response;

use crate::flow::file_identifier::{FileIdentifier, IdentifierType, ParsedFileIdentifier};
use crate::flow::uid::{UID, WLTOKEN};
use crate::flow::{Flow, FlowFuture, FlowHandler, FlowMessage, FlowResponse, Frame, Peer, Result};
use crate::services::ConnectionKeeper;

use flatbuffers::FlatBufferBuilder;

use std::net::SocketAddr;
use std::sync::Arc;
use std::thread_local;

const NETWORK_TEST_REQUEST_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 0x3f4551,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("NetworkTestRequest"),
};

thread_local! {
    static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}
pub fn serialize_request(
    builder: &mut FlatBufferBuilder<'static>,
    dst: SocketAddr,
    request_len: u32,
    reply_size: u32,
) -> Result<FlowMessage> {
    let completion = UID::random_token();
    let wltoken = UID::well_known_token(WLTOKEN::ReservedForTesting);
    use network_test_request::{
        FakeRoot, FakeRootArgs, NetworkTestRequest, NetworkTestRequestArgs, ReplyPromise,
        ReplyPromiseArgs,
    };
    // let mut builder = FlatBufferBuilder::with_capacity(usize::min(
    //     128 + (request_len as usize),
    //     FLATBUFFERS_MAX_BUFFER_SIZE,
    // ));
    let request_len: usize = request_len.try_into()?;
    builder.start_vector::<u8>(request_len);
    for _i in 0..request_len {
        builder.push('.' as u8);
    }
    let payload = Some(builder.end_vector(request_len));
    let uid = network_test_request::UID::new(completion.uid[0], completion.uid[1]);
    let uid = Some(&uid);
    let reply_promise = Some(ReplyPromise::create(builder, &ReplyPromiseArgs { uid }));
    let network_test_request = Some(NetworkTestRequest::create(
        builder,
        &NetworkTestRequestArgs {
            payload,
            reply_size,
            reply_promise,
        },
    ));
    let fake_root = FakeRoot::create(
        builder,
        &FakeRootArgs {
            network_test_request,
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let mut payload: Vec<u8> = builder.finished_data().into();
    FileIdentifier::new(4146513)?.rewrite_flatbuf(&mut payload)?;
    // println!("reply: {:x?}", builder.finished_data());
    FlowMessage::new(
        Flow {
            dst: Peer::Remote(dst),
            src: Peer::Local(Some(completion)),
        },
        Frame::new(wltoken, payload, 0),
    )
}

pub fn deserialize_response(frame: Frame) -> Result<()> {
    let _fake_root = network_test_response::root_as_fake_root(frame.payload())?;
    println!("got network test response");
    Ok(())
}

pub struct NetworkTest {
    count: Arc<std::sync::atomic::AtomicU64>,
}

impl NetworkTest {
    pub fn new() -> Self {
        let this = Self {
            count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        let count = this.count.clone();
        tokio::spawn(async move {
            let mut last_count: u64 = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let count = count.load(std::sync::atomic::Ordering::Relaxed);
                println!("{} requests/second", count - last_count);
                last_count = count;
            }
            // TODO: Shutdown!
        });
        this
    }
}

thread_local! {
    static RESPONSE_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}
fn serialize_response(
    builder: &mut FlatBufferBuilder<'static>,
    token: UID,
    reply_size: usize,
) -> Result<Frame> {
    use network_test_response::{
        ErrorOr, FakeRoot, FakeRootArgs, NetworkTestResponse, NetworkTestResponseArgs,
    };
    builder.reset();
    builder.start_vector::<u8>(reply_size);
    for _i in 0..reply_size {
        builder.push('.' as u8);
    }
    let payload = builder.end_vector(reply_size);

    let network_test_response = NetworkTestResponse::create(
        builder,
        &NetworkTestResponseArgs {
            payload: Some(payload),
        },
    );

    let fake_root = FakeRoot::create(
        builder,
        &FakeRootArgs {
            error_or_type: ErrorOr::NetworkTestResponse,
            error_or: Some(network_test_response.as_union_value()),
        },
    );
    builder.finish(fake_root, Some("myfi"));
    let mut payload: Vec<u8> = builder.finished_data().into();
    FileIdentifier::new(14465374)?
        .to_error_or()?
        .rewrite_flatbuf(&mut payload)?;
    // println!("reply: {:x?}", builder.finished_data());
    Ok(Frame::new(token, payload, 0))
}

async fn handle(request: FlowMessage) -> Result<FlowResponse> {
    request
        .file_identifier()
        .ensure_expected(NETWORK_TEST_REQUEST_IDENTIFIER)?;
    // println!("frame: {:?}", frame.payload);
    let fake_root = network_test_request::root_as_fake_root(request.frame.payload())?;
    let network_test_request = fake_root.network_test_request().unwrap();
    // println!("Got: {:?}", network_test_request);
    let reply_promise = network_test_request.reply_promise().unwrap();

    let uid = reply_promise.uid().unwrap();
    let uid = UID {
        uid: [uid.first(), uid.second()],
    };

    let frame = RESPONSE_BUILDER.with(|builder| {
        serialize_response(
            &mut builder.borrow_mut(),
            uid,
            network_test_request.reply_size().try_into()?,
        )
    })?;
    Ok(Some(FlowMessage::new_response(request.flow, frame)?))
}

impl FlowHandler for NetworkTest {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(handle(msg))
    }
}

pub async fn network_test(
    peer: SocketAddr,
    svc: &ConnectionKeeper,
    request_sz: u32,
    response_sz: u32,
) -> Result<()> {
    let req = REQUEST_BUILDER.with(|builder| {
        serialize_request(&mut builder.borrow_mut(), peer, request_sz, response_sz)
    })?;
    let response_frame = svc.rpc(req).await?;
    deserialize_response(response_frame.frame)
}
