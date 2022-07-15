pub mod get_leader;
pub mod network_test;
pub mod ping_request;
pub mod protocol_info;
pub mod register_worker;

use crate::flow::file_identifier::{FileIdentifier, ParsedFileIdentifier};
use crate::flow::{uid::UID, Flow, FlowMessage, Frame, Peer, Result};

use std::net::SocketAddr;

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

fn create_request_stream(
    builder: &mut flatbuffers::FlatBufferBuilder<'static>,
    public_address: &SocketAddr,
) -> Result<(
    UID,
    Option<flatbuffers::WIPOffset<crate::common_generated::RequestStream<'static>>>,
)> {
    use crate::common_generated::{
        Endpoint, EndpointArgs, IPAddress, IPAddressArgs, IPv4, NetworkAddress, NetworkAddressArgs,
        NetworkAddressList, NetworkAddressListArgs, OptionalNetworkAddress, RequestStream,
        RequestStreamArgs, Void, VoidArgs,
    };
    let uid = UID::random_token();
    let token = Some(uid.clone().into());
    let (ip4, port) = match public_address {
        SocketAddr::V4(socket) => (Some(IPv4::new(1, (*socket.ip()).into())), socket.port()), // TODO: Check endian-ness.
        x => {
            return Err(format!("unsupported transport: {:?}", x).into());
        }
    };
    let ip = Some(IPAddress::create(
        builder,
        &IPAddressArgs { ip4: ip4.as_ref() },
    ));
    let address = Some(NetworkAddress::create(
        builder,
        &NetworkAddressArgs {
            ip,
            port,
            flags: 0,
            from_hostname: false,
        },
    ));
    let secondary_address_type = OptionalNetworkAddress::Void;
    let secondary_address = Void::create(builder, &VoidArgs {});
    let addresses = Some(NetworkAddressList::create(
        builder,
        &NetworkAddressListArgs {
            address,
            secondary_address_type,
            secondary_address: Some(secondary_address.as_union_value()),
        },
    ));
    let endpoint = Some(Endpoint::create(
        builder,
        &EndpointArgs {
            addresses,
            token: token.as_ref(),
        },
    ));
    Ok((
        uid,
        Some(RequestStream::create(
            builder,
            &RequestStreamArgs { endpoint },
        )),
    ))
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
