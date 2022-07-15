#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/RecruitMasterRequest_generated.rs"]
mod recruit_master_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/MasterInterface_generated.rs"]
mod master_interface;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/UpdateRecoveryDataRequest_generated.rs"]
mod update_recovery_data_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/GetCommitVersionRequest_generated.rs"]
mod get_commit_version_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/GetCommitVersionReply_generated.rs"]
mod get_commit_version_reply;

use crate::flow::{FlowMessage, FlowFuture, FlowHandler, FlowResponse, Result, uid::UID, file_identifier::{ParsedFileIdentifier, IdentifierType}};
use crate::services::ConnectionKeeper;

use flatbuffers::FlatBufferBuilder;

use std::sync::Arc;

const RECRUIT_MASTER_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 12684574,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("RecruitMasterRequest"),
};

const RECRUIT_MASTER_REPLY_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 5979145,
    inner_wrapper: IdentifierType::ErrorOr, // XXX not sure where this optional came from!
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("MasterInterface"),
};

const UPDATE_RECOVERY_DATA_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 13605417,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("UpdateRecoveryDataRequest"),
};

const GET_COMMIT_VERSION_REQUEST_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 16683181,
    inner_wrapper: IdentifierType::None,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("GetCommitVersionRequest"),
};

const GET_COMMIT_VERSION_REPLY_FILE_IDENTIFIER: ParsedFileIdentifier = ParsedFileIdentifier {
    file_identifier: 3568822,
    inner_wrapper: IdentifierType::ErrorOr,
    outer_wrapper: IdentifierType::None,
    file_identifier_name: Some("GetCommitVersionReply"),
};


// thread_local! {
//     static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
// }

thread_local! {
    static REPLY_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

type Version = i64;

#[allow(dead_code)]
struct MasterState {
    last_epoch_end: Version,
    recovery_transaction_version: Version,
    // TODO: prevTLogVersion, liveCommittedVersion use concurrency primitives we don't have (yet).
    database_locked: bool,
    proxy_metadata_version: Option<Version>,
    min_known_committed_version: Version,

    // coordinators: ServerCoordinators,

    version: Version, // the last version assigned to a proxy by getVersion()
    last_version_time: f64,
    reference_version: Option<Version>,
    // last_commit_proxy_version_replies: std::collections::BTreeMap<UID, CommitProxyVersionReplies>,

    // resolution_balancer: ResolutionBalancer;
    force_recovery: bool,
}

impl MasterState {
    fn new() -> std::sync::Mutex<Self> {
        std::sync::Mutex::new(
            MasterState {
                last_epoch_end: -1,
                recovery_transaction_version: -1,
                database_locked: false,
                proxy_metadata_version: None,
                min_known_committed_version: -1,
                version: -1,
                last_version_time: 0.0, // TODO: Use time type.
                reference_version: None,
                force_recovery: false,
            }
        )
    }
}

struct Master {
    svc: Arc<ConnectionKeeper>,
    wait_failure: UID,
    get_commit_version: UID,
    get_live_committed_version: UID,
    report_live_committed_version: UID,
    update_recovery_data: UID,
    state: std::sync::Mutex<MasterState>,
}

pub struct RecruitMaster {
    master: Arc<Master>,
}

pub struct WaitFailureHandler {
    master: Arc<Master>
}

async fn wait_failure(_request: FlowMessage, _master: Arc<Master>) -> Result<FlowResponse> {
    Err("wait_failure not implemented yet".into())
}

impl FlowHandler for WaitFailureHandler {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(wait_failure(msg, self.master.clone()))
    }
}

pub struct GetCommitVersionHandler {
    master: Arc<Master>
}

async fn get_commit_version(request: FlowMessage, master: Arc<Master>) -> Result<FlowResponse> {
    request.file_identifier().ensure_expected(GET_COMMIT_VERSION_REQUEST_FILE_IDENTIFIER)?;
    let fake_root = get_commit_version_request::root_as_fake_root(request.frame.payload())?;
    let req = fake_root.get_commit_version_request().unwrap();
    println!("{:?}", fake_root);

    let frame = REPLY_BUILDER.with(|builder| {
        use get_commit_version_reply::{ResolverMoveRef, GetCommitVersionReply, GetCommitVersionReplyArgs, FakeRoot, FakeRootArgs, ErrorOr};
        let builder = &mut builder.borrow_mut();

        let mut state = master.state.lock().unwrap();

        // TODO: track set of known proxies, and ignore unknown ones.
        // TODO: wait until we get request N-1 from this proxy.
        
        let prev_version = 
            if state.version == -1 {
                state.version = state.recovery_transaction_version;
                state.last_epoch_end
            } else {
                let prev_version = state.version;
                state.version = state.version + 100000;
                prev_version
            };
        let version = state.version;
        let request_num = req.request_num();
        let resolver_changes_version = 0;

        let resolver_changes = Some(builder.create_vector::<flatbuffers::WIPOffset<ResolverMoveRef>>(&[]));
        println!("== GetCommitVersionReply == version: {} prev_version: {} request_num: {}", version, prev_version, request_num);

        let get_commit_version_reply = GetCommitVersionReply::create(builder, &GetCommitVersionReplyArgs {
            version,
            prev_version,
            request_num,
            resolver_changes,
            resolver_changes_version,
        });
        let fake_root = FakeRoot::create(builder, &FakeRootArgs {
            error_or_type: ErrorOr::GetCommitVersionReply,
            error_or: Some(get_commit_version_reply.as_union_value()),
         });
        builder.finish(fake_root, Some("myfi"));
        super::finalize_reply(builder, req.reply().unwrap().uid().unwrap().into(), GET_COMMIT_VERSION_REPLY_FILE_IDENTIFIER)
    })?;
    Ok(Some(FlowMessage::new_response(request.flow, frame)?))
}

impl FlowHandler for GetCommitVersionHandler {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(get_commit_version(msg, self.master.clone()))
    }
}

pub struct GetLiveCommittedVersionHandler {
    master: Arc<Master>
}

async fn get_live_committed_version(_request: FlowMessage, _master: Arc<Master>) -> Result<FlowResponse> {
    Err("get_live_committed_version not implemented yet".into())
}

impl FlowHandler for GetLiveCommittedVersionHandler {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(get_live_committed_version(msg, self.master.clone()))
    }
}

pub struct ReportLiveCommittedVersionHandler {
    master: Arc<Master>
}

async fn report_live_committed_version(_request: FlowMessage, _master: Arc<Master>) -> Result<FlowResponse> {
    Err("report_live_committed_version not implemented yet".into())
}

impl FlowHandler for ReportLiveCommittedVersionHandler {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(report_live_committed_version(msg, self.master.clone()))
    }
}

pub struct UpdateRecoveryDataHandler {
    master: Arc<Master>
}

async fn update_recovery_data(request: FlowMessage, master: Arc<Master>) -> Result<FlowResponse> {
    request.file_identifier().ensure_expected(UPDATE_RECOVERY_DATA_REQUEST_FILE_IDENTIFIER)?;
    let fake_root = update_recovery_data_request::root_as_fake_root(request.frame.payload())?;
    let req = fake_root.update_recovery_data_request().unwrap();
    // println!("{:?}", fake_root);

    let commit_proxies = &req.commit_proxies().unwrap();
    println!("== UpdateRecoveryDataRequest == RecoveryTxnVersion={} LastEpochEnd={} NumCommitProxies={} VersionEpoch={}",
            req.recovery_transaction_version(),
            req.last_epoch_end(),
            commit_proxies.len(),
            req.version_epoch(),
            );
    let mut state = master.state.lock().unwrap();

    if state.recovery_transaction_version == -1 ||
       req.recovery_transaction_version() > state.recovery_transaction_version {
        state.recovery_transaction_version = req.recovery_transaction_version();
    }

    if state.last_epoch_end == -1 || req.last_epoch_end() > state.last_epoch_end {
        state.last_epoch_end = req.last_epoch_end();
    }

    if commit_proxies.len() > 0 {
        // state.last_commit_proxy_version_replies.clear();
        for _p in commit_proxies {
            // state.last_commit_proxy_version_replies[p.id()] = CommitProxyVersionReplies();
        }
    }

    if req.version_epoch_tag_hack() == 1 { // TODO 0?
        state.reference_version = Some(req.version_epoch());
    }

    // state.resolution_balancer.set_commit_proxies(commit_proxies);
    // state.resolution_balancer.set_resolvers(req.resolvers().unwrap());
    let uid = req.reply().unwrap().uid().unwrap();
    let uid: UID = uid.into();
    use crate::endpoints::ping_request;

    Ok(Some(FlowMessage::new_response(
        request.flow,
        ping_request::REPLY_BUILDER.with(|builder| ping_request::serialize_reply(&mut builder.borrow_mut(), uid))?
    )?))
}

impl FlowHandler for UpdateRecoveryDataHandler {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(update_recovery_data(msg, self.master.clone()))
    }
}


impl RecruitMaster {
    pub fn new(svc: Arc<ConnectionKeeper>) -> Self {
        let base = UID::random_token();
        let this = Self {
            master: Arc::new(Master {
                svc,
                wait_failure: base.clone(),
                get_commit_version: base.get_adjusted_token(1),
                get_live_committed_version: base.get_adjusted_token(2),
                report_live_committed_version: base.get_adjusted_token(3),
                update_recovery_data: base.get_adjusted_token(4),
                state: MasterState::new(),
            }),
        };

        let loopback_handler = this.master.svc.loopback_handler();
        loopback_handler.register_dynamic_endpoint(this.master.wait_failure.clone(), Box::new(WaitFailureHandler { master: this.master.clone() }));
        loopback_handler.register_dynamic_endpoint(this.master.get_commit_version.clone(), Box::new(GetCommitVersionHandler { master: this.master.clone() }));
        loopback_handler.register_dynamic_endpoint(this.master.get_live_committed_version.clone(), Box::new(GetLiveCommittedVersionHandler { master: this.master.clone() }));
        loopback_handler.register_dynamic_endpoint(this.master.report_live_committed_version.clone(), Box::new(ReportLiveCommittedVersionHandler { master: this.master.clone() }));
        loopback_handler.register_dynamic_endpoint(this.master.update_recovery_data.clone(), Box::new(UpdateRecoveryDataHandler { master: this.master.clone() }));
        this
    }
}

async fn recruit_master(request: FlowMessage, master: Arc<Master>) -> Result<FlowResponse> {
    request
        .file_identifier()
        .ensure_expected(RECRUIT_MASTER_REQUEST_FILE_IDENTIFIER)?;
    // println!("frame: {:?}", frame.payload);
    // let fake_root = network_test_request::root_as_fake_root(request.frame.payload())?;
    // let network_test_request = fake_root.network_test_request().unwrap();
    let fake_root = recruit_master_request::root_as_fake_root(request.frame.payload())?;
    println!("{:?}", fake_root);
    let recruit_master_request = fake_root.recruit_master_request().unwrap();

    let public_addr = match master.svc.public_addr {
        Some(public_addr) => public_addr,
        None => { return Err("Can't invoke recruit master; not listening on a port!".into()); }
    };

    REPLY_BUILDER.with(|builder| {
        let builder = &mut builder.borrow_mut();

        let locality = super::create_locality_data(builder);

        use super::serialize_request_stream;
        let wait_failure = serialize_request_stream(builder, &public_addr, &master.wait_failure)?;

        use master_interface::{FakeRoot, FakeRootArgs, ErrorOr, MasterInterface, MasterInterfaceArgs};
        
        let master_interface = MasterInterface::create(builder, &MasterInterfaceArgs {
            locality,
            wait_failure,
        });

        let fake_root = FakeRoot::create(builder, &FakeRootArgs {
            error_or_type: ErrorOr::MasterInterface,
            error_or: Some(master_interface.as_union_value()),
        });

        builder.finish(fake_root, Some("myfi"));
        let frame = super::finalize_reply(builder, recruit_master_request.reply().unwrap().uid().unwrap().into(), RECRUIT_MASTER_REPLY_FILE_IDENTIFIER)?;

        Ok(Some(FlowMessage::new_response(request.flow, frame)?))
    })
}

impl FlowHandler for RecruitMaster {
    fn handle(&self, msg: FlowMessage) -> FlowFuture {
        Box::pin(recruit_master(msg, self.master.clone()))
    }
}
