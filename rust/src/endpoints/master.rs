#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/RecruitMasterRequest_generated.rs"]
mod recruit_master_request;

#[allow(dead_code, unused_imports)]
#[path = "../../target/flatbuffers/MasterInterface_generated.rs"]
mod master_interface;

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
    inner_wrapper: IdentifierType::Optional,
    outer_wrapper: IdentifierType::ErrorOr,
    file_identifier_name: Some("MasterInterface"),
};

// thread_local! {
//     static REQUEST_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
// }

thread_local! {
    static REPLY_BUILDER : std::cell::RefCell<FlatBufferBuilder<'static>> = std::cell::RefCell::new(FlatBufferBuilder::with_capacity(1024));
}

struct Master {
    svc: Arc<ConnectionKeeper>,
    wait_failure: UID,
    get_commit_version: UID,
    get_live_committed_version: UID,
    report_live_committed_version: UID,
    update_recovery_data: UID,
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

async fn get_commit_version(_request: FlowMessage, _master: Arc<Master>) -> Result<FlowResponse> {
    Err("get_commit_version not implemented yet".into())
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

async fn update_recovery_data(_request: FlowMessage, _master: Arc<Master>) -> Result<FlowResponse> {
    Err("update_recovery_data not implemented yet".into())
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
