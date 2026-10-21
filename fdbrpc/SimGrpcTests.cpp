/*
 * SimGrpcTests.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Real gRPC (callback API) between simulated processes over SimEventEngine. Each seed picks its own combination of
// TLS, memory quota, keepalive, buggify and call gating; see the SimGrpcTestOptions trace event.

#ifdef FLOW_GRPC_ENABLED

#ifdef __APPLE__
#include <mach/mach.h>
#else
#include <dirent.h>
#endif

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <grpc/grpc.h>
#include <grpc/support/time.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>

#include "fdbrpc/grpc/SimEventEngine.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/test/echo.grpc.pb.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"

namespace {

using fdbrpc::test::EchoRequest;
using fdbrpc::test::EchoResponse;
using fdbrpc::test::TestEchoService;

int threadCount() {
#ifndef __APPLE__
	DIR* d = opendir("/proc/self/task");
	if (!d) {
		return -1;
	}
	int n = 0;
	while (auto* e = readdir(d)) {
		n += e->d_name[0] != '.';
	}
	closedir(d);
	return n;
#else
	thread_act_array_t threads;
	mach_msg_type_number_t count = 0;
	if (task_threads(mach_task_self(), &threads, &count) != KERN_SUCCESS) {
		return -1;
	}
	for (mach_msg_type_number_t i = 0; i < count; ++i) {
		mach_port_deallocate(mach_task_self(), threads[i]);
	}
	vm_deallocate(mach_task_self(), (vm_address_t)threads, count * sizeof(thread_act_t));
	return static_cast<int>(count);
#endif
}

class EchoService final : public TestEchoService::CallbackService {
	grpc::ServerUnaryReactor* Echo(grpc::CallbackServerContext* ctx,
	                               const EchoRequest* req,
	                               EchoResponse* resp) override {
		resp->set_message(req->message());
		auto* reactor = ctx->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

	// Replies with message+"0" .. message+"9".
	grpc::ServerWriteReactor<EchoResponse>* EchoRecvStream10(grpc::CallbackServerContext*,
	                                                         const EchoRequest* req) override {
		class Reactor final : public grpc::ServerWriteReactor<EchoResponse> {
		public:
			explicit Reactor(std::string msg) : msg_(std::move(msg)) { next(); }
			void OnWriteDone(bool ok) override {
				if (!ok) {
					Finish(grpc::Status(grpc::StatusCode::CANCELLED, "write failed"));
					return;
				}
				next();
			}
			void OnDone() override { delete this; }

		private:
			void next() {
				if (i_ == 10) {
					Finish(grpc::Status::OK);
					return;
				}
				resp_.set_message(msg_ + std::to_string(i_++));
				StartWrite(&resp_);
			}
			std::string msg_;
			EchoResponse resp_;
			int i_ = 0;
		};
		return new Reactor(req->message());
	}

	// Replies with the concatenation of every message the client sent.
	grpc::ServerReadReactor<EchoRequest>* EchoSendStream10(grpc::CallbackServerContext*, EchoResponse* resp) override {
		class Reactor final : public grpc::ServerReadReactor<EchoRequest> {
		public:
			explicit Reactor(EchoResponse* resp) : resp_(resp) { StartRead(&req_); }
			void OnReadDone(bool ok) override {
				if (ok) {
					acc_ += req_.message();
					StartRead(&req_);
				} else {
					resp_->set_message(acc_);
					Finish(grpc::Status::OK);
				}
			}
			void OnDone() override { delete this; }

		private:
			EchoResponse* resp_;
			EchoRequest req_;
			std::string acc_;
		};
		return new Reactor(resp);
	}
};

// gRPC completion callbacks run inside gRPC's own call stack, and on the engine's home process; resume the caller on
// its own process from a fresh task.
Future<Void> deliver(std::shared_ptr<Promise<grpc::Status>> p, grpc::Status s, ISimulator::ProcessInfo* caller) {
	co_await g_simulator->onProcess(caller);
	p->send(s);
}

struct Harness {
	std::shared_ptr<grpc::Channel> channel;
	bool gate = false;
	std::unique_ptr<TestEchoService::Stub> stub;
	std::vector<Future<Void>> deliveries;
	std::map<std::string, int> outcomes;
	int mismatches = 0;
};

// Calls queued inside gRPC while the subchannel is not READY are released in a nondeterministic order, so hold them
// here, in Flow, until it is. Returns false if the channel did not become READY within `timeout`.
Future<bool> admit(Harness* h, std::string phase, double timeout) {
	if (!h->gate) {
		co_return true;
	}
	double giveUp = now() + timeout;
	while (h->channel->GetState(true) != GRPC_CHANNEL_READY) {
		if (now() >= giveUp) {
			++h->outcomes[phase + ":gated-timeout"];
			co_return false;
		}
		co_await delay(0.01);
	}
	co_return true;
}

void setDeadline(grpc::ClientContext* ctx, double timeout) {
	ctx->set_deadline(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
	                               gpr_time_from_micros(static_cast<int64_t>(timeout * 1e6), GPR_TIMESPAN)));
}

class RecvStreamClient final : public grpc::ClientReadReactor<EchoResponse> {
public:
	RecvStreamClient(Harness* h, std::string msg, double timeout, std::shared_ptr<Promise<grpc::Status>> done)
	  : h_(h), done_(std::move(done)), caller_(g_simulator->getCurrentProcess()) {
		req_.set_message(std::move(msg));
		setDeadline(&ctx_, timeout);
		h_->stub->async()->EchoRecvStream10(&ctx_, &req_, this);
		StartRead(&resp_);
		StartCall();
	}
	void OnReadDone(bool ok) override {
		if (ok) {
			if (resp_.message() != req_.message() + std::to_string(received_++)) {
				++h_->mismatches;
			}
			StartRead(&resp_);
		}
	}
	void OnDone(const grpc::Status& s) override {
		if (s.ok() && received_ != 10) {
			++h_->mismatches;
		}
		h_->deliveries.push_back(deliver(done_, s, caller_));
		delete this;
	}

private:
	Harness* h_;
	std::shared_ptr<Promise<grpc::Status>> done_;
	ISimulator::ProcessInfo* caller_;
	grpc::ClientContext ctx_;
	EchoRequest req_;
	EchoResponse resp_;
	int received_ = 0;
};

class SendStreamClient final : public grpc::ClientWriteReactor<EchoRequest> {
public:
	SendStreamClient(Harness* h,
	                 std::vector<std::string> parts,
	                 double timeout,
	                 std::shared_ptr<Promise<grpc::Status>> done)
	  : h_(h), done_(std::move(done)), caller_(g_simulator->getCurrentProcess()), parts_(std::move(parts)) {
		for (auto& p : parts_) {
			expected_ += p;
		}
		setDeadline(&ctx_, timeout);
		h_->stub->async()->EchoSendStream10(&ctx_, &resp_, this);
		next();
		StartCall();
	}
	void OnWriteDone(bool ok) override {
		if (ok) {
			next();
		}
	}
	void OnDone(const grpc::Status& s) override {
		if (s.ok() && resp_.message() != expected_) {
			++h_->mismatches;
		}
		h_->deliveries.push_back(deliver(done_, s, caller_));
		delete this;
	}

private:
	void next() {
		if (sent_ == parts_.size()) {
			StartWritesDone();
			return;
		}
		req_.set_message(parts_[sent_++]);
		StartWrite(&req_);
	}
	Harness* h_;
	std::shared_ptr<Promise<grpc::Status>> done_;
	ISimulator::ProcessInfo* caller_;
	grpc::ClientContext ctx_;
	std::vector<std::string> parts_;
	std::string expected_;
	EchoRequest req_;
	EchoResponse resp_;
	size_t sent_ = 0;
};

std::string randomMessage() {
	return deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, 4096));
}

void countOutcome(Harness* h, const std::string& phase, const std::string& kind, const grpc::Status& s) {
	++h->outcomes[phase + ":" + kind + ":" + std::to_string(static_cast<int>(s.error_code()))];
}

Future<Void> recvStreamOnce(Harness* h, std::string phase, double timeout) {
	if (!co_await admit(h, phase, timeout)) {
		co_return;
	}
	std::string msg = randomMessage();
	auto done = std::make_shared<Promise<grpc::Status>>();
	Future<grpc::Status> result = done->getFuture();
	new RecvStreamClient(h, msg, timeout, done);
	countOutcome(h, phase, "recv", co_await result);
}

Future<Void> sendStreamOnce(Harness* h, std::string phase, double timeout) {
	if (!co_await admit(h, phase, timeout)) {
		co_return;
	}
	std::vector<std::string> parts(deterministicRandom()->randomInt(0, 12));
	for (auto& p : parts) {
		p = randomMessage();
	}
	auto done = std::make_shared<Promise<grpc::Status>>();
	Future<grpc::Status> result = done->getFuture();
	new SendStreamClient(h, std::move(parts), timeout, done);
	countOutcome(h, phase, "send", co_await result);
}

Future<Void> echoOnce(Harness* h, std::string phase, double timeout) {
	if (!co_await admit(h, phase, timeout)) {
		co_return;
	}
	auto ctx = std::make_shared<grpc::ClientContext>();
	auto req = std::make_shared<EchoRequest>();
	auto resp = std::make_shared<EchoResponse>();
	std::string msg = deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, 4096));
	req->set_message(msg);
	ctx->set_deadline(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
	                               gpr_time_from_micros(static_cast<int64_t>(timeout * 1e6), GPR_TIMESPAN)));
	auto done = std::make_shared<Promise<grpc::Status>>();
	Future<grpc::Status> result = done->getFuture();
	auto* caller = g_simulator->getCurrentProcess();
	h->stub->async()->Echo(ctx.get(), req.get(), resp.get(), [h, done, ctx, req, resp, caller](grpc::Status s) {
		h->deliveries.push_back(deliver(done, s, caller));
	});
	grpc::Status s = co_await result;
	if (s.ok() && resp->message() != msg) {
		++h->mismatches;
	}
	countOutcome(h, phase, "unary", s);
}

Future<Void> runPhase(Harness* h, std::string phase, int calls, int concurrency, double timeout) {
	std::vector<Future<Void>> inflight;
	for (int i = 0; i < calls; ++i) {
		inflight.push_back(echoOnce(h, phase, timeout));
		if (static_cast<int>(inflight.size()) >= concurrency) {
			co_await waitForAll(inflight);
			inflight.clear();
		}
		co_await delay(deterministicRandom()->random01() * 0.05);
	}
	co_await waitForAll(inflight);
}

Future<Void> runMixedPhase(Harness* h, std::string phase, int calls, int concurrency, double timeout) {
	std::vector<Future<Void>> inflight;
	for (int i = 0; i < calls; ++i) {
		double r = deterministicRandom()->random01();
		inflight.push_back(r < 0.6   ? echoOnce(h, phase, timeout)
		                   : r < 0.8 ? recvStreamOnce(h, phase, timeout)
		                             : sendStreamOnce(h, phase, timeout));
		if (static_cast<int>(inflight.size()) >= concurrency) {
			co_await waitForAll(inflight);
			inflight.clear();
		}
		co_await delay(deterministicRandom()->random01() * 0.05);
	}
	co_await waitForAll(inflight);
}

Future<Void> onSimProcess(ISimulator::ProcessInfo* p, std::function<Future<Void>()> body) {
	co_await g_simulator->onProcess(p);
	co_await body();
}

ISimulator::ProcessInfo* newSimProcess(const char* name, uint32_t ip, uint16_t port) {
	Standalone<StringRef> id(deterministicRandom()->randomUniqueID().toString());
	auto* p = g_simulator->newProcess(
	    name,
	    IPAddress(ip),
	    port,
	    false,
	    1,
	    LocalityData(Optional<Standalone<StringRef>>(), id, id, Optional<Standalone<StringRef>>()),
	    makeReference<simulator::ProcessInfoMetadata>("test", true),
	    "",
	    "",
	    currentProtocolVersion(),
	    false);
	p->excludeFromRestarts = true;
	return p;
}

struct Options {
	bool buggify = false;
	bool tls = false;
	// Hold calls in Flow until the channel is READY; see admit().
	bool gate = false;
	// Wait for the channel to become READY before the first call.
	bool warmup = false;
	bool keepalive = false;
	int64_t quota = 0; // bytes; 0 = unlimited
	std::string caPem, certPem, keyPem;
};

// Test-only CA and server certificate for DNS:foundationdb.test, valid 2020-2100. Fixed rather than generated:
// generated certificates take their validity window from the wall clock, which changes the signatures, the handshake
// length, and with them the simulation, from run to run.
constexpr const char* kTestCaPem = R"(-----BEGIN CERTIFICATE-----
MIIBojCCAUmgAwIBAgIUCyHKrQjZ8KCo18MkaM5oNHQ1sIkwCgYIKoZIzj0EAwIw
HjEcMBoGA1UEAwwTZmRiLXNpbWdycGMtdGVzdC1jYTAgFw0yMDAxMDEwMDAwMDBa
GA8yMTAwMDEwMTAwMDAwMFowHjEcMBoGA1UEAwwTZmRiLXNpbWdycGMtdGVzdC1j
YTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABK9xrY2LlHy40ZMjJLNnjOXU2KsU
BhcjgyjwALyoubyIX7RNRWWjjs3kolSJDT/3u3cMVitQ2k56t0ZyepjzAlmjYzBh
MB0GA1UdDgQWBBS/4Vk6JcKhZFAvdYFv2rLWL4G93zAfBgNVHSMEGDAWgBS/4Vk6
JcKhZFAvdYFv2rLWL4G93zAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIB
BjAKBggqhkjOPQQDAgNHADBEAiBK9H+8mW5cvYMvFUWOapY3HJRSjNMreSve4i0C
w+BYUAIgYgsnmnj2ss6ulG4xI2U/VCcdmBKsUBDcnPM0kEmirWQ=
-----END CERTIFICATE-----
)";
constexpr const char* kTestServerCertPem = R"(-----BEGIN CERTIFICATE-----
MIIBvTCCAWOgAwIBAgIBAjAKBggqhkjOPQQDAjAeMRwwGgYDVQQDDBNmZGItc2lt
Z3JwYy10ZXN0LWNhMCAXDTIwMDEwMTAwMDAwMFoYDzIxMDAwMTAxMDAwMDAwWjAc
MRowGAYDVQQDDBFmb3VuZGF0aW9uZGIudGVzdDBZMBMGByqGSM49AgEGCCqGSM49
AwEHA0IABKc6WfhSZ/xK7opnuYymLQHDpKsRxPbRfBQhVQM42SidKNyyb2USPcFY
sGS7LaCDHo0UOmI78KOTg8vEfnXD0jGjgZEwgY4wHAYDVR0RBBUwE4IRZm91bmRh
dGlvbmRiLnRlc3QwCQYDVR0TBAIwADAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0lBAww
CgYIKwYBBQUHAwEwHQYDVR0OBBYEFKfbMR3IEpv3h6DWtamKX7lWPVf4MB8GA1Ud
IwQYMBaAFL/hWTolwqFkUC91gW/astYvgb3fMAoGCCqGSM49BAMCA0gAMEUCIDNm
zI+ih1rAZf48o80/Q+4VscJXzlgjqgs5c0n4aRd6AiEA9+JhkFEOSv073YqjNx8y
aVft18GK1w1WtNLePm0+hio=
-----END CERTIFICATE-----
)";
constexpr const char* kTestServerKeyPem = R"(-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg9pzZ6d2KzeqZBqiO
bZsekH/BtHjEeJOH5fmHkgCVaxahRANCAASnOln4Umf8Su6KZ7mMpi0Bw6SrEcT2
0XwUIVUDONkonSjcsm9lEj3BWLBkuy2ggx6NFDpiO/Cjk4PLxH51w9Ix
-----END PRIVATE KEY-----
)";

Options chooseOptions() {
	Options o;
	o.buggify = deterministicRandom()->coinflip();
	o.tls = deterministicRandom()->coinflip();
	o.gate = deterministicRandom()->coinflip();
	if (o.tls) {
		o.caPem = kTestCaPem;
		o.certPem = kTestServerCertPem;
		o.keyPem = kTestServerKeyPem;
	}
	if (o.buggify) {
		enableGeneralBuggify();
	}
	return o;
}

void traceOptions(const char* test, const Options& o) {
	TraceEvent("SimGrpcTestOptions")
	    .detail("Test", test)
	    .detail("Buggify", o.buggify)
	    .detail("TLS", o.tls)
	    .detail("Gate", o.gate)
	    .detail("Warmup", o.warmup)
	    .detail("Keepalive", o.keepalive)
	    .detail("Quota", o.quota);
}

// Logs the outcome of every call and the engine's counters, and checks the test's invariants.
void checkResult(const char* test, const std::map<std::string, int>& outcomes, int mismatches) {
	int ok = 0;
	for (auto& [k, v] : outcomes) {
		TraceEvent("SimGrpcTestOutcome").detail("Test", test).detail("Outcome", k).detail("Count", v);
		ok += k.ends_with(":0") ? v : 0;
	}
	auto stats = sim_grpc::simEventEngineStats();
	TraceEvent("SimGrpcTestResult")
	    .detail("Test", test)
	    .detail("Ok", ok)
	    .detail("Mismatches", mismatches)
	    .detail("Connects", stats.connects)
	    .detail("ClockReads", stats.clockReads)
	    .detail("OffThreadCalls", stats.offThreadCalls)
	    .detail("PendingClosures", stats.droppedClosures)
	    .detail("StrandedClosures", stats.strandedClosures)
	    .detail("Now", now())
	    .detail("FinalRandom", deterministicRandom()->randomUInt64());
	ASSERT_EQ(mismatches, 0);
	ASSERT_GT(ok, 0);
	ASSERT_EQ(stats.offThreadCalls, 0);
}

std::shared_ptr<grpc::ServerCredentials> serverCredentials(const Options& o) {
	if (!o.tls) {
		return grpc::InsecureServerCredentials();
	}
	grpc::SslServerCredentialsOptions opts;
	opts.pem_key_cert_pairs.push_back({ o.keyPem, o.certPem });
	return grpc::SslServerCredentials(opts);
}

std::shared_ptr<grpc::ChannelCredentials> channelCredentials(const Options& o, grpc::ChannelArguments& args) {
	if (!o.tls) {
		return grpc::InsecureChannelCredentials();
	}
	args.SetSslTargetNameOverride("foundationdb.test");
	grpc::SslCredentialsOptions opts;
	opts.pem_root_certs = o.caPem;
	return grpc::SslCredentials(opts);
}

// A small quota makes gRPC's memory reclaimers run.
void applyQuota(const Options& o, grpc::ServerBuilder& builder) {
	if (o.quota > 0) {
		static grpc::ResourceQuota quota("simgrpc-server");
		quota.Resize(o.quota);
		builder.SetResourceQuota(quota);
	}
}

void applyQuota(const Options& o, grpc::ChannelArguments& args) {
	if (o.quota > 0) {
		static grpc::ResourceQuota quota("simgrpc-client");
		quota.Resize(o.quota);
		args.SetResourceQuota(quota);
	}
}

} // namespace

TEST_CASE("/fdbrpc/simgrpc/echo") {
	ASSERT(g_network->isSimulated());
	Options opts = chooseOptions();
	opts.warmup = deterministicRandom()->coinflip();
	// Smaller quotas can wedge the test.
	opts.quota = deterministicRandom()->coinflip() ? 0 : 1 << 20;
	traceOptions("echo", opts);
	int threadsBefore = threadCount();
	auto* original = g_simulator->getCurrentProcess();
	sim_grpc::installSimEventEngine();

	auto* serverProc = newSimProcess("GrpcServer", 0x03030303, 5000);
	auto* clientProc = newSimProcess("GrpcClient", 0x04040404, 5000);
	const std::string target = "ipv4:3.3.3.3:5000";

	co_await g_simulator->onProcess(serverProc);
	auto service = std::make_unique<EchoService>();
	grpc::ServerBuilder builder;
	builder.AddListeningPort("3.3.3.3:5000", serverCredentials(opts));
	applyQuota(opts, builder);
	builder.RegisterService(service.get());
	std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
	ASSERT(server != nullptr);

	co_await g_simulator->onProcess(clientProc);
	auto harness = std::make_unique<Harness>();
	grpc::ChannelArguments channelArgs;
	// gRPC otherwise honours http_proxy/grpc_proxy from the host environment, which is not simulated.
	channelArgs.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
	channelArgs.SetPointer(sim_grpc::kSimProcessChannelArg, clientProc);
	applyQuota(opts, channelArgs);
	std::shared_ptr<grpc::Channel> channel =
	    grpc::CreateCustomChannel(target, channelCredentials(opts, channelArgs), channelArgs);
	harness->channel = channel;
	harness->gate = opts.gate;
	harness->stub = TestEchoService::NewStub(channel);
	int threadsAfterSetup = threadCount();
	if (opts.warmup) {
		while (channel->GetState(true) != GRPC_CHANNEL_READY) {
			co_await delay(0.1);
		}
	}

	co_await runPhase(harness.get(), "steady", 200, 8, 10.0);

	g_simulator->clogInterface(IPAddress(0x03030303), 3.0);
	co_await runPhase(harness.get(), "clogged", 10, 10, 1.0);
	co_await delay(3.0);
	co_await runPhase(harness.get(), "recovered", 50, 8, 10.0);

	g_simulator->killProcess(serverProc, ISimulator::KillType::KillInstantly);
	co_await runPhase(harness.get(), "killed", 10, 10, 2.0);

	TraceEvent("SimGrpcTestThreads")
	    .detail("Before", threadsBefore)
	    .detail("AfterSetup", threadsAfterSetup)
	    .detail("End", threadCount());
	checkResult("echo", harness->outcomes, harness->mismatches);
	// grpc::Server::Shutdown() and channel teardown block on a completion queue that only this thread can drive,
	// so the test deliberately leaks them.
	(void)server.release();
	(void)service.release();
	(void)new std::shared_ptr<grpc::Channel>(channel);
	(void)harness.release();
	co_await g_simulator->onProcess(original);
}

namespace {

// grpc::Server::Shutdown() blocks on a completion queue this thread would have to drive; use the core API with a
// callback completion queue instead. After `grace` seconds, calls still running are cancelled.
struct ShutdownNotify : grpc_completion_queue_functor {
	Promise<Void> done;
	ISimulator::ProcessInfo* caller = nullptr;
	Future<Void> delivery;
};

Future<Void> deliverShutdown(ShutdownNotify* n) {
	co_await g_simulator->onProcess(n->caller);
	n->done.send(Void());
}

void onServerShutdown(grpc_completion_queue_functor* f, int) {
	auto* n = static_cast<ShutdownNotify*>(f);
	n->delivery = deliverShutdown(n);
}

void onCqShutdown(grpc_completion_queue_functor*, int) {}

Future<Void> shutdownServer(grpc::Server* server, double grace) {
	static grpc_completion_queue_functor cqShutdown{ onCqShutdown, 0, 0, nullptr };
	auto* n = new ShutdownNotify();
	n->functor_run = onServerShutdown;
	n->inlineable = 0;
	n->caller = g_simulator->getCurrentProcess();
	grpc_completion_queue* cq = grpc_completion_queue_create_for_callback(&cqShutdown, nullptr);
	grpc_server_shutdown_and_notify(server->c_server(), cq, n);
	Future<Void> done = n->done.getFuture();
	co_await (done || delay(grace));
	if (!done.isReady()) {
		grpc_server_cancel_all_calls(server->c_server());
		co_await done;
	}
	// The grpc::Server object is leaked: its destructor would run the blocking shutdown path again.
}

// Builds and starts an echo server listening on `addr`, from `proc`'s context. The caller leaks the result: see the
// teardown note in /fdbrpc/simgrpc/echo.
Future<grpc::Server*> startServer(ISimulator::ProcessInfo* proc, std::string addr, Options opts) {
	co_await g_simulator->onProcess(proc);
	auto* service = new EchoService();
	grpc::ServerBuilder builder;
	builder.AddListeningPort(addr, serverCredentials(opts));
	applyQuota(opts, builder);
	if (opts.keepalive) {
		builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 500);
		builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
	}
	builder.RegisterService(service);
	grpc::Server* server = builder.BuildAndStart().release();
	ASSERT(server != nullptr);
	co_return server;
}

Future<Harness*> startClient(ISimulator::ProcessInfo* proc, std::string target, Options opts) {
	co_await g_simulator->onProcess(proc);
	auto* h = new Harness();
	grpc::ChannelArguments args;
	args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
	args.SetPointer(sim_grpc::kSimProcessChannelArg, proc);
	if (opts.keepalive) {
		// Without keepalive a connection to a killed peer stays READY forever: nothing is ever sent back.
		args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 1000);
		args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 1000);
		args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
		args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
	}
	applyQuota(opts, args);
	h->channel = grpc::CreateCustomChannel(target, channelCredentials(opts, args), args);
	h->gate = opts.gate;
	h->stub = TestEchoService::NewStub(h->channel);
	co_return h;
}

Future<Void> allClients(std::vector<ISimulator::ProcessInfo*> procs,
                        std::vector<Harness*> clients,
                        std::string phase,
                        int calls,
                        double timeout) {
	std::vector<Future<Void>> runs;
	for (size_t i = 0; i < clients.size(); ++i) {
		Harness* h = clients[i];
		runs.push_back(onSimProcess(procs[i], [=]() { return runMixedPhase(h, phase, calls, 6, timeout); }));
	}
	co_await waitForAll(runs);
}

} // namespace

// Unary + server-streaming + client-streaming RPCs from three clients, then the server is killed and a new server
// process takes over the same address.
TEST_CASE("/fdbrpc/simgrpc/extended") {
	ASSERT(g_network->isSimulated());
	Options opts = chooseOptions();
	opts.keepalive = deterministicRandom()->coinflip();
	// Unlimited quota only: at 1 MiB the server answers with GOAWAY "Buffers full" and this test can wedge.
	traceOptions("extended", opts);
	auto* original = g_simulator->getCurrentProcess();
	sim_grpc::installSimEventEngine();

	const uint32_t serverIp = 0x03030303;
	const std::string addr = "3.3.3.3:5000";
	auto* server1 = newSimProcess("GrpcServer1", serverIp, 5000);
	(void)co_await startServer(server1, addr, opts);

	std::vector<ISimulator::ProcessInfo*> procs;
	std::vector<Harness*> clients;
	for (uint32_t i = 0; i < 3; ++i) {
		procs.push_back(newSimProcess("GrpcClient", 0x04040401 + i, 5000));
		clients.push_back(co_await startClient(procs.back(), "ipv4:" + addr, opts));
	}

	co_await allClients(procs, clients, "steady", 120, 10.0);

	g_simulator->killProcess(server1, ISimulator::KillType::KillInstantly);
	co_await allClients(procs, clients, "down", 10, 2.0);

	auto* server2 = newSimProcess("GrpcServer2", serverIp, 5000);
	grpc::Server* running = co_await startServer(server2, addr, opts);
	co_await allClients(procs, clients, "restarted", 120, 10.0);

	// Graceful shutdown under load: GOAWAY to clients, in-flight calls drain, then a new server on the same process.
	Future<Void> draining = allClients(procs, clients, "draining", 30, 10.0);
	co_await delay(0.2);
	co_await g_simulator->onProcess(server2);
	co_await shutdownServer(running, 2.0);
	co_await g_simulator->onProcess(original);
	co_await draining;
	(void)co_await startServer(server2, addr, opts);
	co_await allClients(procs, clients, "reopened", 60, 10.0);

	std::map<std::string, int> outcomes;
	int mismatches = 0;
	for (auto* h : clients) {
		for (auto& [k, v] : h->outcomes) {
			outcomes[k] += v;
		}
		mismatches += h->mismatches;
	}
	TraceEvent("SimGrpcTestThreads").detail("End", threadCount());
	checkResult("extended", outcomes, mismatches);
	co_await g_simulator->onProcess(original);
}

#endif // FLOW_GRPC_ENABLED
