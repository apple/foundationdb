/*
 * GrpcEcho.cpp
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

// Real gRPC (callback API) inside a full fdbserver simulation, over SimEventEngine. Client 0 runs echo servers on
// dedicated simulated processes and drives load against them while killing, clogging, gracefully restarting and
// replacing those servers.

#ifdef FLOW_GRPC_ENABLED

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <grpc/grpc.h>
#include <grpc/support/time.h>
#include <grpcpp/grpcpp.h>

#include "fdbrpc/grpc/SimEventEngine.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/test/echo.grpc.pb.h"
#include "fdbserver/core/FDBSimulatorProcessInfo.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Trace.h"
#include "flow/flow.h"

namespace {

using fdbrpc::test::EchoRequest;
using fdbrpc::test::EchoResponse;
using fdbrpc::test::TestEchoService;

class EchoService final : public TestEchoService::CallbackService {
	grpc::ServerUnaryReactor* Echo(grpc::CallbackServerContext* ctx,
	                               const EchoRequest* req,
	                               EchoResponse* resp) override {
		resp->set_message(req->message());
		auto* reactor = ctx->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

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
};

struct Stats {
	std::map<std::string, int> outcomes;
	int mismatches = 0;
	std::vector<Future<Void>> deliveries;
};

Future<Void> deliver(std::shared_ptr<Promise<grpc::Status>> p, grpc::Status s, ISimulator::ProcessInfo* caller) {
	co_await g_simulator->onProcess(caller);
	p->send(s);
}

void setDeadline(grpc::ClientContext* ctx, double timeout) {
	ctx->set_deadline(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
	                               gpr_time_from_micros(static_cast<int64_t>(timeout * 1e6), GPR_TIMESPAN)));
}

class RecvStreamClient final : public grpc::ClientReadReactor<EchoResponse> {
public:
	RecvStreamClient(TestEchoService::Stub* stub,
	                 Stats* stats,
	                 std::string msg,
	                 double timeout,
	                 std::shared_ptr<Promise<grpc::Status>> done)
	  : stats_(stats), done_(std::move(done)), caller_(g_simulator->getCurrentProcess()) {
		req_.set_message(std::move(msg));
		setDeadline(&ctx_, timeout);
		stub->async()->EchoRecvStream10(&ctx_, &req_, this);
		StartRead(&resp_);
		StartCall();
	}
	void OnReadDone(bool ok) override {
		if (ok) {
			if (resp_.message() != req_.message() + std::to_string(received_++)) {
				++stats_->mismatches;
			}
			StartRead(&resp_);
		}
	}
	void OnDone(const grpc::Status& s) override {
		if (s.ok() && received_ != 10) {
			++stats_->mismatches;
		}
		stats_->deliveries.push_back(deliver(done_, s, caller_));
		delete this;
	}

private:
	Stats* stats_;
	std::shared_ptr<Promise<grpc::Status>> done_;
	ISimulator::ProcessInfo* caller_;
	grpc::ClientContext ctx_;
	EchoRequest req_;
	EchoResponse resp_;
	int received_ = 0;
};

ISimulator::ProcessInfo* newServerProcess(uint32_t ip) {
	Standalone<StringRef> id(deterministicRandom()->randomUniqueID().toString());
	auto* p = g_simulator->newProcess(
	    "GrpcEchoServer",
	    IPAddress(ip),
	    5000,
	    false,
	    1,
	    LocalityData(Optional<Standalone<StringRef>>(), id, id, Optional<Standalone<StringRef>>()),
	    // Tester class: full simulation inspects every process's class, and testers are never chosen as cluster
	    // workers.
	    makeFDBSimulatorProcessMetadata(ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource)),
	    "",
	    "",
	    currentProtocolVersion(),
	    false);
	p->excludeFromRestarts = true;
	return p;
}

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

} // namespace

struct GrpcEchoWorkload : TestWorkload {
	static constexpr auto NAME = "GrpcEcho";

	double testDuration;
	int servers;
	int channelsPerServer;
	Stats stats;
	std::vector<ISimulator::ProcessInfo*> serverProcs;
	std::vector<grpc::Server*> grpcServers;
	std::vector<std::unique_ptr<TestEchoService::Stub>> stubs;
	std::vector<std::shared_ptr<grpc::Channel>> channels;
	int kills = 0, clogs = 0, graceful = 0;

	explicit GrpcEchoWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 20.0);
		servers = getOption(options, "servers"_sr, 2);
		channelsPerServer = getOption(options, "channelsPerServer"_sr, 2);
	}

	static std::string addr(int i) { return "3.3.4." + std::to_string(i + 1) + ":5000"; }
	static uint32_t ip(int i) { return 0x03030401 + i; }

	Future<Void> startServer(int i) {
		auto* original = g_simulator->getCurrentProcess();
		co_await g_simulator->onProcess(serverProcs[i]);
		auto* service = new EchoService();
		grpc::ServerBuilder builder;
		builder.AddListeningPort(addr(i), grpc::InsecureServerCredentials());
		builder.AddChannelArgument(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 500);
		builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
		builder.RegisterService(service);
		grpcServers[i] = builder.BuildAndStart().release();
		ASSERT(grpcServers[i] != nullptr);
		co_await g_simulator->onProcess(original);
	}

	Future<Void> shutdownServer(int i, double grace) {
		auto* original = g_simulator->getCurrentProcess();
		co_await g_simulator->onProcess(serverProcs[i]);
		static grpc_completion_queue_functor cqShutdown{ onCqShutdown, 0, 0, nullptr };
		auto* n = new ShutdownNotify();
		n->functor_run = onServerShutdown;
		n->inlineable = 0;
		n->caller = g_simulator->getCurrentProcess();
		grpc_completion_queue* cq = grpc_completion_queue_create_for_callback(&cqShutdown, nullptr);
		grpc_server_shutdown_and_notify(grpcServers[i]->c_server(), cq, n);
		Future<Void> done = n->done.getFuture();
		co_await (done || delay(grace));
		if (!done.isReady()) {
			grpc_server_cancel_all_calls(grpcServers[i]->c_server());
			co_await done;
		}
		grpcServers[i] = nullptr; // leaked: grpc::Server's destructor would block
		co_await g_simulator->onProcess(original);
	}

	Future<Void> callOnce(int c) {
		std::string msg = deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, 2048));
		auto done = std::make_shared<Promise<grpc::Status>>();
		Future<grpc::Status> result = done->getFuture();
		std::string kind;
		if (deterministicRandom()->random01() < 0.7) {
			kind = "unary";
			auto ctx = std::make_shared<grpc::ClientContext>();
			auto req = std::make_shared<EchoRequest>();
			auto resp = std::make_shared<EchoResponse>();
			req->set_message(msg);
			setDeadline(ctx.get(), 5.0);
			auto* caller = g_simulator->getCurrentProcess();
			Stats* st = &stats;
			stubs[c]->async()->Echo(
			    ctx.get(), req.get(), resp.get(), [st, done, ctx, req, resp, msg, caller](grpc::Status s) {
				    if (s.ok() && resp->message() != msg) {
					    ++st->mismatches;
				    }
				    st->deliveries.push_back(deliver(done, s, caller));
			    });
		} else {
			kind = "recv";
			new RecvStreamClient(stubs[c].get(), &stats, msg, 5.0, done);
		}
		grpc::Status s = co_await result;
		++stats.outcomes[kind + ":" + std::to_string(static_cast<int>(s.error_code()))];
	}

	Future<Void> loadLoop(int c) {
		while (true) {
			std::vector<Future<Void>> batch;
			int n = deterministicRandom()->randomInt(1, 6);
			for (int k = 0; k < n; ++k) {
				batch.push_back(callOnce(c));
			}
			co_await waitForAll(batch);
			co_await delay(deterministicRandom()->random01() * 0.1);
		}
	}

	Future<Void> chaosLoop() {
		while (true) {
			co_await delay(1.0 + deterministicRandom()->random01() * 3.0);
			int i = deterministicRandom()->randomInt(0, servers);
			if (serverProcs[i]->failed) {
				// Killed, possibly by another workload: replace the process at the same address.
				++kills;
				serverProcs[i] = newServerProcess(ip(i));
				co_await startServer(i);
				continue;
			}
			double r = deterministicRandom()->random01();
			if (r < 0.4) {
				++clogs;
				g_simulator->clogInterface(IPAddress(ip(i)), 0.5 + deterministicRandom()->random01() * 2.0);
			} else if (r < 0.7) {
				++kills;
				g_simulator->killProcess(serverProcs[i], ISimulator::KillType::KillInstantly);
				co_await delay(deterministicRandom()->random01() * 2.0);
				serverProcs[i] = newServerProcess(ip(i));
				co_await startServer(i);
			} else if (grpcServers[i]) {
				++graceful;
				co_await shutdownServer(i, 1.0);
				co_await startServer(i);
			}
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return setupImpl();
	}

	Future<Void> setupImpl() {
		ASSERT(g_network->isSimulated());
		sim_grpc::installSimEventEngine();
		serverProcs.resize(servers);
		grpcServers.resize(servers);
		for (int i = 0; i < servers; ++i) {
			serverProcs[i] = newServerProcess(ip(i));
			co_await startServer(i);
		}
		for (int i = 0; i < servers; ++i) {
			for (int k = 0; k < channelsPerServer; ++k) {
				grpc::ChannelArguments args;
				args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
				args.SetInt("simgrpc.channel", static_cast<int>(channels.size())); // distinct subchannels per channel
				args.SetPointer(sim_grpc::kSimProcessChannelArg, g_simulator->getCurrentProcess());
				args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 1000);
				args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 1000);
				args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
				args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
				channels.push_back(
				    grpc::CreateCustomChannel("ipv4:" + addr(i), grpc::InsecureChannelCredentials(), args));
				stubs.push_back(TestEchoService::NewStub(channels.back()));
			}
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return startImpl();
	}

	Future<Void> startImpl() {
		std::vector<Future<Void>> loops;
		for (int c = 0; c < static_cast<int>(stubs.size()); ++c) {
			loops.push_back(loadLoop(c));
		}
		loops.push_back(chaosLoop());
		co_await delay(testDuration);
		loops.clear();
		// Let in-flight calls finish (deadline 5s) before reporting.
		co_await delay(6.0);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		auto engine = sim_grpc::simEventEngineStats();
		int ok = stats.outcomes["unary:0"] + stats.outcomes["recv:0"];
		TraceEvent ev("GrpcEchoMetrics");
		ev.detail("Connects", engine.connects)
		    .detail("OffThread", engine.offThreadCalls)
		    .detail("Mismatches", stats.mismatches)
		    .detail("Ok", ok)
		    .detail("Kills", kills)
		    .detail("Clogs", clogs)
		    .detail("Graceful", graceful);
		return stats.mismatches == 0 && ok > 0 && engine.offThreadCalls == 0;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GrpcEchoWorkload> GrpcEchoWorkloadFactory;

#endif // FLOW_GRPC_ENABLED
