/*
 * GrpcControlService.cpp
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

// Drives the ControlService that every worker serves over gRPC (configuration `grpcEnabled`), the way an operator
// tool would: status, workers, coordinators, and exclude/include of a storage process, against random workers while
// other workloads inject faults.

#ifdef FLOW_GRPC_ENABLED

#include <map>
#include <memory>
#include <string>

#include <grpc/support/time.h>
#include <grpcpp/grpcpp.h>

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/Status.h"
#include "fdbctl/control_service/control_service.grpc.pb.h"
#include "fdbrpc/grpc/SimEventEngine.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Trace.h"
#include "flow/flow.h"

namespace {

using fdbctl::ControlService;

#define INVOKE(rpc)                                                                                                    \
	[](auto* async, auto* ctx, auto* req, auto* rep, auto cb) { async->rpc(ctx, req, rep, std::move(cb)); }

// Uncancellable: the call completes after the workload has been cancelled whenever the test is cut short.
Future<Void> deliver(std::shared_ptr<Promise<grpc::Status>> p,
                     grpc::Status s,
                     ISimulator::ProcessInfo* caller,
                     Uncancellable) {
	co_await g_simulator->onProcess(caller);
	p->send(s);
}

} // namespace

struct GrpcControlServiceWorkload : TestWorkload {
	static constexpr auto NAME = "GrpcControlService";

	double testDuration;
	std::map<std::string, std::unique_ptr<ControlService::Stub>> stubs;
	std::map<std::string, int> outcomes;
	std::vector<std::string> failures;
	Optional<std::string> excluded;

	explicit GrpcControlServiceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
	}

	// Derived the way the simulator assigns them, so a call does not depend on the GetWorkers being tested.
	static NetworkAddress grpcAddress(const ProcessData& worker) {
		return NetworkAddress(worker.address.ip, worker.address.port + SIM_GRPC_PORT_OFFSET, true, false);
	}

	static std::string target(const NetworkAddress& address) {
		return (address.ip.isV6() ? "ipv6:" : "ipv4:") + address.toString();
	}

	ControlService::Stub* stub(const NetworkAddress& address) {
		auto& s = stubs[target(address)];
		if (!s) {
			grpc::ChannelArguments args;
			args.SetInt(GRPC_ARG_ENABLE_HTTP_PROXY, 0);
			// Own connections, so that dropping the stub after a failed call also drops its connection.
			args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
			args.SetPointer(sim_grpc::kSimProcessChannelArg, g_simulator->getCurrentProcess());
			s = ControlService::NewStub(
			    grpc::CreateCustomChannel(target(address), grpc::InsecureChannelCredentials(), args));
		}
		return s.get();
	}

	// Issues one unary call through `invoke(async, ctx, request, reply, callback)` and records its status under `op`.
	template <class Request, class Reply, class Invoke>
	Future<grpc::Status> call(std::string op, NetworkAddress address, Invoke invoke, Request req, Reply* reply) {
		auto ctx = std::make_shared<grpc::ClientContext>();
		ctx->set_deadline(gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC), gpr_time_from_seconds(10, GPR_TIMESPAN)));
		auto request = std::make_shared<Request>(std::move(req));
		auto response = std::make_shared<Reply>();
		auto done = std::make_shared<Promise<grpc::Status>>();
		Future<grpc::Status> result = done->getFuture();
		auto* caller = g_simulator->getCurrentProcess();
		// gRPC writes into ctx and response until the callback runs, which may be after this coroutine is gone.
		invoke(stub(address)->async(), ctx.get(), request.get(), response.get(), [=](grpc::Status s) {
			(void)ctx;
			(void)request;
			(void)response;
			deliver(done, s, caller, Uncancellable{});
		});
		grpc::Status s = co_await result;
		// The simulator models a killed or rebooted peer as silence, never a reset, and without keepalive gRPC would
		// keep sending to the dead connection. An operator tool would reconnect; so does this.
		if (s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED || s.error_code() == grpc::StatusCode::UNAVAILABLE) {
			stubs.erase(target(address));
		}
		*reply = *response;
		++outcomes[op + ":" + std::to_string(static_cast<int>(s.error_code()))];
		TraceEvent("GrpcControlServiceCall")
		    .detail("Op", op)
		    .detail("Target", address)
		    .detail("Code", static_cast<int>(s.error_code()))
		    .detail("Message", s.error_message());
		co_return s;
	}

	void fail(std::string what) {
		TraceEvent(SevError, "GrpcControlServiceFailure").detail("What", what);
		failures.push_back(std::move(what));
	}

	Future<Void> runOne(Database cx) {
		std::vector<ProcessData> workers = co_await getWorkers(cx);
		if (workers.empty()) {
			co_return;
		}
		NetworkAddress target = grpcAddress(deterministicRandom()->randomChoice(workers));

		double r = deterministicRandom()->random01();
		if (r < 0.2) {
			fdbctl::GetStatusReply rep;
			grpc::Status s = co_await call("GetStatus", target, INVOKE(GetStatus), fdbctl::GetStatusRequest(), &rep);
			if (s.ok()) {
				try {
					readJSONStrictly(rep.result());
				} catch (Error& e) {
					fail("GetStatus returned invalid JSON");
				}
			}
		} else if (r < 0.4) {
			fdbctl::GetWorkersReply rep;
			grpc::Status s = co_await call("GetWorkers", target, INVOKE(GetWorkers), fdbctl::GetWorkersRequest(), &rep);
			// No check for an empty list: a new cluster controller clears the worker list before rewriting it.
			if (s.ok()) {
				for (const auto& w : rep.workers()) {
					NetworkAddress a = NetworkAddress::parse(w.address());
					std::string expected = NetworkAddress(a.ip, a.port + SIM_GRPC_PORT_OFFSET, true, false).toString();
					if (w.grpc_address() != expected) {
						fail("GetWorkers: " + w.address() + " has gRPC address '" + w.grpc_address() + "'");
					}
				}
			}
		} else if (r < 0.6) {
			fdbctl::GetCoordinatorsReply rep;
			grpc::Status s = co_await call(
			    "GetCoordinators", target, INVOKE(GetCoordinators), fdbctl::GetCoordinatorsRequest(), &rep);
			if (s.ok() && rep.coordinators_size() == 0) {
				fail("GetCoordinators returned no coordinators");
			}
		} else if (r < 0.7) {
			fdbctl::ExcludeStatusReply rep;
			co_await call("ExcludeStatus", target, INVOKE(ExcludeStatus), fdbctl::ExcludeStatusRequest(), &rep);
		} else if (!excluded.present()) {
			std::vector<std::string> storage;
			for (const auto& w : workers) {
				auto c = w.processClass.classType();
				if (c == ProcessClass::StorageClass || c == ProcessClass::UnsetClass) {
					storage.push_back(AddressExclusion(w.address.ip, w.address.port).toString());
				}
			}
			if (storage.empty()) {
				co_return;
			}
			std::string victim = deterministicRandom()->randomChoice(storage);
			fdbctl::ExcludeRequest req;
			req.add_processes(victim);
			req.set_no_wait(true);
			fdbctl::ExcludeReply rep;
			// Exclude is not idempotent from the client's view: a call that times out may still have committed.
			excluded = victim;
			grpc::Status s = co_await call("Exclude", target, INVOKE(Exclude), req, &rep);
			if (s.ok() && rep.excluded_addresses_size() == 0) {
				fail("Exclude returned no excluded addresses");
			}
		} else {
			fdbctl::IncludeRequest req;
			req.add_addresses(excluded.get());
			fdbctl::IncludeReply rep;
			grpc::Status s = co_await call("Include", target, INVOKE(Include), req, &rep);
			if (s.ok()) {
				excluded.reset();
			}
		}
	}

	// Re-includes anything this workload excluded, through gRPC, so the end-of-test checks see a fully included
	// cluster.
	Future<Void> includeAll(Database cx) {
		while (excluded.present()) {
			std::vector<ProcessData> workers = co_await getWorkers(cx);
			for (const auto& w : workers) {
				fdbctl::IncludeRequest req;
				req.add_addresses(excluded.get());
				fdbctl::IncludeReply rep;
				grpc::Status s = co_await call("Include", grpcAddress(w), INVOKE(Include), req, &rep);
				if (s.ok()) {
					excluded.reset();
					break;
				}
			}
			co_await delay(1.0);
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return startImpl(cx);
	}

	Future<Void> startImpl(Database cx) {
		ASSERT(fdbSimulationPolicyState().grpcEnabled);
		double end = now() + testDuration;
		while (now() < end) {
			co_await runOne(cx);
			co_await delay(deterministicRandom()->random01());
		}
		co_await includeAll(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		int ok = 0;
		TraceEvent ev("GrpcControlServiceMetrics");
		for (const auto& [k, v] : outcomes) {
			ev.detail(k.substr(0, k.find(':')) + "_" + k.substr(k.find(':') + 1), v);
			if (k.ends_with(":0")) {
				ok += v;
			}
		}
		ev.detail("Ok", ok).detail("Failures", failures.size());
		return failures.empty() && ok > 0;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GrpcControlServiceWorkload> GrpcControlServiceWorkloadFactory;

#endif // FLOW_GRPC_ENABLED
