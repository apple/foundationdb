/**
 * gRPC.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/FlowGrpc.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#ifdef FLOW_GRPC_ENABLED

GrpcServer::GrpcServer(const NetworkAddress& addr) : pool_(1), address_(addr) {}

GrpcServer::~GrpcServer() {
	if (!server_)
		return;

	stopServerSync();
	run_actor_.cancel();
	server_ = nullptr;
	state_ = State::Shutdown;
}

Future<Void> GrpcServer::run() {
	try {
		run_actor_ = run_internal();
		co_await run_actor_;
	} catch (Error& err) {
		if (state_ == State::Shutdown) {
			run_actor_.cancel();
		} else {
			throw err;
		}
	}
}

Future<Void> GrpcServer::run_internal() {
	ASSERT(state_ == State::Stopped);
	ASSERT(server_ == nullptr);
	ASSERT(g_network->isOnMainThread());

	Future<Void> next = Void();
	loop {
		ASSERT(state_ != State::Shutdown);

		loop {
			co_await next;
			co_await delay(CONFIG_STARTUP_DELAY_BETWEEN_RESTART);

			// gRPC can't run a server without registered service.
			if (registered_services_.size() > 0) {
				break;
			} else {
				next = on_services_changed_.onTrigger();
			}
		}

		co_await stopServer();

		// Even if service list is changed after stopServer(), we'll have those here.
		grpc::ServerBuilder builder;
		builder.AddListeningPort(address_.toString(), grpc::InsecureServerCredentials());
		for (auto& [_, services] : registered_services_) {
			for (auto& service : services) {
				builder.RegisterService(service.get());
			}
		}

		server_ = builder.BuildAndStart();
		ASSERT(server_ != nullptr);
		++num_starts_;
		state_ = State::Running;
		on_next_start_.trigger();
		next = on_services_changed_.onTrigger();
	}
}

Future<Void> GrpcServer::shutdown() {
	co_await stopServer();
	registered_services_.clear();
	state_ = State::Shutdown;
	run_actor_.cancel();
	co_return;
}

Future<Void> GrpcServer::stopServer() {
	ASSERT(g_network->isOnMainThread());

	if (server_ == nullptr) {
		ASSERT(state_ == State::Stopped || state_ == State::Shutdown);
		co_return;
	}

	state_ = State::Stopping;
	co_await pool_.post([&]() {
		stopServerSync();
		return Void();
	});

	if (state_ == State::Shutdown) {
		co_return;
	}

	server_ = nullptr;
	state_ = State::Stopped;
}

void GrpcServer::stopServerSync() {
	if (server_ != nullptr) {
		return;
	}

	server_->Shutdown();
	server_->Wait();
}

void GrpcServer::registerService(std::shared_ptr<grpc::Service> service) {
	ASSERT(g_network->isOnMainThread());
	registered_services_[UID()].push_back(service);
	on_services_changed_.trigger();
}

void GrpcServer::registerRoleServices(const UID& owner_id, const ServiceList& services) {
	ASSERT(g_network->isOnMainThread());
	for (const auto& svc : services) {
		registered_services_[owner_id].push_back(svc);
	}
	on_services_changed_.trigger();
}

Future<Void> GrpcServer::deregisterRoleServices(const UID& owner_id) {
	ASSERT(g_network->isOnMainThread());
	co_await stopServer();
	registered_services_.erase(owner_id);
	on_services_changed_.trigger();
}

#endif
