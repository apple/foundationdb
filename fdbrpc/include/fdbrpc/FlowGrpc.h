/**
 * FlowGrpc.h
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

#ifdef FLOW_GRPC_ENABLED
#ifndef FDBRPC_FLOW_GRPC_H
#define FDBRPC_FLOW_GRPC_H

#include "flow/flow.h"
#include "flow/NetworkAddress.h"

#include "fdbrpc/grpc/AsyncGrpcClient.h"
#include "fdbrpc/grpc/AsyncTaskExecutor.h"

// GrpcServer class is responsible for configuring, starting, and shutting down a gRPC server.
// It also manages `grpc::Service` instances associated with the server.
//
// Methods in this class should always be called from main thread.
//
// ## Service Lifecycle:
// - Each FDB worker is assigned a unique UID, which may change across different runs.
// - Workers can register services by calling `registerRoleServices`, providing their UID and references
//   to `grpc::Service` objects. If a UID is already registered, new services are appended to the existing list.
// - Services can be removed using the `deregisterRoleService()` method. Currently, this method only supports
//   removing all services associated with a given worker.
// - When a worker restarts or terminates, its associated services are automatically removed. The worker is
//   responsible for performing a clean shutdown and destruction.
// - Any modification to the service list triggers a restart of the gRPC server. To minimize disruptions,
//   it is recommended to keep these operations minimal.
// - Destruction of GrpcServer will block the thread. This isn't a problem as we use a global singleton.
//
// ## Usage:
//   - Get the singleton instance:
//     auto gs = GrpcServer::instance()
//
//   - Register services:
//     // Usually in the worker actors of each individual roles so that each component
//     // can manage their services.
//     gs->registerRoleServices(my_id, {
//         make_shared<FileTransferService>(),
//         make_shared<FileTransferService>()
//     });
//
//   - Remove services:
//     // Automatically called when worker reboots or dies, but can also be managed by
//     // directly if they need more control for their use-case.
//     gs->deregisterServices()
//
class GrpcServer {
public:
	using ServiceList = std::vector<std::shared_ptr<grpc::Service>>;

	GrpcServer(const NetworkAddress& addr);
	~GrpcServer();

	// Initializes the singleton.
	static GrpcServer* initInstance(const NetworkAddress& addr) {
		GrpcServer* server = new GrpcServer(addr);
		g_network->setGlobal(INetwork::enGrpcServer, (flowGlobalType)server);
		return server;
	}

	// Returns the singleton instance.
	static GrpcServer* instance() { return static_cast<GrpcServer*>((void*)g_network->global(INetwork::enGrpcServer)); }

	// Returns the gRPC server address. Currently, we only listen on single port globally.
	NetworkAddress getAddress() const { return address_; }

	// Starts the server and returns a future which is only fulfilled after shutdown(). However, the gRPC server itself
	// can stop and start internally multiple times within. This is expected when regsitered services are changed.
	Future<Void> run();

	// Stops the server and returns future that is fulfilled when stop is successfully finished. Unlike shutdown()
	// server can be resumed later.
	Future<Void> stopServer();

	// Shutdowns the server and returns future that is fulfilled when stop is successfully finished. Once shutdown,
	// server can't be restarted.
	Future<Void> shutdown();

	// Returns future object which is set when server is running.
	Future<Void> onRunning() const { return state_ == State::Running ? Void() : onNextStart(); }

	// Returns a future object which is set when the server is started. If the server is already
	// running, it is set by next start.
	Future<Void> onNextStart() const { return on_next_start_.onTrigger(); }

	// Returns future object which is set when the server is stopped.
	Future<Void> onStop() const;

	// Registers given services with the gRPC server. Return doesn't necessarily means the service has started.
	// TODO: should we add notification when service is alive?
	void registerService(std::shared_ptr<grpc::Service> service);
	void registerRoleServices(const UID& owner_id, const ServiceList& services);

	// Removes services associated with given `owner_id` from the server. Returns future that is fulfilled onced the
	// services are no longer alive (however, server may not have restarted yet).
	Future<Void> deregisterRoleServices(const UID& owner_id);

	// Returns `true` if the server has running and there is no shutdown in progress.
	bool hasStarted() const { return state_ == State::Running && server_ != nullptr; }

	// Returns number of times gRPC server has started. For testing.
	int numStarts() const { return num_starts_; }

	// How long to wait before restarting the server after change to registered services.
	// It is to give some buffer time to workers from different roles register services
	// indepdendently and avoid multiple restarts.
	//
	// TODO: Make it configurable.
	static const int CONFIG_STARTUP_DELAY_BETWEEN_RESTART = 2; /* seconds */
private:
	Future<Void> run_internal();
	void stopServerSync();

private:
	NetworkAddress address_;

	// Pool is mostly needed for converting the synchronous gRPC server operations into asynchronous.
	AsyncTaskExecutor pool_;

	Future<Void> run_actor_;
	AsyncTrigger on_next_start_;
	AsyncTrigger on_services_changed_;
	std::unordered_map<UID, ServiceList> registered_services_;

	// Underlying gRPC server. `nullptr` when there server is not running/stopped.
	std::unique_ptr<grpc::Server> server_;

	// Represents different states that the server can be in.
	enum class State {
		Running, // Server is actively running and serving requests.
		Stopping, // Server is currently in processs of shutting down.
		Stopped, // Server is stopped, but can be resumed later.
		Shutdown, // End-of-life. Can't be resumed if true.
	};
	State state_ = State::Stopped;

	// Returns true if the server is currently in processs of shutting down.
	// bool is_shutting_down_ = false;

	// Number of server starts. For testing.
	int num_starts_ = 0;
};

#endif // FDBRPC_FLOW_GRPC_H
#endif // FLOW_GRPC_ENABLED
