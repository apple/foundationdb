/*
 * SimEventEngine.cpp
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

#ifdef FLOW_GRPC_ENABLED

#include "fdbrpc/grpc/SimEventEngine.h"

#include <arpa/inet.h>
#include <netinet/in.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <climits>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <absl/log/globals.h>
#include <absl/log/initialize.h>
#include <absl/log/log_sink.h>
#include <absl/log/log_sink_registry.h>
#include <absl/random/internal/seed_material.h>
#include <grpc/event_engine/event_engine.h>
#include <grpc/event_engine/slice.h>
#include <grpc/event_engine/slice_buffer.h>
#include <grpc/grpc.h>
#include <grpc/impl/channel_arg_names.h>
#include <grpc/support/time.h>

#include "flow/flow.h"
#include "flow/IConnection.h"
#include "flow/Trace.h"
#include "flow/genericactors.h"
#include "flow/serialize.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"

// gpr's clock hook; exported as a C data symbol by libgpr.
extern "C" gpr_timespec (*gpr_now_impl)(gpr_clock_type clock_type);

// Legacy iomgr timer manager, still initialised by grpc_init() alongside the EventEngine. Internal but exported.
void grpc_timer_manager_set_start_threaded(bool enabled);
void grpc_timer_manager_tick();

namespace sim_grpc {
namespace {

using grpc_event_engine::experimental::EndpointConfig;
using grpc_event_engine::experimental::EventEngine;
using grpc_event_engine::experimental::MemoryAllocator;
using grpc_event_engine::experimental::MemoryAllocatorFactory;
using grpc_event_engine::experimental::Slice;
using grpc_event_engine::experimental::SliceBuffer;

std::thread::id g_loopThread;
std::atomic<uint64_t> g_offThreadCalls{ 0 };
std::atomic<uint64_t> g_clockReads{ 0 };
uint64_t g_connects = 0;
gpr_timespec (*g_realNow)(gpr_clock_type) = nullptr;

void onLoopThread(const char* where) {
	if (std::this_thread::get_id() != g_loopThread) {
		++g_offThreadCalls;
		// Calling Flow from another thread is undefined; stop rather than produce a misleading result.
		fprintf(stderr, "SimEventEngine: %s called off the run-loop thread\n", where);
		abort();
	}
}

gpr_timespec simNow(gpr_clock_type clock) {
	if (std::this_thread::get_id() != g_loopThread || !g_network) {
		++g_offThreadCalls;
		return g_realNow(clock);
	}
	++g_clockReads;
	// gRPC refuses to initialise with a monotonic clock under one second (grpc_core InitTime).
	double t = g_network->now() + 1e6;
	gpr_timespec ts;
	ts.tv_sec = static_cast<int64_t>(t);
	ts.tv_nsec = static_cast<int32_t>((t - static_cast<double>(ts.tv_sec)) * 1e9);
	ts.clock_type = clock == GPR_TIMESPAN ? GPR_CLOCK_MONOTONIC : clock;
	return ts;
}

EventEngine::ResolvedAddress toResolved(const NetworkAddress& a) {
	if (a.ip.isV6()) {
		sockaddr_in6 sin6{};
#ifdef __APPLE__
		sin6.sin6_len = sizeof(sin6);
#endif
		sin6.sin6_family = AF_INET6;
		sin6.sin6_port = htons(a.port);
		memcpy(&sin6.sin6_addr, a.ip.toV6().data(), 16);
		return EventEngine::ResolvedAddress(reinterpret_cast<const sockaddr*>(&sin6), sizeof(sin6));
	}
	sockaddr_in sin{};
#ifdef __APPLE__
	sin.sin_len = sizeof(sin);
#endif
	sin.sin_family = AF_INET;
	sin.sin_port = htons(a.port);
	sin.sin_addr.s_addr = htonl(a.ip.toV4());
	return EventEngine::ResolvedAddress(reinterpret_cast<const sockaddr*>(&sin), sizeof(sin));
}

Optional<NetworkAddress> toNetworkAddress(const EventEngine::ResolvedAddress& r) {
	// Sim2 keys listeners and processes by public, non-TLS addresses.
	if (r.address()->sa_family == AF_INET6) {
		auto* sin6 = reinterpret_cast<const sockaddr_in6*>(r.address());
		IPAddress::IPAddressStore store;
		memcpy(store.data(), &sin6->sin6_addr, 16);
		return NetworkAddress(IPAddress(store), ntohs(sin6->sin6_port), true, false);
	}
	if (r.address()->sa_family != AF_INET) {
		return {};
	}
	auto* sin = reinterpret_cast<const sockaddr_in*>(r.address());
	return NetworkAddress(IPAddress(ntohl(sin->sin_addr.s_addr)), ntohs(sin->sin_port), true, false);
}

// gRPC's posix engine sets TCP_USER_TIMEOUT to the keepalive timeout when keepalive is on, so a write to a peer that
// has vanished eventually fails. A Sim2 write to a failed process otherwise blocks forever, and with it the keepalive
// ping queued behind it.
double userTimeoutSeconds(const EndpointConfig& config) {
	auto keepalive = config.GetInt(GRPC_ARG_KEEPALIVE_TIME_MS);
	if (!keepalive.has_value() || *keepalive <= 0 || *keepalive == INT_MAX) {
		return 0;
	}
	return config.GetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS).value_or(20000) / 1000.0;
}

class RawSendBuffer : public SendBuffer {
public:
	RawSendBuffer(uint8_t* p, int n) {
		_data = p;
		next = nullptr;
		bytes_written = n;
		bytes_sent = 0;
	}
};

class SimEventEngine;

// State shared with the coroutines that wait on the connection, so they can outlive the gRPC Endpoint object; its
// destructor may run on a different simulated process than the one that owns the connection.
struct EndpointState {
	EndpointState(SimEventEngine* engine,
	              ISimulator::ProcessInfo* owner,
	              Reference<IConnection> conn,
	              double userTimeout)
	  : engine(engine), owner(owner), conn(std::move(conn)), userTimeout(userTimeout) {}

	// Returns bytes appended, 0 if nothing is available, -1 if the connection failed.
	int tryRead(SliceBuffer* buffer);
	// Returns false if the connection failed.
	bool tryWrite();
	void complete(absl::AnyInvocable<void(absl::Status)> cb, absl::Status status);
	bool onOwner() const { return g_simulator->getCurrentProcess() == owner; }

	SimEventEngine* engine;
	// gRPC is one library per OS process, but a Sim2 connection may only be touched from its own simulated process.
	ISimulator::ProcessInfo* owner;
	Reference<IConnection> conn;
	double userTimeout; // seconds a write may stay blocked; 0 = forever
	bool closed = false;

	absl::AnyInvocable<void(absl::Status)> pendingRead;
	SliceBuffer* readBuffer = nullptr;
	Future<Void> readActor;

	absl::AnyInvocable<void(absl::Status)> pendingWrite;
	std::string writeData;
	std::unique_ptr<RawSendBuffer> writeBuffer;
	Future<Void> writeActor;
};

class SimEndpoint final : public EventEngine::Endpoint {
public:
	SimEndpoint(SimEventEngine* engine,
	            ISimulator::ProcessInfo* owner,
	            Reference<IConnection> conn,
	            NetworkAddress local,
	            NetworkAddress peer,
	            double userTimeout)
	  : s_(std::make_shared<EndpointState>(engine, owner, std::move(conn), userTimeout)), local_(toResolved(local)),
	    peer_(toResolved(peer)) {}
	~SimEndpoint() override;

	bool Read(absl::AnyInvocable<void(absl::Status)> on_read, SliceBuffer* buffer, ReadArgs) override {
		return read(std::move(on_read), buffer);
	}
	bool Write(absl::AnyInvocable<void(absl::Status)> on_writable, SliceBuffer* data, WriteArgs) override {
		return write(std::move(on_writable), data);
	}
	std::shared_ptr<TelemetryInfo> GetTelemetryInfo() const override { return nullptr; }
	const EventEngine::ResolvedAddress& GetPeerAddress() const override { return peer_; }
	const EventEngine::ResolvedAddress& GetLocalAddress() const override { return local_; }

private:
	bool read(absl::AnyInvocable<void(absl::Status)> on_read, SliceBuffer* buffer);
	bool write(absl::AnyInvocable<void(absl::Status)> on_writable, SliceBuffer* data);
	static Future<Void> readWhenReadable(std::shared_ptr<EndpointState> s);
	static Future<Void> writeWhenWritable(std::shared_ptr<EndpointState> s);

	std::shared_ptr<EndpointState> s_;
	EventEngine::ResolvedAddress local_, peer_;
};

class SimListener final : public EventEngine::Listener {
public:
	SimListener(SimEventEngine* engine,
	            AcceptCallback on_accept,
	            absl::AnyInvocable<void(absl::Status)> on_shutdown,
	            std::unique_ptr<MemoryAllocatorFactory> factory,
	            double userTimeout)
	  : engine_(engine), owner_(g_simulator->getCurrentProcess()), onAccept_(std::move(on_accept)),
	    onShutdown_(std::move(on_shutdown)), factory_(std::move(factory)), userTimeout_(userTimeout) {}
	~SimListener() override;

	absl::StatusOr<int> Bind(const EventEngine::ResolvedAddress& addr) override;
	absl::Status Start() override;

private:
	static Future<Void> acceptLoop(SimListener* self, Reference<IListener> listener, NetworkAddress local);

	SimEventEngine* engine_;
	ISimulator::ProcessInfo* owner_;
	AcceptCallback onAccept_;
	absl::AnyInvocable<void(absl::Status)> onShutdown_;
	std::unique_ptr<MemoryAllocatorFactory> factory_;
	double userTimeout_;
	std::vector<NetworkAddress> addresses_;
	std::vector<Reference<IListener>> listeners_;
	std::vector<Future<Void>> loops_;
};

class SimDNSResolver final : public EventEngine::DNSResolver {
public:
	explicit SimDNSResolver(SimEventEngine* engine) : engine_(engine) {}
	void LookupHostname(LookupHostnameCallback on_resolve,
	                    absl::string_view name,
	                    absl::string_view default_port) override;
	void LookupSRV(LookupSRVCallback on_resolve, absl::string_view) override;
	void LookupTXT(LookupTXTCallback on_resolve, absl::string_view) override;

private:
	SimEventEngine* engine_;
};

class SimEventEngine final : public EventEngine {
public:
	explicit SimEventEngine(ISimulator::ProcessInfo* home) : home_(home) {}

	bool IsWorkerThread() override { return false; }

	absl::StatusOr<std::unique_ptr<DNSResolver>> GetDNSResolver(const DNSResolver::ResolverOptions&) override {
		onLoopThread("GetDNSResolver");
		return std::make_unique<SimDNSResolver>(this);
	}

	void Run(Closure* closure) override {
		onLoopThread("Run");
		schedule(0, Task(closure));
	}
	void Run(absl::AnyInvocable<void()> closure) override {
		onLoopThread("Run");
		schedule(0, Task(std::move(closure)));
	}
	TaskHandle RunAfter(Duration when, Closure* closure) override {
		onLoopThread("RunAfter");
		return schedule(toSeconds(when), Task(closure));
	}
	TaskHandle RunAfter(Duration when, absl::AnyInvocable<void()> closure) override {
		onLoopThread("RunAfter");
		return schedule(toSeconds(when), Task(std::move(closure)));
	}
	bool Cancel(TaskHandle handle) override {
		onLoopThread("Cancel");
		auto it = tasks_.find(handle.keys[0]);
		if (handle.keys[1] != kMagic || it == tasks_.end() || it->second.fired) {
			return false;
		}
		tasks_.erase(it); // cancels the waiting coroutine and destroys the invocable
		return true;
	}

	absl::StatusOr<std::unique_ptr<Listener>> CreateListener(Listener::AcceptCallback on_accept,
	                                                         absl::AnyInvocable<void(absl::Status)> on_shutdown,
	                                                         const EndpointConfig& config,
	                                                         std::unique_ptr<MemoryAllocatorFactory> factory) override {
		onLoopThread("CreateListener");
		return std::make_unique<SimListener>(
		    this, std::move(on_accept), std::move(on_shutdown), std::move(factory), userTimeoutSeconds(config));
	}

	ConnectionHandle Connect(OnConnectCallback on_connect,
	                         const ResolvedAddress& addr,
	                         const EndpointConfig& config,
	                         MemoryAllocator,
	                         Duration timeout) override {
		onLoopThread("Connect");
		++g_connects;
		intptr_t id = nextId_++;
		auto to = toNetworkAddress(addr);
		if (!to.present()) {
			Run([cb = std::move(on_connect)]() mutable {
				cb(absl::InvalidArgumentError("unsupported address family"));
			});
			return ConnectionHandle::kInvalid;
		}
		auto* owner = static_cast<ISimulator::ProcessInfo*>(config.GetVoidPointer(kSimProcessChannelArg));
		if (!owner) {
			owner = g_simulator->getCurrentProcess();
		}
		connects_[id] =
		    connectActor(this, owner, to.get(), toSeconds(timeout), userTimeoutSeconds(config), std::move(on_connect));
		return ConnectionHandle{ { id, kMagic } };
	}

	bool CancelConnect(ConnectionHandle handle) override {
		onLoopThread("CancelConnect");
		auto it = connects_.find(handle.keys[0]);
		if (handle.keys[1] != kMagic || it == connects_.end() || it->second.isReady()) {
			return false;
		}
		connects_.erase(it);
		return true;
	}

	bool insideClosure() const { return running_ != 0; }

	void startDrain(ISimulator::ProcessInfo* owner, Reference<IListener> listener, NetworkAddress addr) {
		drains_[addr] = drainLoop(owner, listener);
	}
	void stopDrain(const NetworkAddress& addr) { drains_.erase(addr); }

	// Runs `fn` in `process`'s context, detached from any caller.
	void onProcess(ISimulator::ProcessInfo* process, absl::AnyInvocable<void()> fn) {
		detached_.push_back(runOnProcess(process, std::move(fn)));
	}

	uint64_t strandedTasks() const {
		uint64_t n = 0;
		for (auto& [_, t] : tasks_) {
			n += (!t.fired && t.process && t.process->failed) ? 1 : 0;
		}
		return n;
	}

	uint64_t pendingTasks() const {
		uint64_t n = 0;
		for (auto& [_, t] : tasks_) {
			n += t.fired ? 0 : 1;
		}
		return n;
	}

private:
	static constexpr intptr_t kMagic = 0x5113;

	struct Task {
		Task() = default;
		explicit Task(absl::AnyInvocable<void()> fn) : fn(std::move(fn)) {}
		explicit Task(Closure* closure) : closure(closure) {}

		absl::AnyInvocable<void()> fn;
		Closure* closure = nullptr;
		Future<Void> actor;
		bool fired = false;
		// Sim2 never runs a task scheduled on a process that has since failed.
		ISimulator::ProcessInfo* process = nullptr;
	};

	static double toSeconds(Duration d) { return std::max<double>(0, static_cast<double>(d.count()) / 1e9); }

	void reap() {
		std::vector<intptr_t> keep;
		for (auto id : fired_) {
			if (id == running_) {
				keep.push_back(id);
			} else {
				tasks_.erase(id);
			}
		}
		fired_ = std::move(keep);
		for (auto it = connects_.begin(); it != connects_.end();) {
			it = it->second.isReady() ? connects_.erase(it) : std::next(it);
		}
	}

	TaskHandle schedule(double seconds, Task task) {
		reap();
		intptr_t id = nextId_++;
		task.process = g_simulator->getCurrentProcess();
		auto& slot = tasks_[id] = std::move(task);
		slot.actor = fire(this, id, seconds);
		return TaskHandle{ { id, kMagic } };
	}

	static Future<Void> fire(SimEventEngine* self, intptr_t id, double seconds) {
		// gRPC's closures belong to the library, not to whichever simulated process happened to schedule them (an
		// ExecCtx flush runs other calls' work too). Run them on a process that is never killed, or a kill strands
		// unrelated calls' timers forever.
		if (g_simulator->getCurrentProcess() != self->home_) {
			co_await g_simulator->onProcess(self->home_);
		}
		co_await delay(seconds);
		auto it = self->tasks_.find(id);
		if (it == self->tasks_.end()) {
			co_return;
		}
		// Destroying this coroutine's own Future here would cancel it mid-run, so the entry is reaped later.
		it->second.fired = true;
		self->fired_.push_back(id);
		self->running_ = id;
		if (it->second.closure) {
			it->second.closure->Run();
		} else {
			auto fn = std::move(it->second.fn);
			fn();
		}
		self->running_ = 0;
	}

	static Future<Void> connectActor(SimEventEngine* self,
	                                 ISimulator::ProcessInfo* owner,
	                                 NetworkAddress to,
	                                 double timeout,
	                                 double userTimeout,
	                                 OnConnectCallback cb) {
		absl::StatusOr<std::unique_ptr<Endpoint>> result = absl::UnavailableError("connect failed");
		// A Sim2 connection that is dropped without close() is reported as leaked (a SevError).
		Reference<IConnection> conn;
		try {
			if (g_simulator->getCurrentProcess() != owner) {
				co_await g_simulator->onProcess(owner);
			}
			conn = co_await timeoutError(INetworkConnections::net()->connect(to), timeout);
			co_await timeoutError(conn->connectHandshake(), timeout);
			result = std::make_unique<SimEndpoint>(self, owner, conn, owner->address, to, userTimeout);
		} catch (Error& e) {
			if (!result.ok() && conn) {
				conn->close();
			}
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			result = e.code() == error_code_timed_out ? absl::DeadlineExceededError("connect timed out")
			                                          : absl::UnavailableError(e.name());
		}
		self->Run([cb = std::move(cb), result = std::move(result)]() mutable { cb(std::move(result)); });
	}

	std::unordered_map<intptr_t, Task> tasks_;
	static Future<Void> drainLoop(ISimulator::ProcessInfo* owner, Reference<IListener> listener) {
		if (g_simulator->getCurrentProcess() != owner) {
			co_await g_simulator->onProcess(owner);
		}
		while (true) {
			Reference<IConnection> conn = co_await listener->accept();
			conn->close();
		}
	}

	std::map<NetworkAddress, Future<Void>> drains_;
	static Future<Void> runOnProcess(ISimulator::ProcessInfo* process, absl::AnyInvocable<void()> fn) {
		co_await g_simulator->onProcess(process);
		fn();
	}

	ISimulator::ProcessInfo* home_;
	std::vector<Future<Void>> detached_;
	std::vector<intptr_t> fired_;
	intptr_t running_ = 0;
	std::unordered_map<intptr_t, Future<Void>> connects_;
	intptr_t nextId_ = 1;
};

// ---- SimEndpoint ----

SimEndpoint::~SimEndpoint() {
	s_->closed = true;
	if (s_->pendingRead) {
		s_->complete(std::move(s_->pendingRead), absl::CancelledError("endpoint destroyed"));
	}
	if (s_->pendingWrite) {
		s_->complete(std::move(s_->pendingWrite), absl::CancelledError("endpoint destroyed"));
	}
	// Cancelling a coroutine that waits on a Sim2 connection must also happen on the connection's process.
	auto shutdown = [s = s_]() {
		s->readActor.cancel();
		s->writeActor.cancel();
		s->conn->close();
	};
	if (s_->onOwner()) {
		shutdown();
	} else {
		s_->engine->onProcess(s_->owner, std::move(shutdown));
	}
}

void EndpointState::complete(absl::AnyInvocable<void(absl::Status)> cb, absl::Status status) {
	// Always asynchronous: gRPC may issue the next Read/Write from inside the callback.
	engine->Run([cb = std::move(cb), status = std::move(status)]() mutable { cb(std::move(status)); });
}

int EndpointState::tryRead(SliceBuffer* buffer) {
	uint8_t tmp[16384];
	int total = 0;
	try {
		while (true) {
			int n = conn->read(tmp, tmp + sizeof(tmp));
			if (n <= 0) {
				break;
			}
			buffer->Append(Slice::FromCopiedBuffer(tmp, n));
			total += n;
		}
	} catch (Error& e) {
		if (total == 0) {
			return -1;
		}
	}
	return total;
}

bool SimEndpoint::read(absl::AnyInvocable<void(absl::Status)> on_read, SliceBuffer* buffer) {
	onLoopThread("Endpoint::Read");
	ASSERT(!s_->pendingRead);
	if (s_->onOwner()) {
		int n = s_->tryRead(buffer);
		if (n > 0) {
			return true;
		}
		if (n < 0) {
			s_->complete(std::move(on_read), absl::UnavailableError("connection failed"));
			return false;
		}
	}
	s_->pendingRead = std::move(on_read);
	s_->readBuffer = buffer;
	s_->readActor = readWhenReadable(s_);
	return false;
}

Future<Void> SimEndpoint::readWhenReadable(std::shared_ptr<EndpointState> s) {
	bool first = true;
	while (true) {
		bool failed = false;
		if (first && !s->onOwner()) {
			co_await g_simulator->onProcess(s->owner);
		} else {
			try {
				co_await s->conn->onReadable();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				failed = true;
			}
		}
		first = false;
		if (s->closed) {
			co_return;
		}
		int n = failed ? -1 : s->tryRead(s->readBuffer);
		if (n != 0) {
			s->complete(std::move(s->pendingRead),
			            n > 0 ? absl::OkStatus() : absl::UnavailableError("connection failed"));
			co_return;
		}
	}
}

bool EndpointState::tryWrite() {
	try {
		while (writeBuffer->bytes_unsent() > 0) {
			int n = conn->write(writeBuffer.get(), writeBuffer->bytes_unsent());
			if (n <= 0) {
				break;
			}
			writeBuffer->bytes_sent += n;
		}
	} catch (Error& e) {
		return false;
	}
	return true;
}

bool SimEndpoint::write(absl::AnyInvocable<void(absl::Status)> on_writable, SliceBuffer* data) {
	onLoopThread("Endpoint::Write");
	ASSERT(!s_->pendingWrite);
	s_->writeData.clear();
	s_->writeData.reserve(data->Length());
	for (size_t i = 0; i < data->Count(); ++i) {
		Slice slice = data->RefSlice(i);
		s_->writeData.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
	}
	if (s_->writeData.empty()) {
		return true;
	}
	s_->writeBuffer = std::make_unique<RawSendBuffer>(reinterpret_cast<uint8_t*>(s_->writeData.data()),
	                                                  static_cast<int>(s_->writeData.size()));
	if (s_->onOwner()) {
		if (!s_->tryWrite()) {
			s_->complete(std::move(on_writable), absl::UnavailableError("connection failed"));
			return false;
		}
		if (s_->writeBuffer->bytes_unsent() == 0) {
			return true;
		}
	}
	s_->pendingWrite = std::move(on_writable);
	s_->writeActor = writeWhenWritable(s_);
	return false;
}

Future<Void> SimEndpoint::writeWhenWritable(std::shared_ptr<EndpointState> s) {
	bool ok = true;
	if (!s->onOwner()) {
		co_await g_simulator->onProcess(s->owner);
		if (s->closed) {
			co_return;
		}
		ok = s->tryWrite();
	}
	while (ok && s->writeBuffer->bytes_unsent() > 0) {
		try {
			if (s->userTimeout > 0) {
				co_await timeoutError(s->conn->onWritable(), s->userTimeout);
			} else {
				co_await s->conn->onWritable();
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			ok = false;
			break;
		}
		if (s->closed) {
			co_return;
		}
		ok = s->tryWrite();
	}
	s->complete(std::move(s->pendingWrite), ok ? absl::OkStatus() : absl::UnavailableError("connection failed"));
}

// ---- SimListener ----

SimListener::~SimListener() {
	loops_.clear();
	// Sim2 keeps queueing connections for this address after gRPC stops listening; unaccepted ones are never closed
	// and are reported as leaked when their peer gives up. Refuse them until a new listener takes the address.
	for (size_t i = 0; i < listeners_.size(); ++i) {
		engine_->startDrain(owner_, listeners_[i], addresses_[i]);
	}
	if (onShutdown_) {
		engine_->Run([cb = std::move(onShutdown_)]() mutable { cb(absl::OkStatus()); });
	}
}

absl::StatusOr<int> SimListener::Bind(const EventEngine::ResolvedAddress& addr) {
	onLoopThread("Listener::Bind");
	auto a = toNetworkAddress(addr);
	if (!a.present() || a.get().port == 0) {
		return absl::InvalidArgumentError("SimEventEngine listens only on an explicit IPv4 address and port");
	}
	addresses_.push_back(a.get());
	return a.get().port;
}

absl::Status SimListener::Start() {
	onLoopThread("Listener::Start");
	ASSERT(g_simulator->getCurrentProcess() == owner_);
	for (auto& a : addresses_) {
		engine_->stopDrain(a);
		listeners_.push_back(INetworkConnections::net()->listen(a));
		loops_.push_back(acceptLoop(this, listeners_.back(), a));
	}
	return absl::OkStatus();
}

Future<Void> SimListener::acceptLoop(SimListener* self, Reference<IListener> listener, NetworkAddress local) {
	while (true) {
		Reference<IConnection> conn = co_await listener->accept();
		try {
			co_await conn->acceptHandshake();
		} catch (Error& e) {
			conn->close();
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			continue;
		}
		auto ep = std::make_unique<SimEndpoint>(
		    self->engine_, self->owner_, conn, local, conn->getPeerAddress(), self->userTimeout_);
		self->onAccept_(std::move(ep), self->factory_->CreateMemoryAllocator("sim-endpoint"));
	}
}

// ---- SimDNSResolver ----

void SimDNSResolver::LookupHostname(LookupHostnameCallback on_resolve,
                                    absl::string_view name,
                                    absl::string_view default_port) {
	onLoopThread("LookupHostname");
	std::string target(name);
	if (target.find(':') == std::string::npos) {
		target += ":" + std::string(default_port);
	}
	absl::StatusOr<std::vector<EventEngine::ResolvedAddress>> result =
	    absl::NotFoundError("simulated DNS: numeric only");
	try {
		result = std::vector<EventEngine::ResolvedAddress>{ toResolved(NetworkAddress::parse(target)) };
	} catch (Error&) {
	}
	// gRPC uses this API both blocking (grpc_server_add_http2_port -> LookupHostnameBlocking, called from Flow code)
	// and asynchronously under its own locks (client channel resolver, called from an engine closure). Only the first
	// may be answered inline; deferring it would deadlock the single run-loop thread.
	if (engine_->insideClosure()) {
		engine_->Run([cb = std::move(on_resolve), result = std::move(result)]() mutable { cb(std::move(result)); });
	} else {
		on_resolve(std::move(result));
	}
}

void SimDNSResolver::LookupSRV(LookupSRVCallback on_resolve, absl::string_view) {
	engine_->Run([cb = std::move(on_resolve)]() mutable { cb(std::vector<SRVRecord>{}); });
}

void SimDNSResolver::LookupTXT(LookupTXTCallback on_resolve, absl::string_view) {
	engine_->Run([cb = std::move(on_resolve)]() mutable { cb(std::vector<std::string>{}); });
}

std::shared_ptr<SimEventEngine> g_engine;

// gRPC logs through abseil, which writes to stderr; test harnesses treat any stderr output as a failure. Send gRPC's
// log lines to the trace log instead. Even gRPC errors map to SevWarnAlways: gRPC logs expected failures at ERROR.
class TraceLogSink final : public absl::LogSink {
public:
	void Send(const absl::LogEntry& entry) override {
		Severity sev = entry.log_severity() >= absl::LogSeverity::kError     ? SevWarnAlways
		               : entry.log_severity() >= absl::LogSeverity::kWarning ? SevWarn
		                                                                     : SevInfo;
		TraceEvent(sev, "GrpcLog")
		    .detail("Source", std::string(entry.source_basename()) + ":" + std::to_string(entry.source_line()))
		    .detail("Message", entry.text_message().substr(0, 400));
	}
};

Future<Void> g_legacyTimerTicker;

// Drives legacy iomgr timers from the run loop instead of gRPC's own timer thread.
Future<Void> tickLegacyTimers() {
	while (true) {
		co_await delay(0.01);
		// The timer list exists only once gRPC is initialised, which may be well after installation.
		if (grpc_is_initialized()) {
			grpc_timer_manager_tick();
		}
	}
}

// Seeds gRPC's abseil random generators (e.g. reconnect backoff jitter) reproducibly.
uint64_t g_seedState = 0;
bool simSeedMaterial(absl::Span<uint32_t> values) {
	for (auto& v : values) {
		uint64_t z = (g_seedState += 0x9E3779B97F4A7C15ULL);
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
		z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
		v = static_cast<uint32_t>(z ^ (z >> 31));
	}
	return true;
}

} // namespace

void installSimEventEngine() {
	if (g_engine) {
		return;
	}
	static TraceLogSink sink;
	absl::InitializeLog();
	absl::SetStderrThreshold(absl::LogSeverityAtLeast::kInfinity);
	absl::AddLogSink(&sink);
	g_loopThread = std::this_thread::get_id();
	g_seedState = deterministicRandom()->randomUInt64();
	absl::random_internal::SetSeedMaterialSource(simSeedMaterial);
	g_realNow = gpr_now_impl;
	gpr_now_impl = simNow;
	grpc_timer_manager_set_start_threaded(false);
	g_legacyTimerTicker = tickLegacyTimers();
	g_engine = std::make_shared<SimEventEngine>(g_simulator->getCurrentProcess());
	grpc_event_engine::experimental::SetDefaultEventEngine(g_engine);
	grpc_event_engine::experimental::SetEventEngineFactory([]() -> std::shared_ptr<EventEngine> { return g_engine; });
}

SimEventEngineStats simEventEngineStats() {
	SimEventEngineStats s;
	s.connects = g_connects;
	s.clockReads = g_clockReads;
	s.offThreadCalls = g_offThreadCalls;
	s.droppedClosures = g_engine ? g_engine->pendingTasks() : 0;
	s.strandedClosures = g_engine ? g_engine->strandedTasks() : 0;
	return s;
}

} // namespace sim_grpc

#endif // FLOW_GRPC_ENABLED
