/*
 * ExternalWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "flow/ThreadHelper.actor.h"
#include "flow/Platform.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "foundationdb/CppWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

extern void flushTraceFileVoid();

namespace {

template <class T>
struct FDBPromiseImpl : FDBPromise {
	Promise<T> impl;
	FDBPromiseImpl(Promise<T> impl) : impl(impl) {}
	void send(void* value) override {
		if (g_network->isOnMainThread()) {
			impl.send(*reinterpret_cast<T*>(value));
		} else {
			onMainThreadVoid([impl = impl, val = *reinterpret_cast<T*>(value)]() -> Future<Void> {
				impl.send(val);
				return Void();
			});
		}
	}
};

ACTOR template <class F, class T>
void keepAlive(F until, T db) {
	try {
		wait(success(until));
	} catch (...) {
	}
}

struct FDBLoggerImpl : FDBLogger {
	static FDBLogger* instance() {
		static FDBLoggerImpl impl;
		return &impl;
	}
	void trace(FDBSeverity sev,
	           const std::string& name,
	           const std::vector<std::pair<std::string, std::string>>& details) override {
		auto traceFun = [=]() -> Future<Void> {
			Severity severity;
			switch (sev) {
			case FDBSeverity::Debug:
				severity = SevDebug;
				break;
			case FDBSeverity::Info:
				severity = SevInfo;
				break;
			case FDBSeverity::Warn:
				severity = SevWarn;
				break;
			case FDBSeverity::WarnAlways:
				severity = SevWarnAlways;
				break;
			case FDBSeverity::Error:
				severity = SevError;
				break;
			}
			TraceEvent evt(severity, name.c_str());
			for (const auto& p : details) {
				evt.detail(p.first.c_str(), p.second);
			}
			return Void();
		};
		if (g_network->isOnMainThread()) {
			traceFun();
			flushTraceFileVoid();
		} else {
			onMainThreadVoid([traceFun]() -> Future<Void> {
				traceFun();
				flushTraceFileVoid();
				return Void();
			});
		}
	}
};

namespace capi {
	#include "foundationdb/CWorkload.h"
}
namespace translator {
	template <typename T>
	struct Wrapper {
		T inner;
	};

	namespace context {
		void trace(capi::FDBWorkloadContext* c_context, capi::FDBSeverity c_severity, const char* name, capi::CVector vec) {
			auto context = (FDBWorkloadContext*)c_context;
			auto severity = (FDBSeverity)c_severity;
			std::vector<std::pair<std::string, std::string>> details;
			auto pairs = (capi::CStringPair*)vec.elements;
			for (int i = 0; i < vec.n; i++) {
				details.push_back(std::pair<std::string, std::string>(pairs[i].key, pairs[i].val));
			}
			return context->trace(severity, name, details);
		}
		uint64_t getProcessID(capi::FDBWorkloadContext* c_context) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->getProcessID();
		}
		void setProcessID(capi::FDBWorkloadContext* c_context, uint64_t processID) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->setProcessID(processID);
		}
		double now(capi::FDBWorkloadContext* c_context) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->now();
		}
		uint32_t rnd(capi::FDBWorkloadContext* c_context) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->rnd();
		}
		char* getOption(capi::FDBWorkloadContext* c_context, const char* name, const char* defaultValue) {
			auto context = (FDBWorkloadContext*)c_context;
			std::string str = context->getOption(name, std::string(defaultValue));
			size_t len = str.length() + 1;
			char* c_str = (char*)malloc(len);
			memcpy(c_str, str.c_str(), len);
			return c_str;
		}
		int clientId(capi::FDBWorkloadContext* c_context) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->clientId();
		}
		int clientCount(capi::FDBWorkloadContext* c_context) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->clientCount();
		}
		int64_t sharedRandomNumber(capi::FDBWorkloadContext* c_context) {
			auto context = (FDBWorkloadContext*)c_context;
			return context->sharedRandomNumber();
		}
	} // namespace context

	namespace promise {
		void send(capi::FDBPromise* c_promise, bool value) {
			auto promise = (Wrapper<GenericPromise<bool>>*)c_promise;
			promise->inner.send(value);
		}
		void free(capi::FDBPromise* c_promise) {
			auto promise = (Wrapper<GenericPromise<bool>>*)c_promise;
			delete promise;
		}
	} // namespace promise

	class Workload : public FDBWorkload {
	private:
		capi::BridgeToClient c;

	public:
		Workload(capi::BridgeToClient bridgeToClient) : c(bridgeToClient) {}
		~Workload() { this->c.free(this->c.workload); }

		virtual bool init(FDBWorkloadContext* context) override { return true; }
		virtual void setup(FDBDatabase* db, GenericPromise<bool> done) override {
			auto wrapped = (capi::FDBPromise*)new Wrapper<GenericPromise<bool>>{ done };
			return this->c.setup(this->c.workload, db, wrapped);
		}
		virtual void start(FDBDatabase* db, GenericPromise<bool> done) override {
			auto wrapped = (capi::FDBPromise*)new Wrapper<GenericPromise<bool>>{ done };
			return this->c.start(this->c.workload, db, wrapped);
		}
		virtual void check(FDBDatabase* db, GenericPromise<bool> done) override {
			auto wrapped = (capi::FDBPromise*)new Wrapper<GenericPromise<bool>>{ done };
			return this->c.check(this->c.workload, db, wrapped);
		}
		virtual void getMetrics(std::vector<FDBPerfMetric>& out) const override {
			auto vec = this->c.getMetrics(this->c.workload);
			auto metrics = (capi::CMetric*)vec.elements;
			for (int i = 0; i < vec.n; i++) {
				out.emplace_back(FDBPerfMetric{
					std::string(metrics[i].name),
					metrics[i].value,
					metrics[i].averaged,
					std::string(metrics[i].format_code),
				});
			}
		}
		virtual double getCheckTimeout() override { return this->c.getCheckTimeout(this->c.workload); }
	};
} // namespace translator

struct ExternalWorkload : TestWorkload, FDBWorkloadContext {
	std::string libraryName, libraryPath;
	bool success = true;
	void* library = nullptr;
	std::shared_ptr<FDBWorkload> workloadImpl;

	constexpr static auto NAME = "External";

	static std::string getDefaultLibraryPath() {
		auto self = exePath();
		// we try to resolve self/../../share/foundationdb/libame.so
		return abspath(joinPath(joinPath(popPath(popPath(self)), "share"), "foundationdb"));
	}

	static std::string toLibName(const std::string& name) {
#if defined(__unixish__) && !defined(__APPLE__)
		return format("lib%s.so", name.c_str());
#elif defined(__APPLE__)
		return format("lib%s.dylib", name.c_str());
#elif defined(_WIN32)
		return format("lib%s.dll", name.c_str());
#else
#error Port me!
#endif
	}

	explicit ExternalWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		bool useCAPI = ::getOption(options, "useCAPI"_sr, false);
		libraryName = ::getOption(options, "libraryName"_sr, ""_sr).toString();
		libraryPath = ::getOption(options, "libraryPath"_sr, Value(getDefaultLibraryPath())).toString();
		auto wName = ::getOption(options, "workloadName"_sr, ""_sr);
		auto fullPath = joinPath(libraryPath, toLibName(libraryName));
		// std::cout << "ExternalWorkload::useCAPI: " << useCAPI << "\n";
		TraceEvent("ExternalWorkloadLoad")
		    .detail("LibraryName", libraryName)
		    .detail("LibraryPath", fullPath)
		    .detail("WorkloadName", wName);
		library = loadLibrary(fullPath.c_str());
		if (library == nullptr) {
			TraceEvent(SevError, "ExternalWorkloadLoadError").log();
			success = false;
			return;
		}

		if (useCAPI) {
			capi::BridgeToClient (*workloadInstantiate)(char*, FDBWorkloadContext*, capi::BridgeToServer);
			workloadInstantiate =
			    reinterpret_cast<decltype(workloadInstantiate)>(loadFunction(library, "workloadInstantiate"));
			if (workloadInstantiate == nullptr) {
				TraceEvent(SevError, "ExternalInstantiateNotFound").log();
				success = false;
				return;
			}
			capi::BridgeToServer bridgeToServer = {
				.context = {
					.trace = translator::context::trace,
					.getProcessID = translator::context::getProcessID,
					.setProcessID = translator::context::setProcessID,
					.now = translator::context::now,
					.rnd = translator::context::rnd,
					.getOption = translator::context::getOption,
					.clientId = translator::context::clientId,
					.clientCount = translator::context::clientCount,
					.sharedRandomNumber = translator::context::sharedRandomNumber,
				},
				.promise = {
					.send = translator::promise::send,
					.free = translator::promise::free,
				},
			};
			auto name = wName.toString();
			size_t len = name.length() + 1;
			char* c_str = (char*)malloc(len);
			memcpy(c_str, name.c_str(), len);
			capi::BridgeToClient bridgeToClient = (*workloadInstantiate)(c_str, this, bridgeToServer);
			workloadImpl = std::make_shared<translator::Workload>(bridgeToClient);
		} else {
			FDBWorkloadFactory* (*workloadFactory)(FDBLogger*);
			workloadFactory = reinterpret_cast<decltype(workloadFactory)>(loadFunction(library, "workloadFactory"));
			if (workloadFactory == nullptr) {
				TraceEvent(SevError, "ExternalFactoryNotFound").log();
				success = false;
				return;
			}
			workloadImpl = (*workloadFactory)(FDBLoggerImpl::instance())->create(wName.toString());
			if (!workloadImpl) {
				TraceEvent(SevError, "WorkloadNotFound").log();
				success = false;
				return;
			}
		}
		workloadImpl->init(this);
	}

	~ExternalWorkload() override {
		workloadImpl = nullptr;
		if (library) {
			closeLibrary(library);
		}
	}

	ACTOR Future<Void> assertTrue(StringRef stage, Future<bool> f) {
		bool res = wait(f);
		if (!res) {
			TraceEvent(SevError, "ExternalWorkloadFailure").detail("Stage", stage);
		}
		return Void();
	}

	Future<Void> setup(Database const& cx) override {
		if (!success) {
			return Void();
		}
		auto db = cx.getPtr();
		db->addref();
		Reference<IDatabase> database(new ThreadSafeDatabase(db));
		Promise<bool> promise;
		auto f = promise.getFuture();
		keepAlive(f, database);
		workloadImpl->setup(reinterpret_cast<FDBDatabase*>(database.getPtr()),
		                    GenericPromise<bool>(new FDBPromiseImpl(promise)));
		return assertTrue("setup"_sr, f);
	}

	Future<Void> start(Database const& cx) override {
		if (!success) {
			return Void();
		}
		auto db = cx.getPtr();
		db->addref();
		Reference<IDatabase> database(new ThreadSafeDatabase(db));
		Promise<bool> promise;
		auto f = promise.getFuture();
		keepAlive(f, database);
		workloadImpl->start(reinterpret_cast<FDBDatabase*>(database.getPtr()),
		                    GenericPromise<bool>(new FDBPromiseImpl(promise)));
		return assertTrue("start"_sr, f);
	}
	Future<bool> check(Database const& cx) override {
		if (!success) {
			return false;
		}
		auto db = cx.getPtr();
		db->addref();
		Reference<IDatabase> database(new ThreadSafeDatabase(db));
		Promise<bool> promise;
		auto f = promise.getFuture();
		keepAlive(f, database);
		workloadImpl->check(reinterpret_cast<FDBDatabase*>(database.getPtr()),
		                    GenericPromise<bool>(new FDBPromiseImpl(promise)));
		return f;
	}
	void getMetrics(std::vector<PerfMetric>& out) override {
		if (!success) {
			return;
		}
		std::vector<FDBPerfMetric> metrics;
		workloadImpl->getMetrics(metrics);
		for (const auto& m : metrics) {
			out.emplace_back(m.name, m.value, Averaged{ m.averaged }, m.format_code);
		}
	}

	double getCheckTimeout() const override {
		if (!success) {
			return 3000;
		}
		return workloadImpl->getCheckTimeout();
	}

	// context implementation
	void trace(FDBSeverity sev,
	           const std::string& name,
	           const std::vector<std::pair<std::string, std::string>>& details) override {
		return FDBLoggerImpl::instance()->trace(sev, name, details);
	}
	uint64_t getProcessID() const override {
		if (g_network->isSimulated()) {
			return reinterpret_cast<uint64_t>(g_simulator->getCurrentProcess());
		} else {
			return 0ul;
		}
	}
	void setProcessID(uint64_t processID) override {
		if (g_network->isSimulated()) {
			g_simulator->currentProcess = reinterpret_cast<ISimulator::ProcessInfo*>(processID);
		}
	}
	double now() const override { return g_network->now(); }
	uint32_t rnd() const override { return deterministicRandom()->randomUInt32(); }
	bool getOption(const std::string& name, bool defaultValue) override {
		return ::getOption(options, Value(name), defaultValue);
	}
	long getOption(const std::string& name, long defaultValue) override {
		return ::getOption(options, Value(name), int64_t(defaultValue));
	}
	unsigned long getOption(const std::string& name, unsigned long defaultValue) override {
		return ::getOption(options, Value(name), uint64_t(defaultValue));
	}
	double getOption(const std::string& name, double defaultValue) override {
		return ::getOption(options, Value(name), defaultValue);
	}
	std::string getOption(const std::string& name, std::string defaultValue) override {
		return ::getOption(options, Value(name), Value(defaultValue)).toString();
	}

	int clientId() const override { return WorkloadContext::clientId; }

	int clientCount() const override { return WorkloadContext::clientCount; }

	int64_t sharedRandomNumber() const override { return WorkloadContext::sharedRandomNumber; }
};
} // namespace

WorkloadFactory<ExternalWorkload> CycleWorkloadFactory;
