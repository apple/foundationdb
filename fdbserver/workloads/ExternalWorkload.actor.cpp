/*
 * ExternalWorkload.actor.cpp
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

namespace metrics {
void reserve(capi::OpaqueMetrics* c_metrics, int n) {
	auto metrics = (std::vector<FDBPerfMetric>*)c_metrics;
	metrics->reserve(metrics->size() + n);
}
void push(capi::OpaqueMetrics* c_metrics, capi::FDBMetric c_metric) {
	auto metrics = (std::vector<FDBPerfMetric>*)c_metrics;
	auto fmt = c_metric.fmt ? c_metric.fmt : "%.3g";
	auto metric = FDBPerfMetric{
		.name = std::string(c_metric.key),
		.value = c_metric.val,
		.averaged = c_metric.avg,
		.format_code = std::string(fmt),
	};
	metrics->emplace_back(metric);
}
capi::FDBMetrics wrap(std::vector<FDBPerfMetric>* metrics) {
	static capi::FDBMetrics::FDBMetrics_VT vt = {
		.reserve = reserve,
		.push = push,
	};
	return capi::FDBMetrics{
		.inner = (capi::OpaqueMetrics*)metrics,
		.vt = &vt,
	};
}
} // namespace metrics

namespace promise {
void send(capi::OpaquePromise* c_promise, bool value) {
	auto promise = (Wrapper<GenericPromise<bool>>*)c_promise;
	promise->inner.send(value);
}
void free(capi::OpaquePromise* c_promise) {
	auto promise = (Wrapper<GenericPromise<bool>>*)c_promise;
	delete promise;
}
capi::FDBPromise wrap(GenericPromise<bool> promise) {
	static capi::FDBPromise::FDBPromise_VT vt{
		.free = free,
		.send = send,
	};
	auto wrapped = new Wrapper<GenericPromise<bool>>{ promise };
	return capi::FDBPromise{
		.inner = (capi::OpaquePromise*)wrapped,
		.vt = &vt,
	};
}
} // namespace promise

namespace context {
void trace(capi::OpaqueWorkloadContext* c_context,
           capi::FDBSeverity c_severity,
           const char* name,
           const capi::FDBStringPair* c_details,
           int n) {
	auto context = (FDBWorkloadContext*)c_context;
	FDBSeverity severity;
	switch (c_severity) {
	case capi::FDBSeverity_Debug:
		severity = FDBSeverity::Debug;
		break;
	case capi::FDBSeverity_Info:
		severity = FDBSeverity::Info;
		break;
	case capi::FDBSeverity_Warn:
		severity = FDBSeverity::Warn;
		break;
	case capi::FDBSeverity_WarnAlways:
		severity = FDBSeverity::WarnAlways;
		break;
	case capi::FDBSeverity_Error:
		severity = FDBSeverity::Error;
		break;
	}
	std::vector<std::pair<std::string, std::string>> details;
	details.reserve(n);
	for (int i = 0; i < n; i++) {
		details.emplace_back(std::pair<std::string, std::string>(c_details[i].key, c_details[i].val));
	}
	context->trace(severity, name, details);
}
uint64_t getProcessID(capi::OpaqueWorkloadContext* c_context) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->getProcessID();
}
void setProcessID(capi::OpaqueWorkloadContext* c_context, uint64_t processID) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->setProcessID(processID);
}
double now(capi::OpaqueWorkloadContext* c_context) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->now();
}
uint32_t rnd(capi::OpaqueWorkloadContext* c_context) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->rnd();
}
capi::FDBString getOption(capi::OpaqueWorkloadContext* c_context, const char* name, const char* defaultValue) {
	static capi::FDBString::FDBString_VT vt{
		.free = (void (*)(const char*))free,
	};
	auto context = (FDBWorkloadContext*)c_context;
	std::string value = context->getOption(name, std::string(defaultValue));
	size_t len = value.length() + 1;
	char* c_value = (char*)malloc(len);
	memcpy(c_value, value.c_str(), len);
	return capi::FDBString{
		.inner = c_value,
		.vt = &vt,
	};
}
int clientId(capi::OpaqueWorkloadContext* c_context) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->clientId();
}
int clientCount(capi::OpaqueWorkloadContext* c_context) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->clientCount();
}
int64_t sharedRandomNumber(capi::OpaqueWorkloadContext* c_context) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->sharedRandomNumber();
}
FDBFuture* delay(capi::OpaqueWorkloadContext* c_context, double seconds) {
	auto context = (FDBWorkloadContext*)c_context;
	return context->delay(seconds);
}
capi::FDBWorkloadContext wrap(FDBWorkloadContext* context) {
	static capi::FDBWorkloadContext::FDBWorkloadContext_VT vt{
		.trace = trace,
		.getProcessID = getProcessID,
		.setProcessID = setProcessID,
		.now = now,
		.rnd = rnd,
		.getOption = getOption,
		.clientId = clientId,
		.clientCount = clientCount,
		.sharedRandomNumber = sharedRandomNumber,
		.delay = delay,
	};
	return capi::FDBWorkloadContext{
		.api_version = FDB_WORKLOAD_API_VERSION,
		.inner = (capi::OpaqueWorkloadContext*)context,
		.vt = &vt,
	};
}
} // namespace context

class Workload : public FDBWorkload {
private:
	capi::OpaqueWorkload* inner;
	capi::FDBWorkload::FDBWorkload_VT* vt;

public:
	Workload(capi::FDBWorkload c_workload) : inner(c_workload.inner), vt(c_workload.vt) {}
	~Workload() { this->vt->free(this->inner); }

	virtual bool init(FDBWorkloadContext* context) override { return true; }
	virtual void setup(FDBDatabase* db, GenericPromise<bool> done) override {
		return this->vt->setup(this->inner, (capi::FDBDatabase*)db, promise::wrap(done));
	}
	virtual void start(FDBDatabase* db, GenericPromise<bool> done) override {
		return this->vt->start(this->inner, (capi::FDBDatabase*)db, promise::wrap(done));
	}
	virtual void check(FDBDatabase* db, GenericPromise<bool> done) override {
		return this->vt->check(this->inner, (capi::FDBDatabase*)db, promise::wrap(done));
	}
	virtual void getMetrics(std::vector<FDBPerfMetric>& out) const override {
		this->vt->getMetrics(this->inner, metrics::wrap(&out));
	}
	virtual double getCheckTimeout() override { return this->vt->getCheckTimeout(this->inner); }
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
			capi::FDBWorkload (*workloadCFactory)(const char*, capi::FDBWorkloadContext);
			workloadCFactory = reinterpret_cast<decltype(workloadCFactory)>(loadFunction(library, "workloadCFactory"));
			if (workloadCFactory == nullptr) {
				TraceEvent(SevError, "ExternalCFactoryNotFound").log();
				success = false;
				return;
			}
			auto name = wName.toString();
			capi::FDBWorkload c_workload = (*workloadCFactory)(name.c_str(), translator::context::wrap(this));
			workloadImpl = std::make_shared<translator::Workload>(c_workload);
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

	FDBFuture* delay(double seconds) const override {
		ThreadFuture<Void> future =
		    onMainThread([seconds]() -> Future<Void> { return g_network->delay(seconds, TaskPriority::DefaultDelay); });
		return (capi::FDBFuture*)future.extractPtr();
	}
};
} // namespace

WorkloadFactory<ExternalWorkload> CycleWorkloadFactory;
