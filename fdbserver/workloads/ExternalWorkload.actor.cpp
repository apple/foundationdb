/*
 * ExternalWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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
#include "foundationdb/ClientWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace {

template <class T>
struct FDBPromiseImpl : FDBPromise {
	Promise<T> impl;
	FDBPromiseImpl(Promise<T> impl) : impl(impl) {}
	void send(void* value) override { impl.send(*reinterpret_cast<T*>(value)); }
};

struct ExternalWorkload : TestWorkload, FDBWorkloadContext {
	std::string libraryName, libraryPath;
	bool success = true;
	void* library = nullptr;
	FDBWorkloadFactory* workloadFactory;
	std::shared_ptr<FDBWorkload> workloadImpl;

	constexpr static const char* NAME = "External";

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
#elif defined(__WIN32__)
		return format("lib%s.dll", name.c_str());
#else
#error Port me!
#endif
	}

	explicit ExternalWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		libraryName = ::getOption(options, LiteralStringRef("libraryName"), LiteralStringRef("")).toString();
		libraryPath = ::getOption(options, LiteralStringRef("LibraryPath"), Value(getDefaultLibraryPath())).toString();
		auto wName = ::getOption(options, LiteralStringRef("workloadName"), LiteralStringRef(""));
		auto fullPath = joinPath(libraryPath, toLibName(libraryName));
		library = loadLibrary(fullPath.c_str());
		if (library == nullptr) {
			success = false;
			return;
		}
		workloadFactory = reinterpret_cast<decltype(workloadFactory)>(loadFunction(library, "workloadFactory"));
		if (workloadFactory == nullptr) {
			TraceEvent(SevError, "ExternalFactoryNotFound");
			success = false;
			return;
		}
		workloadImpl = workloadFactory->create(wName.toString());
		if (!workloadImpl) {
			TraceEvent(SevError, "WorkloadNotFound");
			success = false;
		}
		workloadImpl->init(this);
	}

	~ExternalWorkload() {
		workloadImpl = nullptr;
		if (library) {
			closeLibrary(library);
		}
	}

	std::string description() override { return NAME; }

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
		workloadImpl->setup(reinterpret_cast<FDBDatabase*>(database.extractPtr()),
							GenericPromise<bool>(new FDBPromiseImpl(promise)));
		return assertTrue(LiteralStringRef("setup"), f);
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
		workloadImpl->start(reinterpret_cast<FDBDatabase*>(database.extractPtr()),
							GenericPromise<bool>(new FDBPromiseImpl(promise)));
		return assertTrue(LiteralStringRef("start"), f);
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
		workloadImpl->start(reinterpret_cast<FDBDatabase*>(database.extractPtr()),
							GenericPromise<bool>(new FDBPromiseImpl(promise)));
		return f;
	}
	void getMetrics(vector<PerfMetric>& out) override {
		if (!success) {
			return;
		}
		std::vector<FDBPerfMetric> metrics;
		workloadImpl->getMetrics(metrics);
		for (const auto& m : metrics) {
			out.emplace_back(m.name, m.value, m.averaged, m.format_code);
		}
	}

	double getCheckTimeout() override {
		if (!success) {
			return 3000;
		}
		return workloadImpl->getCheckTimeout();
	}

	// context implementation
	FDBFuture* trace(FDBSeverity sev, const std::string& name,
					 const std::vector<std::pair<std::string, std::string>>& details) override {
		auto res = onMainThread([=]() -> Future<Void> {
			Severity sev;
			TraceEvent evt(sev, name.c_str());
			for (const auto& p : details) {
				evt.detail(p.first.c_str(), p.second);
			}
			return Void();
		});
		return reinterpret_cast<FDBFuture*>(res.extractPtr());
	}
	bool getOption(const std::string& name, bool defaultValue) override {
		return ::getOption(options, Value(name), defaultValue);
	}
	long getOption(const std::string& name, long defaultValue) override {
		return ::getOption(options, Value(name), int64_t(defaultValue));
	}
	unsigned long getOption(const std::string& name, unsigned long defaultValue) {
		return ::getOption(options, Value(name), uint64_t(defaultValue));
	}
	double getOption(const std::string& name, double defaultValue) {
		return ::getOption(options, Value(name), defaultValue);
	}
	std::string getOption(const std::string& name, std::string defaultValue) {
		return ::getOption(options, Value(name), Value(defaultValue)).toString();
	}
};
} // namespace

WorkloadFactory<ExternalWorkload> CycleWorkloadFactory(ExternalWorkload::NAME);
