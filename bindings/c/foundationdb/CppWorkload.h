/*
 * CppWorkload.h
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

#pragma once
#ifndef CPP_WORKLOAD_H
#define CPP_WORKLOAD_H
#include <string>
#include <vector>
#include <functional>
#include <memory>

#ifndef DLLEXPORT
#if defined(_MSC_VER)
#define DLLEXPORT __declspec(dllexport)
#elif defined(__GNUG__)
#define DLLEXPORT __attribute__((visibility("default")))
#else
#error Missing symbol export
#endif
#endif

typedef struct FDB_future FDBFuture;
typedef struct FDB_result FDBResult;
typedef struct FDB_database FDBDatabase;
typedef struct FDB_transaction FDBTransaction;

enum class FDBSeverity { Debug, Info, Warn, WarnAlways, Error };

class FDBLogger {
public:
	virtual void trace(FDBSeverity sev,
	                   const std::string& name,
	                   const std::vector<std::pair<std::string, std::string>>& details) = 0;
};

class FDBWorkloadContext : public FDBLogger {
public:
	virtual uint64_t getProcessID() const = 0;
	virtual void setProcessID(uint64_t processID) = 0;
	virtual double now() const = 0;
	virtual uint32_t rnd() const = 0;
	virtual bool getOption(const std::string& name, bool defaultValue) = 0;
	virtual long getOption(const std::string& name, long defaultValue) = 0;
	virtual unsigned long getOption(const std::string& name, unsigned long defaultValue) = 0;
	virtual double getOption(const std::string& name, double defaultValue) = 0;
	virtual std::string getOption(const std::string& name, std::string defaultValue) = 0;
	virtual int clientId() const = 0;
	virtual int clientCount() const = 0;
	virtual int64_t sharedRandomNumber() const = 0;
	virtual FDBFuture* delay(double seconds) const = 0;
};

struct FDBPromise {
	virtual ~FDBPromise() = default;
	virtual void send(void*) = 0;
};

template <class T>
class GenericPromise {
	std::shared_ptr<FDBPromise> impl;

public:
	template <class Ptr>
	explicit GenericPromise(Ptr&& impl) : impl(std::forward<Ptr>(impl)) {}
	void send(T val) { impl->send(&val); }
};

struct FDBPerfMetric {
	std::string name;
	double value;
	bool averaged;
	std::string format_code = "%.3g";
};

class DLLEXPORT FDBWorkload {
public:
	// virtual std::string description() const = 0;
	virtual bool init(FDBWorkloadContext* context) = 0;
	virtual void setup(FDBDatabase* db, GenericPromise<bool> done) = 0;
	virtual void start(FDBDatabase* db, GenericPromise<bool> done) = 0;
	virtual void check(FDBDatabase* db, GenericPromise<bool> done) = 0;
	virtual void getMetrics(std::vector<FDBPerfMetric>& out) const = 0;
	virtual double getCheckTimeout() { return 3000; }
};

class DLLEXPORT FDBWorkloadFactory {
public:
	virtual std::shared_ptr<FDBWorkload> create(const std::string& name) = 0;
};

#endif
