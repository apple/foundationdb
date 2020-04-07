/*
 * FBTrace.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/FastRef.h"
#include "flow/ObjectSerializer.h"
#include <type_traits>

class FBTraceImpl : public ReferenceCounted<FBTraceImpl> {
protected:
	virtual void write(ObjectWriter& writer) = 0;
public:
	virtual ~FBTraceImpl();
};

template <class T>
class FDBTrace : public FBTraceImpl {
	protected:
	void write(ObjectWriter& writer) override {
		writer.serialize(*static_cast<T*>(this));
	}
};

class GetValueDebugTrace : public FDBTrace<GetValueDebugTrace> {
public:
	constexpr static FileIdentifier file_identifier = 617894;
	enum codeLocation {
		STORAGESERVER_GETVALUE_RECEIVED = 0,
		STORAGESERVER_GETVALUE_DO_READ = 1,
		STORAGESERVER_GETVALUE_AFTER_VERSION = 2,
		STORAGESERVER_GETVALUE_AFTER_READ = 3,
		STORAGECACHE_GETVALUE_RECEIVED = 4,
		STORAGECACHE_GETVALUE_DO_READ = 5,
		STORAGECACHE_GETVALUE_AFTER_VERSION = 6,
		STORAGECACHE_GETVALUE_AFTER_READ = 7,
		READER_GETVALUE_BEFORE = 8,
		READER_GETVALUE_AFTER = 9,
		READER_GETVALUEPREFIX_BEFORE = 10,
		READER_GETVALUEPREFIX_AFTER = 11
	};

	uint64_t id;
	double time;
	int32_t location;

	GetValueDebugTrace() {}
	GetValueDebugTrace(uint64_t debugID, double t, codeLocation loc) : id(debugID), time(t), location(loc) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, time, location);
	}
};

class WatchValueDebugTrace : public FDBTrace<WatchValueDebugTrace> {
public:
	constexpr static FileIdentifier file_identifier = 14486715;
	enum codeLocation {
		STORAGESERVER_WATCHVALUE_BEFORE = 1,
		STORAGESERVER_WATCHVALUE_AFTER_VERSION = 2,
		STORAGESERVER_WATCHVALUE_AFTER_READ = 3,
		NATIVEAPI_WATCHVALUE_BEFORE = 4,
		NATIVEAPI_WATCHVALUE_AFTER_READ = 5
	};

	uint64_t id;
	double time;
	int32_t location;

	WatchValueDebugTrace() {}
	WatchValueDebugTrace(uint64_t debugID, double t, codeLocation loc) : id(debugID), time(t), location(loc) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, time, location);
	}
};

class CommitDebugTrace : public FDBTrace<CommitDebugTrace> {
public:
	constexpr static FileIdentifier file_identifier = 7691518;
	enum codeLocation {
		STORAGESERVER_COMMIT_BEORE = 0,
		STORAGESERVER_COMMIT_AFTER_VERSION = 1,
		STORAGESERVER_COMMIT_AFTER_READ = 2,
		NATIVEAPI_COMMIT_BEORE = 3,
		NATIVEAPI_COMMIT_AFTER = 4,
		MASTERROXYSERVER_BATCHER = 5,
		MASTERPROXYSERVER_COMMITBATCH_BEFORE = 6,
		MASTERPROXYSERVER_COMMITBATCH_GETTINGCOMMITVERSION = 7,
		MASTERPROXYSERVER_COMMITBATCH_GOTCOMMITVERSION = 8,
		MASTERPROXYSERVER_COMMITBATCH_AFTERRESOLUTION = 9,
		MASTERPROXYSERVER_COMMITBATCH_PROCESSINGMUTATIONS = 10,
		MASTERPROXYSERVER_COMMITBATCH_AFTERSTORECOMMITS = 11,
		MASTERPROXYSERVER_COMMITBATCH_AFTERLOGPUSH = 12,
		RESOLVER_RESOLVEBATCH_BEFORE = 13,
		RESOLVER_RESOLVEBATCH_AFTERQUEUESIZECHECK = 14,
		RESOLVER_RESOLVEBATCH_AFTERORDERER = 15,
		RESOLVER_RESOLVEBATCH_AFTER = 16,
		TLOG_TLOGCOMMIT_BEFOREWAITFORVERSION = 17,
		TLOG_TLOGCOMMIT_BEFORE = 18,
		TLOG_TLOGCOMMIT_AFTERTLOGCOMMIT = 19,
		TLOG_TLOGCOMMIT_AFTER = 20
	};

	uint64_t id;
	double time;
	int32_t location;

	CommitDebugTrace() {}
	CommitDebugTrace(uint64_t debugID, double t, codeLocation loc) : id(debugID), time(t), location(loc) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, time, location);
	}
};

class TransactionDebugTrace : public FDBTrace<TransactionDebugTrace> {
public:
	constexpr static FileIdentifier file_identifier = 6868728;
	enum codeLocation {
		STORAGESERVER_GETKEYVALUES_BEFORE = 0,
		STORAGESERVER_GETKEYVALUES_AFTERVERSION = 1,
		STORAGESERVER_GETKEYVALUES_AFTERKEYS = 2,
		STORAGESERVER_GETKEYVALUES_SEND = 3,
		STORAGESERVER_GETKEYVALUES_AFTERREADRANGE = 4,
		NATIVEAPI_GETKEYLOCATION_BEFORE = 5,
		NATIVEAPI_GETKEYLOCATION_AFTER = 6,
		NATIVEAPI_GETKEYLOCATIONS_BEFORE = 7,
		NATIVEAPI_GETKEYLOCATIONS_AFTER = 8,
		NATIVEAPI_GETVALUE_BEFORE = 9,
		NATIVEAPI_GETVALUE_AFTER = 10,
		NATIVEAPI_GETVALUE_ERROR = 11,
		NATIVEAPI_GETKEY_AFTERVERSION = 12,
		NATIVEAPI_GETKEY_BEFORE = 13,
		NATIVEAPI_GETKEY_AFTER = 14,
		NATIVEAPI_GETEXACTRANGE_BEFORE = 15,
		NATIVEAPI_GETEXACTRANGE_AFTER = 16,
		NATIVEAPI_GETRANGE_BEFORE = 17,
		NATIVEAPI_GETRANGE_AFTER = 18,
		NATIVEAPI_GETRANGE_ERROR = 19,
		NATIVEAPI_GETCONSISTENTREADVERSION_BEFORE = 20,
		NATIVEAPI_GETCONSISTENTREADVERSION_AFTER = 21,
		STORAGECACHE_GETKEYVALUES_BEFORE = 22,
		STORAGECACHE_GETKEYVALUES_AFTERVERSION = 23,
		STORAGECACHE_GETKEYVALUES_AFTERKEYS = 24,
		STORAGECACHE_GETKEYVALUES_SEND = 25,
		STORAGECACHE_GETKEYVALUES_AFTERREADRANGE = 26,
		MASTERPROXYSERVER_QUEUETRANSACTIONSTARTREQUESTS_BEFORE = 27,
		MASTERPROXYSERVER_GETLIVECOMMITTEDVERSION_CONFIRMEPOCHLIVE = 28,
		MASTERPROXYSERVER_GETLIVECOMMITTEDVERSION_AFTER = 29,
		MASTERPROXYSERVER_MASTERPROXYSERVERCORE_BROADCAST = 30,
		MASTERPROXYSERVER_MASTERPROXYSERVERCORE_GETRAWCOMMITTEDVERSION = 31,
		TLOGSERVER_TLOGCONFIRMRUNNINGREQUEST = 33,
		READWRITE_RANDOMREADWRITECLIENT_BEFORE = 34,
		READWRITE_RANDOMREADWRITECLIENT_AFTER = 35
	};

	uint64_t id;
	double time;
	int32_t location;

	TransactionDebugTrace() {}
	TransactionDebugTrace(uint64_t debugID, double t, codeLocation loc) : id(debugID), time(t), location(loc) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, time, location);
	}
};

void fbTrace(Reference<FBTraceImpl> const& traceLine);

template <class T>
void fbTrace(Reference<T> traceLine) {
	static_assert(std::is_base_of<FBTraceImpl, T>::value, "fbTrace only accepts FBTraceImpl as argument");
	fbTrace(traceLine.template castTo<FBTraceImpl>());
}
