/*
 * RestoreRoleCommon.h
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

// This file delcares common struct and functions shared by restore roles, i.e.,
// RestoreMaster, RestoreLoader, RestoreApplier

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RestoreRoleCommon_G_H)
#define FDBSERVER_RestoreRoleCommon_G_H
#include "fdbserver/RestoreRoleCommon.actor.g.h"
#elif !defined(FDBSERVER_RestoreRoleCommon_H)
#define FDBSERVER_RestoreRoleCommon_H

#include <sstream>
#include "flow/Stats.h"
#include "flow/SystemMonitor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Notified.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/RestoreWorkerInterface.actor.h"
#include "fdbserver/RestoreUtil.h"

#include "flow/actorcompiler.h" // has to be last include

struct RestoreRoleInterface;
struct RestoreLoaderInterface;
struct RestoreApplierInterface;

struct RestoreRoleData;
struct RestoreMasterData;

struct RestoreSimpleRequest;

using VersionedMutationsMap = std::map<Version, MutationsVec>;

ACTOR Future<Void> isSchedulable(Reference<RestoreRoleData> self, int actorBatchIndex, std::string name);
ACTOR Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id);
ACTOR Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self);
void handleFinishRestoreRequest(const RestoreFinishRequest& req, Reference<RestoreRoleData> self);

// Helper class for reading restore data from a buffer and throwing the right errors.
// This struct is mostly copied from StringRefReader. We add a sanity check in this struct.
// We want to decouple code between fast restore and old restore. So we keep this duplicate struct
struct BackupStringRefReader {
	BackupStringRefReader(StringRef s = StringRef(), Error e = Error())
	  : rptr(s.begin()), end(s.end()), failure_error(e), str_size(s.size()) {}

	// Return remainder of data as a StringRef
	StringRef remainder() { return StringRef(rptr, end - rptr); }

	// Return a pointer to len bytes at the current read position and advance read pos
	// Consume a little-Endian data. Since we only run on little-Endian machine, the data on storage is little Endian
	const uint8_t* consume(unsigned int len) {
		if (rptr == end && len != 0) throw end_of_stream();
		const uint8_t* p = rptr;
		rptr += len;
		if (rptr > end) {
			printf("[ERROR] BackupStringRefReader throw error! string length:%d\n", str_size);
			printf("!!!!!!!!!!!![ERROR]!!!!!!!!!!!!!! Worker may die due to the error. Master will stuck when a worker "
			       "die\n");
			throw failure_error;
		}
		return p;
	}

	// Return a T from the current read position and advance read pos
	template <typename T>
	const T consume() {
		return *(const T*)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte oselfer) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	int32_t consumeNetworkInt32() { return (int32_t)bigEndian32((uint32_t)consume<int32_t>()); }
	uint32_t consumeNetworkUInt32() { return bigEndian32(consume<uint32_t>()); }

	// Convert big Endian value (e.g., encoded in log file) into a littleEndian uint64_t value.
	int64_t consumeNetworkInt64() { return (int64_t)bigEndian64((uint32_t)consume<int64_t>()); }
	uint64_t consumeNetworkUInt64() { return bigEndian64(consume<uint64_t>()); }

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	const int str_size;
	Error failure_error;
};

class RoleVersionBatchState {
public:
	static const int INVALID = -1;

	virtual int get() {
		return vbState;
	}

	virtual void operator = (int newState) {
		vbState = newState;
	}

	explicit RoleVersionBatchState() : vbState(INVALID) {}
	explicit RoleVersionBatchState(int newState) : vbState(newState) {}

	virtual ~RoleVersionBatchState() = default;

	int vbState;
};

struct RestoreRoleData : NonCopyable, public ReferenceCounted<RestoreRoleData> {
public:
	RestoreRole role;
	UID nodeID;
	int nodeIndex;

	double cpuUsage;
	double memory;
	double residentMemory;

	AsyncTrigger checkMemory;
	int delayedActors; // actors that are delayed to release because of low memory

	std::map<UID, RestoreLoaderInterface> loadersInterf; // UID: loaderInterf's id
	std::map<UID, RestoreApplierInterface> appliersInterf; // UID: applierInterf's id

	NotifiedVersion versionBatchId; // The index of the version batch that has been initialized and put into pipeline
	NotifiedVersion finishedBatch; // The highest batch index all appliers have applied mutations

	bool versionBatchStart = false;

	RestoreRoleData() : role(RestoreRole::Invalid), cpuUsage(0.0), memory(0.0), residentMemory(0.0), delayedActors(0){};

	virtual ~RestoreRoleData() = default;

	UID id() const { return nodeID; }

	virtual void initVersionBatch(int batchIndex) = 0;
	virtual void resetPerRestoreRequest() = 0;
	virtual int getVersionBatchState(int batchIndex) = 0;
	virtual void setVersionBatchState(int batchIndex, int vbState) = 0;

	void clearInterfaces() {
		loadersInterf.clear();
		appliersInterf.clear();
	}

	virtual std::string describeNode() = 0;
};

void updateProcessStats(Reference<RestoreRoleData> self);
ACTOR Future<Void> traceProcessMetrics(Reference<RestoreRoleData> self, std::string role);
ACTOR Future<Void> traceRoleVersionBatchProgress(Reference<RestoreRoleData> self, std::string role);

#include "flow/unactorcompiler.h"
#endif
