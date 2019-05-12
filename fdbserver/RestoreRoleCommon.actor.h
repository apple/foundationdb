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

// Delcare commone struct and functions used in fast restore

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RestoreRoleCommon_G_H)
	#define FDBSERVER_RestoreRoleCommon_G_H
	#include "fdbserver/RestoreRoleCommon.actor.g.h"
#elif !defined(FDBSERVER_RestoreRoleCommon_H)
	#define FDBSERVER_RestoreRoleCommon_H

#include <sstream>
#include "flow/Stats.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbrpc/Locality.h"

#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreWorkerInterface.h"

extern bool debug_verbose;
extern double mutationVectorThreshold;

struct RestoreRoleInterface;
struct RestoreLoaderInterface;
struct RestoreApplierInterface;

struct RestoreRoleData;
struct RestoreMasterData;

struct RestoreSimpleRequest;

ACTOR Future<Void> handleHeartbeat(RestoreSimpleRequest req, UID id);
ACTOR Future<Void> handleCollectRestoreRoleInterfaceRequest(RestoreSimpleRequest req, Reference<RestoreRoleData> self, Database cx);
ACTOR Future<Void> handleInitVersionBatchRequest(RestoreVersionBatchRequest req, Reference<RestoreRoleData> self);
ACTOR Future<Void> handlerFinishRestoreRequest(RestoreSimpleRequest req, Reference<RestoreRoleData> self, Database cx);

ACTOR Future<Void> _collectRestoreRoleInterfaces(Reference<RestoreRoleData> self, Database cx);

// Helper class for reading restore data from a buffer and throwing the right errors.
// This struct is mostly copied from StringRefReader. We add a sanity check in this struct.
// TODO: Merge this struct with StringRefReader.
struct StringRefReaderMX {
	StringRefReaderMX(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e), str_size(s.size()) {}

	// Return remainder of data as a StringRef
	StringRef remainder() {
		return StringRef(rptr, end - rptr);
	}

	// Return a pointer to len bytes at the current read position and advance read pos
	//Consume a little-Endian data. Since we only run on little-Endian machine, the data on storage is little Endian
	const uint8_t * consume(unsigned int len) {
		if(rptr == end && len != 0)
			throw end_of_stream();
		const uint8_t *p = rptr;
		rptr += len;
		if(rptr > end) {
			printf("[ERROR] StringRefReaderMX throw error! string length:%d\n", str_size);
			printf("!!!!!!!!!!!![ERROR]!!!!!!!!!!!!!! Worker may die due to the error. Master will stuck when a worker die\n");
			throw failure_error;
		}
		return p;
	}

	// Return a T from the current read position and advance read pos
	template<typename T> const T consume() {
		return *(const T *)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte oselfer) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	const int32_t  consumeNetworkInt32()  { return (int32_t)bigEndian32((uint32_t)consume< int32_t>());}
	const uint32_t consumeNetworkUInt32() { return          bigEndian32(          consume<uint32_t>());}

	const int64_t  consumeNetworkInt64()  { return (int64_t)bigEndian64((uint32_t)consume< int64_t>());}
	const uint64_t consumeNetworkUInt64() { return          bigEndian64(          consume<uint64_t>());}

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	const int str_size;
	Error failure_error;
};

struct RestoreRoleData :  NonCopyable, public ReferenceCounted<RestoreRoleData> {
public:	
	RestoreRole role;
	UID nodeID; // RestoreLoader role ID
	int nodeIndex; // RestoreLoader role index, which is continuous and easy for debuggging

	std::map<UID, RestoreLoaderInterface> loadersInterf;
	std::map<UID, RestoreApplierInterface> appliersInterf;
	RestoreApplierInterface masterApplierInterf;

	std::map<CMDUID, int> processedCmd;
	uint32_t inProgressFlag = 0;

	RestoreRoleData() : role(RestoreRole::Invalid) {};

	~RestoreRoleData() {};

	UID id() const { return nodeID; }

	bool isCmdProcessed(CMDUID const &cmdID) {
		return processedCmd.find(cmdID) != processedCmd.end();
	}

	// Helper functions to set/clear the flag when a worker is in the middle of processing an actor.
	void setInProgressFlag(RestoreCommandEnum phaseEnum) {
		int phase = (int) phaseEnum;
		ASSERT(phase < 32);
		inProgressFlag |= (1UL << phase);
	}

	void clearInProgressFlag(RestoreCommandEnum phaseEnum) {
		int phase = (int) phaseEnum;
		ASSERT(phase < 32);
		inProgressFlag &= ~(1UL << phase);
	}

	bool isInProgress(RestoreCommandEnum phaseEnum) {
		int phase = (int) phaseEnum;
		ASSERT(phase < 32);
		return (inProgressFlag & (1UL << phase));
	}

	void resetPerVersionBatch() {
		processedCmd.clear();		
		inProgressFlag = 0;
	}

	void clearInterfaces() {
		loadersInterf.clear();
		appliersInterf.clear();
	}

	std::string describeNode() {
		std::stringstream ss;
		ss << "RestoreRoleData role:" << getRoleStr(role) << " nodeID:%s" << nodeID.toString();
		return ss.str();
	}

	void printRestoreRoleInterfaces() {
		printf("Dump restore loaders and appliers info:\n");
		for (auto &loader : loadersInterf) {
			printf("Loader:%s\n", loader.first.toString().c_str());
		}

		for (auto &applier : appliersInterf) {
			printf("Applier:%s\n", applier.first.toString().c_str());
		}
	}

	// TODO: To remove this function
	std::vector<UID> getApplierIDs() {
		std::vector<UID> applierIDs;
		for (auto &applier : appliersInterf) {
			applierIDs.push_back(applier.first);
		}
		return applierIDs;
	}

	// TODO: To remove this function
	std::vector<UID> getLoaderIDs() {
		std::vector<UID> loaderIDs;
		for (auto &loader : loadersInterf) {
			loaderIDs.push_back(loader.first);
		}

		return loaderIDs;
	}

	// TODO: To remove this function
	std::vector<UID> getWorkerIDs() {
		std::vector<UID> workerIDs;
		for (auto &loader : loadersInterf) {
			workerIDs.push_back(loader.first);
		}
		for (auto &applier : appliersInterf) {
			workerIDs.push_back(applier.first);
		}

		return workerIDs;
	}

};

void printLowerBounds(std::vector<Standalone<KeyRef>> lowerBounds);
void printApplierKeyRangeInfo(std::map<UID, Standalone<KeyRangeRef>>  appliers);

#endif