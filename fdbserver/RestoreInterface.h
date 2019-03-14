/*
 * RestoreInterface.h
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

#ifndef FDBCLIENT_RestoreInterface_H
#define FDBCLIENT_RestoreInterface_H
#pragma once

#include <sstream>
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
//#include "fdbclient/NativeAPI.h" //MX: Cannot have NativeAPI.h in this .h
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbrpc/Locality.h"

class RestoreConfig;
enum class RestoreRole {Invalid = 0, Master = 1, Loader, Applier};
extern std::vector<std::string> RestoreRoleStr;
BINARY_SERIALIZABLE( RestoreRole );


// Timeout threshold in seconds for restore commands
extern int FastRestore_Failure_Timeout;


// RestoreCommandEnum is also used as the phase ID for CMDUID
enum class RestoreCommandEnum {Init = -1,
		Set_Role = 0, Set_Role_Done,
		Assign_Applier_KeyRange = 2, Assign_Applier_KeyRange_Done,
								Assign_Loader_Range_File = 4, Assign_Loader_Log_File = 5, Assign_Loader_File_Done = 6,
								Loader_Send_Mutations_To_Applier = 7, Loader_Send_Mutations_To_Applier_Done = 8,
								Apply_Mutation_To_DB = 9, Apply_Mutation_To_DB_Skip = 10,
								Loader_Notify_Appler_To_Apply_Mutation = 11,
								Notify_Loader_ApplierKeyRange = 12, Notify_Loader_ApplierKeyRange_Done = 13,
								Sample_Range_File = 14, Sample_Log_File = 15, Sample_File_Done = 16,
								Loader_Send_Sample_Mutation_To_Applier = 17, Loader_Send_Sample_Mutation_To_Applier_Done = 18,
								Calculate_Applier_KeyRange = 19, Get_Applier_KeyRange=20, Get_Applier_KeyRange_Done = 21};
BINARY_SERIALIZABLE(RestoreCommandEnum);

// Restore command's UID. uint64_t part[2];
// part[0] is the phase id, part[1] is the command index in the phase.
// TODO: Add another field to indicate version-batch round
class CMDUID {
public:
	uint64_t part[2];
	CMDUID() { part[0] = part[1] = 0; }
	CMDUID( uint64_t a, uint64_t b ) { part[0]=a; part[1]=b; }
	CMDUID(const CMDUID &cmduid) { part[0] = cmduid.part[0]; part[1] = cmduid.part[1]; }

	void initPhase(RestoreCommandEnum phase);

	void nextPhase(); // Set to the next phase.

	void nextCmd(); // Increase the command index at the same phase

	RestoreCommandEnum getPhase();

	uint64_t getIndex();

	std::string toString() const;

	bool operator == ( const CMDUID& r ) const { return part[0]==r.part[0] && part[1]==r.part[1]; }
	bool operator != ( const CMDUID& r ) const { return part[0]!=r.part[0] || part[1]!=r.part[1]; }
	bool operator < ( const CMDUID& r ) const { return part[0] < r.part[0] || (part[0] == r.part[0] && part[1] < r.part[1]); }

	uint64_t hash() const { return first(); }
	uint64_t first() const { return part[0]; }
	uint64_t second() const { return part[1]; }

	//

	template <class Ar>
	void serialize_unversioned(Ar& ar) { // Changing this serialization format will affect key definitions, so can't simply be versioned!
		serializer(ar, part[0], part[1]);
	}
};

template <class Ar> void load( Ar& ar, CMDUID& uid ) { uid.serialize_unversioned(ar); }
template <class Ar> void save( Ar& ar, CMDUID const& uid ) { const_cast<CMDUID&>(uid).serialize_unversioned(ar); }


// NOTE: is cmd's Endpoint token the same with the request's token for the same node?
struct RestoreCommandInterface {
	RequestStream< struct RestoreCommand > cmd; // Restore commands from master to loader and applier
//	RequestStream< struct RestoreRequest > request; // Restore requests used by loader and applier

	bool operator == (RestoreCommandInterface const& r) const { return id() == r.id(); }
	bool operator != (RestoreCommandInterface const& r) const { return id() != r.id(); }
	UID id() const { return cmd.getEndpoint().token; }

	NetworkAddress address() const { return cmd.getEndpoint().address; }

	void initEndpoints() {
		cmd.getEndpoint( TaskClusterController );
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, cmd);
//		ar & cmd & request;
	}
};

struct RestoreCommand {
	RestoreCommandEnum cmd; // 0: set role, -1: end of the command stream
	CMDUID cmdId; // monotonically increase index for commands.
	UID id; // Node id that will receive the command
	int nodeIndex; // The index of the node in the global node status
	UID masterApplier;
	RestoreRole role; // role of the command;


	KeyRange keyRange;
	uint64_t commitVersion;
	MutationRef mutation; //TODO: change to a vector
	KeyRef applierKeyRangeLB;
	UID applierID;
	int keyRangeIndex;


	struct LoadingParam {
		Key url;
		Version version;
		std::string filename;
		int64_t offset;
		int64_t length;
		int64_t blockSize;
		KeyRange restoreRange;
		Key addPrefix;
		Key removePrefix;
		Key mutationLogPrefix;

		template <class Ar>
		void serialize(Ar& ar) {
			serializer(ar, url, version, filename, offset, length, blockSize, restoreRange, addPrefix, removePrefix, mutationLogPrefix);
			//ar & url & version & filename & offset & length & blockSize & restoreRange & addPrefix & removePrefix & mutationLogPrefix;
		}

		std::string toString() {
			std::stringstream str;
			str << "url:" << url.toString() << "version:" << version
				<<  " filename:" << filename  << " offset:" << offset << " length:" << length << " blockSize:" << blockSize
				<< " restoreRange:" << restoreRange.toString()
				<< " addPrefix:" << addPrefix.toString() << " removePrefix:" << removePrefix.toString();
			return str.str();
		}
	};
	LoadingParam loadingParam;

	ReplyPromise< struct RestoreCommandReply > reply;

	RestoreCommand() : id(UID()), role(RestoreRole::Invalid) {}
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id): cmd(cmd), cmdId(cmdId), id(id) {};
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, RestoreRole role) : cmd(cmd), cmdId(cmdId), id(id), role(role) {}
	// Set_Role
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, RestoreRole role, int nodeIndex, UID masterApplier) : cmd(cmd), cmdId(cmdId), id(id), role(role), nodeIndex(nodeIndex), masterApplier(masterApplier) {} // Temporary when we use masterApplier to apply mutations
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, KeyRange keyRange): cmd(cmd), cmdId(cmdId), id(id), keyRange(keyRange) {};
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, LoadingParam loadingParam): cmd(cmd), cmdId(cmdId), id(id), loadingParam(loadingParam) {};
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, int keyRangeIndex): cmd(cmd), cmdId(cmdId), id(id), keyRangeIndex(keyRangeIndex) {};
	// For loader send mutation to applier
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, uint64_t commitVersion, struct MutationRef mutation): cmd(cmd), cmdId(cmdId), id(id), commitVersion(commitVersion), mutation(mutation) {};
	// Notify loader about applier key ranges
	explicit RestoreCommand(RestoreCommandEnum cmd, CMDUID cmdId, UID id, KeyRef applierKeyRangeLB, UID applierID): cmd(cmd), cmdId(cmdId), id(id), applierKeyRangeLB(applierKeyRangeLB), applierID(applierID) {};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar , cmd  , cmdId , nodeIndex, id , masterApplier , role , keyRange ,  commitVersion , mutation , applierKeyRangeLB ,  applierID , keyRangeIndex , loadingParam , reply);
		//ar & cmd  & cmdIndex & id & masterApplier & role & keyRange &  commitVersion & mutation & applierKeyRangeLB &  applierID & keyRangeIndex & loadingParam & reply;
	}
};
typedef RestoreCommand::LoadingParam LoadingParam;

struct RestoreCommandReply {
	UID id; // placeholder, which reply the worker's node id back to master
	CMDUID cmdId;
	int num; // num is the number of key ranges calculated for appliers
	Standalone<KeyRef> lowerBound;

	RestoreCommandReply() : id(UID()), cmdId(CMDUID()) {}
	//explicit RestoreCommandReply(UID id) : id(id) {}
	explicit RestoreCommandReply(UID id, CMDUID cmdId) : id(id), cmdId(cmdId) {}
	explicit RestoreCommandReply(UID id, CMDUID cmdId, int num) : id(id), cmdId(cmdId), num(num) {}
	explicit RestoreCommandReply(UID id, CMDUID cmdId, KeyRef lowerBound) : id(id), cmdId(cmdId), lowerBound(lowerBound) {}

	std::string toString() const {
		std::stringstream ret;
		ret << "ServerNodeID:" + id.toString() + " CMDID:" + cmdId.toString() + " num:" + std::to_string(num) + " lowerBound:" + lowerBound.toHexString();
		return ret.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id , cmdId , num , lowerBound);
		//ar & id & cmdIndex & num & lowerBound;
	}
};


struct RestoreRequest {
	//Database cx;
	int index;
	Key tagName;
	Key url;
	bool waitForComplete;
	Version targetVersion;
	bool verbose;
	KeyRange range;
	Key addPrefix;
	Key removePrefix;
	bool lockDB;
	UID randomUid;

	int testData;
	std::vector<int> restoreRequests;
	//Key restoreTag;

	ReplyPromise< struct RestoreReply > reply;

	RestoreRequest() : testData(0) {}
	explicit RestoreRequest(int testData) : testData(testData) {}
	explicit RestoreRequest(int testData, std::vector<int> &restoreRequests) : testData(testData), restoreRequests(restoreRequests) {}

	explicit RestoreRequest(const int index, const Key &tagName, const Key &url, bool waitForComplete, Version targetVersion, bool verbose,
							const KeyRange &range, const Key &addPrefix, const Key &removePrefix, bool lockDB,
							const UID &randomUid) : index(index), tagName(tagName), url(url), waitForComplete(waitForComplete),
													targetVersion(targetVersion), verbose(verbose), range(range),
													addPrefix(addPrefix), removePrefix(removePrefix), lockDB(lockDB),
													randomUid(randomUid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, index , tagName , url ,  waitForComplete , targetVersion , verbose , range , addPrefix , removePrefix , lockDB , randomUid ,
		testData , restoreRequests , reply);
//		ar & index & tagName & url &  waitForComplete & targetVersion & verbose & range & addPrefix & removePrefix & lockDB & randomUid &
//		testData & restoreRequests & reply;
	}

	std::string toString() const {
		return "index:" + std::to_string(index) + " tagName:" + tagName.contents().toString() + " url:" + url.contents().toString()
			   + " waitForComplete:" + std::to_string(waitForComplete) + " targetVersion:" + std::to_string(targetVersion)
			   + " verbose:" + std::to_string(verbose) + " range:" + range.toString() + " addPrefix:" + addPrefix.contents().toString()
			   + " removePrefix:" + removePrefix.contents().toString() + " lockDB:" + std::to_string(lockDB) + " randomUid:" + randomUid.toString();
	}
};


struct RestoreReply {
	int replyData;

	RestoreReply() : replyData(0) {}
	explicit RestoreReply(int replyData) : replyData(replyData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, replyData);
		//ar & replyData;
	}
};


////--- Fast restore logic structure

//std::vector<std::string> RestoreRoleStr; // = {"Master", "Loader", "Applier"};
//int numRoles = RestoreRoleStr.size();
std::string getRoleStr(RestoreRole role);


struct RestoreNodeStatus {
	// ConfigureKeyRange is to determine how to split the key range and apply the splitted key ranges to appliers
	// NotifyKeyRange is to notify the Loaders and Appliers about the key range each applier is responsible for
	// Loading is to notify all Loaders to load the backup data and send the mutation to appliers
	// Applying is to notify appliers to apply the aggregated mutations to DB
	// Done is to notify the test workload (or user) that we have finished restore
	enum class MasterState {Invalid = -1, Ready, ConfigureRoles, Sampling, ConfigureKeyRange, NotifyKeyRange, Loading, Applying, Done};
	enum class LoaderState {Invalid = -1, Ready, Sampling, LoadRange, LoadLog, Done};
	enum class ApplierState {Invalid = -1, Ready, Aggregating, ApplyToDB, Done};

	UID nodeID;
	int nodeIndex; // The continuous number to indicate which worker it is. It is an alias for nodeID
	RestoreRole role;
	MasterState masterState;
	LoaderState loaderState;
	ApplierState applierState;

	double lastStart; // The most recent start time. now() - lastStart = execution time
	double totalExecTime; // The total execution time.
	double lastSuspend; // The most recent time when the process stops exeuction

	double processedDataSize; // The size of all data processed so far


	RestoreNodeStatus() : nodeID(UID()), role(RestoreRole::Invalid),
		masterState(MasterState::Invalid), loaderState(LoaderState::Invalid), applierState(ApplierState::Invalid),
		lastStart(0), totalExecTime(0), lastSuspend(0) {}

	std::string toString() {
		std::stringstream str;
		str << "nodeID:" << nodeID.toString() << " role:" << getRoleStr(role)
			<< " masterState:" << (int) masterState << " loaderState:" << (int) loaderState << " applierState:" << (int) applierState
			<< " lastStart:" << lastStart << " totalExecTime:" << totalExecTime << " lastSuspend:" << lastSuspend;

		return str.str();
	}

	void init(RestoreRole newRole) {
		role = newRole;
		if ( newRole == RestoreRole::Loader ) {
			loaderState = LoaderState::Ready;
		} else if ( newRole == RestoreRole::Applier) {
			applierState = ApplierState::Ready;
		} else if ( newRole == RestoreRole::Master) {
			masterState == MasterState::Ready;
		}
		lastStart = 0;
		totalExecTime = 0;
		lastSuspend = 0;
	}

};


std::string getRoleStr(RestoreRole role);

////--- Interface functions
Future<Void> _restoreWorker(Database const& cx, LocalityData const& locality);
Future<Void> restoreWorker(Reference<ClusterConnectionFile> const& ccf, LocalityData const& locality);

#endif