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
#include "flow/Stats.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbrpc/Locality.h"


class RestoreConfig;
enum class RestoreRole {Invalid = 0, Master = 1, Loader, Applier};
extern std::vector<std::string> RestoreRoleStr;
BINARY_SERIALIZABLE( RestoreRole );


// Timeout threshold in seconds for restore commands
extern int FastRestore_Failure_Timeout;

struct RestoreCommonReply;
struct GetKeyRangeReply;
struct GetKeyRangeReply;
struct RestoreSetRoleRequest;
struct RestoreSimpleRequest;
struct RestoreSendMutationRequest;
struct RestoreLoadFileRequest;
struct RestoreGetApplierKeyRangeRequest;
struct RestoreSetApplierKeyRangeRequest;
struct GetKeyRangeNumberReply;
struct RestoreVersionBatchRequest;
struct RestoreCalculateApplierKeyRangeRequest;
struct RestoreSendMutationVectorRequest;
struct RestoreSetApplierKeyRangeVectorRequest;

// RestoreCommandEnum is also used as the phase ID for CMDUID
enum class RestoreCommandEnum {Init = 0,
		Set_Role, Set_Role_Done,
		Sample_Range_File, Sample_Log_File, Sample_File_Done,
		Loader_Send_Sample_Mutation_To_Applier, Loader_Send_Sample_Mutation_To_Applier_Done, //7
		Calculate_Applier_KeyRange, Get_Applier_KeyRange, Get_Applier_KeyRange_Done, //10
		Assign_Applier_KeyRange, Assign_Applier_KeyRange_Done, //12
		Assign_Loader_Range_File, Assign_Loader_Log_File, Assign_Loader_File_Done,//15
		Loader_Send_Mutations_To_Applier, Loader_Send_Mutations_To_Applier_Done,//17
		Apply_Mutation_To_DB, Apply_Mutation_To_DB_Skip, //19
		Loader_Notify_Appler_To_Apply_Mutation,
		Notify_Loader_ApplierKeyRange, Notify_Loader_ApplierKeyRange_Done, //22
		Finish_Restore, RESET_VersionBatch, Set_WorkerInterface}; //23
BINARY_SERIALIZABLE(RestoreCommandEnum);

// Restore command's UID. uint64_t part[2];
// part[0] is the phase id, part[1] is the command index in the phase.
// TODO: Add another field to indicate version-batch round
class CMDUID {
public:
	uint16_t batch;
	uint16_t phase;
	uint64_t cmdID;
	CMDUID() : batch(0), phase(0), cmdID(0) { }
	CMDUID( uint16_t a, uint64_t b ) { batch = 0; phase=a; cmdID=b; }
	CMDUID(const CMDUID &cmd) { batch = cmd.batch; phase = cmd.phase; cmdID = cmd.cmdID; }

	void initPhase(RestoreCommandEnum phase);

	void nextPhase(); // Set to the next phase.

	void nextCmd(); // Increase the command index at the same phase

	RestoreCommandEnum getPhase();
	void setPhase(RestoreCommandEnum newPhase);
	void setBatch(int newBatchIndex);

	uint64_t getIndex();

	std::string toString() const;

	bool operator == ( const CMDUID& r ) const { return batch == r.batch && phase == r.phase && cmdID == r.cmdID; }
	bool operator != ( const CMDUID& r ) const { return batch != r.batch || phase != r.phase || cmdID != r.cmdID; }
	bool operator < ( const CMDUID& r ) const { return batch < r.batch || (batch == r.batch && phase < r.phase) || (batch == r.batch && phase == r.phase && cmdID < r.cmdID); }

	//uint64_t hash() const { return first(); }
	//uint64_t first() const { return part[0]; }
	//uint64_t second() const { return part[1]; }

	template <class Ar>
	void serialize_unversioned(Ar& ar) { // Changing this serialization format will affect key definitions, so can't simply be versioned!
		serializer(ar, batch, phase, cmdID);
	}
};

template <class Ar> void load( Ar& ar, CMDUID& uid ) { uid.serialize_unversioned(ar); }
template <class Ar> void save( Ar& ar, CMDUID const& uid ) { const_cast<CMDUID&>(uid).serialize_unversioned(ar); }


// NOTE: is cmd's Endpoint token the same with the request's token for the same node?
struct RestoreInterface {
	UID nodeID;

	RequestStream<RestoreSimpleRequest> heartbeat;

	RequestStream<RestoreSetRoleRequest> setRole;
	RequestStream<RestoreLoadFileRequest> sampleRangeFile;
	RequestStream<RestoreLoadFileRequest> sampleLogFile;
	RequestStream<RestoreSendMutationRequest> sendSampleMutation;
	RequestStream<RestoreSendMutationVectorRequest> sendSampleMutationVector;

	RequestStream<RestoreCalculateApplierKeyRangeRequest> calculateApplierKeyRange;
	RequestStream<RestoreGetApplierKeyRangeRequest> getApplierKeyRangeRequest;
	RequestStream<RestoreSetApplierKeyRangeRequest> setApplierKeyRangeRequest; // To delete
	RequestStream<RestoreSetApplierKeyRangeVectorRequest> setApplierKeyRangeVectorRequest;

	RequestStream<RestoreLoadFileRequest> loadRangeFile;
	RequestStream<RestoreLoadFileRequest> loadLogFile;
	RequestStream<RestoreSendMutationRequest> sendMutation;
	RequestStream<RestoreSendMutationVectorRequest> sendMutationVector;
	RequestStream<RestoreSimpleRequest> applyToDB;

	RequestStream<RestoreVersionBatchRequest> initVersionBatch;

	RequestStream<RestoreSimpleRequest> setWorkerInterface;

	RequestStream<RestoreSimpleRequest> finishRestore;

	// ToDelete
//	RequestStream< struct RestoreCommand > cmd; // Restore commands from master to loader and applier
//	RequestStream< struct RestoreRequest > request; // Restore requests used by loader and applier

	bool operator == (RestoreInterface const& r) const { return id() == r.id(); }
	bool operator != (RestoreInterface const& r) const { return id() != r.id(); }

	UID id() const { return nodeID; } //cmd.getEndpoint().token;

	NetworkAddress address() const { return setRole.getEndpoint().addresses.address; }

	void initEndpoints() {
		heartbeat.getEndpoint( TaskClusterController );
		
		setRole.getEndpoint( TaskClusterController );// Q: Why do we need this? 
		sampleRangeFile.getEndpoint( TaskClusterController ); 
		sampleLogFile.getEndpoint( TaskClusterController ); 
		sendSampleMutation.getEndpoint( TaskClusterController ); 
		sendSampleMutationVector.getEndpoint( TaskClusterController ); 

		calculateApplierKeyRange.getEndpoint( TaskClusterController ); 
		getApplierKeyRangeRequest.getEndpoint( TaskClusterController ); 
		setApplierKeyRangeRequest.getEndpoint( TaskClusterController );
		setApplierKeyRangeVectorRequest.getEndpoint( TaskClusterController ); 

		loadRangeFile.getEndpoint( TaskClusterController ); 
		loadLogFile.getEndpoint( TaskClusterController ); 
		sendMutation.getEndpoint( TaskClusterController ); 
		sendMutationVector.getEndpoint( TaskClusterController ); 
		applyToDB.getEndpoint( TaskClusterController ); 
		
		initVersionBatch.getEndpoint( TaskClusterController );

		setWorkerInterface.getEndpoint( TaskClusterController ); 
		finishRestore.getEndpoint( TaskClusterController ); 

		nodeID = g_random->randomUniqueID();
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, nodeID, heartbeat, setRole, sampleRangeFile, sampleLogFile, sendSampleMutation, sendSampleMutationVector,
				calculateApplierKeyRange, getApplierKeyRangeRequest, setApplierKeyRangeRequest, setApplierKeyRangeVectorRequest,
				loadRangeFile, loadLogFile, sendMutation, sendMutationVector, applyToDB, initVersionBatch, setWorkerInterface,
				finishRestore);
	}
};

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


struct RestoreSetRoleRequest : TimedRequest {
	CMDUID cmdID;
	RestoreRole role;
	int nodeIndex;
	UID masterApplierID;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSetRoleRequest() : cmdID(CMDUID()), role(RestoreRole::Invalid) {}
	explicit RestoreSetRoleRequest(CMDUID cmdID, RestoreRole role, int nodeIndex, UID masterApplierID) : 
				cmdID(cmdID), role(role), nodeIndex(nodeIndex), masterApplierID(masterApplierID) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, role, nodeIndex, masterApplierID, reply);
	}
};

// Sample_Range_File and Assign_Loader_Range_File, Assign_Loader_Log_File
struct RestoreLoadFileRequest : TimedRequest {
	CMDUID cmdID;
	LoadingParam param;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreLoadFileRequest() : cmdID(CMDUID()) {}
	explicit RestoreLoadFileRequest(CMDUID cmdID, LoadingParam param) : cmdID(cmdID), param(param) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, param, reply);
	}
};

// Send mutation from loader to applier
// Loader_Send_Sample_Mutation_To_Applier and Loader_Send_Mutations_To_Applier
struct RestoreSendMutationRequest : TimedRequest {
	CMDUID cmdID;
	uint64_t commitVersion;
	MutationRef mutation;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendMutationRequest() : cmdID(CMDUID()), commitVersion(0), mutation(MutationRef()) {}
	explicit RestoreSendMutationRequest(CMDUID cmdID, uint64_t commitVersion, MutationRef mutation) : cmdID(cmdID), commitVersion(commitVersion),  mutation(mutation) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, commitVersion, mutation, reply);
	}
};

struct RestoreSendMutationVectorRequest : TimedRequest {
	CMDUID cmdID;
	uint64_t commitVersion;
	VectorRef<MutationRef> mutations;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendMutationVectorRequest() : cmdID(CMDUID()), commitVersion(0), mutations(VectorRef<MutationRef>()) {}
	explicit RestoreSendMutationVectorRequest(CMDUID cmdID, uint64_t commitVersion, VectorRef<MutationRef> mutations) : cmdID(cmdID), commitVersion(commitVersion),  mutations(mutations) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, commitVersion, mutations, reply);
	}
};

// CalculateApplierKeyRange, applyToDB
struct RestoreSimpleRequest : TimedRequest {
	CMDUID cmdID;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSimpleRequest() : cmdID(CMDUID()) {}
	explicit RestoreSimpleRequest(CMDUID cmdID) : cmdID(cmdID) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, reply);
	}
};

struct RestoreCalculateApplierKeyRangeRequest : TimedRequest {
	CMDUID cmdID;
	int numAppliers;

	ReplyPromise<GetKeyRangeNumberReply> reply;

	RestoreCalculateApplierKeyRangeRequest() : cmdID(CMDUID()), numAppliers(0) {}
	explicit RestoreCalculateApplierKeyRangeRequest(CMDUID cmdID, int numAppliers) : cmdID(cmdID), numAppliers(numAppliers) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, numAppliers, reply);
	}
};

struct RestoreVersionBatchRequest : TimedRequest {
	CMDUID cmdID;
	int batchID;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreVersionBatchRequest() : cmdID(CMDUID()), batchID(0) {}
	explicit RestoreVersionBatchRequest(CMDUID cmdID, int batchID) : cmdID(cmdID), batchID(batchID) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, batchID, reply);
	}
};

struct RestoreGetApplierKeyRangeRequest : TimedRequest {
	CMDUID cmdID;
	int applierIndex; // The applier ID whose key range will be replied // TODO: Maybe change to use applier's UID

	ReplyPromise<GetKeyRangeReply> reply;

	RestoreGetApplierKeyRangeRequest() : cmdID(CMDUID()), applierIndex(0) {}
	explicit RestoreGetApplierKeyRangeRequest(CMDUID cmdID, int applierIndex) : cmdID(cmdID), applierIndex(applierIndex) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, applierIndex, reply);
	}
};

// Notify the server node about the key range the applier node (nodeID) is responsible for
struct RestoreSetApplierKeyRangeRequest : TimedRequest {
	CMDUID cmdID;
	UID applierID;
	KeyRange range; // the key range that will be assigned to the node

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSetApplierKeyRangeRequest() : cmdID(CMDUID()), applierID(UID()), range(KeyRange()) {}
	explicit RestoreSetApplierKeyRangeRequest(CMDUID cmdID, UID applierID, KeyRange range) : cmdID(cmdID), applierID(applierID), range(range) {}

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, applierID, range, reply);
	}
};

struct RestoreSetApplierKeyRangeVectorRequest : TimedRequest {
	CMDUID cmdID;
	VectorRef<UID> applierIDs;
	VectorRef<KeyRange> ranges; // the key range that will be assigned to the node

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSetApplierKeyRangeVectorRequest() : cmdID(CMDUID()), applierIDs(VectorRef<UID>()), ranges(VectorRef<KeyRange>()) {}
	explicit RestoreSetApplierKeyRangeVectorRequest(CMDUID cmdID, VectorRef<UID> applierIDs, VectorRef<KeyRange> ranges) : cmdID(cmdID), applierIDs(applierIDs), ranges(ranges) { ASSERT(applierIDs.size() == ranges.size()); }

	template <class Ar> 
	void serialize( Ar& ar ) {
		serializer(ar, cmdID, applierIDs, ranges, reply);
	}
};



// Reply type
struct RestoreCommonReply { 
	UID id; // unique ID of the server who sends the reply
	CMDUID cmdID; // The restore command for the reply
	
	RestoreCommonReply() : id(UID()), cmdID(CMDUID()) {}
	explicit RestoreCommonReply(UID id, CMDUID cmdID) : id(id), cmdID(cmdID) {}
	
	std::string toString() const {
		std::stringstream ss;
		ss << "ServerNodeID:" << id.toString() << " CMDID:" << cmdID.toString();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, cmdID);
	}
};

struct GetKeyRangeReply : RestoreCommonReply {
	int index;
	Standalone<KeyRef> lowerBound; // inclusive
	Standalone<KeyRef> upperBound; // exclusive

	GetKeyRangeReply() : index(0), lowerBound(KeyRef()), upperBound(KeyRef()) {}
	explicit GetKeyRangeReply(int index, KeyRef lowerBound,  KeyRef upperBound) : index(index), lowerBound(lowerBound), upperBound(upperBound) {}
	explicit GetKeyRangeReply(UID id, CMDUID cmdID, int index, KeyRef lowerBound,  KeyRef upperBound) : 
	 						RestoreCommonReply(id, cmdID), index(index), lowerBound(lowerBound), upperBound(upperBound) {}
	// explicit GetKeyRangeReply(UID id, CMDUID cmdID) : 
	//  						RestoreCommonReply(id, cmdID) {}

	std::string toString() const {
		std::stringstream ss;
		ss << "ServerNodeID:" << id.toString() << " CMDID:" << cmdID.toString() 
			<< " index:" << std::to_string(index) << " lowerBound:" << lowerBound.toHexString()
			<< " upperBound:" << upperBound.toHexString();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreCommonReply *) this, index, lowerBound, upperBound);
	}
};


struct GetKeyRangeNumberReply : RestoreCommonReply {
	int keyRangeNum;

	GetKeyRangeNumberReply() : keyRangeNum(0) {}
	explicit GetKeyRangeNumberReply(int keyRangeNum) : keyRangeNum(keyRangeNum) {}
	explicit GetKeyRangeNumberReply(UID id, CMDUID cmdID) : RestoreCommonReply(id, cmdID) {}

	std::string toString() const {
		std::stringstream ss;
		ss << "ServerNodeID:" << id.toString() << " CMDID:" << cmdID.toString() 
			<< " keyRangeNum:" << std::to_string(keyRangeNum);
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreCommonReply *) this, keyRangeNum);
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
	}

	std::string toString() const {
		std::stringstream ss;
		ss <<  "index:" << std::to_string(index) << " tagName:" << tagName.contents().toString() << " url:" << url.contents().toString()
			   << " waitForComplete:" << std::to_string(waitForComplete) << " targetVersion:" << std::to_string(targetVersion)
			   << " verbose:" << std::to_string(verbose) << " range:" << range.toString() << " addPrefix:" << addPrefix.contents().toString()
			   << " removePrefix:" << removePrefix.contents().toString() << " lockDB:" << std::to_string(lockDB) << " randomUid:" << randomUid.toString();
		return ss.str();
	}
};


struct RestoreReply {
	int replyData;

	RestoreReply() : replyData(0) {}
	explicit RestoreReply(int replyData) : replyData(replyData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, replyData);
	}
};

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
			masterState = MasterState::Ready;
		}
		lastStart = 0;
		totalExecTime = 0;
		lastSuspend = 0;
	}

};

////--- Interface functions
Future<Void> _restoreWorker(Database const& cx, LocalityData const& locality);
Future<Void> restoreWorker(Reference<ClusterConnectionFile> const& ccf, LocalityData const& locality);

#endif