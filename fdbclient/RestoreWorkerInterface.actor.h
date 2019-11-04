/*
 * RestoreWorkerInterface.actor.h
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

// This file declare and define the interface for RestoreWorker and restore roles
// which are RestoreMaster, RestoreLoader, and RestoreApplier

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_G_H)
	#define FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_G_H
	#include "fdbclient/RestoreWorkerInterface.actor.g.h"
#elif !defined(FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_H)
	#define FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_H

#include <sstream>
#include "flow/Stats.h"
#include "flow/flow.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RestoreUtil.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

class RestoreConfigFR;

struct RestoreCommonReply;
struct RestoreRecruitRoleRequest;
struct RestoreSysInfoRequest;
struct RestoreLoadFileRequest;
struct RestoreVersionBatchRequest;
struct RestoreSendMutationVectorVersionedRequest;
struct RestoreSetApplierKeyRangeVectorRequest;
struct RestoreSysInfo;
struct RestoreApplierInterface;

// RestoreSysInfo includes information each (type of) restore roles should know.
// At this moment, it only include appliers. We keep the name for future extension.
// TODO: If it turns out this struct only has appliers in the final version, we will rename it to a more specific name, e.g., AppliersMap
struct RestoreSysInfo {
	constexpr static FileIdentifier file_identifier = 68098739;
	std::map<UID, RestoreApplierInterface> appliers;

	RestoreSysInfo() = default;
	explicit RestoreSysInfo(const std::map<UID, RestoreApplierInterface> appliers) : appliers(appliers) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, appliers);
	}
};

struct RestoreWorkerInterface {
	constexpr static FileIdentifier file_identifier = 99601798;
	UID interfID;

	RequestStream<RestoreSimpleRequest> heartbeat;
	RequestStream<RestoreRecruitRoleRequest> recruitRole;
	RequestStream<RestoreSimpleRequest> terminateWorker;

	bool operator==(RestoreWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(RestoreWorkerInterface const& r) const { return id() != r.id(); }

	UID id() const { return interfID; } // cmd.getEndpoint().token;

	NetworkAddress address() const { return recruitRole.getEndpoint().addresses.address; }

	void initEndpoints() {
		heartbeat.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		recruitRole.getEndpoint(TaskPriority::LoadBalancedEndpoint); // Q: Why do we need this?
		terminateWorker.getEndpoint(TaskPriority::LoadBalancedEndpoint);

		interfID = deterministicRandom()->randomUniqueID();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interfID, heartbeat, recruitRole, terminateWorker);
	}
};

struct RestoreRoleInterface {
	constexpr static FileIdentifier file_identifier = 62531339;
	UID nodeID;
	RestoreRole role;

	RestoreRoleInterface() { role = RestoreRole::Invalid; }

	explicit RestoreRoleInterface(RestoreRoleInterface const& interf) : nodeID(interf.nodeID), role(interf.role){};

	UID id() const { return nodeID; }

	std::string toString() {
		std::stringstream ss;
		ss << "Role:" << getRoleStr(role) << " interfID:" << nodeID.toString();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, nodeID, role);
	}
};

struct RestoreLoaderInterface : RestoreRoleInterface {
	constexpr static FileIdentifier file_identifier = 84244651;

	RequestStream<RestoreSimpleRequest> heartbeat;
	RequestStream<RestoreSysInfoRequest> updateRestoreSysInfo;
	RequestStream<RestoreSetApplierKeyRangeVectorRequest> setApplierKeyRangeVectorRequest;
	RequestStream<RestoreLoadFileRequest> loadFile;
	RequestStream<RestoreVersionBatchRequest> initVersionBatch;
	RequestStream<RestoreSimpleRequest> collectRestoreRoleInterfaces; // TODO: Change to collectRestoreRoleInterfaces
	RequestStream<RestoreVersionBatchRequest> finishRestore;

	bool operator==(RestoreWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(RestoreWorkerInterface const& r) const { return id() != r.id(); }

	RestoreLoaderInterface() {
		role = RestoreRole::Loader;
		nodeID = deterministicRandom()->randomUniqueID();
	}

	NetworkAddress address() const { return heartbeat.getEndpoint().addresses.address; }

	void initEndpoints() {
		heartbeat.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		updateRestoreSysInfo.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		setApplierKeyRangeVectorRequest.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		loadFile.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		initVersionBatch.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		collectRestoreRoleInterfaces.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		finishRestore.getEndpoint(TaskPriority::LoadBalancedEndpoint);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreRoleInterface*)this, heartbeat, updateRestoreSysInfo, setApplierKeyRangeVectorRequest,
		           loadFile, initVersionBatch, collectRestoreRoleInterfaces, finishRestore);
	}
};

struct RestoreApplierInterface : RestoreRoleInterface {
	constexpr static FileIdentifier file_identifier = 54253048;

	RequestStream<RestoreSimpleRequest> heartbeat;
	RequestStream<RestoreSendMutationVectorVersionedRequest> sendMutationVector;
	RequestStream<RestoreVersionBatchRequest> applyToDB;
	RequestStream<RestoreVersionBatchRequest> initVersionBatch;
	RequestStream<RestoreSimpleRequest> collectRestoreRoleInterfaces;
	RequestStream<RestoreVersionBatchRequest> finishRestore;

	bool operator==(RestoreWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(RestoreWorkerInterface const& r) const { return id() != r.id(); }

	RestoreApplierInterface() {
		role = RestoreRole::Applier;
		nodeID = deterministicRandom()->randomUniqueID();
	}

	NetworkAddress address() const { return heartbeat.getEndpoint().addresses.address; }

	void initEndpoints() {
		heartbeat.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		sendMutationVector.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		applyToDB.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		initVersionBatch.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		collectRestoreRoleInterfaces.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		finishRestore.getEndpoint(TaskPriority::LoadBalancedEndpoint);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreRoleInterface*)this, heartbeat, sendMutationVector, applyToDB, initVersionBatch,
		           collectRestoreRoleInterfaces, finishRestore);
	}

	std::string toString() { return nodeID.toString(); }
};

// TODO: It is probably better to specify the (beginVersion, endVersion] for each loadingParam.
// beginVersion (endVersion) is the version the applier is before (after) it receives the request.
struct LoadingParam {
	constexpr static FileIdentifier file_identifier = 17023837;

	bool isRangeFile;
	Key url;
	Version prevVersion;
	Version endVersion;
	int fileIndex;
	Version version;
	std::string filename;
	int64_t offset;
	int64_t length;
	int64_t blockSize;
	KeyRange restoreRange;
	Key addPrefix;
	Key removePrefix;
	Key mutationLogPrefix;

	// TODO: Compare all fields for loadingParam
	bool operator==(const LoadingParam& r) const { return isRangeFile == r.isRangeFile && filename == r.filename; }
	bool operator!=(const LoadingParam& r) const { return isRangeFile != r.isRangeFile || filename != r.filename; }
	bool operator<(const LoadingParam& r) const {
		return (isRangeFile < r.isRangeFile) || (isRangeFile == r.isRangeFile && filename < r.filename);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, isRangeFile, url, prevVersion, endVersion, fileIndex, version, filename, offset, length,
		           blockSize, restoreRange, addPrefix, removePrefix, mutationLogPrefix);
	}

	std::string toString() {
		std::stringstream str;
		str << "isRangeFile:" << isRangeFile << " url:" << url.toString() << " prevVersion:" << prevVersion
		    << " fileIndex:" << fileIndex << " endVersion:" << endVersion << " version:" << version
		    << " filename:" << filename << " offset:" << offset << " length:" << length << " blockSize:" << blockSize
		    << " restoreRange:" << restoreRange.toString() << " addPrefix:" << addPrefix.toString();
		return str.str();
	}
};

struct RestoreRecruitRoleReply : TimedRequest {
	constexpr static FileIdentifier file_identifier = 30310092;

	UID id;
	RestoreRole role;
	Optional<RestoreLoaderInterface> loader;
	Optional<RestoreApplierInterface> applier;

	RestoreRecruitRoleReply() = default;
	explicit RestoreRecruitRoleReply(UID id, RestoreRole role, RestoreLoaderInterface const& loader)
	  : id(id), role(role), loader(loader) {}
	explicit RestoreRecruitRoleReply(UID id, RestoreRole role, RestoreApplierInterface const& applier)
	  : id(id), role(role), applier(applier) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, role, loader, applier);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "roleInterf role:" << getRoleStr(role) << " replyID:" << id.toString();
		if (loader.present()) {
			ss << "loader:" << loader.get().toString();
		}
		if (applier.present()) {
			ss << "applier:" << applier.get().toString();
		}

		return ss.str();
	}
};

struct RestoreRecruitRoleRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 87022360;

	RestoreRole role;
	int nodeIndex; // Each role is a node

	ReplyPromise<RestoreRecruitRoleReply> reply;

	RestoreRecruitRoleRequest() : role(RestoreRole::Invalid) {}
	explicit RestoreRecruitRoleRequest(RestoreRole role, int nodeIndex) : role(role), nodeIndex(nodeIndex) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, role, nodeIndex, reply);
	}

	std::string printable() {
		std::stringstream ss;
		ss << "RestoreRecruitRoleRequest Role:" << getRoleStr(role) << " NodeIndex:" << nodeIndex;
		return ss.str();
	}

	std::string toString() { return printable(); }
};

struct RestoreSysInfoRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 75960741;

	RestoreSysInfo sysInfo;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSysInfoRequest() = default;
	explicit RestoreSysInfoRequest(RestoreSysInfo sysInfo) : sysInfo(sysInfo) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sysInfo, reply);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreSysInfoRequest";
		return ss.str();
	}
};

// Sample_Range_File and Assign_Loader_Range_File, Assign_Loader_Log_File
struct RestoreLoadFileRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 26557364;

	LoadingParam param;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreLoadFileRequest() = default;
	explicit RestoreLoadFileRequest(LoadingParam param) : param(param) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, param, reply);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreLoadFileRequest param:" << param.toString();
		return ss.str();
	}
};

struct RestoreSendMutationVectorVersionedRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 69764565;

	Version prevVersion, version; // version is the commitVersion of the mutation vector.
	int fileIndex; // Unique index for a backup file
	bool isRangeFile;
	Standalone<VectorRef<MutationRef>> mutations; // All mutations are at version

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendMutationVectorVersionedRequest() = default;
	explicit RestoreSendMutationVectorVersionedRequest(int fileIndex, Version prevVersion, Version version,
	                                                   bool isRangeFile, VectorRef<MutationRef> mutations)
	  : fileIndex(fileIndex), prevVersion(prevVersion), version(version), isRangeFile(isRangeFile),
	    mutations(mutations) {}

	std::string toString() {
		std::stringstream ss;
		ss << "fileIndex" << fileIndex << " prevVersion:" << prevVersion << " version:" << version
		   << " isRangeFile:" << isRangeFile << " mutations.size:" << mutations.size();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, fileIndex, prevVersion, version, isRangeFile, mutations, reply);
	}
};

struct RestoreVersionBatchRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 13018413;

	int batchID;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreVersionBatchRequest() = default;
	explicit RestoreVersionBatchRequest(int batchID) : batchID(batchID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchID, reply);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreVersionBatchRequest BatchID:" << batchID;
		return ss.str();
	}
};

struct RestoreSetApplierKeyRangeVectorRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 92038306;

	std::map<Standalone<KeyRef>, UID> rangeToApplier;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSetApplierKeyRangeVectorRequest() = default;
	explicit RestoreSetApplierKeyRangeVectorRequest(std::map<Standalone<KeyRef>, UID> rangeToApplier)
	  : rangeToApplier(rangeToApplier) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeToApplier, reply);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreVersionBatchRequest rangeToApplierSize:" << rangeToApplier.size();
		return ss.str();
	}
};

struct RestoreRequest {
	constexpr static FileIdentifier file_identifier = 49589770;

	// Database cx;
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
	// Key restoreTag;

	ReplyPromise<struct RestoreCommonReply> reply;

	RestoreRequest() : testData(0) {}
	explicit RestoreRequest(int testData) : testData(testData) {}
	explicit RestoreRequest(int testData, std::vector<int>& restoreRequests)
	  : testData(testData), restoreRequests(restoreRequests) {}

	explicit RestoreRequest(const int index, const Key& tagName, const Key& url, bool waitForComplete,
	                        Version targetVersion, bool verbose, const KeyRange& range, const Key& addPrefix,
	                        const Key& removePrefix, bool lockDB, const UID& randomUid)
	  : index(index), tagName(tagName), url(url), waitForComplete(waitForComplete), targetVersion(targetVersion),
	    verbose(verbose), range(range), addPrefix(addPrefix), removePrefix(removePrefix), lockDB(lockDB),
	    randomUid(randomUid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, index, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix,
		           lockDB, randomUid, testData, restoreRequests, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "index:" << std::to_string(index) << " tagName:" << tagName.contents().toString()
		   << " url:" << url.contents().toString() << " waitForComplete:" << std::to_string(waitForComplete)
		   << " targetVersion:" << std::to_string(targetVersion) << " verbose:" << std::to_string(verbose)
		   << " range:" << range.toString() << " addPrefix:" << addPrefix.contents().toString()
		   << " removePrefix:" << removePrefix.contents().toString() << " lockDB:" << std::to_string(lockDB)
		   << " randomUid:" << randomUid.toString();
		return ss.str();
	}
};

std::string getRoleStr(RestoreRole role);

////--- Interface functions
ACTOR Future<Void> _restoreWorker(Database cx, LocalityData locality);
ACTOR Future<Void> restoreWorker(Reference<ClusterConnectionFile> ccf, LocalityData locality);

#include "flow/unactorcompiler.h"
#endif
