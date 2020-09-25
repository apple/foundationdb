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
// which are RestoreController, RestoreLoader, and RestoreApplier

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_G_H)
	#define FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_G_H
	#include "fdbclient/RestoreWorkerInterface.actor.g.h"
#elif !defined(FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_H)
	#define FDBCLIENT_RESTORE_WORKER_INTERFACE_ACTOR_H

#include <sstream>
#include <string>
#include "flow/flow.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Stats.h"
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
struct RestoreSendMutationsToAppliersRequest;
struct RestoreSendVersionedMutationsRequest;
struct RestoreSysInfo;
struct RestoreApplierInterface;
struct RestoreFinishRequest;
struct RestoreSamplesRequest;

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
	constexpr static FileIdentifier file_identifier = 15715718;
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

	//To change this serialization, ProtocolVersion::RestoreWorkerInterfaceValue must be updated, and downgrades need to be considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, interfID, heartbeat, recruitRole, terminateWorker);
	}
};

struct RestoreRoleInterface {
	constexpr static FileIdentifier file_identifier = 12199691;
	UID nodeID;
	RestoreRole role;

	RestoreRoleInterface() { role = RestoreRole::Invalid; }

	explicit RestoreRoleInterface(RestoreRoleInterface const& interf) : nodeID(interf.nodeID), role(interf.role){};

	UID id() const { return nodeID; }

	std::string toString() const {
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
	constexpr static FileIdentifier file_identifier = 358571;

	RequestStream<RestoreSimpleRequest> heartbeat;
	RequestStream<RestoreSysInfoRequest> updateRestoreSysInfo;
	RequestStream<RestoreLoadFileRequest> loadFile;
	RequestStream<RestoreSendMutationsToAppliersRequest> sendMutations;
	RequestStream<RestoreVersionBatchRequest> initVersionBatch;
	RequestStream<RestoreVersionBatchRequest> finishVersionBatch;
	RequestStream<RestoreSimpleRequest> collectRestoreRoleInterfaces;
	RequestStream<RestoreFinishRequest> finishRestore;

	bool operator==(RestoreWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(RestoreWorkerInterface const& r) const { return id() != r.id(); }

	RestoreLoaderInterface() {
		role = RestoreRole::Loader;
		nodeID = deterministicRandom()->randomUniqueID();
	}

	NetworkAddress address() const { return heartbeat.getEndpoint().addresses.address; }

	void initEndpoints() {
		// Endpoint in a later restore phase has higher priority
		heartbeat.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		updateRestoreSysInfo.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		initVersionBatch.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		loadFile.getEndpoint(TaskPriority::RestoreLoaderLoadFiles);
		sendMutations.getEndpoint(TaskPriority::RestoreLoaderSendMutations);
		finishVersionBatch.getEndpoint(TaskPriority::RestoreLoaderFinishVersionBatch);
		collectRestoreRoleInterfaces.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		finishRestore.getEndpoint(TaskPriority::LoadBalancedEndpoint);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreRoleInterface*)this, heartbeat, updateRestoreSysInfo, loadFile, sendMutations,
		           initVersionBatch, finishVersionBatch, collectRestoreRoleInterfaces, finishRestore);
	}
};

struct RestoreApplierInterface : RestoreRoleInterface {
	constexpr static FileIdentifier file_identifier = 3921400;

	RequestStream<RestoreSimpleRequest> heartbeat;
	RequestStream<RestoreSendVersionedMutationsRequest> sendMutationVector;
	RequestStream<RestoreVersionBatchRequest> applyToDB;
	RequestStream<RestoreVersionBatchRequest> initVersionBatch;
	RequestStream<RestoreSimpleRequest> collectRestoreRoleInterfaces;
	RequestStream<RestoreFinishRequest> finishRestore;

	bool operator==(RestoreWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(RestoreWorkerInterface const& r) const { return id() != r.id(); }

	RestoreApplierInterface() {
		role = RestoreRole::Applier;
		nodeID = deterministicRandom()->randomUniqueID();
	}

	NetworkAddress address() const { return heartbeat.getEndpoint().addresses.address; }

	void initEndpoints() {
		// Endpoint in a later restore phase has higher priority
		heartbeat.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		sendMutationVector.getEndpoint(TaskPriority::RestoreApplierReceiveMutations);
		applyToDB.getEndpoint(TaskPriority::RestoreApplierWriteDB);
		initVersionBatch.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		collectRestoreRoleInterfaces.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		finishRestore.getEndpoint(TaskPriority::LoadBalancedEndpoint);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreRoleInterface*)this, heartbeat, sendMutationVector, applyToDB, initVersionBatch,
		           collectRestoreRoleInterfaces, finishRestore);
	}

	std::string toString() const { return nodeID.toString(); }
};

struct RestoreControllerInterface : RestoreRoleInterface {
	constexpr static FileIdentifier file_identifier = 11642024;

	RequestStream<RestoreSamplesRequest> samples;

	bool operator==(RestoreWorkerInterface const& r) const { return id() == r.id(); }
	bool operator!=(RestoreWorkerInterface const& r) const { return id() != r.id(); }

	RestoreControllerInterface() {
		role = RestoreRole::Controller;
		nodeID = deterministicRandom()->randomUniqueID();
	}

	NetworkAddress address() const { return samples.getEndpoint().addresses.address; }

	void initEndpoints() { samples.getEndpoint(TaskPriority::LoadBalancedEndpoint); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, *(RestoreRoleInterface*)this, samples);
	}

	std::string toString() const { return nodeID.toString(); }
};

// RestoreAsset uniquely identifies the work unit done by restore roles;
// It is used to ensure exact-once processing on restore loader and applier;
// By combining all RestoreAssets across all verstion batches, restore should process all mutations in
// backup range and log files up to the target restore version.
struct RestoreAsset {
	UID uid;

	Version beginVersion, endVersion; // Only use mutation in [begin, end) versions;
	KeyRange range; // Only use mutations in range

	int fileIndex;
	// Partition ID for mutation log files, which is also encoded in the filename of mutation logs.
	int partitionId = -1;
	std::string filename;
	int64_t offset;
	int64_t len;

	Key addPrefix;
	Key removePrefix;

	int batchIndex; // for progress tracking and performance investigation

	RestoreAsset() = default;

	// Q: Can we simply use uid for == and use different comparison rule for less than operator.
	// The ordering of RestoreAsset may change, will that affect correctness or performance?
	bool operator==(const RestoreAsset& r) const {
		return batchIndex == r.batchIndex && beginVersion == r.beginVersion && endVersion == r.endVersion &&
		       range == r.range && fileIndex == r.fileIndex && partitionId == r.partitionId && filename == r.filename &&
		       offset == r.offset && len == r.len && addPrefix == r.addPrefix && removePrefix == r.removePrefix;
	}
	bool operator!=(const RestoreAsset& r) const {
		return !(*this == r);
	}
	bool operator<(const RestoreAsset& r) const {
		return std::make_tuple(batchIndex, fileIndex, filename, offset, len, beginVersion, endVersion, range.begin,
		                       range.end, addPrefix, removePrefix) <
		       std::make_tuple(r.batchIndex, r.fileIndex, r.filename, r.offset, r.len, r.beginVersion, r.endVersion,
		                       r.range.begin, r.range.end, r.addPrefix, r.removePrefix);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, uid, beginVersion, endVersion, range, filename, fileIndex, partitionId, offset, len, addPrefix,
		           removePrefix, batchIndex);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "UID:" << uid.toString() << " begin:" << beginVersion << " end:" << endVersion
		   << " range:" << range.toString() << " filename:" << filename << " fileIndex:" << fileIndex
		   << " partitionId:" << partitionId << " offset:" << offset << " len:" << len
		   << " addPrefix:" << addPrefix.toString() << " removePrefix:" << removePrefix.toString()
		   << " BatchIndex:" << batchIndex;
		return ss.str();
	}

	bool hasPrefix() const { return addPrefix.size() > 0 || removePrefix.size() > 0; }

	// RestoreAsset and VersionBatch both use endVersion as exclusive in version range
	bool isInVersionRange(Version commitVersion) const {
		return commitVersion >= beginVersion && commitVersion < endVersion;
	}

	// Is mutation's begin and end keys are in RestoreAsset's range
	bool isInKeyRange(MutationRef mutation) const {
		if (hasPrefix()) {
			Key begin = range.begin; // Avoid creating new keys if we do not have addPrefix or removePrefix
			Key end = range.end;
			begin = begin.removePrefix(removePrefix).withPrefix(addPrefix);
			end = end.removePrefix(removePrefix).withPrefix(addPrefix);
			if (isRangeMutation(mutation)) {
				// Range mutation's right side is exclusive
				return mutation.param1 >= begin && mutation.param2 <= end;
			} else {
				return mutation.param1 >= begin && mutation.param1 < end;
			}
		} else {
			if (isRangeMutation(mutation)) {
				// Range mutation's right side is exclusive
				return mutation.param1 >= range.begin && mutation.param2 <= range.end;
			} else {
				return mutation.param1 >= range.begin && mutation.param1 < range.end;
			}
		}
	}
};

struct LoadingParam {
	constexpr static FileIdentifier file_identifier = 246621;

	bool isRangeFile;
	Key url;
	Optional<Version> rangeVersion; // range file's version

	int64_t blockSize;
	RestoreAsset asset;

	LoadingParam() = default;

	// TODO: Compare all fields for loadingParam
	bool operator==(const LoadingParam& r) const { return isRangeFile == r.isRangeFile && asset == r.asset; }
	bool operator!=(const LoadingParam& r) const { return isRangeFile != r.isRangeFile || asset != r.asset; }
	bool operator<(const LoadingParam& r) const {
		return (isRangeFile < r.isRangeFile) || (isRangeFile == r.isRangeFile && asset < r.asset);
	}

	bool isPartitionedLog() const {
		return !isRangeFile && asset.partitionId >= 0;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, isRangeFile, url, rangeVersion, blockSize, asset);
	}

	std::string toString() const {
		std::stringstream str;
		str << "isRangeFile:" << isRangeFile << " url:" << url.toString()
		    << " rangeVersion:" << (rangeVersion.present() ? rangeVersion.get() : -1) << " blockSize:" << blockSize
		    << " RestoreAsset:" << asset.toString();
		return str.str();
	}
};

struct RestoreRecruitRoleReply : TimedRequest {
	constexpr static FileIdentifier file_identifier = 13532876;

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

	std::string toString() const {
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
	constexpr static FileIdentifier file_identifier = 3136280;

	RestoreControllerInterface ci;
	RestoreRole role;
	int nodeIndex; // Each role is a node

	ReplyPromise<RestoreRecruitRoleReply> reply;

	RestoreRecruitRoleRequest() : role(RestoreRole::Invalid) {}
	explicit RestoreRecruitRoleRequest(RestoreControllerInterface ci, RestoreRole role, int nodeIndex)
	  : ci(ci), role(role), nodeIndex(nodeIndex) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ci, role, nodeIndex, reply);
	}

	std::string printable() const {
		std::stringstream ss;
		ss << "RestoreRecruitRoleRequest Role:" << getRoleStr(role) << " NodeIndex:" << nodeIndex
		   << " RestoreController:" << ci.id().toString();
		return ss.str();
	}

	std::string toString() const { return printable(); }
};

// Static info. across version batches
struct RestoreSysInfoRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 8851877;

	RestoreSysInfo sysInfo;
	Standalone<VectorRef<std::pair<KeyRangeRef, Version>>> rangeVersions;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSysInfoRequest() = default;
	explicit RestoreSysInfoRequest(RestoreSysInfo sysInfo,
	                               Standalone<VectorRef<std::pair<KeyRangeRef, Version>>> rangeVersions)
	  : sysInfo(sysInfo), rangeVersions(rangeVersions) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sysInfo, rangeVersions, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "RestoreSysInfoRequest "
		   << "rangeVersions.size:" << rangeVersions.size();
		return ss.str();
	}
};

struct RestoreSamplesRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 10751035;
	UID id; // deduplicate data
	int batchIndex;
	SampledMutationsVec samples; // sampled mutations

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSamplesRequest() = default;
	explicit RestoreSamplesRequest(UID id, int batchIndex, SampledMutationsVec samples)
	  : id(id), batchIndex(batchIndex), samples(samples) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, batchIndex, samples, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "ID:" << id.toString() << " BatchIndex:" << batchIndex << " samples:" << samples.size();
		return ss.str();
	}
};

struct RestoreLoadFileReply : TimedRequest {
	constexpr static FileIdentifier file_identifier = 523470;

	LoadingParam param;
	bool isDuplicated; // true if loader thinks the request is a duplicated one

	RestoreLoadFileReply() = default;
	explicit RestoreLoadFileReply(LoadingParam param, bool isDuplicated) : param(param), isDuplicated(isDuplicated) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, param, isDuplicated);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "LoadingParam:" << param.toString() << " isDuplicated:" << isDuplicated;
		return ss.str();
	}
};

// Sample_Range_File and Assign_Loader_Range_File, Assign_Loader_Log_File
struct RestoreLoadFileRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 9780148;

	int batchIndex;
	LoadingParam param;

	ReplyPromise<RestoreLoadFileReply> reply;

	RestoreLoadFileRequest() = default;
	explicit RestoreLoadFileRequest(int batchIndex, LoadingParam& param) : batchIndex(batchIndex), param(param){};

	bool operator<(RestoreLoadFileRequest const& rhs) const { return batchIndex > rhs.batchIndex; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, param, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "RestoreLoadFileRequest batchIndex:" << batchIndex << " param:" << param.toString();
		return ss.str();
	}
};

struct RestoreSendMutationsToAppliersRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 1718441;

	int batchIndex; // version batch index
	std::map<Key, UID> rangeToApplier;
	bool useRangeFile; // Send mutations parsed from range file?

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendMutationsToAppliersRequest() = default;
	explicit RestoreSendMutationsToAppliersRequest(int batchIndex, std::map<Key, UID> rangeToApplier, bool useRangeFile)
	  : batchIndex(batchIndex), rangeToApplier(rangeToApplier), useRangeFile(useRangeFile) {}

	bool operator<(RestoreSendMutationsToAppliersRequest const& rhs) const { return batchIndex > rhs.batchIndex; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, rangeToApplier, useRangeFile, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "RestoreSendMutationsToAppliersRequest batchIndex:" << batchIndex
		   << " keyToAppliers.size:" << rangeToApplier.size() << " useRangeFile:" << useRangeFile;
		return ss.str();
	}
};

struct RestoreSendVersionedMutationsRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 2655701;

	int batchIndex; // version batch index
	RestoreAsset asset; // Unique identifier for the current restore asset

	Version msgIndex; // Monitonically increasing index of mutation messages
	bool isRangeFile;
	VersionedMutationsVec versionedMutations; // Versioned mutations may be at different versions parsed by one loader

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendVersionedMutationsRequest() = default;
	explicit RestoreSendVersionedMutationsRequest(int batchIndex, const RestoreAsset& asset, Version msgIndex,
	                                              bool isRangeFile, VersionedMutationsVec versionedMutations)
	  : batchIndex(batchIndex), asset(asset), msgIndex(msgIndex), isRangeFile(isRangeFile),
	    versionedMutations(versionedMutations) {}

	std::string toString() const {
		std::stringstream ss;
		ss << "VersionBatchIndex:" << batchIndex << " msgIndex:" << msgIndex << " isRangeFile:" << isRangeFile
		   << " versionedMutations.size:" << versionedMutations.size() << " RestoreAsset:" << asset.toString();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, asset, msgIndex, isRangeFile, versionedMutations, reply);
	}
};

struct RestoreVersionBatchRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 13337457;

	int batchIndex;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreVersionBatchRequest() = default;
	explicit RestoreVersionBatchRequest(int batchIndex) : batchIndex(batchIndex) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "RestoreVersionBatchRequest batchIndex:" << batchIndex;
		return ss.str();
	}
};

struct RestoreFinishRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 13018413;

	bool terminate; // role exits if terminate = true

	ReplyPromise<RestoreCommonReply> reply;

	RestoreFinishRequest() = default;
	explicit RestoreFinishRequest(bool terminate) : terminate(terminate) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, terminate, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "RestoreFinishRequest terminate:" << terminate;
		return ss.str();
	}
};

struct RestoreRequest {
	constexpr static FileIdentifier file_identifier = 16035338;

	int index;
	Key tagName;
	Key url;
	Version targetVersion;
	KeyRange range;
	UID randomUid;

	// Every key in backup will first removePrefix and then addPrefix;
	// Simulation testing does not cover when both addPrefix and removePrefix exist yet.
	Key addPrefix;
	Key removePrefix;

	ReplyPromise<struct RestoreCommonReply> reply;

	RestoreRequest() = default;
	explicit RestoreRequest(const int index, const Key& tagName, const Key& url, Version targetVersion,
	                        const KeyRange& range, const UID& randomUid, Key& addPrefix, Key removePrefix)
	  : index(index), tagName(tagName), url(url), targetVersion(targetVersion), range(range), randomUid(randomUid),
	    addPrefix(addPrefix), removePrefix(removePrefix) {}

	//To change this serialization, ProtocolVersion::RestoreRequestValue must be updated, and downgrades need to be considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, index, tagName, url, targetVersion, range, randomUid, addPrefix, removePrefix, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "index:" << std::to_string(index) << " tagName:" << tagName.contents().toString()
		   << " url:" << url.contents().toString() << " targetVersion:" << std::to_string(targetVersion)
		   << " range:" << range.toString() << " randomUid:" << randomUid.toString()
		   << " addPrefix:" << addPrefix.toString() << " removePrefix:" << removePrefix.toString();
		return ss.str();
	}
};

std::string getRoleStr(RestoreRole role);

////--- Interface functions
ACTOR Future<Void> _restoreWorker(Database cx, LocalityData locality);
ACTOR Future<Void> restoreWorker(Reference<ClusterConnectionFile> ccf, LocalityData locality, std::string coordFolder);

#include "flow/unactorcompiler.h"
#endif
