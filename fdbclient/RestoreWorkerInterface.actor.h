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
#include <string>
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
struct RestoreSendMutationsToAppliersRequest;
struct RestoreSendVersionedMutationsRequest;
struct RestoreSysInfo;
struct RestoreApplierInterface;
struct RestoreFinishRequest;

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
		heartbeat.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		updateRestoreSysInfo.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		loadFile.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		sendMutations.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		initVersionBatch.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		finishVersionBatch.getEndpoint(TaskPriority::LoadBalancedEndpoint);
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
	constexpr static FileIdentifier file_identifier = 54253048;

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

// RestoreAsset uniquely identifies the work unit done by restore roles;
// It is used to ensure exact-once processing on restore loader and applier;
// By combining all RestoreAssets across all verstion batches, restore should process all mutations in
// backup range and log files up to the target restore version.
struct RestoreAsset {
	Version beginVersion, endVersion; // Only use mutation in [begin, end) versions;
	KeyRange range; // Only use mutations in range

	int fileIndex;
	std::string filename;
	int64_t offset;
	int64_t len;

	UID uid;

	RestoreAsset() = default;

	bool operator==(const RestoreAsset& r) const {
		return fileIndex == r.fileIndex && filename == r.filename && offset == r.offset && len == r.len &&
		       beginVersion == r.beginVersion && endVersion == r.endVersion && range == r.range;
	}
	bool operator!=(const RestoreAsset& r) const {
		return fileIndex != r.fileIndex || filename != r.filename || offset != r.offset || len != r.len ||
		       beginVersion != r.beginVersion || endVersion != r.endVersion || range != r.range;
	}
	bool operator<(const RestoreAsset& r) const {
		return std::make_tuple(fileIndex, filename, offset, len, beginVersion, endVersion, range.begin, range.end) <
		       std::make_tuple(r.fileIndex, r.filename, r.offset, r.len, r.beginVersion, r.endVersion, r.range.begin,
		                       r.range.end);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, beginVersion, endVersion, range, filename, fileIndex, offset, len, uid);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "UID:" << uid.toString() << " begin:" << beginVersion << " end:" << endVersion
		   << " range:" << range.toString() << " filename:" << filename << " fileIndex:" << fileIndex
		   << " offset:" << offset << " len:" << len;
		return ss.str();
	}

	// RestoreAsset and VersionBatch both use endVersion as exclusive in version range
	bool isInVersionRange(Version commitVersion) const {
		return commitVersion >= beginVersion && commitVersion < endVersion;
	}
};

struct LoadingParam {
	constexpr static FileIdentifier file_identifier = 17023837;

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

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, isRangeFile, url, rangeVersion, blockSize, asset);
	}

	std::string toString() {
		std::stringstream str;
		str << "isRangeFile:" << isRangeFile << " url:" << url.toString()
		    << " rangeVersion:" << (rangeVersion.present() ? rangeVersion.get() : -1) << " blockSize:" << blockSize
		    << " RestoreAsset:" << asset.toString();
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

// Static info. across version batches
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

struct RestoreLoadFileReply : TimedRequest {
	constexpr static FileIdentifier file_identifier = 34077902;

	LoadingParam param;
	MutationsVec samples; // sampled mutations
	bool isDuplicated; // true if loader thinks the request is a duplicated one

	RestoreLoadFileReply() = default;
	explicit RestoreLoadFileReply(LoadingParam param, MutationsVec samples, bool isDuplicated)
	  : param(param), samples(samples), isDuplicated(isDuplicated) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, param, samples, isDuplicated);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "LoadingParam:" << param.toString() << " samples.size:" << samples.size()
		   << " isDuplicated:" << isDuplicated;
		return ss.str();
	}
};

// Sample_Range_File and Assign_Loader_Range_File, Assign_Loader_Log_File
struct RestoreLoadFileRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 26557364;

	int batchIndex;
	LoadingParam param;

	ReplyPromise<RestoreLoadFileReply> reply;

	RestoreLoadFileRequest() = default;
	explicit RestoreLoadFileRequest(int batchIndex, LoadingParam& param) : batchIndex(batchIndex), param(param){};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, param, reply);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreLoadFileRequest batchIndex:" << batchIndex << " param:" << param.toString();
		return ss.str();
	}
};

struct RestoreSendMutationsToAppliersRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 68827305;

	int batchIndex; // version batch index
	std::map<Key, UID> rangeToApplier;
	bool useRangeFile; // Send mutations parsed from range file?

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendMutationsToAppliersRequest() = default;
	explicit RestoreSendMutationsToAppliersRequest(int batchIndex, std::map<Key, UID> rangeToApplier, bool useRangeFile)
	  : batchIndex(batchIndex), rangeToApplier(rangeToApplier), useRangeFile(useRangeFile) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, rangeToApplier, useRangeFile, reply);
	}

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreSendMutationsToAppliersRequest batchIndex:" << batchIndex
		   << " keyToAppliers.size:" << rangeToApplier.size() << " useRangeFile:" << useRangeFile;
		return ss.str();
	}
};

struct RestoreSendVersionedMutationsRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 69764565;

	int batchIndex; // version batch index
	RestoreAsset asset; // Unique identifier for the current restore asset

	Version prevVersion, version; // version is the commitVersion of the mutation vector.
	bool isRangeFile;
	MutationsVec mutations; // All mutations at the same version parsed by one loader

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSendVersionedMutationsRequest() = default;
	explicit RestoreSendVersionedMutationsRequest(int batchIndex, const RestoreAsset& asset, Version prevVersion,
	                                              Version version, bool isRangeFile, MutationsVec mutations)
	  : batchIndex(batchIndex), asset(asset), prevVersion(prevVersion), version(version), isRangeFile(isRangeFile),
	    mutations(mutations) {}

	std::string toString() {
		std::stringstream ss;
		ss << "VersionBatchIndex:" << batchIndex << "RestoreAsset:" << asset.toString()
		   << " prevVersion:" << prevVersion << " version:" << version << " isRangeFile:" << isRangeFile
		   << " mutations.size:" << mutations.size();
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, asset, prevVersion, version, isRangeFile, mutations, reply);
	}
};

struct RestoreVersionBatchRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 97223537;

	int batchIndex;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreVersionBatchRequest() = default;
	explicit RestoreVersionBatchRequest(int batchIndex) : batchIndex(batchIndex) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, batchIndex, reply);
	}

	std::string toString() {
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

	std::string toString() {
		std::stringstream ss;
		ss << "RestoreFinishRequest terminate:" << terminate;
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

	std::vector<int> restoreRequests;
	// Key restoreTag;

	ReplyPromise<struct RestoreCommonReply> reply;

	RestoreRequest() = default;
	explicit RestoreRequest(const int index, const Key& tagName, const Key& url, bool waitForComplete,
	                        Version targetVersion, bool verbose, const KeyRange& range, const Key& addPrefix,
	                        const Key& removePrefix, bool lockDB, const UID& randomUid)
	  : index(index), tagName(tagName), url(url), waitForComplete(waitForComplete), targetVersion(targetVersion),
	    verbose(verbose), range(range), addPrefix(addPrefix), removePrefix(removePrefix), lockDB(lockDB),
	    randomUid(randomUid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, index, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix,
		           lockDB, randomUid, restoreRequests, reply);
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
ACTOR Future<Void> restoreWorker(Reference<ClusterConnectionFile> ccf, LocalityData locality, std::string coordFolder);

#include "flow/unactorcompiler.h"
#endif
