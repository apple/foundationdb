/*
 * ClusterInterface.h
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

#ifndef FDBCLIENT_ClusterInterface_H
#define FDBCLIENT_ClusterInterface_H
#pragma once

#include "FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "Status.h"
#include "ClientDBInfo.h"
#include "ClientWorkerInterface.h"

struct ClusterInterface {
	RequestStream< struct OpenDatabaseRequest > openDatabase;
	RequestStream< struct FailureMonitoringRequest > failureMonitoring;
	RequestStream< struct StatusRequest > databaseStatus;
	RequestStream< ReplyPromise<Void> > ping;
	RequestStream< struct GetClientWorkersRequest > getClientWorkers;

	bool operator == (ClusterInterface const& r) const { return id() == r.id(); }
	bool operator != (ClusterInterface const& r) const { return id() != r.id(); }
	UID id() const { return openDatabase.getEndpoint().token; }
	NetworkAddress address() const { return openDatabase.getEndpoint().address; }

	void initEndpoints() {
		openDatabase.getEndpoint( TaskClusterController );
		failureMonitoring.getEndpoint( TaskFailureMonitor );
		databaseStatus.getEndpoint( TaskClusterController );
		ping.getEndpoint( TaskClusterController );
		getClientWorkers.getEndpoint( TaskClusterController );
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & openDatabase & failureMonitoring & databaseStatus & ping & getClientWorkers;
	}
};

struct ClientVersionRef {
	StringRef clientVersion;
	StringRef sourceVersion;
	StringRef protocolVersion;

	ClientVersionRef() {
		initUnknown();
	}

	ClientVersionRef(Arena &arena, ClientVersionRef const& cv) : clientVersion(arena, cv.clientVersion), sourceVersion(arena, cv.sourceVersion), protocolVersion(arena, cv.protocolVersion) {}
	ClientVersionRef(std::string versionString) {
		size_t index = versionString.find(",");
		if(index == versionString.npos) {
			initUnknown();
			return;
		}

		clientVersion = StringRef((uint8_t*)&versionString[0], index);

		size_t nextIndex = versionString.find(",", index+1);
		if(index == versionString.npos) {
			initUnknown();
			return;
		}

		sourceVersion = StringRef((uint8_t*)&versionString[index+1], nextIndex-(index+1));
		protocolVersion = StringRef((uint8_t*)&versionString[nextIndex+1], versionString.length()-(nextIndex+1));
	}

	void initUnknown() {
		clientVersion = LiteralStringRef("Unknown");
		sourceVersion = LiteralStringRef("Unknown");
		protocolVersion = LiteralStringRef("Unknown");
	}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & clientVersion & sourceVersion & protocolVersion;
	}

	size_t expectedSize() const { return clientVersion.size() + sourceVersion.size() + protocolVersion.size(); }

	bool operator<(const ClientVersionRef& rhs) const {
		if(protocolVersion != rhs.protocolVersion) {
			return protocolVersion < rhs.protocolVersion;
		}

		// These comparisons are arbitrary because they aren't ordered
		if(clientVersion != rhs.clientVersion) {
			return clientVersion < rhs.clientVersion;
		}

		return sourceVersion < rhs.sourceVersion;
	}
};

struct OpenDatabaseRequest {
	// Sent by the native API to the cluster controller to open a database and track client
	//   info changes.  Returns immediately if the current client info id is different from
	//   knownClientInfoID; otherwise returns when it next changes (or perhaps after a long interval)
	Arena arena;
	StringRef dbName, issues, traceLogGroup;
	VectorRef<ClientVersionRef> supportedVersions;
	UID knownClientInfoID;
	ReplyPromise< struct ClientDBInfo > reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A400040001LL );
		ar & dbName & issues & supportedVersions & traceLogGroup & knownClientInfoID & reply & arena;
	}
};

struct SystemFailureStatus {
	NetworkAddress address;
	FailureStatus status;

	SystemFailureStatus() : address(0,0) {}
	SystemFailureStatus( NetworkAddress const& a, FailureStatus const& s ) : address(a), status(s) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & address & status;
	}
};

struct FailureMonitoringRequest {
	// Sent by all participants to the cluster controller reply.clientRequestIntervalMS
	//   ms after receiving the previous reply.
	// Provides the controller the self-diagnosed status of the sender, and also 
	//   requests the status of other systems.  Failure to timely send one of these implies
	//   a failed status.
	// If !senderStatus.present(), the sender wants to receive the latest failure information
	//   but doesn't want to be monitored.
	// The failureInformationVersion returned in reply should be passed back to the
	//   next request to facilitate delta compression of the failure information.

	Optional<FailureStatus> senderStatus;
	Version failureInformationVersion;
	ReplyPromise< struct FailureMonitoringReply > reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & senderStatus & failureInformationVersion & reply;
	}
};

struct FailureMonitoringReply {
	VectorRef< SystemFailureStatus > changes;
	Version failureInformationVersion;
	bool allOthersFailed;							// If true, changes are relative to all servers being failed, otherwise to the version given in the request
	int clientRequestIntervalMS,        // after this many milliseconds, send another request
		considerServerFailedTimeoutMS;  // after this many additional milliseconds, consider the ClusterController itself to be failed
	Arena arena;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & changes & failureInformationVersion & allOthersFailed & clientRequestIntervalMS & considerServerFailedTimeoutMS & arena;
	}
};

struct StatusRequest {
	ReplyPromise< struct StatusReply > reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & reply;
	}
};

struct StatusReply {
	StatusObject statusObj;

	StatusReply() {}
	StatusReply( StatusObject statusObj ) : statusObj(statusObj) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & statusObj;
	}
};

struct GetClientWorkersRequest {
	ReplyPromise<vector<ClientWorkerInterface>> reply;

	GetClientWorkersRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & reply;
	}
};

#endif