/*
 * TCServerInfo.h
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

#include "fdbrpc/ReplicationTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/FastRef.h"
#include "flow/genericactors.actor.h"

struct DDTeamCollection;
struct TCTeamInfo;
struct TCMachineInfo;

class TCServerInfo : public ReferenceCounted<TCServerInfo> {
	Future<Void> tracker;
	UID id;
	std::vector<Reference<TCTeamInfo>> teams;
	DDTeamCollection* collection;

public:
	StorageServerInterface lastKnownInterface;
	ProcessClass lastKnownClass;
	Reference<TCMachineInfo> machine;
	int64_t dataInFlightToServer;
	ErrorOr<GetStorageMetricsReply> serverMetrics;
	Promise<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged;
	AsyncTrigger removed;
	Promise<Void> updated;
	Promise<Void> wakeUpTracker;
	bool inDesiredDC;
	LocalityEntry localityEntry;
	AsyncVar<bool> wrongStoreTypeToRemove;
	AsyncVar<bool> ssVersionTooFarBehind;
	// A storage server's StoreType does not change.
	// To change storeType for an ip:port, we destroy the old one and create a new one.
	KeyValueStoreType storeType; // Storage engine type
	friend class TCServerInfoImpl;

public:
	TCServerInfo(StorageServerInterface ssi, DDTeamCollection* collection, ProcessClass processClass, bool inDesiredDC,
	             Reference<LocalitySet> storageServerSet);
	UID const& getID() const;
	Future<std::pair<StorageServerInterface, ProcessClass>> onInterfaceChanged() const;
	std::vector<Reference<TCTeamInfo>> const& getTeams() const;
	bool removeTeam(Reference<TCTeamInfo> const& team);
	void addTeam(Reference<TCTeamInfo> const& team);
	void setTracker(Future<Void>&& tracker);
	void cancelTracker();
	bool isCorrectStoreType(KeyValueStoreType configStoreType) const;
	Future<Void> updateServerMetrics();
	Future<Void> serverMetricsPolling();
	void clearCollection();
	~TCServerInfo();
};
