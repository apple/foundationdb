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

#include "fdbclient/FDBTypes.h"
#include "fdbserver/DataDistribution.actor.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/flow.h"

class TCTeamInfo;
class TCMachineInfo;

class TCServerInfo : public ReferenceCounted<TCServerInfo> {
	friend class TCServerInfoImpl;

public:
	UID id;
	Version addedVersion; // Read version when this Server is added
	DDTeamCollection* collection;
	StorageServerInterface lastKnownInterface;
	ProcessClass lastKnownClass;
	std::vector<Reference<TCTeamInfo>> teams;
	Reference<TCMachineInfo> machine;
	Future<Void> tracker;
	int64_t dataInFlightToServer;
	ErrorOr<GetStorageMetricsReply> serverMetrics;
	Promise<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged;
	Future<std::pair<StorageServerInterface, ProcessClass>> onInterfaceChanged;
	Promise<Void> removed;
	Future<Void> onRemoved;
	Future<Void> onTSSPairRemoved;
	Promise<Void> killTss;
	Promise<Void> wakeUpTracker;
	bool inDesiredDC;
	LocalityEntry localityEntry;
	Promise<Void> updated;
	AsyncVar<bool> wrongStoreTypeToRemove;
	AsyncVar<bool> ssVersionTooFarBehind;
	// A storage server's StoreType does not change.
	// To change storeType for an ip:port, we destroy the old one and create a new one.
	KeyValueStoreType storeType; // Storage engine type

	TCServerInfo(StorageServerInterface ssi,
	             DDTeamCollection* collection,
	             ProcessClass processClass,
	             bool inDesiredDC,
	             Reference<LocalitySet> storageServerSet,
	             Version addedVersion = 0)
	  : id(ssi.id()), addedVersion(addedVersion), collection(collection), lastKnownInterface(ssi),
	    lastKnownClass(processClass), dataInFlightToServer(0), onInterfaceChanged(interfaceChanged.getFuture()),
	    onRemoved(removed.getFuture()), onTSSPairRemoved(Never()), inDesiredDC(inDesiredDC),
	    storeType(KeyValueStoreType::END) {

		if (!ssi.isTss()) {
			localityEntry = ((LocalityMap<UID>*)storageServerSet.getPtr())->add(ssi.locality, &id);
		}
	}

	bool isCorrectStoreType(KeyValueStoreType configStoreType) const {
		// A new storage server's store type may not be set immediately.
		// If a storage server does not reply its storeType, it will be tracked by failure monitor and removed.
		return (storeType == configStoreType || storeType == KeyValueStoreType::END);
	}

	Future<Void> updateServerMetrics();

	static Future<Void> updateServerMetrics(Reference<TCServerInfo> server);

	Future<Void> serverMetricsPolling();

	~TCServerInfo();
};
