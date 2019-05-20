/*
 * BackupInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_BACKUPINTERFACE_H
#define FDBSERVER_BACKUPINTERFACE_H

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

// The interface for backup workers.
struct BackupInterface {
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltBackupRequest> haltBackup;
	struct LocalityData locality;
	UID myId;

	BackupInterface() = default;
	explicit BackupInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return waitFailure.getEndpoint().getPrimaryAddress(); }
	UID getToken() const { return haltBackup.getEndpoint().token; }
	bool operator== (const BackupInterface& r) const {
		return id() == r.id();
	}
	bool operator!= (const BackupInterface& r) const {
		return !(*this == r);
	}

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, waitFailure, haltBackup, locality, myId);
	}
};

struct HaltBackupRequest {
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltBackupRequest() = default;
	explicit HaltBackupRequest(UID uid) : requesterID(uid) {}

	template<class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

#endif //FDBSERVER_BACKUPINTERFACE_H
