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

#include "fdbclient/FDBTypes.h"
//#include "fdbclient/NativeAPI.h" //MX: Cannot have NativeAPI.h in this .h
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbrpc/Locality.h"

class RestoreConfig;

struct RestoreInterface {
	RequestStream< struct TestRequest > test;
	RequestStream< struct RestoreRequest > request;

	bool operator == (RestoreInterface const& r) const { return id() == r.id(); }
	bool operator != (RestoreInterface const& r) const { return id() != r.id(); }
	UID id() const { return test.getEndpoint().token; }
	NetworkAddress address() const { return test.getEndpoint().address; }

	void initEndpoints() {
		test.getEndpoint( TaskClusterController );
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & test & request;
	}
};

struct TestRequest {
	int testData;
	ReplyPromise< struct TestReply > reply;

	TestRequest() : testData(0) {}
	explicit TestRequest(int testData) : testData(testData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & testData & reply;
	}
};

struct TestReply {
	int replyData;

	TestReply() : replyData(0) {}
	explicit TestReply(int replyData) : replyData(replyData) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & replyData;
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
		ar & index & tagName & url &  waitForComplete & targetVersion & verbose & range & addPrefix & removePrefix & lockDB & randomUid &
		testData & restoreRequests & reply;
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
	std::vector<int> restoreReplies;

	RestoreReply() : replyData(0) {}
	explicit RestoreReply(int replyData) : replyData(replyData) {}
	explicit RestoreReply(int replyData, std::vector<int> restoreReplies) : replyData(replyData), restoreReplies(restoreReplies) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & replyData & restoreReplies;
	}
};

Future<Void> _restoreWorker(Database const& cx, LocalityData const& locality);
Future<Void> restoreWorker(Reference<ClusterConnectionFile> const& ccf, LocalityData const& locality);

#endif
