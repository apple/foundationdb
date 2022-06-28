/*
 * RestoreInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/fdbrpc.h"

struct RestoreCommonReply {
	constexpr static FileIdentifier file_identifier = 5808787;
	UID id; // unique ID of the server who sends the reply
	bool isDuplicated;

	RestoreCommonReply() = default;
	explicit RestoreCommonReply(UID id, bool isDuplicated = false) : id(id), isDuplicated(isDuplicated) {}

	std::string toString() const {
		std::stringstream ss;
		ss << "ServerNodeID:" << id.toString() << " isDuplicated:" << isDuplicated;
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, isDuplicated);
	}
};

struct RestoreRequest {
	constexpr static FileIdentifier file_identifier = 16035338;

	int index;
	Key tagName;
	Key url;
	Optional<std::string> proxy;
	Version targetVersion;
	KeyRange range;
	UID randomUid;

	// Every key in backup will first removePrefix and then addPrefix;
	// Simulation testing does not cover when both addPrefix and removePrefix exist yet.
	Key addPrefix;
	Key removePrefix;

	ReplyPromise<struct RestoreCommonReply> reply;

	RestoreRequest() = default;
	explicit RestoreRequest(const int index,
	                        const Key& tagName,
	                        const Key& url,
	                        const Optional<std::string>& proxy,
	                        Version targetVersion,
	                        const KeyRange& range,
	                        const UID& randomUid,
	                        Key& addPrefix,
	                        Key removePrefix)
	  : index(index), tagName(tagName), url(url), proxy(proxy), targetVersion(targetVersion), range(range),
	    randomUid(randomUid), addPrefix(addPrefix), removePrefix(removePrefix) {}

	// To change this serialization, ProtocolVersion::RestoreRequestValue must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, index, tagName, url, proxy, targetVersion, range, randomUid, addPrefix, removePrefix, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "index:" << std::to_string(index) << " tagName:" << tagName.contents().toString()
		   << " url:" << url.contents().toString() << " proxy:" << (proxy.present() ? proxy.get() : "")
		   << " targetVersion:" << std::to_string(targetVersion) << " range:" << range.toString()
		   << " randomUid:" << randomUid.toString() << " addPrefix:" << addPrefix.toString()
		   << " removePrefix:" << removePrefix.toString();
		return ss.str();
	}
};

extern const KeyRef restoreRequestDoneKey;
extern const KeyRef restoreRequestTriggerKey;
extern const KeyRangeRef restoreRequestKeys;

Value restoreRequestTriggerValue(UID randomID, int numRequests);
int decodeRequestRequestTriggerValue(ValueRef const&);
Key restoreRequestKeyFor(int index);
Value restoreRequestValue(RestoreRequest const&);
