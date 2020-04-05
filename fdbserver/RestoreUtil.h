/*
 * RestoreUtil.h
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

// This file defines the commonly used data structure and functions
// that are used by both RestoreWorker and RestoreRoles(Master, Loader, and Applier)

#ifndef FDBSERVER_RESTOREUTIL_H
#define FDBSERVER_RESTOREUTIL_H
#pragma once

#include "fdbclient/Tuple.h"
#include "fdbclient/CommitTransaction.h"
#include "flow/flow.h"
#include "flow/Stats.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/IAsyncFile.h"
#include <cstdint>
#include <cstdarg>

//#define SevFRMutationInfo SevVerbose
#define SevFRMutationInfo SevInfo

using MutationsVec = Standalone<VectorRef<MutationRef>>;
using SubSequenceVec = Standalone<VectorRef<uint32_t>>;

enum class RestoreRole { Invalid = 0, Master = 1, Loader, Applier };
BINARY_SERIALIZABLE(RestoreRole);
std::string getRoleStr(RestoreRole role);
extern const std::vector<std::string> RestoreRoleStr;
extern int numRoles;

std::string getHexString(StringRef input);

bool debugFRMutation( const char* context, Version version, MutationRef const& mutation );

struct RestoreCommonReply {
	constexpr static FileIdentifier file_identifier = 56140435;
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

struct RestoreSimpleRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 83557801;

	ReplyPromise<RestoreCommonReply> reply;

	RestoreSimpleRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "RestoreSimpleRequest";
		return ss.str();
	}
};

bool isRangeMutation(MutationRef m);

#endif // FDBSERVER_RESTOREUTIL_ACTOR_H