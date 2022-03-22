/*
 * RestoreUtil.h
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

// This file defines the commonly used data structure and functions
// that are used by both RestoreWorker and RestoreRoles(Controller, Loader, and Applier)

#ifndef FDBSERVER_RESTOREUTIL_H
#define FDBSERVER_RESTOREUTIL_H

#pragma once

#include "fdbclient/Tuple.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/RestoreInterface.h"
#include "flow/flow.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbrpc/Stats.h"
#include <cstdint>
#include <cstdarg>

#define SevFRMutationInfo SevVerbose
//#define SevFRMutationInfo SevInfo

#define SevFRDebugInfo SevVerbose
//#define SevFRDebugInfo SevInfo

struct VersionedMutation {
	MutationRef mutation;
	LogMessageVersion version;

	VersionedMutation() = default;
	explicit VersionedMutation(MutationRef mutation, LogMessageVersion version)
	  : mutation(mutation), version(version) {}
	explicit VersionedMutation(Arena& arena, const VersionedMutation& vm)
	  : mutation(arena, vm.mutation), version(vm.version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutation, version);
	}
};

struct SampledMutation {
	KeyRef key;
	long size;

	explicit SampledMutation(KeyRef key, long size) : key(key), size(size) {}
	explicit SampledMutation(Arena& arena, const SampledMutation& sm) : key(arena, sm.key), size(sm.size) {}
	SampledMutation() = default;

	int totalSize() { return key.size() + sizeof(size); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, size);
	}
};

using MutationsVec = Standalone<VectorRef<MutationRef>>;
using LogMessageVersionVec = Standalone<VectorRef<LogMessageVersion>>;
using VersionedMutationsVec = Standalone<VectorRef<VersionedMutation>>;
using SampledMutationsVec = Standalone<VectorRef<SampledMutation>>;

enum class RestoreRole { Invalid = 0, Controller = 1, Loader, Applier };
std::string getRoleStr(RestoreRole role);
extern const std::vector<std::string> RestoreRoleStr;
extern int numRoles;

std::string getHexString(StringRef input);

bool debugFRMutation(const char* context, Version version, MutationRef const& mutation);

struct RestoreSimpleRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 16448937;

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

#endif // FDBSERVER_RESTOREUTIL_H
