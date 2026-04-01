/*
 * RestoreUtil.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

// This file defines restoreworker-specific data structures and functions
// shared by RestoreWorker and RestoreRoles (Controller, Loader, and Applier).

#ifndef FDBSERVER_RESTOREWORKER_RESTOREUTIL_H
#define FDBSERVER_RESTOREWORKER_RESTOREUTIL_H

#pragma once

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include "fdbclient/RestoreInterface.h"
#include "fdbserver/core/RestoreCoreUtil.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbrpc/fdbrpc.h"

#define SevFRMutationInfo SevVerbose
// #define SevFRMutationInfo SevInfo

#define SevFRDebugInfo SevVerbose
// #define SevFRDebugInfo SevInfo

enum class RestoreRole { Invalid = 0, Controller = 1, Loader, Applier };
std::string getRoleStr(RestoreRole role);
extern const std::vector<std::string> RestoreRoleStr;
extern int numRoles;

std::string getHexString(StringRef input);

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

#endif // FDBSERVER_RESTOREWORKER_RESTOREUTIL_H
