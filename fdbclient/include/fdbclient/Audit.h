/*
 * Audit.h
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

#ifndef FDBCLIENT_AUDIT_H
#define FDBCLIENT_AUDIT_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"

enum class AuditPhase : uint8_t {
	Invalid = 0,
	Running = 1,
	Complete = 2,
	Error = 3,
	Failed = 4,
};

enum class AuditType : uint8_t {
	Invalid = 0,
	ValidateHA = 1,
};

struct AuditStorageState {
	constexpr static FileIdentifier file_identifier = 13804340;

	AuditStorageState() = default;
	AuditStorageState(UID id, AuditType type) : id(id), type(static_cast<uint8_t>(type)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, range, type, phase, error);
	}

	void setType(AuditType type) { this->type = static_cast<uint8_t>(type); }
	AuditType getType() const { return static_cast<AuditType>(this->type); }

	void setPhase(AuditPhase phase) { this->phase = static_cast<uint8_t>(phase); }
	AuditPhase getPhase() const { return static_cast<AuditPhase>(this->phase); }

	UID id;
	KeyRange range;
	uint8_t type;
	uint8_t phase;
	std::string error;
};

struct AuditStorageRequest {
	constexpr static FileIdentifier file_identifier = 13804341;

	AuditStorageRequest() = default;
	AuditStorageRequest(UID id, KeyRange range, AuditType type)
	  : id(id), range(range), type(static_cast<uint8_t>(type)) {}

	void setType(AuditType type) { this->type = static_cast<uint8_t>(this->type); }
	AuditType getType() const { return static_cast<AuditType>(this->type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, range, type, targetServers, reply);
	}

	UID id;
	KeyRange range;
	uint8_t type;
	std::vector<UID> targetServers;
	ReplyPromise<AuditStorageState> reply;
};

// Triggers an audit of the specific type, an audit id is returned if an audit is scheduled successfully.
// If there is an running audit, the corresponding id will be returned, unless force is true;
// When force is set, the ongoing audit will be cancelled, and a new audit will be scheduled.
struct TriggerAuditRequest {
	constexpr static FileIdentifier file_identifier = 1384445;

	TriggerAuditRequest() = default;
	TriggerAuditRequest(AuditType type, KeyRange range)
	  : type(static_cast<uint8_t>(type)), range(range), force(false), async(false) {}

	void setType(AuditType type) { this->type = static_cast<uint8_t>(this->type); }
	AuditType getType() const { return static_cast<AuditType>(this->type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, type, range, force, async, reply);
	}

	uint8_t type;
	KeyRange range;
	bool force;
	bool async;
	ReplyPromise<UID> reply;
};

#endif
