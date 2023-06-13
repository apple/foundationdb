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
	ValidateReplica = 2,
	ValidateLocationMetadata = 3,
	ValidateStorageServerShard = 4,
};

struct AuditStorageState {
	constexpr static FileIdentifier file_identifier = 13804340;

	AuditStorageState() : type(0), auditServerId(UID()), phase(0), ddId(UID()) {}
	AuditStorageState(UID id, UID auditServerId, AuditType type)
	  : id(id), auditServerId(auditServerId), type(static_cast<uint8_t>(type)), phase(0), ddId(UID()) {}
	AuditStorageState(UID id, KeyRange range, AuditType type)
	  : id(id), auditServerId(UID()), range(range), type(static_cast<uint8_t>(type)), phase(0), ddId(UID()) {}
	AuditStorageState(UID id, AuditType type)
	  : id(id), auditServerId(UID()), type(static_cast<uint8_t>(type)), phase(0), ddId(UID()) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, auditServerId, range, type, phase, error, ddId);
	}

	inline void setType(AuditType type) { this->type = static_cast<uint8_t>(type); }
	inline AuditType getType() const { return static_cast<AuditType>(this->type); }

	inline void setPhase(AuditPhase phase) { this->phase = static_cast<uint8_t>(phase); }
	inline AuditPhase getPhase() const { return static_cast<AuditPhase>(this->phase); }

	std::string toString() const {
		std::string res = "AuditStorageState: [ID]: " + id.toString() +
		                  ", [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		                  ", [Type]: " + std::to_string(type) + ", [Phase]: " + std::to_string(phase);
		if (!error.empty()) {
			res += "[Error]: " + error;
		}

		return res;
	}

	UID id;
	UID ddId; // ddId indicates this audit is managed by which dd
	// ddId is used to check if dd has changed
	// When a new dd starts in the middle of an onging audit,
	// The ongoing audit's ddId gets updated
	// When SS updates the progress, it checks ddId
	// If the ddId is updated, SS Audit actors of the old dd will stop themselves
	// New dd will issue new requests to SSes to continue the remaining work
	UID auditServerId; // UID of SS who is working on this audit task
	KeyRange range;
	uint8_t type;
	uint8_t phase;
	std::string error;
};

struct AuditStorageRequest {
	constexpr static FileIdentifier file_identifier = 13804341;

	AuditStorageRequest() = default;
	// for audit user data
	AuditStorageRequest(UID id, KeyRange range, AuditType type)
	  : id(id), range(range), type(static_cast<uint8_t>(type)) {}

	inline void setType(AuditType type) { this->type = static_cast<uint8_t>(this->type); }
	inline AuditType getType() const { return static_cast<AuditType>(this->type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, range, type, targetServers, reply, ddId);
	}

	UID id;
	UID ddId; // UID of DD who claims the audit
	KeyRange range;
	uint8_t type;
	std::vector<UID> targetServers;
	ReplyPromise<AuditStorageState> reply;
};

// Triggers an audit of the specific type, an audit id is returned if an audit is scheduled successfully.
// If there is an running audit, the corresponding id will be returned, unless force is true;
// When cancel is set, the ongoing audit will be cancelled.
struct TriggerAuditRequest {
	constexpr static FileIdentifier file_identifier = 1384445;

	TriggerAuditRequest() = default;

	TriggerAuditRequest(AuditType type, KeyRange range)
	  : type(static_cast<uint8_t>(type)), range(range), cancel(false), periodic(false) {}

	TriggerAuditRequest(AuditType type, KeyRange range, double periodHours)
	  : type(static_cast<uint8_t>(type)), range(range), cancel(false), periodHours(periodHours), periodic(true) {}

	TriggerAuditRequest(AuditType type) : type(static_cast<uint8_t>(type)), cancel(true), periodic(true) {}

	TriggerAuditRequest(AuditType type, UID id)
	  : type(static_cast<uint8_t>(type)), id(id), cancel(true), periodic(false) {}

	inline void setType(AuditType type) { this->type = static_cast<uint8_t>(this->type); }
	inline AuditType getType() const { return static_cast<AuditType>(this->type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, type, range, id, cancel, reply, periodic, periodHours);
	}

	UID id;
	uint8_t type;
	KeyRange range;
	bool cancel;
	bool periodic;
	double periodHours;
	ReplyPromise<UID> reply;
};

struct AuditStorageScheduleState {
	constexpr static FileIdentifier file_identifier = 1384446;

	AuditStorageScheduleState() = default;

	AuditStorageScheduleState(TriggerAuditRequest req)
	  : id(deterministicRandom()->randomUniqueID()), type(static_cast<uint8_t>(req.getType())), range(req.range),
	    periodHours(req.periodHours), remainWaitHours(req.periodHours), cancelled(false) {}

	inline void setType(AuditType type) { this->type = static_cast<uint8_t>(this->type); }
	inline AuditType getType() const { return static_cast<AuditType>(this->type); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, type, range, periodHours, remainWaitHours, cancelled);
	}

	std::string toString() const {
		std::string res = "AuditStorageScheduleState: [ID]: " + id.toString() +
		                  ", [Range]: " + Traceable<KeyRangeRef>::toString(range) +
		                  ", [Type]: " + std::to_string(type) + ", [PeriodHours]: " + std::to_string(periodHours);
		return res;
	}

	UID id;
	uint8_t type;
	KeyRange range;
	double periodHours;
	double remainWaitHours;
	bool cancelled;
};

#endif
