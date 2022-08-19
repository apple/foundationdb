/*
 * ClientLogEvents.h
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
#ifndef FDBCLIENT_CLIENTLOGEVENTS_H
#define FDBCLIENT_CLIENTLOGEVENTS_H

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitProxyInterface.h"

namespace FdbClientLogEvents {
enum class EventType {
	GET_VERSION_LATENCY = 0,
	GET_LATENCY = 1,
	GET_RANGE_LATENCY = 2,
	COMMIT_LATENCY = 3,
	ERROR_GET = 4,
	ERROR_GET_RANGE = 5,
	ERROR_COMMIT = 6,
	UNSET
};

enum class TransactionPriorityType : int { PRIORITY_DEFAULT = 0, PRIORITY_BATCH = 1, PRIORITY_IMMEDIATE = 2, UNSET };
static_assert(sizeof(TransactionPriorityType) == 4, "transaction_profiling_analyzer.py assumes this field has size 4");

struct Event {
	Event(EventType t, double ts, const Optional<Standalone<StringRef>>& dc, const Optional<TenantName>& tenant)
	  : type(t), startTs(ts), tenant(tenant) {
		if (dc.present())
			dcId = dc.get();
	}
	Event() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (ar.protocolVersion().hasTenants()) {
			return serializer(ar, type, startTs, dcId, tenant);
		} else if (ar.protocolVersion().version() >= (uint64_t)0x0FDB00B063010001LL) {
			return serializer(ar, type, startTs, dcId);
		} else {
			return serializer(ar, type, startTs);
		}
	}

	EventType type{ EventType::UNSET };
	double startTs{ 0 };
	Key dcId{};
	Optional<TenantName> tenant{};

	void logEvent(std::string id, int maxFieldLength) const {}
	void augmentTraceEvent(TraceEvent& event) const { event.detail("Tenant", tenant); }
};

struct EventGetVersion : public Event {
	EventGetVersion() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), latency);
		else
			return serializer(ar, latency);
	}

	double latency;

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_GetVersion");
		event.detail("TransactionID", id).detail("Latency", latency);
		augmentTraceEvent(event);
	}
};

// Version V2 of EventGetVersion starting at 6.2
struct EventGetVersion_V2 : public Event {
	EventGetVersion_V2() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), latency, priorityType);
		else
			return serializer(ar, latency, priorityType);
	}

	double latency;
	TransactionPriorityType priorityType{ TransactionPriorityType::UNSET };

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_GetVersion");
		event.detail("TransactionID", id).detail("Latency", latency).detail("PriorityType", priorityType);
		augmentTraceEvent(event);
	}
};

// Version V3 of EventGetVersion starting at 6.3
struct EventGetVersion_V3 : public Event {
	EventGetVersion_V3(double ts,
	                   const Optional<Standalone<StringRef>>& dcId,
	                   double lat,
	                   TransactionPriority priority,
	                   Version version,
	                   const Optional<TenantName>& tenant)
	  : Event(EventType::GET_VERSION_LATENCY, ts, dcId, tenant), latency(lat), readVersion(version) {
		switch (priority) {
		// Unfortunately, the enum serialized here disagrees with the enum used elsewhere for the values used by each
		// priority
		case TransactionPriority::IMMEDIATE:
			priorityType = TransactionPriorityType::PRIORITY_IMMEDIATE;
			break;
		case TransactionPriority::DEFAULT:
			priorityType = TransactionPriorityType::PRIORITY_DEFAULT;
			break;
		case TransactionPriority::BATCH:
			priorityType = TransactionPriorityType::PRIORITY_BATCH;
			break;
		default:
			ASSERT(false);
		}
	}
	EventGetVersion_V3() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), latency, priorityType, readVersion);
		else
			return serializer(ar, latency, priorityType, readVersion);
	}

	double latency;
	TransactionPriorityType priorityType{ TransactionPriorityType::UNSET };
	Version readVersion;

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_GetVersion");
		event.detail("TransactionID", id)
		    .detail("Latency", latency)
		    .detail("PriorityType", priorityType)
		    .detail("ReadVersion", readVersion);
		augmentTraceEvent(event);
	}
};

struct EventGet : public Event {
	EventGet(double ts,
	         const Optional<Standalone<StringRef>>& dcId,
	         double lat,
	         int size,
	         const KeyRef& in_key,
	         const Optional<TenantName>& tenant)
	  : Event(EventType::GET_LATENCY, ts, dcId, tenant), latency(lat), valueSize(size), key(in_key) {}
	EventGet() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), latency, valueSize, key);
		else
			return serializer(ar, latency, valueSize, key);
	}

	double latency;
	int valueSize;
	Key key;

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_Get");
		event.setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .detail("Latency", latency)
		    .detail("ValueSizeBytes", valueSize)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Key", key);
		augmentTraceEvent(event);
	}
};

struct EventGetRange : public Event {
	EventGetRange(double ts,
	              const Optional<Standalone<StringRef>>& dcId,
	              double lat,
	              int size,
	              const KeyRef& start_key,
	              const KeyRef& end_key,
	              const Optional<TenantName>& tenant)
	  : Event(EventType::GET_RANGE_LATENCY, ts, dcId, tenant), latency(lat), rangeSize(size), startKey(start_key),
	    endKey(end_key) {}
	EventGetRange() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), latency, rangeSize, startKey, endKey);
		else
			return serializer(ar, latency, rangeSize, startKey, endKey);
	}

	double latency;
	int rangeSize;
	Key startKey;
	Key endKey;

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_GetRange");
		event.setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .detail("Latency", latency)
		    .detail("RangeSizeBytes", rangeSize)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("StartKey", startKey)
		    .detail("EndKey", endKey);
		augmentTraceEvent(event);
	}
};

struct EventCommit : public Event {
	EventCommit() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), latency, numMutations, commitBytes, req.transaction, req.arena);
		else
			return serializer(ar, latency, numMutations, commitBytes, req.transaction, req.arena);
	}

	double latency;
	int numMutations;
	int commitBytes;
	CommitTransactionRequest
	    req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

	void logEvent(std::string id, int maxFieldLength) const {
		for (auto& read_range : req.transaction.read_conflict_ranges) {
			TraceEvent ev1("TransactionTrace_Commit_ReadConflictRange");
			ev1.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Begin", read_range.begin)
			    .detail("End", read_range.end);
			augmentTraceEvent(ev1);
		}

		for (auto& write_range : req.transaction.write_conflict_ranges) {
			TraceEvent ev2("TransactionTrace_Commit_WriteConflictRange");
			ev2.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Begin", write_range.begin)
			    .detail("End", write_range.end);
			augmentTraceEvent(ev2);
		}

		for (auto& mutation : req.transaction.mutations) {
			TraceEvent ev3("TransactionTrace_Commit_Mutation");
			ev3.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Mutation", mutation);
			augmentTraceEvent(ev3);
		}

		TraceEvent ev4("TransactionTrace_Commit");
		ev4.detail("TransactionID", id)
		    .detail("Latency", latency)
		    .detail("NumMutations", numMutations)
		    .detail("CommitSizeBytes", commitBytes);
		augmentTraceEvent(ev4);
	}
};

// Version V2 of EventGetVersion starting at 6.3
struct EventCommit_V2 : public Event {
	EventCommit_V2(double ts,
	               const Optional<Standalone<StringRef>>& dcId,
	               double lat,
	               int mut,
	               int bytes,
	               Version version,
	               const CommitTransactionRequest& commit_req,
	               const Optional<TenantName>& tenant)
	  : Event(EventType::COMMIT_LATENCY, ts, dcId, tenant), latency(lat), numMutations(mut), commitBytes(bytes),
	    commitVersion(version), req(commit_req) {}
	EventCommit_V2() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(
			    Event::serialize(ar), latency, numMutations, commitBytes, commitVersion, req.transaction, req.arena);
		else
			return serializer(ar, latency, numMutations, commitBytes, commitVersion, req.transaction, req.arena);
	}

	double latency;
	int numMutations;
	int commitBytes;
	Version commitVersion;
	CommitTransactionRequest
	    req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

	void logEvent(std::string id, int maxFieldLength) const {
		for (auto& read_range : req.transaction.read_conflict_ranges) {
			TraceEvent ev1("TransactionTrace_Commit_ReadConflictRange");
			ev1.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Begin", read_range.begin)
			    .detail("End", read_range.end);
			augmentTraceEvent(ev1);
		}

		for (auto& write_range : req.transaction.write_conflict_ranges) {
			TraceEvent ev2("TransactionTrace_Commit_WriteConflictRange");
			ev2.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Begin", write_range.begin)
			    .detail("End", write_range.end);
			augmentTraceEvent(ev2);
		}

		for (auto& mutation : req.transaction.mutations) {
			TraceEvent ev3("TransactionTrace_Commit_Mutation");
			ev3.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Mutation", mutation);
			augmentTraceEvent(ev3);
		}

		TraceEvent ev4("TransactionTrace_Commit");
		ev4.detail("TransactionID", id)
		    .detail("CommitVersion", commitVersion)
		    .detail("Latency", latency)
		    .detail("NumMutations", numMutations)
		    .detail("CommitSizeBytes", commitBytes);
		augmentTraceEvent(ev4);
	}
};

struct EventGetError : public Event {
	EventGetError(double ts,
	              const Optional<Standalone<StringRef>>& dcId,
	              int err_code,
	              const KeyRef& in_key,
	              const Optional<TenantName>& tenant)
	  : Event(EventType::ERROR_GET, ts, dcId, tenant), errCode(err_code), key(in_key) {}
	EventGetError() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), errCode, key);
		else
			return serializer(ar, errCode, key);
	}

	int errCode;
	Key key;

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_GetError");
		event.setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .detail("ErrCode", errCode)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Key", key);
		augmentTraceEvent(event);
	}
};

struct EventGetRangeError : public Event {
	EventGetRangeError(double ts,
	                   const Optional<Standalone<StringRef>>& dcId,
	                   int err_code,
	                   const KeyRef& start_key,
	                   const KeyRef& end_key,
	                   const Optional<TenantName>& tenant)
	  : Event(EventType::ERROR_GET_RANGE, ts, dcId, tenant), errCode(err_code), startKey(start_key), endKey(end_key) {}
	EventGetRangeError() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), errCode, startKey, endKey);
		else
			return serializer(ar, errCode, startKey, endKey);
	}

	int errCode;
	Key startKey;
	Key endKey;

	void logEvent(std::string id, int maxFieldLength) const {
		TraceEvent event("TransactionTrace_GetRangeError");
		event.setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .detail("ErrCode", errCode)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("StartKey", startKey)
		    .detail("EndKey", endKey);
		augmentTraceEvent(event);
	}
};

struct EventCommitError : public Event {
	EventCommitError(double ts,
	                 const Optional<Standalone<StringRef>>& dcId,
	                 int err_code,
	                 const CommitTransactionRequest& commit_req,
	                 const Optional<TenantName>& tenant)
	  : Event(EventType::ERROR_COMMIT, ts, dcId, tenant), errCode(err_code), req(commit_req) {}
	EventCommitError() {}

	template <typename Ar>
	Ar& serialize(Ar& ar) {
		if (!ar.isDeserializing)
			return serializer(Event::serialize(ar), errCode, req.transaction, req.arena);
		else
			return serializer(ar, errCode, req.transaction, req.arena);
	}

	int errCode;
	CommitTransactionRequest
	    req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

	void logEvent(std::string id, int maxFieldLength) const {
		for (auto& read_range : req.transaction.read_conflict_ranges) {
			TraceEvent ev1("TransactionTrace_CommitError_ReadConflictRange");
			ev1.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Begin", read_range.begin)
			    .detail("End", read_range.end);
			augmentTraceEvent(ev1);
		}

		for (auto& write_range : req.transaction.write_conflict_ranges) {
			TraceEvent ev2("TransactionTrace_CommitError_WriteConflictRange");
			ev2.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Begin", write_range.begin)
			    .detail("End", write_range.end);
			augmentTraceEvent(ev2);
		}

		for (auto& mutation : req.transaction.mutations) {
			TraceEvent ev3("TransactionTrace_CommitError_Mutation");
			ev3.setMaxEventLength(-1)
			    .detail("TransactionID", id)
			    .setMaxFieldLength(maxFieldLength)
			    .detail("Mutation", mutation);
			augmentTraceEvent(ev3);
		}

		TraceEvent ev4("TransactionTrace_CommitError");
		ev4.detail("TransactionID", id).detail("ErrCode", errCode);
		augmentTraceEvent(ev4);
	}
};
} // namespace FdbClientLogEvents

#endif
