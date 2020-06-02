/*
 * ClientLogEvents.h
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
#ifndef FDBCLIENT_CLIENTLOGEVENTS_H
#define FDBCLIENT_CLIENTLOGEVENTS_H

namespace FdbClientLogEvents {
	typedef int EventType;
	enum {	GET_VERSION_LATENCY	= 0,
			GET_LATENCY			= 1,
			GET_RANGE_LATENCY	= 2,
			COMMIT_LATENCY		= 3,
			ERROR_GET			= 4,
			ERROR_GET_RANGE		= 5,
			ERROR_COMMIT		= 6,

			EVENTTYPEEND	// End of EventType
	     };

	typedef int TrasactionPriorityType;
	enum {
		PRIORITY_DEFAULT   = 0,
		PRIORITY_BATCH     = 1,
		PRIORITY_IMMEDIATE = 2,
		PRIORITY_END
	};

	struct Event {
		Event(EventType t, double ts, const Optional<Standalone<StringRef>> &dc) : type(t), startTs(ts){
			if (dc.present())
				dcId = dc.get(); 
		 }
		Event() { }

		template <typename Ar>	Ar& serialize(Ar &ar) { 
			if (ar.protocolVersion().version() >= (uint64_t) 0x0FDB00B063010001LL) {
				return serializer(ar, type, startTs, dcId);
			} else {
				return serializer(ar, type, startTs); 
			}
		}

		EventType type{ EVENTTYPEEND };
		double startTs{ 0 };
		Key dcId{};

		void logEvent(std::string id, int maxFieldLength) const {}
	};

	struct EventGetVersion : public Event {
		EventGetVersion() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency);
			else
				return serializer(ar, latency);
		}

		double latency;

		void logEvent(std::string id, int maxFieldLength) const {
			TraceEvent("TransactionTrace_GetVersion")
			.detail("TransactionID", id)
			.detail("Latency", latency);
		}
	};

	// Version V2 of EventGetVersion starting at 6.2
	struct EventGetVersion_V2 : public Event {
		EventGetVersion_V2() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency, priorityType);
			else
				return serializer(ar, latency, priorityType);
		}

		double latency;
		TrasactionPriorityType priorityType {PRIORITY_END};

		void logEvent(std::string id, int maxFieldLength) const {
			TraceEvent("TransactionTrace_GetVersion")
			.detail("TransactionID", id)
			.detail("Latency", latency)
			.detail("PriorityType", priorityType);
		}
	};

	// Version V3 of EventGetVersion starting at 6.3
	struct EventGetVersion_V3 : public Event {
		EventGetVersion_V3(double ts, const Optional<Standalone<StringRef>> &dcId, double lat, TransactionPriority priority, Version version) : Event(GET_VERSION_LATENCY, ts, dcId), latency(lat), readVersion(version) {
			switch(priority) {
				// Unfortunately, the enum serialized here disagrees with the enum used elsewhere for the values used by each priority
				case TransactionPriority::IMMEDIATE:
					priorityType = PRIORITY_IMMEDIATE;
					break;
				case TransactionPriority::DEFAULT:
					priorityType = PRIORITY_DEFAULT;
					break;
				case TransactionPriority::BATCH:
					priorityType = PRIORITY_BATCH;
					break;
				default:
					ASSERT(false);
			}
		}
		EventGetVersion_V3() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency, priorityType, readVersion);
			else
				return serializer(ar, latency, priorityType, readVersion);
		}

		double latency;
		TrasactionPriorityType priorityType {PRIORITY_END};
		Version readVersion;

		void logEvent(std::string id, int maxFieldLength) const {
			TraceEvent("TransactionTrace_GetVersion")
			.detail("TransactionID", id)
			.detail("Latency", latency)
			.detail("PriorityType", priorityType)
			.detail("ReadVersion", readVersion);
		}
	};

	struct EventGet : public Event {
		EventGet(double ts, const Optional<Standalone<StringRef>> &dcId, double lat, int size, const KeyRef &in_key) : Event(GET_LATENCY, ts, dcId), latency(lat), valueSize(size), key(in_key) { }
		EventGet() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency, valueSize, key);
			else
				return serializer(ar, latency, valueSize, key);
		}

		double latency;
		int valueSize;
		Key key;

		void logEvent(std::string id, int maxFieldLength) const {
			TraceEvent("TransactionTrace_Get")
			.setMaxEventLength(-1)
			.detail("TransactionID", id)
			.detail("Latency", latency)
			.detail("ValueSizeBytes", valueSize)
			.setMaxFieldLength(maxFieldLength)
			.detail("Key", key);
		}
	};

	struct EventGetRange : public Event {
		EventGetRange(double ts, const Optional<Standalone<StringRef>> &dcId, double lat, int size, const KeyRef &start_key, const KeyRef & end_key) : Event(GET_RANGE_LATENCY, ts, dcId), latency(lat), rangeSize(size), startKey(start_key), endKey(end_key) { }
		EventGetRange() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
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
			TraceEvent("TransactionTrace_GetRange")
			.setMaxEventLength(-1)
			.detail("TransactionID", id)
			.detail("Latency", latency)
			.detail("RangeSizeBytes", rangeSize)
			.setMaxFieldLength(maxFieldLength)
			.detail("StartKey", startKey)
			.detail("EndKey", endKey);
		}
	};

	struct EventCommit : public Event {
		EventCommit() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency, numMutations, commitBytes, req.transaction, req.arena);
			else
				return serializer(ar, latency, numMutations, commitBytes, req.transaction, req.arena);
		}

		double latency;
		int numMutations;
		int commitBytes;
		CommitTransactionRequest req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

		void logEvent(std::string id, int maxFieldLength) const {
			for (auto &read_range : req.transaction.read_conflict_ranges) {
				TraceEvent("TransactionTrace_Commit_ReadConflictRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Begin", read_range.begin)
				.detail("End", read_range.end);
			}

			for (auto &write_range : req.transaction.write_conflict_ranges) {
				TraceEvent("TransactionTrace_Commit_WriteConflictRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Begin", write_range.begin)
				.detail("End", write_range.end);
			}

			for (auto &mutation : req.transaction.mutations) {
				TraceEvent("TransactionTrace_Commit_Mutation")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Mutation", mutation.toString());
			}

			TraceEvent("TransactionTrace_Commit")
			.detail("TransactionID", id)
			.detail("Latency", latency)
			.detail("NumMutations", numMutations)
			.detail("CommitSizeBytes", commitBytes);
		}
	};

	// Version V2 of EventGetVersion starting at 6.3
	struct EventCommit_V2 : public Event {
		EventCommit_V2(double ts, const Optional<Standalone<StringRef>> &dcId, double lat, int mut, int bytes, Version version, const CommitTransactionRequest &commit_req) 
			: Event(COMMIT_LATENCY, ts, dcId), latency(lat), numMutations(mut), commitBytes(bytes), commitVersion(version), req(commit_req) { }
		EventCommit_V2() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency, numMutations, commitBytes, commitVersion, req.transaction, req.arena);
			else
				return serializer(ar, latency, numMutations, commitBytes, commitVersion, req.transaction, req.arena);
		}

		double latency;
		int numMutations;
		int commitBytes;
		Version commitVersion;
		CommitTransactionRequest req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

		void logEvent(std::string id, int maxFieldLength) const {
			for (auto &read_range : req.transaction.read_conflict_ranges) {
				TraceEvent("TransactionTrace_Commit_ReadConflictRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Begin", read_range.begin)
				.detail("End", read_range.end);
			}

			for (auto &write_range : req.transaction.write_conflict_ranges) {
				TraceEvent("TransactionTrace_Commit_WriteConflictRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Begin", write_range.begin)
				.detail("End", write_range.end);
			}

			for (auto &mutation : req.transaction.mutations) {
				TraceEvent("TransactionTrace_Commit_Mutation")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Mutation", mutation.toString());
			}

			TraceEvent("TransactionTrace_Commit")
			.detail("TransactionID", id)
			.detail("CommitVersion", commitVersion)
			.detail("Latency", latency)
			.detail("NumMutations", numMutations)
			.detail("CommitSizeBytes", commitBytes);
		}
	};

	struct EventGetError : public Event {
		EventGetError(double ts, const Optional<Standalone<StringRef>> &dcId, int err_code, const KeyRef &in_key) : Event(ERROR_GET, ts, dcId), errCode(err_code), key(in_key) { }
		EventGetError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), errCode, key);
			else
				return serializer(ar, errCode, key);
		}

		int errCode;
		Key key;

		void logEvent(std::string id, int maxFieldLength) const {
			TraceEvent("TransactionTrace_GetError")
			.setMaxEventLength(-1)
			.detail("TransactionID", id)
			.detail("ErrCode", errCode)
			.setMaxFieldLength(maxFieldLength)
			.detail("Key", key);
		}
	};

	struct EventGetRangeError : public Event {
		EventGetRangeError(double ts, const Optional<Standalone<StringRef>> &dcId, int err_code, const KeyRef &start_key, const KeyRef & end_key) : Event(ERROR_GET_RANGE, ts, dcId), errCode(err_code), startKey(start_key), endKey(end_key) { }
		EventGetRangeError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), errCode, startKey, endKey);
			else
				return serializer(ar, errCode, startKey, endKey);
		}

		int errCode;
		Key startKey;
		Key endKey;

		void logEvent(std::string id, int maxFieldLength) const {
			TraceEvent("TransactionTrace_GetRangeError")
			.setMaxEventLength(-1)
			.detail("TransactionID", id)
			.detail("ErrCode", errCode)
			.setMaxFieldLength(maxFieldLength)
			.detail("StartKey", startKey)
			.detail("EndKey", endKey);
		}
	};

	struct EventCommitError : public Event {
		EventCommitError(double ts, const Optional<Standalone<StringRef>> &dcId, int err_code, const CommitTransactionRequest &commit_req) : Event(ERROR_COMMIT, ts, dcId), errCode(err_code), req(commit_req) { }
		EventCommitError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), errCode, req.transaction, req.arena);
			else
				return serializer(ar, errCode, req.transaction, req.arena);
		}

		int errCode;
		CommitTransactionRequest req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

		void logEvent(std::string id, int maxFieldLength) const {
			for (auto &read_range : req.transaction.read_conflict_ranges) {
				TraceEvent("TransactionTrace_CommitError_ReadConflictRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Begin", read_range.begin)
				.detail("End", read_range.end);
			}

			for (auto &write_range : req.transaction.write_conflict_ranges) {
				TraceEvent("TransactionTrace_CommitError_WriteConflictRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Begin", write_range.begin)
				.detail("End", write_range.end);
			}

			for (auto &mutation : req.transaction.mutations) {
				TraceEvent("TransactionTrace_CommitError_Mutation")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.setMaxFieldLength(maxFieldLength)
				.detail("Mutation", mutation.toString());
			}

			TraceEvent("TransactionTrace_CommitError")
			.detail("TransactionID", id)
			.detail("ErrCode", errCode);
		}
	};
}

#endif
