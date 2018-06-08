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

	struct Event {
		Event(EventType t, double ts) : type(t), startTs(ts) { }
		Event() { }

		template <typename Ar>	Ar& serialize(Ar &ar) { return ar & type & startTs; }

		EventType type{ EVENTTYPEEND };
		double startTs{ 0 };

		void logEvent(std::string id) const {}
	};

	struct EventGetVersion : public Event {
		EventGetVersion(double ts, double lat) : Event(GET_VERSION_LATENCY, ts), latency(lat) { }
		EventGetVersion() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & latency;
			else
				return ar & latency;
		}

		double latency;

		void logEvent(std::string id) const {
			TraceEvent("TransactionTrace_GetVersion").detail("TransactionID", id).detail("Latency", latency);
		}
	};

	struct EventGet : public Event {
		EventGet(double ts, double lat, int size, const KeyRef &in_key) : Event(GET_LATENCY, ts), latency(lat), valueSize(size), key(in_key) { }
		EventGet() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & latency & valueSize & key;
			else
				return ar & latency & valueSize & key;
		}

		double latency;
		int valueSize;
		Key key;

		void logEvent(std::string id) const {
			TraceEvent("TransactionTrace_Get").detail("TransactionID", id).detail("Latency", latency).detail("ValueSizeBytes", valueSize).detail("Key", printable(key));
		}
	};

	struct EventGetRange : public Event {
		EventGetRange(double ts, double lat, int size, const KeyRef &start_key, const KeyRef & end_key) : Event(GET_RANGE_LATENCY, ts), latency(lat), rangeSize(size), startKey(start_key), endKey(end_key) { }
		EventGetRange() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & latency & rangeSize & startKey & endKey;
			else
				return ar & latency & rangeSize & startKey & endKey;
		}

		double latency;
		int rangeSize;
		Key startKey;
		Key endKey;

		void logEvent(std::string id) const {
			TraceEvent("TransactionTrace_GetRange").detail("TransactionID", id).detail("Latency", latency).detail("RangeSizeBytes", rangeSize).detail("StartKey", printable(startKey)).detail("EndKey", printable(endKey));
		}
	};

	struct EventCommit : public Event {
		EventCommit(double ts, double lat, int mut, int bytes, const CommitTransactionRequest &commit_req) : Event(COMMIT_LATENCY, ts), latency(lat), numMutations(mut), commitBytes(bytes), req(commit_req) { }
		EventCommit() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & latency & numMutations & commitBytes & req.transaction & req.arena;
			else
				return ar & latency & numMutations & commitBytes & req.transaction & req.arena;
		}

		double latency;
		int numMutations;
		int commitBytes;
		CommitTransactionRequest req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

		void logEvent(std::string id) const {
			for (auto &read_range : req.transaction.read_conflict_ranges) {
				TraceEvent("TransactionTrace_Commit_ReadConflictRange").detail("TransactionID", id).detail("Begin", printable(read_range.begin)).detail("End", printable(read_range.end));
			}

			for (auto &write_range : req.transaction.write_conflict_ranges) {
				TraceEvent("TransactionTrace_Commit_WriteConflictRange").detail("TransactionID", id).detail("Begin", printable(write_range.begin)).detail("End", printable(write_range.end));
			}

			for (auto &mutation : req.transaction.mutations) {
				TraceEvent("TransactionTrace_Commit_Mutation").detail("TransactionID", id).detail("Mutation", mutation.toString());
			}

			TraceEvent("TransactionTrace_Commit").detail("TransactionID", id).detail("Latency", latency).detail("NumMutations", numMutations).detail("CommitSizeBytes", commitBytes);
		}
	};

	struct EventGetError : public Event {
		EventGetError(double ts, int err_code, const KeyRef &in_key) : Event(ERROR_GET, ts), errCode(err_code), key(in_key) { }
		EventGetError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & errCode & key;
			else
				return ar & errCode & key;
		}

		int errCode;
		Key key;

		void logEvent(std::string id) const {
			TraceEvent("TransactionTrace_GetError").detail("TransactionID", id).detail("ErrCode", errCode).detail("Key", printable(key));
		}
	};

	struct EventGetRangeError : public Event {
		EventGetRangeError(double ts, int err_code, const KeyRef &start_key, const KeyRef & end_key) : Event(ERROR_GET_RANGE, ts), errCode(err_code), startKey(start_key), endKey(end_key) { }
		EventGetRangeError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & errCode & startKey & endKey;
			else
				return ar & errCode & startKey & endKey;
		}

		int errCode;
		Key startKey;
		Key endKey;

		void logEvent(std::string id) const {
			TraceEvent("TransactionTrace_GetRangeError").detail("TransactionID", id).detail("ErrCode", errCode).detail("StartKey", printable(startKey)).detail("EndKey", printable(endKey));
		}
	};

	struct EventCommitError : public Event {
		EventCommitError(double ts, int err_code, const CommitTransactionRequest &commit_req) : Event(ERROR_COMMIT, ts), errCode(err_code), req(commit_req) { }
		EventCommitError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return Event::serialize(ar) & errCode & req.transaction & req.arena;
			else
				return ar & errCode & req.transaction & req.arena;
		}

		int errCode;
		CommitTransactionRequest req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

		void logEvent(std::string id) const {
			for (auto &read_range : req.transaction.read_conflict_ranges) {
				TraceEvent("TransactionTrace_CommitError_ReadConflictRange").detail("TransactionID", id).detail("Begin", printable(read_range.begin)).detail("End", printable(read_range.end));
			}

			for (auto &write_range : req.transaction.write_conflict_ranges) {
				TraceEvent("TransactionTrace_CommitError_WriteConflictRange").detail("TransactionID", id).detail("Begin", printable(write_range.begin)).detail("End", printable(write_range.end));
			}

			for (auto &mutation : req.transaction.mutations) {
				TraceEvent("TransactionTrace_CommitError_Mutation").detail("TransactionID", id).detail("Mutation", mutation.toString());
			}

			TraceEvent("TransactionTrace_CommitError").detail("TransactionID", id).detail("ErrCode", errCode);
		}
	};
}

#endif
