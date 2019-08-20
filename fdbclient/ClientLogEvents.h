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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/JSONDoc.h"

#include <algorithm>

struct RequestStats {
	RequestStats() : nextReadId(0) {}

	struct ReadStats {
		ReadStats(int readId, int bytesFetched, int keysFetched, Key beginKey, Key endKey, NetworkAddress storageContacted, double latency) :
			readId(readId), bytesFetched(bytesFetched), keysFetched(keysFetched), beginKey(beginKey), endKey(endKey), storageContacted(storageContacted), latency(latency) {}

		uint64_t readId;
		uint64_t bytesFetched;
		uint64_t keysFetched;
		Key beginKey;
		Key endKey;
		NetworkAddress storageContacted;
		double latency;
	};

	std::vector<ReadStats> reads;
	std::vector<NetworkAddress> proxies;

	int getNextReadId() { return nextReadId++; };

	json_spirit::Object getJson() const {
		json_spirit::Array proxiesContacted;
		for (const auto &p : proxies) {
			proxiesContacted.push_back(json_spirit::Value(p.toString()));
		}
		json_spirit::Array readStatsList;
		for (const auto &r : reads) {
			json_spirit::Object readStats;
			readStats.push_back(json_spirit::Pair("readId", json_spirit::Value(r.readId)));
			readStats.push_back(json_spirit::Pair("beginKey", json_spirit::Value(r.beginKey.toString())));
			readStats.push_back(json_spirit::Pair("endKey", json_spirit::Value(r.endKey.toString())));
			readStats.push_back(json_spirit::Pair("storageContacted", json_spirit::Value(r.storageContacted.toString())));
			readStats.push_back(json_spirit::Pair("keysFetched", json_spirit::Value(r.keysFetched)));
			readStats.push_back(json_spirit::Pair("bytesFetched", json_spirit::Value(r.bytesFetched)));
			readStats.push_back(json_spirit::Pair("latency", json_spirit::Value(r.latency)));
			readStatsList.push_back(readStats);
		}
		json_spirit::Object result;
		result.push_back(json_spirit::Pair("readStatistics", json_spirit::Value(readStatsList)));
		result.push_back(json_spirit::Pair("proxiesContacted", json_spirit::Value(proxiesContacted)));
		return std::move(result);
	}
private:
	int nextReadId;
};

namespace FdbClientLogEvents {
	typedef int EventType;
    enum {
	    GET_VERSION_LATENCY = 0,
	    GET_VALUE = 1,
	    GET_RANGE = 2,
	    COMMIT_LATENCY = 3,
	    ERROR_GET = 4,
	    ERROR_GET_RANGE = 5,
	    ERROR_COMMIT = 6,
	    GET_KEY = 7,
	    GET_SUBRANGE = 8,
	    CONTACTED_PROXY = 9,

	    EVENTTYPEEND // End of EventType
    };

    typedef int TrasactionPriorityType;
    enum {
		PRIORITY_DEFAULT   = 0,
		PRIORITY_BATCH     = 1,
		PRIORITY_IMMEDIATE = 2,
		PRIORITY_END
	};

	struct Event {
		Event(EventType t, double ts) : type(t), startTs(ts) { }
		Event() { }

		template <typename Ar>	Ar& serialize(Ar &ar) { return serializer(ar, type, startTs); }

		EventType type{ EVENTTYPEEND };
		double startTs{ 0 };

		virtual void logEvent(std::string id, int maxFieldLength) const {}
		virtual void addToReqStats(RequestStats &reqStats) const {}
	};

	struct EventGetVersion : public Event {
		EventGetVersion(double ts, double lat) : Event(GET_VERSION_LATENCY, ts), latency(lat) { }
		EventGetVersion() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency);
			else
				return serializer(ar, latency);
		}

		double latency;

		void logEvent(std::string id, int maxFieldLength) const override {
			TraceEvent("TransactionTrace_GetVersion")
				.detail("TransactionID", id)
				.detail("Latency", latency);
		}
	};

	// Version V2 of EventGetVersion starting at 6.2
	struct EventGetVersion_V2 : public Event {
		EventGetVersion_V2(double ts, double lat, uint32_t type) : Event(GET_VERSION_LATENCY, ts), latency(lat) {
			if(type == GetReadVersionRequest::PRIORITY_DEFAULT) {
				priorityType = PRIORITY_DEFAULT;
			} else if (type == GetReadVersionRequest::PRIORITY_BATCH) {
				priorityType = PRIORITY_BATCH;
			} else if (type == GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE){
				priorityType = PRIORITY_IMMEDIATE;
			} else {
				ASSERT(0);
			}
		 }
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

	struct EventGetValue : public Event {
	    EventGetValue(double ts, double lat, int size, const KeyRef& in_key)
	      : Event(GET_VALUE, ts), latency(lat), valueSize(size), key(in_key) {}
	    EventGetValue() {}

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

	    void logEvent(std::string id, int maxFieldLength) const override {
		    TraceEvent("TransactionTrace_Get")
		        .setMaxEventLength(-1)
		        .detail("TransactionID", id)
		        .detail("Latency", latency)
		        .detail("ValueSizeBytes", valueSize)
		        .setMaxFieldLength(maxFieldLength)
		        .detail("Key", key);
	    }
    };

    struct EventGetValue_V3 : public Event {
	    EventGetValue_V3(double ts, int readId, double lat, int size, const KeyRef& in_key,
	                     NetworkAddress storageContacted)
	      : Event(GET_VALUE, ts), readId(readId), latency(lat), valueSize(size), key(in_key),
	        storageContacted(storageContacted) {}
	    EventGetValue_V3() {}

	    template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), readId, latency, valueSize, key, storageContacted);
			else
				return serializer(ar, readId, latency, valueSize, key, storageContacted);
		}

		int readId;
		double latency;
		int valueSize;
		Key key;
		NetworkAddress storageContacted;

		void logEvent(std::string id, int maxFieldLength) const override {
			TraceEvent("TransactionTrace_GetValue")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.detail("ReadID", readId)
				.detail("Latency", latency)
				.detail("ValueSizeBytes", valueSize)
				.detail("StorageContacted", storageContacted)
				.setMaxFieldLength(maxFieldLength)
				.detail("Key", printable(key));
		}

		void addToReqStats(RequestStats &reqStats) const override {
			reqStats.reads.push_back(
				RequestStats::ReadStats {
					readId,
					valueSize,
					(valueSize == 0) ? 0 : 1,
					key,
					key,
					storageContacted,
					latency
				}
			);
		}
    };

    struct EventGetKey : public Event {
	    EventGetKey(double ts, int readId, double latency, Key key, NetworkAddress storageContacted) :
			Event(GET_KEY, ts), readId(readId), latency(latency), key(key), storageContacted(storageContacted) {}
	    EventGetKey() {}

		template <typename Ar> Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), latency, readId, key, storageContacted);
			else
				return serializer(ar, readId, latency, key, storageContacted);
		}

		int readId;
		double latency;
		Key key;
		NetworkAddress storageContacted;

		void logEvent(std::string id, int maxFieldLength) const override {
			TraceEvent("TransactionTrace_GetKey")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.detail("ReadId", readId)
				.detail("Latency", latency)
				.detail("BytesFetched", key.size())
				.detail("StorageContacted", storageContacted)
				.setMaxFieldLength(maxFieldLength)
				.detail("Key", key);
		}

		void addToReqStats(RequestStats &reqStats) const override {
			reqStats.reads.push_back(
				RequestStats::ReadStats {
					readId,
					key.size(),
					1,
					key,
					key,
					storageContacted,
					latency
				}
			);
		}
    };

    struct EventGetRange : public Event {
		EventGetRange(double ts, double lat, int size, const KeyRef &start_key, const KeyRef & end_key) : Event(GET_RANGE, ts), latency(lat), rangeSize(size), startKey(start_key), endKey(end_key) { }
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

		void logEvent(std::string id, int maxFieldLength) const override {
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

	struct EventGetSubRange : public Event {
		EventGetSubRange(double ts, int readId, double latency, int bytesFetched, int keysFetched, Key beginKey, Key endKey, NetworkAddress storageContacted) :
			Event(GET_SUBRANGE, ts), readId(readId), latency(latency), bytesFetched(bytesFetched), keysFetched(keysFetched), beginKey(beginKey), endKey(endKey), storageContacted(storageContacted) {}
		EventGetSubRange() {}

		template <typename Ar> Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), readId, latency, bytesFetched, keysFetched, beginKey, endKey, storageContacted);
			else
				return serializer(ar, readId, latency, bytesFetched, keysFetched, beginKey, endKey, storageContacted);
		}

		int readId;
		double latency;
		int bytesFetched;
		int keysFetched;
		Key beginKey;
		Key endKey;
		NetworkAddress storageContacted;

		void logEvent(std::string id, int maxFieldLength) const override {
			TraceEvent("TransactionTrace_GetSubRange")
				.setMaxEventLength(-1)
				.detail("TransactionID", id)
				.detail("ReadId", readId)
				.detail("Latency", latency)
				.detail("BytesFetched", bytesFetched)
				.detail("KeysFetched", keysFetched)
				.detail("StorageContacted", storageContacted)
				.setMaxFieldLength(maxFieldLength)
				.detail("BeginKey", beginKey)
				.detail("EndKey", endKey);
		}

		void addToReqStats(RequestStats &reqStats) const override {
			reqStats.reads.push_back(
				RequestStats::ReadStats {
					readId,
					bytesFetched,
					keysFetched,
					beginKey,
					endKey,
					storageContacted,
					latency
				}
			);
		}
	};

	struct EventCommit : public Event {
		EventCommit(double ts, double lat, int mut, int bytes, const CommitTransactionRequest &commit_req) : Event(COMMIT_LATENCY, ts), latency(lat), numMutations(mut), commitBytes(bytes), req(commit_req) { }
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

		void logEvent(std::string id, int maxFieldLength) const override {
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

	struct EventGetError : public Event {
		EventGetError(double ts, int err_code, const KeyRef &in_key) : Event(ERROR_GET, ts), errCode(err_code), key(in_key) { }
		EventGetError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), errCode, key);
			else
				return serializer(ar, errCode, key);
		}

		int errCode;
		Key key;

		void logEvent(std::string id, int maxFieldLength) const override {
			TraceEvent("TransactionTrace_GetError")
			.setMaxEventLength(-1)
			.detail("TransactionID", id)
			.detail("ErrCode", errCode)
			.setMaxFieldLength(maxFieldLength)
			.detail("Key", key);
		}
	};

	struct EventGetRangeError : public Event {
		EventGetRangeError(double ts, int err_code, const KeyRef &start_key, const KeyRef & end_key) : Event(ERROR_GET_RANGE, ts), errCode(err_code), startKey(start_key), endKey(end_key) { }
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

		void logEvent(std::string id, int maxFieldLength) const override {
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
		EventCommitError(double ts, int err_code, const CommitTransactionRequest &commit_req) : Event(ERROR_COMMIT, ts), errCode(err_code), req(commit_req) { }
		EventCommitError() { }

		template <typename Ar>	Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), errCode, req.transaction, req.arena);
			else
				return serializer(ar, errCode, req.transaction, req.arena);
		}

		int errCode;
		CommitTransactionRequest req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized

		void logEvent(std::string id, int maxFieldLength) const override {
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

	struct EventContactedProxy : public Event {
		EventContactedProxy(double ts, NetworkAddress proxyContacted) :
			Event(CONTACTED_PROXY, ts),
			proxyContacted(proxyContacted) {}
	    EventContactedProxy() {}

		NetworkAddress proxyContacted;

		template <typename Ar> Ar& serialize(Ar &ar) {
			if (!ar.isDeserializing)
				return serializer(Event::serialize(ar), proxyContacted);
			else
				return serializer(ar, proxyContacted);
		}

		void logEvent(std::string id, int maxFieldLength) const override {
			TraceEvent("TransactionTrace_ContactedProxy")
				.detail("TransactionID", id)
				.detail("ProxyContacted", proxyContacted);
		}

		void addToReqStats(RequestStats &reqStats) const override {
			if (std::find(reqStats.proxies.begin(), reqStats.proxies.end(), proxyContacted) == reqStats.proxies.end())
				reqStats.proxies.push_back(proxyContacted);
		}
	};
}

#endif
