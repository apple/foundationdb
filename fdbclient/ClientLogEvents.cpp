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

#include "flow/FastAlloc.h"
#include "fdbclient/ClientLogEvents.h"
#include "fdbclient/MasterProxyInterface.h"
#include "serialize.h"

namespace FdbClientLogEvents {
EventGetVersion_V2::EventGetVersion_V2(double ts, double lat, uint32_t type)
  : Event(GET_VERSION_LATENCY, ts), latency(lat) {
	if (type == GetReadVersionRequest::PRIORITY_DEFAULT) {
		priorityType = PRIORITY_DEFAULT;
	} else if (type == GetReadVersionRequest::PRIORITY_BATCH) {
		priorityType = PRIORITY_BATCH;
	} else if (type == GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE) {
		priorityType = PRIORITY_IMMEDIATE;
	} else {
		ASSERT(0);
	}
}

struct EventCommitImpl : FastAllocated<EventCommitImpl> {
	double latency;
	int numMutations;
	int commitBytes;
	CommitTransactionRequest
	    req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized
	EventCommitImpl(double latency, int numMutations, int commitBytes, const CommitTransactionRequest& req)
	  : latency(latency), numMutations(numMutations), commitBytes(commitBytes), req(req) {}
};

EventCommit::EventCommit(double ts, double lat, int mut, int bytes, const CommitTransactionRequest& commit_req)
  : Event(COMMIT_LATENCY, ts), impl(new EventCommitImpl{ lat, mut, bytes, commit_req }) {}

EventCommit::~EventCommit() {
	delete impl;
}

double EventCommit::latency() const {
	return impl->latency;
}

int EventCommit::numMutations() const {
	return impl->numMutations;
}

int EventCommit::commitBytes() const {
	return impl->commitBytes;
}

template <class Ar>
Ar& EventCommit::serialize(Ar& ar) {
	if (!ar.isDeserializing)
		return serializer(Event::serialize(ar), impl->latency, impl->numMutations, impl->commitBytes,
		                  impl->req.transaction, impl->req.arena);
	else
		return serializer(ar, impl->latency, impl->numMutations, impl->commitBytes, impl->req.transaction,
		                  impl->req.arena);
}

template BinaryReader& EventCommit::serialize(BinaryReader&);
template BinaryWriter& EventCommit::serialize(BinaryWriter&);

void EventCommit::logEvent(std::string id, int maxFieldLength) const {
	for (auto& read_range : impl->req.transaction.read_conflict_ranges) {
		TraceEvent("TransactionTrace_Commit_ReadConflictRange")
		    .setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Begin", read_range.begin)
		    .detail("End", read_range.end);
	}

	for (auto& write_range : impl->req.transaction.write_conflict_ranges) {
		TraceEvent("TransactionTrace_Commit_WriteConflictRange")
		    .setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Begin", write_range.begin)
		    .detail("End", write_range.end);
	}

	for (auto& mutation : impl->req.transaction.mutations) {
		TraceEvent("TransactionTrace_Commit_Mutation")
		    .setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Mutation", mutation.toString());
	}

	TraceEvent("TransactionTrace_Commit")
	    .detail("TransactionID", id)
	    .detail("Latency", impl->latency)
	    .detail("NumMutations", impl->numMutations)
	    .detail("CommitSizeBytes", impl->commitBytes);
}

struct EventCommitErrorImpl : FastAllocated<EventCommitErrorImpl> {
	int errCode;
	CommitTransactionRequest
	    req; // Only CommitTransactionRef and Arena object within CommitTransactionRequest is serialized
	EventCommitErrorImpl(int errCode, const CommitTransactionRequest& req) : errCode(errCode), req(req) {}
};

EventCommitError::EventCommitError(double ts, int err_code, const CommitTransactionRequest& commit_req)
  : Event(ERROR_COMMIT, ts), impl(new EventCommitErrorImpl{ err_code, commit_req }) {}

EventCommitError::~EventCommitError() {
	delete impl;
}

int EventCommitError::errCode() const {
	return impl->errCode;
}

template <class Ar>
Ar& EventCommitError::serialize(Ar& ar) {
	if (!ar.isDeserializing)
		return serializer(Event::serialize(ar), impl->errCode, impl->req.transaction, impl->req.arena);
	else
		return serializer(ar, impl->errCode, impl->req.transaction, impl->req.arena);
}

template BinaryReader& EventCommitError::serialize(BinaryReader&);
template BinaryWriter& EventCommitError::serialize(BinaryWriter&);

void EventCommitError::logEvent(std::string id, int maxFieldLength) const {
	for (auto& read_range : impl->req.transaction.read_conflict_ranges) {
		TraceEvent("TransactionTrace_CommitError_ReadConflictRange")
		    .setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Begin", read_range.begin)
		    .detail("End", read_range.end);
	}

	for (auto& write_range : impl->req.transaction.write_conflict_ranges) {
		TraceEvent("TransactionTrace_CommitError_WriteConflictRange")
		    .setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Begin", write_range.begin)
		    .detail("End", write_range.end);
	}

	for (auto& mutation : impl->req.transaction.mutations) {
		TraceEvent("TransactionTrace_CommitError_Mutation")
		    .setMaxEventLength(-1)
		    .detail("TransactionID", id)
		    .setMaxFieldLength(maxFieldLength)
		    .detail("Mutation", mutation.toString());
	}

	TraceEvent("TransactionTrace_CommitError").detail("TransactionID", id).detail("ErrCode", impl->errCode);
}

} // namespace FdbClientLogEvents
