/*
 * BlobGranuleRequest.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_BLOB_GRANULE_REQUEST_ACTOR_G_H)
#define FDBCLIENT_BLOB_GRANULE_REQUEST_ACTOR_G_H
#include "fdbclient/BlobGranuleRequest.actor.g.h"
#elif !defined(FDBCLIENT_BLOB_GRANULE_REQUEST_ACTOR_H)
#define FDBCLIENT_BLOB_GRANULE_REQUEST_ACTOR_H

#include "flow/flow.h"
#include "flow/Knobs.h"

// #include "fdbclient/NativeAPI.actor.h"
#include "flow/Arena.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/BlobWorkerInterface.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define BGR_DEBUG false

ACTOR template <class Request, bool P>
Future<Standalone<VectorRef<REPLY_TYPE(Request)>>> txnDoBlobGranuleRequests(
    Transaction* tr,
    Key* beginKey,
    Key endKey,
    Request request,
    RequestStream<Request, P> BlobWorkerInterface::*channel) {
	// TODO KNOB
	state RangeResult blobGranuleMapping = wait(krmGetRanges(
	    tr, blobGranuleMappingKeys.begin, KeyRangeRef(*beginKey, endKey), 64, GetRangeLimits::BYTE_LIMIT_UNLIMITED));

	state int i = 0;
	state std::vector<Future<ErrorOr<REPLY_TYPE(Request)>>> requests;
	state Standalone<VectorRef<REPLY_TYPE(Request)>> results;

	for (; i < blobGranuleMapping.size() - 1; i++) {
		if (!blobGranuleMapping[i].value.size()) {
			if (BGR_DEBUG) {
				fmt::print("ERROR: No valid granule data for range [{0} - {1}) \n",
				           blobGranuleMapping[i].key.printable(),
				           blobGranuleMapping[i + 1].key.printable());
			}
			// no granule for range
			throw blob_granule_transaction_too_old();
		}

		state UID workerId = decodeBlobGranuleMappingValue(blobGranuleMapping[i].value);
		if (workerId == UID()) {
			if (BGR_DEBUG) {
				fmt::print("ERROR: Invalid Blob Worker ID for range [{0} - {1}) \n",
				           blobGranuleMapping[i].key.printable(),
				           blobGranuleMapping[i + 1].key.printable());
			}
			// no worker for granule
			throw blob_granule_transaction_too_old();
		}

		if (!tr->trState->cx->blobWorker_interf.count(workerId)) {
			Optional<Value> workerInterface = wait(tr->get(blobWorkerListKeyFor(workerId)));
			// from the time the mapping was read from the db, the associated blob worker
			// could have died and so its interface wouldn't be present as part of the blobWorkerList
			// we persist in the db.
			if (workerInterface.present()) {
				tr->trState->cx->blobWorker_interf[workerId] = decodeBlobWorkerListValue(workerInterface.get());
			} else {
				if (BGR_DEBUG) {
					fmt::print("ERROR: Worker  for range [{1} - {2}) does not exist!\n",
					           workerId.toString().substr(0, 5),
					           blobGranuleMapping[i].key.printable(),
					           blobGranuleMapping[i + 1].key.printable());
				}
				// throw to force read version to increase and to retry reading mapping
				throw blob_granule_request_failed();
			}
		}

		if (BGR_DEBUG) {
			fmt::print("Requesting range [{0} - {1}) from worker {2}!\n",
			           blobGranuleMapping[i].key.printable(),
			           blobGranuleMapping[i + 1].key.printable(),
			           workerId.toString().substr(0, 5));
		}

		KeyRangeRef range(blobGranuleMapping[i].key, blobGranuleMapping[i + 1].key);
		request.reply.reset();
		request.setRange(range);
		// TODO consolidate?
		BlobWorkerInterface bwi = tr->trState->cx->blobWorker_interf[workerId];
		RequestStream<Request, P> const* stream = &(bwi.*channel);
		Future<ErrorOr<REPLY_TYPE(Request)>> response = stream->tryGetReply(request);
		requests.push_back(response);
	}
	// wait for each request. If it has an error, retry from there if it is a retriable error
	state int j = 0;
	for (; j < requests.size(); j++) {
		try {
			ErrorOr<REPLY_TYPE(Request)> result = wait(requests[j]);
			if (result.isError()) {
				throw result.getError();
			}
			results.push_back(results.arena(), result.get());
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_request_maybe_delivered ||
			    e.code() == error_code_broken_promise || e.code() == error_code_connection_failed) {
				// re-read mapping and retry from failed req
				i = j;
				break;
			} else {
				if (BGR_DEBUG) {
					fmt::print("ERROR: Error doing request for range [{0} - {1}): {2}!\n",
					           blobGranuleMapping[j].key.printable(),
					           blobGranuleMapping[j + 1].key.printable(),
					           e.name());
				}
				throw;
			}
		}
	}
	if (i < blobGranuleMapping.size() - 1) {
		// a request failed, retry from that point next time
		*beginKey = blobGranuleMapping[i].key;
		throw blob_granule_request_failed();
	} else if (blobGranuleMapping.more) {
		*beginKey = blobGranuleMapping.back().key;
		// no requests failed but there is more to read, continue reading
	} else {
		*beginKey = endKey;
	}
	return results;
}

// FIXME: port other request types to this function
ACTOR template <class Request, bool P>
Future<Standalone<VectorRef<REPLY_TYPE(Request)>>> doBlobGranuleRequests(
    Database cx,
    KeyRange range,
    Request request,
    RequestStream<Request, P> BlobWorkerInterface::*channel) {
	state Key beginKey = range.begin;
	state Key endKey = range.end;
	state Transaction tr(cx);
	state Standalone<VectorRef<REPLY_TYPE(Request)>> results;
	loop {
		if (beginKey >= endKey) {
			return results;
		}
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			// raw access for avoiding tenant check in required mode
			tr.setOption(FDBTransactionOptions::RAW_ACCESS);
			Standalone<VectorRef<REPLY_TYPE(Request)>> partialResults =
			    wait(txnDoBlobGranuleRequests(&tr, &beginKey, endKey, request, channel));
			if (!partialResults.empty()) {
				results.arena().dependsOn(partialResults.arena());
				results.append(results.arena(), partialResults.begin(), partialResults.size());
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

#include "flow/unactorcompiler.h"

#endif