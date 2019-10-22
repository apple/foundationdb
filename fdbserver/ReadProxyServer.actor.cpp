/*
 * ReadProxyServer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ReadProxyInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "fdbrpc/genericactors.actor.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using std::pair;

// If isBackward == true, returns the shard containing the key before 'key' (an infinitely long, inexpressible key).
// Otherwise returns the shard containing key
ACTOR Future<pair<KeyRange, Reference<LocationInfo>>> getKeyLocation_internal(Database cx, Key key,
                                                                              bool isBackward = false) {
	if (isBackward) {
		ASSERT(key != allKeys.begin && key <= allKeys.end);
	} else {
		ASSERT(key < allKeys.end);
	}

	loop {
		choose {
			when(wait(cx->onMasterProxiesChanged())) {}
			when(GetKeyServerLocationsReply rep = wait(
			         loadBalance(cx->getMasterProxies(true), &MasterProxyInterface::getKeyServersLocations,
			                     GetKeyServerLocationsRequest(key, Optional<KeyRef>(), 100, isBackward, key.arena()),
			                     TaskPriority::DefaultPromiseEndpoint))) {
				ASSERT( rep.results.size() == 1 );
				auto locationInfo = cx->setCachedLocation(rep.results[0].first, rep.results[0].second);
				return std::make_pair(KeyRange(rep.results[0].first, rep.arena), locationInfo);
			}
		}
	}
}

template <class F>
Future<pair<KeyRange, Reference<LocationInfo>>> getKeyLocation(Database const& cx, Key const& key,
                                                               F StorageServerInterface::*member,
                                                               bool isBackward = false) {
	auto ssi = cx->getCachedLocation(key, isBackward);
	if (!ssi.second) {
		return getKeyLocation_internal(cx, key, isBackward);
	}

	for (int i = 0; i < ssi.second->size(); i++) {
		if (IFailureMonitor::failureMonitor().onlyEndpointFailed(ssi.second->get(i, member).getEndpoint())) {
			cx->invalidateCache(key);
			ssi.second.clear();
			return getKeyLocation_internal(cx, key, isBackward);
		}
	}

	return ssi;
}

ACTOR Future<Void> getKey(GetKeyRequest req, Database cx) {
	loop {
		try {
			state KeySelectorRef keySel = req.sel;
			state KeyRef locationKey(keySel.getKey());
			state std::pair<KeyRange, Reference<LocationInfo>> ssi =
			    wait(getKeyLocation(cx, locationKey, &StorageServerInterface::getKey, keySel.isBackward()));
			GetKeyReply reply = wait(loadBalance(
			    ssi.second, &StorageServerInterface::getKey, GetKeyRequest(keySel, req.version),
			    TaskPriority::DefaultPromiseEndpoint, false, cx->enableLocalityLoadBalance ? &cx->queueModel : NULL));
			req.reply.send(reply);
			break;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache(keySel.getKey(), keySel.isBackward());

				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DefaultEndpoint));
			} else if (e.code() != error_code_actor_cancelled) {
				req.reply.sendError(e);
				break;
			} else {
				break;
			}
		}
	}

	return Void();
}

ACTOR Future<Void> getValue(GetValueRequest req, Database cx) {
	state Key key = req.key;
	state Version ver = req.version;

	loop {
		state Optional<UID> getValueID = Optional<UID>();
		state uint64_t startTime;
		state double startTimeD;
		try {
			++cx->getValueSubmitted;
			startTime = timer_int();
			startTimeD = now();
			++cx->transactionPhysicalReads;
			if (CLIENT_BUGGIFY) {
				throw deterministicRandom()->randomChoice(
				    std::vector<Error>{ transaction_too_old(), future_version() });
			}

			pair<KeyRange, Reference<LocationInfo>> ssi =
			    wait(getKeyLocation(cx, key, &StorageServerInterface::getValue));
			GetValueReply reply = wait(loadBalance(
			    ssi.second, &StorageServerInterface::getValue, GetValueRequest(key, ver, getValueID),
			    TaskPriority::DefaultPromiseEndpoint, false, cx->enableLocalityLoadBalance ? &cx->queueModel : NULL));

			double latency = now() - startTimeD;
			cx->readLatencies.addSample(latency);
			cx->getValueCompleted->latency = timer_int() - startTime;
			cx->getValueCompleted->log();

			req.reply.send(reply);
			return Void();
		} catch (Error& e) {
			cx->getValueCompleted->latency = timer_int() - startTime;
			cx->getValueCompleted->log();
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
			    (e.code() == error_code_transaction_too_old && ver == latestVersion)) {
				cx->invalidateCache(key);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::DefaultOnMainThread));
			} else if (e.code() == error_code_actor_cancelled) {
				req.reply.sendError(transaction_too_old());
				throw e;
			} else {
				req.reply.sendError(e);
				break;
			}
		}
	}

	return Void();
}

static void transformRangeLimits(GetRangeLimits limits, bool reverse, GetKeyValuesRequest& req) {
	if (limits.bytes != 0) {
		if (!limits.hasRowLimit())
			req.limit = CLIENT_KNOBS->REPLY_BYTE_LIMIT; // Can't get more than this many rows anyway
		else
			req.limit = std::min(CLIENT_KNOBS->REPLY_BYTE_LIMIT, limits.rows);

		if (reverse) req.limit *= -1;

		if (!limits.hasByteLimit())
			req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
		else
			req.limitBytes = std::min(CLIENT_KNOBS->REPLY_BYTE_LIMIT, limits.bytes);
	} else {
		req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
		req.limit = reverse ? -limits.minRows : limits.minRows;
	}
}

ACTOR Future<vector<pair<KeyRange, Reference<LocationInfo>>>> getKeyRangeLocations_internal(Database cx, KeyRange keys,
                                                                                            int limit, bool reverse) {
	loop {
		choose {
			when(wait(cx->onMasterProxiesChanged())) {}
			when(GetKeyServerLocationsReply _rep = wait(loadBalance(
			         cx->getMasterProxies(false), &MasterProxyInterface::getKeyServersLocations,
			         GetKeyServerLocationsRequest(keys.begin, keys.end, limit, reverse, keys.arena()),
			         TaskPriority::DefaultPromiseEndpoint))) {
				state GetKeyServerLocationsReply rep = _rep;
				ASSERT(rep.results.size());

				state vector<pair<KeyRange, Reference<LocationInfo>>> results;
				state int shard = 0;
				for (; shard < rep.results.size(); shard++) {
					// FIXME: these shards are being inserted into the map sequentially, it would be much more CPU
					// efficient to save the map pairs and insert them all at once.
					results.emplace_back(rep.results[shard].first & keys,
					                     cx->setCachedLocation(rep.results[shard].first, rep.results[shard].second));
					wait(yield());
				}

				return results;
			}
		}
	}
}

template <class F>
Future<vector<pair<KeyRange, Reference<LocationInfo>>>> getKeyRangeLocations(Database const& cx, KeyRange const& keys,
                                                                             int limit, bool reverse,
                                                                             F StorageServerInterface::*member) {
	ASSERT(!keys.empty());

	vector<pair<KeyRange, Reference<LocationInfo>>> locations;
	if (!cx->getCachedLocations(keys, locations, limit, reverse)) {
		return getKeyRangeLocations_internal(cx, keys, limit, reverse);
	}

	bool foundFailed = false;
	for (auto& it : locations) {
		bool onlyEndpointFailed = false;
		for (int i = 0; i < it.second->size(); i++) {
			if (IFailureMonitor::failureMonitor().onlyEndpointFailed(it.second->get(i, member).getEndpoint())) {
				onlyEndpointFailed = true;
				break;
			}
		}

		if (onlyEndpointFailed) {
			cx->invalidateCache(it.first.begin);
			foundFailed = true;
		}
	}

	if (foundFailed) {
		return getKeyRangeLocations_internal(cx, keys, limit, reverse);
	}

	return locations;
}

ACTOR Future<Void> getKeyValues(GetKeyValuesRequest _req, Database cx) {
	state KeySelector begin = _req.begin;
	state KeySelector end = _req.end;
	state GetRangeLimits limits(abs(_req.limit), _req.limitBytes);
	state bool reverse = _req.limit < 0;

	try {
		state Promise<std::pair<Key, Key>> conflictRange;
		GetRangeResult result = wait(
		    NativeAPI::getRange(cx, Reference<TransactionLogInfo>(), Future<Version>(_req.version), begin, end, limits,
		                        conflictRange, false, reverse, TransactionInfo(TaskPriority::DefaultOnMainThread)));
		GetKeyValuesReply finalReply;
		finalReply.version = result.version;
		finalReply.more = result.output.more;
		finalReply.data = result.output;
		finalReply.usingReadProxy = true;
		finalReply.readToBegin = result.output.readToBegin;
		finalReply.readThroughEnd = result.output.readThroughEnd;
		_req.reply.send(finalReply);
		return Void();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			_req.reply.sendError(transaction_too_old());
			throw e;
		}

		_req.reply.sendError(e);
		return Void();
	}
}

ACTOR Future<Void> watchValue(WatchValueRequest req, Database cx) {
	TransactionInfo info(TaskPriority::DefaultOnMainThread);

	state Version ver = req.version;
	cx->validateVersion(ver);
	ASSERT(ver != latestVersion);

	loop {
		state pair<KeyRange, Reference<LocationInfo>> ssi =
		    wait(getKeyLocation(cx, req.key, &StorageServerInterface::watchValue));

		try {
			state WatchValueReply resp;
			choose {
				when(WatchValueReply r = wait(loadBalance(ssi.second, &StorageServerInterface::watchValue,
				                                          WatchValueRequest(req.key, req.value, req.version, req.debugID),
				                                          TaskPriority::DefaultPromiseEndpoint))) {
					resp = r;
				}
				when(wait(cx->connectionFile ? cx->connectionFile->onChange() : Never())) { wait(Never()); }
			}

			Version v = wait(waitForCommittedVersion(cx, resp.version));

			// False if there is a master failure between getting the response and getting the committed version,
			// Dependent on SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT
			if (v - resp.version < 50000000) {
				req.reply.send(resp);
				return Void();
			}
			ver = v;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				req.reply.sendError(watch_cancelled());
				throw e;
			} else if (e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed) {
				cx->invalidateCache(req.key);
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY));
			} else if (e.code() == error_code_watch_cancelled || e.code() == error_code_process_behind) {
				TEST(e.code() ==
				     error_code_watch_cancelled); // Too many watches on the storage server, poll for changes instead
				TEST(e.code() == error_code_process_behind); // The storage servers are all behind
				wait(delay(CLIENT_KNOBS->WATCH_POLLING_TIME));
			} else if (e.code() == error_code_timed_out) { // The storage server occasionally times out watches in case
				                                           // it was cancelled
				TEST(true); // A watch timed out
				wait(delay(CLIENT_KNOBS->FUTURE_VERSION_RETRY_DELAY));
			} else {
				state Error err = e;
				req.reply.sendError(err);
				return Void();
			}
		}
	}
}

ACTOR Future<Void> readProxyServerCore(ReadProxyInterface readProxy, Reference<AsyncVar<ServerDBInfo>> serverDBInfo) {
	state Database cx = openDBOnServer(serverDBInfo, TaskPriority::DefaultEndpoint, true, true);
	state ActorCollection actors(false);
	actors.add(waitFailureServer(readProxy.waitFailure.getFuture()));

	loop choose {
		when(GetKeyRequest req = waitNext(readProxy.getKey.getFuture())) { actors.add(getKey(req, cx)); }
		when(GetValueRequest req = waitNext(readProxy.getValue.getFuture())) { actors.add(getValue(req, cx)); }
		when(GetKeyValuesRequest req = waitNext(readProxy.getKeyValues.getFuture())) {
			actors.add(getKeyValues(req, cx));
		}
		when(WatchValueRequest req = waitNext(readProxy.watchValue.getFuture())) {
			actors.add(watchValue(req, cx));
		}
		when(wait(actors.getResult())) {}
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, uint64_t recoveryCount,
                                ReadProxyInterface interface) {
	loop {
		if (db->get().recoveryCount >= recoveryCount &&
		    !std::count(db->get().client.readProxies.begin(), db->get().client.readProxies.end(), interface)) {
			TraceEvent("ReadProxyServer_Removed", interface.id());
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> readProxyServer(ReadProxyInterface proxy, InitializeReadProxyRequest req,
                                   Reference<AsyncVar<ServerDBInfo>> db) {
	TraceEvent("ReadProxyServer_Started", proxy.id());
	try {
		state Future<Void> core = readProxyServerCore(proxy, db);
		loop choose {
			when(wait(core)) { return Void(); }
			when(wait(checkRemoved(db, req.recoveryCount, proxy))) {}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed) {
			TraceEvent("ReadProxyServer_Terminated", proxy.id()).error(e, true);
			return Void();
		}
		throw;
	}
}
