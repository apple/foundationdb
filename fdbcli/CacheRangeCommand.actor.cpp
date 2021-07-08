/*
 * CacheRangeCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/SystemData.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> changeCachedRange(Reference<IDatabase> db, KeyRangeRef range, bool add) {
	state Reference<ITransaction> tr = db->createTransaction();
	state KeyRange sysRange = KeyRangeRef(storageCacheKey(range.begin), storageCacheKey(range.end));
	state KeyRange sysRangeClear = KeyRangeRef(storageCacheKey(range.begin), keyAfter(storageCacheKey(range.end)));
	state KeyRange privateRange = KeyRangeRef(cacheKeysKey(0, range.begin), cacheKeysKey(0, range.end));
	state Value trueValue = storageCacheValue(std::vector<uint16_t>{ 0 });
	state Value falseValue = storageCacheValue(std::vector<uint16_t>{});
	loop {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		try {
			tr->clear(sysRangeClear);
			tr->clear(privateRange);
			tr->addReadConflictRange(privateRange);
			// hold the returned standalone object's memory
			state ThreadFuture<RangeResult> previousFuture =
			    tr->getRange(KeyRangeRef(storageCachePrefix, sysRange.begin), 1, false, true);
			RangeResult previous = wait(safeThreadFutureToFuture(previousFuture));
			bool prevIsCached = false;
			if (!previous.empty()) {
				std::vector<uint16_t> prevVal;
				decodeStorageCacheValue(previous[0].value, prevVal);
				prevIsCached = !prevVal.empty();
			}
			if (prevIsCached && !add) {
				// we need to uncache from here
				tr->set(sysRange.begin, falseValue);
				tr->set(privateRange.begin, serverKeysFalse);
			} else if (!prevIsCached && add) {
				// we need to cache, starting from here
				tr->set(sysRange.begin, trueValue);
				tr->set(privateRange.begin, serverKeysTrue);
			}
			// hold the returned standalone object's memory
			state ThreadFuture<RangeResult> afterFuture =
			    tr->getRange(KeyRangeRef(sysRange.end, storageCacheKeys.end), 1, false, false);
			RangeResult after = wait(safeThreadFutureToFuture(afterFuture));
			bool afterIsCached = false;
			if (!after.empty()) {
				std::vector<uint16_t> afterVal;
				decodeStorageCacheValue(after[0].value, afterVal);
				afterIsCached = afterVal.empty();
			}
			if (afterIsCached && !add) {
				tr->set(sysRange.end, trueValue);
				tr->set(privateRange.end, serverKeysTrue);
			} else if (!afterIsCached && add) {
				tr->set(sysRange.end, falseValue);
				tr->set(privateRange.end, serverKeysFalse);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			TraceEvent(SevDebug, "ChangeCachedRangeError").error(e);
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace

namespace fdb_cli {

ACTOR Future<bool> cacheRangeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 4) {
		printUsage(tokens[0]);
		return false;
	} else {
		state KeyRangeRef cacheRange(tokens[2], tokens[3]);
		if (tokencmp(tokens[1], "set")) {
			wait(changeCachedRange(db, cacheRange, true));
		} else if (tokencmp(tokens[1], "clear")) {
			wait(changeCachedRange(db, cacheRange, false));
		} else {
			printUsage(tokens[0]);
			return false;
		}
	}
	return true;
}

CommandFactory cacheRangeFactory(
    "cache_range",
    CommandHelp(
        "cache_range <set|clear> <BEGINKEY> <ENDKEY>",
        "Mark a key range to add to or remove from storage caches.",
        "Use the storage caches to assist in balancing hot read shards. Set the appropriate ranges when experiencing "
        "heavy load, and clear them when they are no longer necessary."));

} // namespace fdb_cli