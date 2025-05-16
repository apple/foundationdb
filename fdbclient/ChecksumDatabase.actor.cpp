/*
 * ChecksumDatabase.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

// Standard Library
#include <fmt/format.h> // For fmt::format
#include <limits> // For std::numeric_limits
#include <vector> // For std::vector
#include <algorithm> // For std::sort, std::unique

// Flow Headers (excluding actorcompiler.h)
#include "flow/flow.h" // Basic Flow types
#include "flow/xxhash.h" // For XXH64_state_t and functions
#include "flow/genericactors.actor.h" // For FlowLock and other generic actors

// FDBClient Headers (excluding actorcompiler.h and self .actor.h for now)
#include "fdbclient/NativeAPI.actor.h" // For Database, Key, KeyRange, etc.
#include "fdbclient/ReadYourWrites.h" // For ReadYourWritesTransaction
#include "fdbclient/ClientKnobs.h" // For CLIENT_KNOBS
#include "fdbclient/SystemData.h" // For allKeys, normalKeys
// #include "fdbclient/TenantAttentionRouter.h" // Reverted path - Now commented out
#include "fdbclient/MonitorLeader.h"

// Own .actor.h file
#include "fdbclient/ChecksumDatabase.actor.h"

// ACTOR COMPILER - MUST BE LAST
#include "flow/actorcompiler.h"

// The fdbclient/ChecksumDatabase.actor.h should have already included its .actor.g.h
// so we don't need to include it explicitly here.

namespace fdb {

// Struct to hold the result of checksumming a single shard
struct ShardResult {
	uint64_t shardChecksum;
	int64_t shardTotalKeys;
	int64_t shardTotalBytes;
	Key shardBeginKey; // For potential logging/association

	ShardResult() : shardChecksum(0), shardTotalKeys(0), shardTotalBytes(0) {}
};

// Actor to retrieve shard boundaries for a given key range
// TODO: Generalize... This is like fdbclient/AuditUtils.actor.cpp getShardMapFromKeyServers.
ACTOR Future<Standalone<VectorRef<KeyRef>>> getShardBoundaries(Database cx, KeyRangeRef rangeToScanBoundariesFor) {
	state std::vector<Key> tempBoundaries; // Use std::vector for easy manipulation
	state Key currentBoundaryScanBegin = rangeToScanBoundariesFor.begin;

	// Add the beginning of the overall range as the first boundary.
	tempBoundaries.push_back(rangeToScanBoundariesFor.begin);

	state ReadYourWritesTransaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // Recommended for system key reads

			// Construct the range to query within keyServersPrefix
			std::string beginScanKeyStr = keyServersPrefix.toString() + currentBoundaryScanBegin.toString();
			Key beginScanKey =
			    Key(StringRef(reinterpret_cast<const uint8_t*>(beginScanKeyStr.data()), beginScanKeyStr.size()));
			std::string endScanKeyStr = keyServersPrefix.toString() + rangeToScanBoundariesFor.end.toString();
			Key endScanKey =
			    Key(StringRef(reinterpret_cast<const uint8_t*>(endScanKeyStr.data()), endScanKeyStr.size()));
			KeyRangeRef scanRange = KeyRangeRef(beginScanKey, endScanKey);

			TraceEvent(SevDebug, "GetShardBoundariesScanRange")
			    .detail("Begin", scanRange.begin.printable())
			    .detail("End", scanRange.end.printable());

			RangeResult results = wait(
			    tr.getRange(scanRange, CLIENT_KNOBS->TOO_MANY)); // TOO_MANY should be fine for typical number of shards

			for (auto const& kv : results) {
				Key boundaryKey = kv.key.removePrefix(keyServersPrefix);
				// Avoid duplicate entries if currentBoundaryScanBegin was already a boundary
				if (tempBoundaries.empty() || tempBoundaries.back() != boundaryKey) {
					tempBoundaries.push_back(boundaryKey);
				}
			}

			if (!results.more || results.empty()) {
				break; // All boundaries in the range fetched
			}

			// Prepare for the next scan if there's more data
			currentBoundaryScanBegin = keyAfter(results.back().key.removePrefix(keyServersPrefix));
			if (currentBoundaryScanBegin >= rangeToScanBoundariesFor.end) {
				break;
			}
			tr.reset(); // Reset for the next iteration if needed, though getRange might do this
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent(SevWarn, "GetShardBoundariesError").error(e);
			wait(tr.onError(e));
		}
	}

	// Add the end of the overall range as the last boundary, if not already present.
	if (tempBoundaries.empty() || tempBoundaries.back() != rangeToScanBoundariesFor.end) {
		tempBoundaries.push_back(rangeToScanBoundariesFor.end);
	}

	// Sort and unique
	std::sort(tempBoundaries.begin(), tempBoundaries.end());
	tempBoundaries.erase(std::unique(tempBoundaries.begin(), tempBoundaries.end()), tempBoundaries.end());

	// Convert std::vector<Key> to Standalone<VectorRef<KeyRef>>
	state Standalone<VectorRef<KeyRef>> boundaries;
	for (const auto& k : tempBoundaries) {
		boundaries.push_back_deep(boundaries.arena(), k);
	}

	TraceEvent(SevInfo, "GetShardBoundariesResult").detail("NumBoundaries", boundaries.size());
	// .detail("Boundaries", boundaries); // Potentially too verbose for default logging

	return boundaries;
}

// Actor to calculate the checksum for a single shard or a part of a shard
ACTOR Future<ShardResult> calculateSingleShardChecksumActor(Database cx, KeyRangeRef shardRangeToProcess) {
	state ShardResult result;
	result.shardBeginKey = shardRangeToProcess.begin;
	state XXH64_state_t xxhStateShardLocal;
	XXH64_reset(&xxhStateShardLocal, 0);

	state Key currentProcessingKey = shardRangeToProcess.begin;
	state int64_t maxBytesPerTransactionAttempt = 10 * 1024 * 1024; // 10 MB example limit, same as outer actor

	// Loop for processing the given shardRangeToProcess, potentially in multiple transactions if byte limit is hit
	loop {
		if (currentProcessingKey >= shardRangeToProcess.end) {
			break; // Entire shard range processed
		}

		state ReadYourWritesTransaction tr(cx);
		state uint64_t txAttemptBytes = 0;
		state uint64_t txAttemptKeys = 0;
		state Key lastKeyReadInTx;

		// Transaction retry loop for the current part of the shard
		loop {
			try {
				txAttemptBytes = 0;
				txAttemptKeys = 0;
				lastKeyReadInTx = currentProcessingKey;

				state KeySelector flussoBegin = firstGreaterOrEqual(currentProcessingKey);
				state KeySelector flussoEnd = firstGreaterOrEqual(shardRangeToProcess.end);
				state bool moreDataInThisTx = true;
				state int64_t bytesReadThisTxAccumulator = 0;

				TraceEvent(SevDebug, "SingleShardActorTxBegin")
				    .detail("ShardRangeBegin", shardRangeToProcess.begin.printable())
				    .detail("ShardRangeEnd", shardRangeToProcess.end.printable())
				    .detail("CurrentProcessingKey", currentProcessingKey.printable());

				// Innermost getRange chunking loop for this transaction
				loop {
					if (!moreDataInThisTx)
						break;
					if (flussoBegin.getKey() >= shardRangeToProcess.end && flussoBegin.offset == 1) {
						moreDataInThisTx = false;
						break;
					}
					if (bytesReadThisTxAccumulator >= maxBytesPerTransactionAttempt && txAttemptKeys > 0) {
						TraceEvent(SevInfo, "SingleShardActorTxByteLimit")
						    .detail("BytesRead", bytesReadThisTxAccumulator)
						    .detail("MaxBytes", maxBytesPerTransactionAttempt);
						break;
					}

					state Standalone<RangeResultRef> chunkReadResult = wait(tr.getRange(
					    flussoBegin,
					    flussoEnd,
					    GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, CLIENT_KNOBS->REPLY_BYTE_LIMIT)));

					TraceEvent(SevDebug, "SingleShardActorGetRangeResult")
					    .detail("ChunkSize", chunkReadResult.size())
					    .detail("ChunkMore", chunkReadResult.more);

					if (chunkReadResult.empty()) {
						moreDataInThisTx = false;
					} else {
						for (const auto& kv : chunkReadResult) {
							XXH64_update(&xxhStateShardLocal, kv.key.begin(), kv.key.size());
							XXH64_update(&xxhStateShardLocal, kv.value.begin(), kv.value.size());
							uint64_t kvBytes = kv.key.size() + kv.value.size();
							txAttemptBytes += kvBytes;
							bytesReadThisTxAccumulator += kvBytes;
							txAttemptKeys++;
							lastKeyReadInTx = kv.key;
						}
						if (!chunkReadResult.more)
							moreDataInThisTx = false;
						flussoBegin = firstGreaterThan(chunkReadResult.back().key);
					}
				} // End of getRange chunking loop

				result.shardTotalBytes += txAttemptBytes;
				result.shardTotalKeys += txAttemptKeys;

				TraceEvent(SevDebug, "SingleShardActorTxSuccess")
				    .detail("OriginalShardBegin", shardRangeToProcess.begin.printable())
				    .detail("ProcessedUpTo", lastKeyReadInTx.printable())
				    .detail("KeysInTx", txAttemptKeys)
				    .detail("BytesInTx", txAttemptBytes);

				if (txAttemptKeys == 0) {
					currentProcessingKey = shardRangeToProcess.end; // No keys processed, advance to end of shard
				} else {
					currentProcessingKey = keyAfter(lastKeyReadInTx);
				}
				tr.reset();
				break; // Successfully processed this transaction for a part of the shard
			} catch (Error& e) {
				TraceEvent(SevWarn, "SingleShardActorTxError").error(e);
				if (e.code() == error_code_actor_cancelled)
					throw;
				wait(tr.onError(e));
			}
		} // End of transaction retry loop
	} // End of loop for processing current shard range

	result.shardChecksum = XXH64_digest(&xxhStateShardLocal);
	TraceEvent(SevInfo, "SingleShardActorCompleted")
	    .detail("ShardBeginKey", result.shardBeginKey.printable())
	    .detail("ShardEndKey", shardRangeToProcess.end.printable())
	    .detail("ShardChecksum", result.shardChecksum)
	    .detail("ShardTotalKeys", result.shardTotalKeys)
	    .detail("ShardTotalBytes", result.shardTotalBytes);
	return result;
}

// Helper actor that acquires a FlowLock, runs a single shard checksum, and returns its result.
ACTOR Future<ShardResult> launchSingleShardProcessingWithLock(Database cx,
                                                              KeyRangeRef shardRange,
                                                              FlowLock* concurrencyLock) {
	wait(concurrencyLock->take());
	state FlowLock::Releaser releaser(*concurrencyLock);

	// TODO: Consider adding a try-catch here to log errors from calculateSingleShardChecksumActor
	// and potentially return a default/error ShardResult or rethrow. For now, assume
	// calculateSingleShardChecksumActor handles its own errors or throws, and waitForAll will catch it.
	ShardResult result = wait(calculateSingleShardChecksumActor(cx, shardRange));
	return result;
}

// Actor to calculate the checksum of the database or a key range.
ACTOR Future<ChecksumResult> calculateDatabaseChecksum(Database cx, Optional<KeyRange> range) {
	state ChecksumResult finalResult;

	state KeyRangeRef overallRangeToProcess = range.present() ? range.get() : normalKeys;

	TraceEvent(SevInfo, "ChecksumEffectiveOverallRange")
	    .detail("Begin", overallRangeToProcess.begin.printable())
	    .detail("End", overallRangeToProcess.end.printable());

	state Standalone<VectorRef<KeyRef>> shardBoundaries = wait(getShardBoundaries(cx, overallRangeToProcess));

	if (shardBoundaries.empty() || shardBoundaries.front() != overallRangeToProcess.begin ||
	    shardBoundaries.back() != overallRangeToProcess.end) {
		TraceEvent(SevWarn, "ChecksumInvalidBoundaries")
		    .detail("Reason", "Boundaries from getShardBoundaries do not properly cover overallRangeToProcess");
		if (overallRangeToProcess.empty()) {
			// Empty overall range is fine, result will be 0/0/0
			return finalResult; // Default 0s
		}
		// For non-empty overall range but bad boundaries, this is problematic.
		// Consider throwing an error here. For now, will likely result in 0 processed.
		// Or, if only one boundary (begin==end), it might also be fine.
		if (shardBoundaries.size() < 2 && !overallRangeToProcess.empty()) {
			// This implies we can't even form one shard range.
			// Throw an error or return an empty/error result.
			// For now, returning default (likely 0s) to avoid crash, but this needs thought.
			return finalResult;
		}
	}

	if (overallRangeToProcess.empty()) {
		TraceEvent(SevInfo, "ChecksumSkippingEmptyOverallRange");
		return finalResult; // Already initialized to 0s
	}

	// If shardBoundaries only contains begin and end, and they are the same (empty range),
	// getShardBoundaries should ideally return just [begin, end].
	// If overallRangeToProcess is not empty, but shardBoundaries.size() < 2, it's an issue.
	if (shardBoundaries.size() < 2) {
		TraceEvent(SevWarn, "ChecksumNotEnoughBoundariesForNonEmptyRange")
		    .detail("NumBoundaries", shardBoundaries.size())
		    .detail("OverallRangeBegin", overallRangeToProcess.begin.printable())
		    .detail("OverallRangeEnd", overallRangeToProcess.end.printable());
		// This could happen if overallRangeToProcess.begin == overallRangeToProcess.end but it wasn't caught by
		// overallRangeToProcess.empty() Or if getShardBoundaries has an issue.
		return finalResult; // Return 0s
	}

	state std::vector<Future<ShardResult>> allShardFutures;
	state Reference<FlowLock> concurrencyLimiter =
	    makeReference<FlowLock>(CLIENT_KNOBS->CHECKSUM_MAX_CONCURRENT_SHARDS);

	for (int i = 0; i < shardBoundaries.size() - 1; ++i) {
		Key shardBegin = shardBoundaries[i];
		Key shardEnd = shardBoundaries[i + 1];

		if (shardBegin >= shardEnd) {
			TraceEvent(SevWarn, "ChecksumSkippingEmptyShardRangeInLoop")
			    .detail("ShardBegin", shardBegin.printable())
			    .detail("ShardEnd", shardEnd.printable())
			    .detail("BoundaryIndex", i);
			continue;
		}
		KeyRangeRef currentShardRange = KeyRangeRef(shardBegin, shardEnd);
		allShardFutures.push_back(
		    launchSingleShardProcessingWithLock(cx, currentShardRange, concurrencyLimiter.getPtr()));
	}

	if (allShardFutures.empty() && !overallRangeToProcess.empty()) {
		// This case implies that even though overallRangeToProcess was non-empty,
		// and we had at least 2 boundaries, no valid shard ranges were formed.
		// This could happen if all shard ranges were skipped (e.g. all begin >= end).
		// This is unusual if getShardBoundaries and overallRangeToProcess are consistent.
		TraceEvent(SevWarn, "ChecksumNoShardFuturesGeneratedForNonEmptyRange")
		    .detail("OverallRangeBegin", overallRangeToProcess.begin.printable())
		    .detail("OverallRangeEnd", overallRangeToProcess.end.printable())
		    .detail("NumBoundaries", shardBoundaries.size());
		// Return current finalResult which is 0s.
	}

	wait(waitForAll(allShardFutures));

	finalResult.checksum = 0; // Initialize for XOR
	for (const auto& fut : allShardFutures) {
		if (fut.isReady() && !fut.isError()) {
			ShardResult shardRes = fut.get();
			finalResult.checksum ^= shardRes.shardChecksum;
			finalResult.totalKeys += shardRes.shardTotalKeys;
			finalResult.totalBytes += shardRes.shardTotalBytes;
		} else if (fut.isError()) {
			// How to handle errors from one of the shards?
			// Option 1: Propagate the first error and stop.
			// Option 2: Log error, skip its result, and checksum the rest (partial checksum).
			// Option 3: Collect all errors and report them.
			// For now, propagating the error from waitForAll or the first future.get() that throws.
			// waitForAll itself might throw if one future has an error not caught by isError().
			// Let's assume fut.get() will throw if there was an error.
			// This explicit check helps if we want custom error aggregation later.
			// For now, if waitForAll passed, then all futures should be ready and not in an error state
			// that wasn't handled by the single shard actor's try-catch.
			// However, an actor_cancelled could still come through.
			// The try-catch in calculateSingleShardChecksumActor should handle most things and return a ShardResult.
			// If an error still propagates here, it's likely serious.
			// Re-throwing the error from future.get() if it's an error.
			// Ensure that if fut.get() throws, it stops processing.
			try {
				ShardResult shardRes = fut.get(); // Can throw
				finalResult.checksum ^= shardRes.shardChecksum;
				finalResult.totalKeys += shardRes.shardTotalKeys;
				finalResult.totalBytes += shardRes.shardTotalBytes;
			} catch (Error& e) {
				TraceEvent(SevWarn, "ChecksumFailedToGetShardResult").error(e);
				// Option: rethrow e; to stop entire checksum on first shard error
				// Option: continue; to XOR with 0 for this shard and sum 0s (partial checksum)
				// For now, let's rethrow to indicate overall failure if any shard fails critically.
				throw;
			}
		} else if (fut.isError()) { // Should ideally be caught by fut.get() above
			TraceEvent(SevWarn, "ChecksumShardFutureIsError");
			// To be safe, attempt to get the error to ensure it's handled or rethrown.
			try {
				fut.get();
			} catch (Error& e) {
				throw;
			} // Re-throw
		}
	}

	TraceEvent(SevInfo, "ChecksumFinalResult")
	    .detail("Checksum", finalResult.checksum) // This is now the XORed checksum
	    .detail("TotalKeys", finalResult.totalKeys)
	    .detail("TotalBytes", finalResult.totalBytes);
	return finalResult;
}

} // namespace fdb