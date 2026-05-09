/*
 * MutationLogReader.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/MutationLogReader.h"
#include "fdbrpc/simulator.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"

namespace {

Key versionToKey(Version version, Key prefix) {
	uint64_t versionBigEndian = bigEndian64(version);
	return KeyRef((uint8_t*)&versionBigEndian, sizeof(uint64_t)).withPrefix(prefix);
}

Version keyRefToVersion(KeyRef key, int prefixLen) {
	return (Version)bigEndian64(*((uint64_t*)key.substr(prefixLen).begin()));
}

} // namespace

namespace mutation_log_reader {

Standalone<RangeResultRef> RangeResultBlock::consume() {
	Version stopVersion = std::min(lastVersion,
	                               (firstVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) /
	                                   CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE) +
	                      1; // firstVersion rounded up to the nearest 1M versions, then + 1
	int startIndex = indexToRead;
	while (indexToRead < result.size() && keyRefToVersion(result[indexToRead].key, prefixLen) < stopVersion) {
		++indexToRead;
	}
	if (indexToRead < result.size()) {
		firstVersion = keyRefToVersion(result[indexToRead].key, prefixLen); // the version of result[indexToRead]
	}
	return Standalone<RangeResultRef>(
	    RangeResultRef(result.slice(startIndex, indexToRead), result.more, result.readThrough), result.arena());
}

void PipelinedReader::startReading(Database cx) {
	reader = getNext(cx);
}

Future<Void> PipelinedReader::getNext(Database cx) {
	return getNext_impl(this, cx);
}

Future<Void> PipelinedReader::getNext_impl(PipelinedReader* self, Database cx) {
	Transaction tr(cx);

	GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED,
	                      (g_network->isSimulated() && !g_simulator->speedUpSimulation)
	                          ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES
	                          : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

	Key begin = versionToKey(self->currentBeginVersion, self->prefix);
	Key end = versionToKey(self->endVersion, self->prefix);

	while (true) {
		// Get the lock
		co_await self->readerLimit.take();

		// Read begin to end forever until successful
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				RangeResult kvs = co_await tr.getRange(KeyRangeRef(begin, end), limits);

				// No more results, send end of stream
				if (!kvs.empty()) {
					// Send results to the reads stream
					self->reads.send(
					    RangeResultBlock{ .result = kvs,
					                      .firstVersion = keyRefToVersion(kvs.front().key, self->prefix.size()),
					                      .lastVersion = keyRefToVersion(kvs.back().key, self->prefix.size()),
					                      .hash = self->hash,
					                      .prefixLen = self->prefix.size(),
					                      .indexToRead = 0 });
				}

				if (!kvs.more) {
					self->reads.sendError(end_of_stream());
					co_return;
				}

				begin = kvs.getReadThrough();

				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_transaction_too_old) {
				// We are using this transaction until it's too old and then resetting to a fresh one,
				// so we don't need to delay.
				tr.fullReset();
			} else {
				co_await tr.onError(err);
			}
		}
	}
}

} // namespace mutation_log_reader

Future<Void> MutationLogReader::initializePQ(MutationLogReader* self) {
	int h{ 0 };
	for (h = 0; h < 256; ++h) {
		try {
			mutation_log_reader::RangeResultBlock front = co_await self->pipelinedReaders[h]->reads.getFuture();
			self->priorityQueue.push(front);
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream) {
				throw e;
			}
			++self->finished;
		}
	}
}

Future<Standalone<RangeResultRef>> MutationLogReader::getNext() {
	return getNext_impl(this);
}

Future<Standalone<RangeResultRef>> MutationLogReader::getNext_impl(MutationLogReader* self) {
	while (true) {
		if (self->finished == 256) {
			int i{ 0 };
			for (i = 0; i < self->pipelinedReaders.size(); ++i) {
				co_await self->pipelinedReaders[i]->done();
			}
			throw end_of_stream();
		}
		mutation_log_reader::RangeResultBlock top = self->priorityQueue.top();
		self->priorityQueue.pop();
		uint8_t hash = top.hash;
		Standalone<RangeResultRef> ret = top.consume();
		if (top.empty()) {
			self->pipelinedReaders[(int)hash]->release();
			try {
				mutation_log_reader::RangeResultBlock next =
				    co_await self->pipelinedReaders[(int)hash]->reads.getFuture();
				self->priorityQueue.push(next);
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream) {
					++self->finished;
				} else {
					throw e;
				}
			}
		} else {
			self->priorityQueue.push(top);
		}
		if (!ret.empty()) {
			co_return ret;
		}
	}
}

namespace {
// UNIT TESTS
TEST_CASE("/fdbclient/mutationlogreader/VersionKeyRefConversion") {
	Key prefix = "foos"_sr;

	ASSERT(keyRefToVersion(versionToKey(0, prefix), prefix.size()) == 0);
	ASSERT(keyRefToVersion(versionToKey(1, prefix), prefix.size()) == 1);
	ASSERT(keyRefToVersion(versionToKey(-1, prefix), prefix.size()) == -1);
	ASSERT(keyRefToVersion(versionToKey(std::numeric_limits<int64_t>::min(), prefix), prefix.size()) ==
	       std::numeric_limits<int64_t>::min());
	ASSERT(keyRefToVersion(versionToKey(std::numeric_limits<int64_t>::max(), prefix), prefix.size()) ==
	       std::numeric_limits<int64_t>::max());

	return Void();
}
} // namespace

void forceLinkMutationLogReaderTests() {}
