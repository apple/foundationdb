/*
 * MutationLogReaderCorrectness.cpp
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

#include <cstdint>
#include <limits>
#include "fmt/format.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/MutationLogReader.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"

struct MutationLogReaderCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "MutationLogReaderCorrectness";

	bool enabled;
	int records;
	Version versionRange;
	Version versionIncrement;
	Version beginVersion;
	Version endVersion;
	Key uid;
	Key baLogRangePrefix;
	bool debug = false;

	Version recordVersion(int index) { return beginVersion + versionIncrement * index; }

	Key recordKey(int index) { return getLogKey(recordVersion(index), uid); }

	Value recordValue(int index) {
		Version v = recordVersion(index);
		return StringRef(format("%lld (%llx)", static_cast<long long>(v), static_cast<long long>(v)));
	}

	MutationLogReaderCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		uid = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
		baLogRangePrefix = uid.withPrefix(backupLogKeys.begin);

		beginVersion = deterministicRandom()->randomInt64(
		    0, std::numeric_limits<int32_t>::max()); // intentionally not max of int64
		records = deterministicRandom()->randomInt(0, 500e3);
		versionRange = deterministicRandom()->randomInt64(records, std::numeric_limits<Version>::max());
		versionIncrement = versionRange / (records + 1);

		// The version immediately after the last actual record version
		endVersion = recordVersion(records - 1) + 1;
	}

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return _start(cx, this);
		}
		return Void();
	}

	Future<Void> _start(Database cx, MutationLogReaderCorrectnessWorkload* self) {
		Transaction tr(cx);
		int iStart = 0;
		int batchSize = 1000;
		fmt::print("Records: {}\n", self->records);
		fmt::print("BeginVersion: {}\n", self->beginVersion);
		fmt::print("EndVersion: {}\n", self->endVersion);

		while (iStart < self->records) {
			while (true) {
				Error err;
				try {
					tr.reset();
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					int iEnd = std::min(iStart + batchSize, self->records);

					for (int i = iStart; i < iEnd; ++i) {
						Key key = self->recordKey(i);
						Value value = self->recordValue(i);
						tr.set(key, value);
					}

					co_await tr.commit();
					iStart = iEnd;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}

		Reference<MutationLogReader> reader = co_await MutationLogReader::Create(
		    cx, self->beginVersion, self->endVersion, self->uid, backupLogKeys.begin, /*pipelineDepth=*/1);

		int nextExpectedRecord = 0;

		try {
			while (true) {
				Standalone<RangeResultRef> results = co_await reader->getNext();

				for (const auto& rec : results) {
					Key expectedKey = self->recordKey(nextExpectedRecord);
					Value expectedValue = self->recordValue(nextExpectedRecord);

					bool keyMatch = rec.key == expectedKey;
					bool valueMatch = rec.value == expectedValue;

					if (self->debug) {
						if (!keyMatch) {
							printf("key:            %s\n", rec.key.printable().c_str());
							printf("expected key:   %s\n", expectedKey.printable().c_str());
						}
						if (!valueMatch) {
							printf("value:          %s\n", rec.value.printable().c_str());
							printf("expected value: %s\n", expectedValue.printable().c_str());
						}
					}

					ASSERT(keyMatch);
					ASSERT(valueMatch);
					++nextExpectedRecord;
				}
			}
		} catch (Error& e) {
			if (e.code() != error_code_end_of_stream) {
				throw e;
			}
		}

		printf("records expected: %d\n", self->records);
		printf("records found:    %d\n", nextExpectedRecord);

		ASSERT_EQ(nextExpectedRecord, self->records);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MutationLogReaderCorrectnessWorkload> MutationLogReaderCorrectnessWorkloadFactory;
