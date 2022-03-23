/*
 * MutationLogReaderCorrectness.actor.cpp
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

#include <cstdint>
#include <limits>
#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/MutationLogReader.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct MutationLogReaderCorrectnessWorkload : TestWorkload {
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
		return StringRef(format("%" PRId64 " (%" PRIx64 ")", v, v));
	}

	MutationLogReaderCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		uid = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
		baLogRangePrefix = uid.withPrefix(backupLogKeys.begin);

		beginVersion = deterministicRandom()->randomInt64(
		    0, std::numeric_limits<int32_t>::max()); // intentionally not max of int64
		records = deterministicRandom()->randomInt(0, 1e6);
		versionRange = deterministicRandom()->randomInt64(records, std::numeric_limits<Version>::max());
		versionIncrement = versionRange / (records + 1);

		// The version immediately after the last actual record version
		endVersion = recordVersion(records - 1) + 1;
	}

	std::string description() const override { return "MutationLogReaderCorrectness"; }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return _start(cx, this);
		}
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, MutationLogReaderCorrectnessWorkload* self) {
		state Transaction tr(cx);
		state int iStart = 0;
		state int batchSize = 1000;
		fmt::print("Records: {}\n", self->records);
		fmt::print("BeginVersion: {}\n", self->beginVersion);
		fmt::print("EndVersion: {}\n", self->endVersion);

		while (iStart < self->records) {
			loop {
				try {
					tr.reset();
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

					int i = iStart;
					state int iEnd = std::min(iStart + batchSize, self->records);

					for (; i < iEnd; ++i) {
						Key key = self->recordKey(i);
						Value value = self->recordValue(i);
						tr.set(key, value);
					}

					wait(tr.commit());
					iStart = iEnd;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		state Reference<MutationLogReader> reader = wait(MutationLogReader::Create(
		    cx, self->beginVersion, self->endVersion, self->uid, backupLogKeys.begin, /*pipelineDepth=*/1));

		state int nextExpectedRecord = 0;

		try {
			loop {
				state Standalone<RangeResultRef> results = wait(reader->getNext());

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

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<MutationLogReaderCorrectnessWorkload> MutationLogReaderCorrectnessWorkloadFactory(
    "MutationLogReaderCorrectness");
