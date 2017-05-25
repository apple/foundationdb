/*
 * VersionStamp.actor.cpp
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

#include "flow/actorcompiler.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "workloads.h"

struct VersionStampWorkload : TestWorkload {
	uint64_t nodeCount;
	double testDuration;
	double transactionsPerSecond;
	vector<Future<Void>> clients;
	int64_t nodePrefix;
	int keyBytes;
	std::map<Key, std::pair<Version, Standalone<StringRef>>> key_commit;
	std::map<Key, std::pair<Version, Standalone<StringRef>>> versionStampKey_commit;

	VersionStampWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 5000.0);
		nodeCount = getOption(options, LiteralStringRef("nodeCount"), (uint64_t)10000);
		keyBytes = std::max(getOption(options, LiteralStringRef("keyBytes"), 16), 4);
	}

	virtual std::string description() { return "VersionStamp"; }

	virtual Future<Void> setup(Database const& cx) { return Void(); }

	virtual Future<Void> start(Database const& cx) {
		if (clientId == 0)
			return _start(cx, this, 1 / transactionsPerSecond);
		return Void();
	}

	Key keyForIndex(uint64_t index) {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodeCount;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}

	Key versionStampKeyForIndex(uint64_t index) {
		Key result = makeString(38);
		uint8_t* data = mutateString(result);
		memset(&data[0], 'V', 38);

		double d = double(index) / nodeCount;
		emplaceIndex(data, 4, *(int64_t*)&d);

		data[38 - 2] = 24;
		data[38 - 1] = 0;
		return result;
	}

	Key endOfRange(Key startOfRange) {
		int n = startOfRange.size();
		Key result = makeString(n);
		uint8_t* data = mutateString(result);
		uint8_t* src = mutateString(startOfRange);
		memcpy(data, src, n);
		data[n - 1] += 1;
		return result;
	}

	virtual Future<bool> check(Database const& cx) {
		if (clientId == 0)
			return _check(cx, this);
		return true;
	}

	ACTOR Future<bool> _check(Database cx, VersionStampWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		loop{
			try {
				Standalone<RangeResultRef> result = wait(tr.getRange(KeyRangeRef(LiteralStringRef(""), LiteralStringRef("V")), self->nodeCount + 1));
				ASSERT(result.size() <= self->nodeCount);
				//TraceEvent("VST_check0").detail("size", result.size()).detail("nodeCount", self->nodeCount).detail("key_commit", self->key_commit.size());
				ASSERT(result.size() == self->key_commit.size());
				for (auto it : result) {
					Version commitVersion = self->key_commit[it.key].first;
					Standalone<StringRef> commitVersionstamp = self->key_commit[it.key].second;
					//TraceEvent("VST_check0").detail("itKey", printable(it.key)).detail("itValue", printable(it.value)).detail("version", commitVersion);
					Version parsedVersion = 0;
					Standalone<StringRef> parsedVersionstamp = makeString(10);
					memcpy(&parsedVersion, it.value.begin(), sizeof(Version));
					memcpy(mutateString(parsedVersionstamp), it.value.begin(), 10);
					parsedVersion = bigEndian64(parsedVersion);
					ASSERT(commitVersion == parsedVersion);
					ASSERT(commitVersionstamp.compare(parsedVersionstamp) == 0);
				}

				Standalone<RangeResultRef> result = wait(tr.getRange(KeyRangeRef(LiteralStringRef("V"), LiteralStringRef("\xFF")), self->nodeCount + 1));
				ASSERT(result.size() <= self->nodeCount);

				//TraceEvent("VST_check1").detail("size", result.size()).detail("vsKey_commit_size", self->versionStampKey_commit.size());
				for (auto it : result) {
					Key vsKey = it.key.substr(4, 16);
					Version commitVersion = self->versionStampKey_commit[vsKey].first;
					Standalone<StringRef> commitVersionstamp = self->versionStampKey_commit[vsKey].second;
					Version parsedVersion = 0;
					Standalone<StringRef> parsedVersionstamp = makeString(10);
					memcpy(&parsedVersion, &(it.key.begin())[24], sizeof(Version));
					memcpy(mutateString(parsedVersionstamp), &(it.key.begin())[24], 10);
					parsedVersion = bigEndian64(parsedVersion);
					//TraceEvent("VST_check2").detail("itKey", printable(it.key)).detail("vsKey", printable(vsKey)).detail("commitVersion", commitVersion).detail("parsedVersion", parsedVersion);
					ASSERT(commitVersion == parsedVersion);
					ASSERT(commitVersionstamp.compare(parsedVersionstamp) == 0);
				}
				break;
			}
			catch (Error &e) {
				Void _ = wait(tr.onError(e));
			}
		}
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR Future<Void> _start(Database cx, VersionStampWorkload* self, double delay) {
		state double startTime = now();
		state double lastTime = now();

		loop{
			Void _ = wait(poisson(&lastTime, delay));

			state ReadYourWritesTransaction tr(cx);
			state Key key = self->keyForIndex(g_random->randomInt(0, self->nodeCount));
			state Value value = std::string(g_random->randomInt(10, 1000), 'x');
			state Key versionStampKey = self->versionStampKeyForIndex(g_random->randomInt(0, self->nodeCount));
			state StringRef prefix = versionStampKey.substr(0, 20);
			state Key endOfRange = self->endOfRange(prefix);
			state KeyRangeRef range(prefix, endOfRange);
			loop{
				//TraceEvent("VST_commit").detail("key", printable(key)).detail("vsKey", printable(versionStampKey)).detail("clear", printable(range));
				try {
					tr.atomicOp(key, value, MutationRef::SetVersionstampedValue);
					tr.clear(range);
					tr.atomicOp(versionStampKey, value, MutationRef::SetVersionstampedKey);
					state Future<Standalone<StringRef>> fTrVs = tr.getVersionstamp();
					Void _ = wait(tr.commit());
					//TraceEvent("VST_commit_success").detail("key", printable(key)).detail("vsKey", printable(versionStampKey)).detail("clear", printable(range));
					Standalone<StringRef> trVs = wait(fTrVs);
					std::pair<Version, Standalone<StringRef>> committedVersionPair = std::make_pair(tr.getCommittedVersion(), trVs);
					self->key_commit[key] = committedVersionPair;
					self->versionStampKey_commit[versionStampKey.substr(4, 16)] = committedVersionPair;
					break;
				}
				catch (Error &e) {
					Void _ = wait(tr.onError(e));
				}
			}

			if (now() - startTime > self->testDuration)
				break;
		}
		//TraceEvent("VST_start").detail("count", count).detail("nodeCount", self->nodeCount);
		return Void();
	}
};

WorkloadFactory<VersionStampWorkload> VersionStampWorkloadFactory("VersionStamp");
