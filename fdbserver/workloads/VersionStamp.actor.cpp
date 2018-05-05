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
	bool failIfDataLost;
	Key vsKeyPrefix;
	Key vsValuePrefix;
	bool validateExtraDB;
	std::map<Key, std::vector<std::pair<Version, Standalone<StringRef>>>> key_commit;
	std::map<Key, std::vector<std::pair<Version, Standalone<StringRef>>>> versionStampKey_commit;

	VersionStampWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 5000.0);
		nodeCount = getOption(options, LiteralStringRef("nodeCount"), (uint64_t)10000);
		keyBytes = std::max(getOption(options, LiteralStringRef("keyBytes"), 16), 4);
		failIfDataLost = getOption(options, LiteralStringRef("failIfDataLost"), true);
		const Key prefix = getOption(options, LiteralStringRef("prefix"), LiteralStringRef("VS_"));
		vsKeyPrefix = LiteralStringRef("K_").withPrefix(prefix);
		vsValuePrefix = LiteralStringRef("V_").withPrefix(prefix);
		validateExtraDB = getOption(options, LiteralStringRef("validateExtraDB"), false);
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

		return result.withPrefix(vsValuePrefix);
	}

	Key versionStampKeyForIndex(uint64_t index) {
		Key result = makeString(38);
		uint8_t* data = mutateString(result);
		memset(&data[0], 'V', 38);

		double d = double(index) / nodeCount;
		emplaceIndex(data, 4, *(int64_t*)&d);

		data[38 - 2] = 24 + vsKeyPrefix.size();
		data[38 - 1] = 0;
		return result.withPrefix(vsKeyPrefix);
	}

	static Key endOfRange(Key startOfRange) {
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

	static std::pair<Version, Standalone<StringRef>> versionFromValue(const Standalone<StringRef>& value) {
		Version parsedVersion;
		Standalone<StringRef> parsedVersionstamp = makeString(10);
		memcpy(&parsedVersion, value.begin(), sizeof(Version));
		memcpy(mutateString(parsedVersionstamp), value.begin(), 10);
		return {bigEndian64(parsedVersion), parsedVersionstamp};
	}

	// `key` needs to be the non-prefixed key, as we use a fixed offset for the versionstamp location.
	static std::pair<Version, Standalone<StringRef>> versionFromKey(const Standalone<StringRef>& key) {
		Version parsedVersion;
		Standalone<StringRef> parsedVersionstamp = makeString(10);
		memcpy(&parsedVersion, &(key.begin())[24], sizeof(Version));
		memcpy(mutateString(parsedVersionstamp), &(key.begin())[24], 10);
		return {bigEndian64(parsedVersion), parsedVersionstamp};
	}

	ACTOR Future<bool> _check(Database cx, VersionStampWorkload* self) {
		if (self->validateExtraDB) {
			Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
			Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
			cx = extraCluster->createDatabase(LiteralStringRef("DB")).get();
		}
		state ReadYourWritesTransaction tr(cx);
		// We specifically wish to grab the smalles read version that we can get and maintain it, to
		// have the strictest check we can on versionstamps monotonically increasing.
		state Version readVersion = wait(tr.getReadVersion());
		loop{
			try {
				Standalone<RangeResultRef> result = wait(tr.getRange(KeyRangeRef(self->vsValuePrefix, endOfRange(self->vsValuePrefix)), self->nodeCount + 1));
				ASSERT(result.size() <= self->nodeCount);
				if (self->failIfDataLost) ASSERT(result.size() == self->key_commit.size());
				else TEST(result.size() > 0);  // Not all data should always be lost.

				//TraceEvent("VST_check0").detail("size", result.size()).detail("nodeCount", self->nodeCount).detail("key_commit", self->key_commit.size()).detail("readVersion", readVersion);
				for (auto it : result) {
					const Standalone<StringRef> key = it.key.removePrefix(self->vsValuePrefix);
					Version parsedVersion;
					Standalone<StringRef> parsedVersionstamp;
					std::tie(parsedVersion, parsedVersionstamp) = versionFromValue(it.value);

					//TraceEvent("VST_check0a").detail("itKey", printable(it.key)).detail("itValue", printable(it.value)).detail("parsedVersion", parsedVersion);
					const auto& all_values_iter = self->key_commit.find(key);
					ASSERT(all_values_iter != self->key_commit.end());  // Reading a key that we didn't commit.
					const auto& all_values = all_values_iter->second;

					const auto& value_pair_iter = std::find_if(all_values.cbegin(), all_values.cend(),
					    [parsedVersion](const std::pair<Version, Standalone<StringRef>>& pair) { return pair.first == parsedVersion; });
					ASSERT(value_pair_iter != all_values.cend());  // The key exists, but we never wrote the timestamp.
					if (self->failIfDataLost) {
						auto last_element_iter = all_values.cend();  last_element_iter--;
						ASSERT(value_pair_iter == last_element_iter);
					}
					Version commitVersion = value_pair_iter->first;
					Standalone<StringRef> commitVersionstamp = value_pair_iter->second;

					//TraceEvent("VST_check0b").detail("version", commitVersion).detail("commitVersion", printable(commitVersionstamp));
					ASSERT(parsedVersion <= readVersion);
					ASSERT(commitVersionstamp.compare(parsedVersionstamp) == 0);
				}

				Standalone<RangeResultRef> result = wait(tr.getRange(KeyRangeRef(self->vsKeyPrefix, endOfRange(self->vsKeyPrefix)), self->nodeCount + 1));
				ASSERT(result.size() <= self->nodeCount);
				if (self->failIfDataLost) ASSERT(result.size() == self->versionStampKey_commit.size());
				else TEST(result.size() > 0);  // Not all data should always be lost.

				//TraceEvent("VST_check1").detail("size", result.size()).detail("vsKey_commit_size", self->versionStampKey_commit.size());
				for (auto it : result) {
					const Standalone<StringRef> key = it.key.removePrefix(self->vsKeyPrefix);
					Version parsedVersion;
					Standalone<StringRef> parsedVersionstamp;
					std::tie(parsedVersion, parsedVersionstamp) = versionFromKey(key);

					const Key vsKey = key.substr(4, 16);
					//TraceEvent("VST_check1a").detail("itKey", printable(it.key)).detail("vsKey", printable(vsKey)).detail("itValue", printable(it.value)).detail("parsedVersion", parsedVersion);
					const auto& all_values_iter = self->versionStampKey_commit.find(vsKey);
					ASSERT(all_values_iter != self->versionStampKey_commit.end());  // Reading a key that we didn't commit.
					const auto& all_values = all_values_iter->second;

					const auto& value_pair_iter = std::find_if(all_values.cbegin(), all_values.cend(),
					    [parsedVersion](const std::pair<Version, Standalone<StringRef>>& pair) { return pair.first == parsedVersion; });
					ASSERT(value_pair_iter != all_values.cend());  // The key exists, but we never wrote the timestamp.
					if (self->failIfDataLost) {
						auto last_element_iter = all_values.cend();  last_element_iter--;
						ASSERT(value_pair_iter == last_element_iter);
					}

					Version commitVersion = value_pair_iter->first;
					Standalone<StringRef> commitVersionstamp = value_pair_iter->second;
					//TraceEvent("VST_check1b").detail("version", commitVersion).detail("commitVersion", printable(commitVersionstamp));
					ASSERT(parsedVersion <= readVersion);
					ASSERT(commitVersionstamp.compare(parsedVersionstamp) == 0);
				}
				break;
			}
			catch (Error &e) {
				Void _ = wait(tr.onError(e));
			}
		}
		TraceEvent("VST_check_end");
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR Future<Void> _start(Database cx, VersionStampWorkload* self, double delay) {
		state double startTime = now();
		state double lastTime = now();

		if (g_simulator.extraDB != NULL) {
			Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
			Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
			state Database extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();
		}

		loop{
			Void _ = wait(poisson(&lastTime, delay));

			state bool cx_is_primary = true;
			state ReadYourWritesTransaction tr(cx);
			state Key key = self->keyForIndex(g_random->randomInt(0, self->nodeCount));
			state Value value = std::string(g_random->randomInt(10, 100), 'x');
			state Key versionStampKey = self->versionStampKeyForIndex(g_random->randomInt(0, self->nodeCount));
			state StringRef prefix = versionStampKey.substr(0, 20+self->vsKeyPrefix.size());
			state Key endOfRange = self->endOfRange(prefix);
			state KeyRangeRef range(prefix, endOfRange);
			state Standalone<StringRef> committedVersionStamp;
			state Version committedVersion;
			loop{
				state bool error = false;
				//TraceEvent("VST_commit_begin").detail("key", printable(key)).detail("vsKey", printable(versionStampKey)).detail("clear", printable(range));
				try {
					tr.atomicOp(key, value, MutationRef::SetVersionstampedValue);
					tr.clear(range);
					tr.atomicOp(versionStampKey, value, MutationRef::SetVersionstampedKey);
					state Future<Standalone<StringRef>> fTrVs = tr.getVersionstamp();
					Void _ = wait(tr.commit());

					committedVersion = tr.getCommittedVersion();
					Standalone<StringRef> committedVersionStamp_ = wait(fTrVs);
					committedVersionStamp = committedVersionStamp_;
				}
				catch (Error &e) {
					state Error err = e;
					if (err.code() == error_code_database_locked) {
						//TraceEvent("VST_commit_database_locked");
						cx_is_primary = !cx_is_primary;
						tr = ReadYourWritesTransaction(cx_is_primary ? cx : extraDB);
						break;
					} else if (err.code() == error_code_commit_unknown_result) {
						//TraceEvent("VST_commit_unknown_result").detail("key", printable(key)).detail("vsKey", printable(versionStampKey)).error(e);
						loop {
							state ReadYourWritesTransaction cur_tr(cx_is_primary ? cx : extraDB);
							cur_tr.setOption(FDBTransactionOptions::LOCK_AWARE);
							try {
								Optional<Value> vs_value = wait(cur_tr.get(key));
								if (!vs_value.present()) {
									error = true;
									break;
								}
								const Version value_version = versionFromValue(vs_value.get()).first;
								//TraceEvent("VST_commit_unknown_read").detail("vs_value", vs_value.present() ? printable(vs_value.get()) : "did not exist");
								const auto& value_ts = self->key_commit[key.removePrefix(self->vsValuePrefix)];
								const auto& iter = std::find_if(value_ts.cbegin(), value_ts.cend(),
								    [value_version](const std::pair<Version, Standalone<StringRef>>& pair) {
								      return value_version == pair.first;
								    });
								if (iter == value_ts.cend()) {
									// The commit was successful, and thus we need to record the new data.
									committedVersion = value_version;
									committedVersionStamp = vs_value.get().substr(0,10);
								} else {
									error = true;
									break;
								}
								break;
							} catch (Error &e) {
								Void _ = wait(cur_tr.onError(e));
							}
						}
					} else {
						error = true;
					}
				}

				if (error) {
					//TraceEvent("VST_commit_failed").detail("key", printable(key)).detail("vsKey", printable(versionStampKey)).error(e);
					Void _ = wait(tr.onError(err));
					continue;
				}

				const Standalone<StringRef> vsKeyKey = versionStampKey.removePrefix(self->vsKeyPrefix).substr(4, 16);
				const auto& committedVersionPair = std::make_pair(committedVersion, committedVersionStamp);
				//TraceEvent("VST_commit_success").detail("key", printable(key)).detail("vsKey", printable(versionStampKey)).detail("vsKeyKey", printable(vsKeyKey)).detail("clear", printable(range)).detail("version", tr.getCommittedVersion()).detail("vsValue", printable(committedVersionPair.second));
				self->key_commit[key.removePrefix(self->vsValuePrefix)].push_back(committedVersionPair);
				self->versionStampKey_commit[vsKeyKey].push_back(committedVersionPair);
				break;
			}

			if (now() - startTime > self->testDuration)
				break;
		}
		//TraceEvent("VST_start").detail("count", count).detail("nodeCount", self->nodeCount);
		return Void();
	}
};

WorkloadFactory<VersionStampWorkload> VersionStampWorkloadFactory("VersionStamp");
