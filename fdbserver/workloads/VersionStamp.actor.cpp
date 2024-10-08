/*
 * VersionStamp.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ApiVersion.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct VersionStampWorkload : TestWorkload {
	static constexpr auto NAME = "VersionStamp";

	uint64_t nodeCount;
	double testDuration;
	double transactionsPerSecond;
	std::vector<Future<Void>> clients;
	int64_t nodePrefix;
	int keyBytes;
	bool failIfDataLost;
	Key vsKeyPrefix;
	Key vsValuePrefix;
	bool validateExtraDB;
	std::map<Key, std::vector<std::pair<Version, Standalone<StringRef>>>> key_commit;
	std::map<Key, std::vector<std::pair<Version, Standalone<StringRef>>>> versionStampKey_commit;
	int apiVersion;
	bool soleOwnerOfMetadataVersionKey;
	bool allowMetadataVersionKey;

	VersionStampWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0);
		nodeCount = getOption(options, "nodeCount"_sr, (uint64_t)10000);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 4);
		failIfDataLost = getOption(options, "failIfDataLost"_sr, true);
		const Key prefix = getOption(options, "prefix"_sr, "VS_"_sr);
		vsKeyPrefix = "K_"_sr.withPrefix(prefix);
		vsValuePrefix = "V_"_sr.withPrefix(prefix);
		validateExtraDB = getOption(options, "validateExtraDB"_sr, false);
		soleOwnerOfMetadataVersionKey = getOption(options, "soleOwnerOfMetadataVersionKey"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		// Versionstamp behavior changed starting with API version 520, so
		// choose a version to check compatibility.
		double choice = deterministicRandom()->random01();
		if (choice < 0.1) {
			apiVersion = 500;
		} else if (choice < 0.2) {
			apiVersion = 510;
		} else if (choice < 0.3) {
			apiVersion = 520;
		} else {
			apiVersion = ApiVersion::LATEST_VERSION;
		}
		TraceEvent("VersionStampApiVersion").detail("ApiVersion", apiVersion);

		allowMetadataVersionKey = apiVersion >= 610 || apiVersion == ApiVersion::LATEST_VERSION;

		cx->apiVersion = ApiVersion(apiVersion);
		if (clientId == 0)
			return _start(cx, this, 1 / transactionsPerSecond);
		return Void();
	}

	Key keyForIndex(uint64_t index) {
		if (allowMetadataVersionKey && index == 0) {
			return metadataVersionKey;
		}

		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodeCount;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result.withPrefix(vsValuePrefix);
	}

	Key versionStampKeyForIndex(uint64_t index, bool oldVSFormat) {
		const size_t keySize = 38 + (oldVSFormat ? 0 : 2);
		Key result = makeString(keySize);
		uint8_t* data = mutateString(result);
		memset(&data[0], 'V', keySize);

		double d = double(index) / nodeCount;
		emplaceIndex(data, 4, *(int64_t*)&d);

		if (oldVSFormat) {
			data[keySize - 2] = 24 + vsKeyPrefix.size();
			data[keySize - 1] = 0;
		} else {
			data[keySize - 4] = 24 + vsKeyPrefix.size();
			data[keySize - 3] = 0;
			data[keySize - 2] = 0;
			data[keySize - 1] = 0;
		}
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

	Future<bool> check(Database const& cx) override {
		if (clientId == 0)
			return _check(cx, this);
		return true;
	}

	static std::pair<Version, Standalone<StringRef>> versionFromValue(const Standalone<StringRef>& value) {
		Version parsedVersion;
		Standalone<StringRef> parsedVersionstamp = makeString(10);
		memcpy(&parsedVersion, value.begin(), sizeof(Version));
		memcpy(mutateString(parsedVersionstamp), value.begin(), 10);
		return { bigEndian64(parsedVersion), parsedVersionstamp };
	}

	// `key` needs to be the non-prefixed key, as we use a fixed offset for the versionstamp location.
	static std::pair<Version, Standalone<StringRef>> versionFromKey(const Standalone<StringRef>& key) {
		Version parsedVersion;
		Standalone<StringRef> parsedVersionstamp = makeString(10);
		memcpy(&parsedVersion, &(key.begin())[24], sizeof(Version));
		memcpy(mutateString(parsedVersionstamp), &(key.begin())[24], 10);
		return { bigEndian64(parsedVersion), parsedVersionstamp };
	}

	ACTOR Future<bool> _check(Database cx, VersionStampWorkload* self) {
		if (self->validateExtraDB) {
			ASSERT(g_simulator->extraDatabases.size() == 1);
			cx = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0], cx->defaultTenant);
		}
		state ReadYourWritesTransaction tr(cx);
		// We specifically wish to grab the smallest read version that we can get and maintain it, to
		// have the strictest check we can on versionstamps monotonically increasing.
		state Version readVersion;
		loop {
			try {
				Version _readVersion = wait(tr.getReadVersion());
				readVersion = _readVersion;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (BUGGIFY) {
			if (deterministicRandom()->random01() < 0.5) {
				loop {
					try {
						tr.makeSelfConflicting();
						wait(tr.commit());
						readVersion = tr.getCommittedVersion() - 1;
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			}
			tr.reset();
			tr.setVersion(readVersion);
		}

		state RangeResult result;
		loop {
			try {
				RangeResult result_ = wait(tr.getRange(
				    KeyRangeRef(self->vsValuePrefix, endOfRange(self->vsValuePrefix)), self->nodeCount + 1));
				result = result_;
				if (self->allowMetadataVersionKey && self->key_commit.count(metadataVersionKey)) {
					Optional<Value> mVal = wait(tr.get(metadataVersionKey));
					if (mVal.present()) {
						result.push_back_deep(result.arena(), KeyValueRef(metadataVersionKey, mVal.get()));
					}
				}
				ASSERT(result.size() <= self->nodeCount);
				if (self->failIfDataLost) {
					ASSERT(result.size() == self->key_commit.size());
				} else {
					CODE_PROBE(result.size() > 0, "Not all data should always be lost.");
				}

				//TraceEvent("VST_Check0").detail("Size", result.size()).detail("NodeCount", self->nodeCount).detail("KeyCommit", self->key_commit.size()).detail("ReadVersion", readVersion);
				for (auto it : result) {
					const Standalone<StringRef> key =
					    it.key == metadataVersionKey ? metadataVersionKey : it.key.removePrefix(self->vsValuePrefix);
					Version parsedVersion;
					Standalone<StringRef> parsedVersionstamp;
					std::tie(parsedVersion, parsedVersionstamp) = versionFromValue(it.value);
					ASSERT(parsedVersion <= readVersion);

					//TraceEvent("VST_Check0a").detail("ItKey", printable(it.key)).detail("ItValue", printable(it.value)).detail("ParsedVersion", parsedVersion);
					const auto& all_values_iter = self->key_commit.find(key);
					ASSERT(all_values_iter != self->key_commit.end()); // Reading a key that we didn't commit.
					const auto& all_values = all_values_iter->second;

					if (it.key == metadataVersionKey && !self->soleOwnerOfMetadataVersionKey) {
						if (self->failIfDataLost) {
							for (auto& it : all_values) {
								ASSERT(it.first <= parsedVersion);
								if (it.first == parsedVersion) {
									ASSERT(it.second.compare(parsedVersionstamp) == 0);
								}
							}
						}
					} else {
						const auto& value_pair_iter =
						    std::find_if(all_values.cbegin(),
						                 all_values.cend(),
						                 [parsedVersion](const std::pair<Version, Standalone<StringRef>>& pair) {
							                 return pair.first == parsedVersion;
						                 });
						ASSERT(value_pair_iter !=
						       all_values.cend()); // The key exists, but we never wrote the timestamp.
						if (self->failIfDataLost) {
							auto last_element_iter = all_values.cend();
							last_element_iter--;
							ASSERT(value_pair_iter == last_element_iter);
						}
						// Version commitVersion = value_pair_iter->first;
						Standalone<StringRef> commitVersionstamp = value_pair_iter->second;

						//TraceEvent("VST_Check0b").detail("Version", commitVersion).detail("CommitVersion", printable(commitVersionstamp));
						ASSERT(commitVersionstamp.compare(parsedVersionstamp) == 0);
					}
				}

				RangeResult result__ = wait(
				    tr.getRange(KeyRangeRef(self->vsKeyPrefix, endOfRange(self->vsKeyPrefix)), self->nodeCount + 1));
				result = result__;
				ASSERT(result.size() <= self->nodeCount);
				if (self->failIfDataLost) {
					ASSERT(result.size() == self->versionStampKey_commit.size());
				} else {
					CODE_PROBE(result.size() > 0, "Not all data should always be lost (2)");
				}

				//TraceEvent("VST_Check1").detail("Size", result.size()).detail("VsKeyCommitSize", self->versionStampKey_commit.size());
				for (auto it : result) {
					const Standalone<StringRef> key = it.key.removePrefix(self->vsKeyPrefix);
					Version parsedVersion;
					Standalone<StringRef> parsedVersionstamp;
					std::tie(parsedVersion, parsedVersionstamp) = versionFromKey(key);

					const Key vsKey = key.substr(4, 16);
					//TraceEvent("VST_Check1a").detail("ItKey", printable(it.key)).detail("VsKey", printable(vsKey)).detail("ItValue", printable(it.value)).detail("ParsedVersion", parsedVersion);
					const auto& all_values_iter = self->versionStampKey_commit.find(vsKey);
					ASSERT(all_values_iter !=
					       self->versionStampKey_commit.end()); // Reading a key that we didn't commit.
					const auto& all_values = all_values_iter->second;

					const auto& value_pair_iter =
					    std::find_if(all_values.cbegin(),
					                 all_values.cend(),
					                 [parsedVersion](const std::pair<Version, Standalone<StringRef>>& pair) {
						                 return pair.first == parsedVersion;
					                 });
					ASSERT(value_pair_iter != all_values.cend()); // The key exists, but we never wrote the timestamp.
					if (self->failIfDataLost) {
						auto last_element_iter = all_values.cend();
						last_element_iter--;
						ASSERT(value_pair_iter == last_element_iter);
					}

					// Version commitVersion = value_pair_iter->first;
					Standalone<StringRef> commitVersionstamp = value_pair_iter->second;
					//TraceEvent("VST_Check1b").detail("Version", commitVersion).detail("CommitVersion", printable(commitVersionstamp));
					ASSERT(parsedVersion <= readVersion);
					ASSERT(commitVersionstamp.compare(parsedVersionstamp) == 0);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		TraceEvent("VST_CheckEnd").log();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR Future<Void> _start(Database cx, VersionStampWorkload* self, double delay) {
		state double startTime = now();
		state double lastTime = now();
		state Database extraDB;

		if (!g_simulator->extraDatabases.empty()) {
			ASSERT(g_simulator->extraDatabases.size() == 1);
			extraDB = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0], cx->defaultTenant);
		}

		state Future<Void> metadataWatch = Void();
		loop {
			wait(poisson(&lastTime, delay));
			bool oldVSFormat = !cx->apiVersionAtLeast(520);

			state bool cx_is_primary = true;
			state ReadYourWritesTransaction tr(cx);
			state Key key = self->keyForIndex(deterministicRandom()->randomInt(0, self->nodeCount));
			state Value value(std::string(deterministicRandom()->randomInt(10, 100), 'x'));
			state Key versionStampKey =
			    self->versionStampKeyForIndex(deterministicRandom()->randomInt(0, self->nodeCount), oldVSFormat);
			state StringRef prefix = versionStampKey.substr(0, 20 + self->vsKeyPrefix.size());
			state Key endOfRange = self->endOfRange(prefix);
			state KeyRangeRef range(prefix, endOfRange);
			state Standalone<StringRef> committedVersionStamp;
			state Version committedVersion;

			state Value versionStampValue;

			if (key == metadataVersionKey) {
				value = metadataVersionRequiredValue;
				versionStampValue = value;
			} else if (oldVSFormat) {
				versionStampValue = value;
			} else {
				versionStampValue = value.withSuffix("\x00\x00\x00\x00"_sr);
			}

			state bool ryw = deterministicRandom()->coinflip();
			loop {
				if (!ryw) {
					tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				}
				state bool error = false;
				state Error err;
				//TraceEvent("VST_CommitBegin").detail("Key", printable(key)).detail("VsKey", printable(versionStampKey)).detail("Clear", printable(range));
				state Key testKey;
				state Future<Void> nextMetadataWatch;
				try {
					tr.atomicOp(key, versionStampValue, MutationRef::SetVersionstampedValue);
					if (key == metadataVersionKey) {
						testKey = "testKey" + deterministicRandom()->randomUniqueID().toString();
						tr.atomicOp(testKey, versionStampValue, MutationRef::SetVersionstampedValue);
					}
					tr.clear(range);
					tr.atomicOp(versionStampKey, value, MutationRef::SetVersionstampedKey);
					if (key == metadataVersionKey) {
						nextMetadataWatch = tr.watch(versionStampKey);
					}
					state Future<Standalone<StringRef>> fTrVs = tr.getVersionstamp();
					wait(tr.commit());

					committedVersion = tr.getCommittedVersion();
					Standalone<StringRef> committedVersionStamp_ = wait(fTrVs);
					committedVersionStamp = committedVersionStamp_;

					if (key == metadataVersionKey) {
						wait(timeoutError(metadataWatch, 30));
						nextMetadataWatch = metadataWatch;
					}

				} catch (Error& e) {
					err = e;
					if (err.code() == error_code_database_locked && !g_simulator->extraDatabases.empty()) {
						//TraceEvent("VST_CommitDatabaseLocked");
						cx_is_primary = !cx_is_primary;
						tr = ReadYourWritesTransaction(cx_is_primary ? cx : extraDB);
						break;
					} else if (err.code() == error_code_commit_unknown_result) {
						//TraceEvent("VST_CommitUnknownResult").error(e).detail("Key", printable(key)).detail("VsKey", printable(versionStampKey));
						loop {
							state ReadYourWritesTransaction cur_tr(cx_is_primary ? cx : extraDB);
							cur_tr.setOption(FDBTransactionOptions::LOCK_AWARE);
							try {
								Optional<Value> vs_value = wait(cur_tr.get(key == metadataVersionKey ? testKey : key));
								if (!vs_value.present()) {
									error = true;
									break;
								}
								const Version value_version = versionFromValue(vs_value.get()).first;
								//TraceEvent("VST_CommitUnknownRead").detail("VsValue", vs_value.present() ? printable(vs_value.get()) : "did not exist");
								const auto& value_ts =
								    self->key_commit[key == metadataVersionKey ? metadataVersionKey
								                                               : key.removePrefix(self->vsValuePrefix)];
								const auto& iter = std::find_if(
								    value_ts.cbegin(),
								    value_ts.cend(),
								    [value_version](const std::pair<Version, Standalone<StringRef>>& pair) {
									    return value_version == pair.first;
								    });
								if (iter == value_ts.cend()) {
									// The commit was successful, and thus we need to record the new data.
									committedVersion = value_version;
									committedVersionStamp = vs_value.get().substr(0, 10);
								} else {
									error = true;
									break;
								}
								break;
							} catch (Error& e) {
								wait(cur_tr.onError(e));
							}
						}
					} else {
						error = true;
					}
				}

				if (error) {
					TraceEvent("VST_CommitFailed")
					    .error(err)
					    .detail("Key", printable(key))
					    .detail("VsKey", printable(versionStampKey));
					wait(tr.onError(err));
					continue;
				}

				const Standalone<StringRef> vsKeyKey = versionStampKey.removePrefix(self->vsKeyPrefix).substr(4, 16);
				const auto& committedVersionPair = std::make_pair(committedVersion, committedVersionStamp);
				//TraceEvent("VST_CommitSuccess").detail("Key", printable(key)).detail("VsKey", printable(versionStampKey)).detail("VsKeyKey", printable(vsKeyKey)).detail("Clear", printable(range)).detail("Version", tr.getCommittedVersion()).detail("VsValue", printable(committedVersionPair.second));
				self->key_commit[key == metadataVersionKey ? metadataVersionKey : key.removePrefix(self->vsValuePrefix)]
				    .push_back(committedVersionPair);
				self->versionStampKey_commit[vsKeyKey].push_back(committedVersionPair);
				break;
			}

			if (now() - startTime > self->testDuration)
				break;
		}
		//TraceEvent("VST_Start").detail("Count", count).detail("NodeCount", self->nodeCount);
		return Void();
	}
};

WorkloadFactory<VersionStampWorkload> VersionStampWorkloadFactory;
