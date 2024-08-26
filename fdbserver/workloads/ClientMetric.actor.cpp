/*
 * ClientMetric.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/Tuple.h"
#include "flow/actorcompiler.h" // has to be last include

static const StringRef sampleTrInfoKey =
    "\xff\x02/fdbClientInfo/client_latency/SSSSSSSSSS/RRRRRRRRRRRRRRRR/NNNNTTTT/XXXX/"_sr;
static const auto versionStampIndex = sampleTrInfoKey.toString().find('S');
static const int versionStampLength = 10;

static const Key CLIENT_LATENCY_INFO_PREFIX = "client_latency/"_sr;
static const Key CLIENT_LATENCY_INFO_CTR_PREFIX = "client_latency_counter/"_sr;

struct ClientMetricWorkload : TestWorkload {
	static constexpr auto NAME = "ClientMetric";
	double samplingProbability;
	double testDuration;
	bool toSet;
	int64_t trInfoSizeLimit;
	std::vector<Future<Void>> clients;

	ClientMetricWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		samplingProbability = getOption(options,
		                                "samplingProbability"_sr,
		                                deterministicRandom()->random01()); // rand range 0 - 1
		toSet = getOption(options, "toSet"_sr, false);
		trInfoSizeLimit = getOption(options,
		                            "trInfoSizeLimit"_sr,
		                            deterministicRandom()->randomInt(100 * 1024, 10 * 1024 * 1024)); // 100 KB - 10 MB
		testDuration = getOption(options, "testDuration"_sr, 1000.0);
	}

	static uint64_t getVersionStamp(KeyRef key) {
		return bigEndian64(
		    BinaryReader::fromStringRef<int64_t>(key.substr(versionStampIndex, versionStampLength), Unversioned()));
	}

	Future<Void> setup(Database const& cx) override {
		if (toSet && this->clientId == 0) {
			return changeProfilingParameters(cx, trInfoSizeLimit, samplingProbability);
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (this->clientId != 0) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(ClientMetricWorkload* self, Database cx) {
		try {
			self->clients.push_back(timeout(self->runner(cx, self), self->testDuration, Void()));
			wait(waitForAll(self->clients));
		} catch (Error& e) {
			TraceEvent("ClientMetricError::_start").error(e);
		}
		return Void();
	}

	ACTOR Future<Void> changeProfilingParameters(Database cx, int64_t sizeLimit, double sampleProbability) {
		wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			Tuple rate = Tuple::makeTuple(sampleProbability);
			Tuple size = Tuple::makeTuple(sizeLimit);
			tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSampleRate), rate.pack());
			tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSizeLimit), size.pack());
			std::cout << "Change globalconfig: sampleRate=" << sampleProbability << " sizeLimit=" << sizeLimit
			          << std::endl;

			return Void();
		}));
		return Void();
	}

	ACTOR Future<RangeResult> latencyRangeQuery(Database cx, int keysLimit, bool reverse) {
		state KeySelector begin =
		    firstGreaterOrEqual(CLIENT_LATENCY_INFO_PREFIX.withPrefix(fdbClientInfoPrefixRange.begin));
		state KeySelector end = firstGreaterOrEqual(strinc(begin.getKey()));
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state RangeResult txInfoEntries;
		// wait to make sure client metrics are updated
		wait(delay(CLIENT_KNOBS->CSI_STATUS_DELAY));
		loop {
			try {
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				std::string sampleRateStr = "default";
				std::string sizeLimitStr = "default";
				const double sampleRateDbl =
				    cx->globalConfig->get<double>(fdbClientInfoTxnSampleRate, std::numeric_limits<double>::infinity());
				if (!std::isinf(sampleRateDbl)) {
					sampleRateStr = std::to_string(sampleRateDbl);
				}
				const int64_t sizeLimit = cx->globalConfig->get<int64_t>(fdbClientInfoTxnSizeLimit, -1);
				if (sizeLimit != -1) {
					sizeLimitStr = std::to_string(sizeLimit);
				}
				std::cout << "Read from globalconfig: rate=" << sampleRateStr << " size=" << sizeLimitStr << std::endl;
				state RangeResult kvRange = wait(
				    tr->getRange(begin, end, keysLimit, Snapshot::False, reverse ? Reverse::True : Reverse::False));
				if (kvRange.empty()) {
					wait(delay(1.0));
					std::cout << "WaitingForLatencyMetricToBePresent" << std::endl;
					TraceEvent("WaitingForLatencyMetricToBePresent").log();
					continue;
				}
				txInfoEntries.arena().dependsOn(kvRange.arena());
				txInfoEntries.append(txInfoEntries.arena(), kvRange.begin(), kvRange.size());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		for (auto& kv : txInfoEntries) {
			uint64_t vs = getVersionStamp(kv.key);
			std::cout << "VersionStamp is " << vs << std::endl;
		}
		return txInfoEntries;
	}

	ACTOR Future<Void> writeRandomKeys(Database cx, int total) {
		state int cnt = 0;
		state Transaction tr(cx);
		try {
			loop {
				try {
					wait(delay(0.001));
					tr.reset();
					tr.set(Key(deterministicRandom()->randomAlphaNumeric(10)),
					       Value(Key(deterministicRandom()->randomAlphaNumeric(10))));
					wait(tr.commit());
					if (cnt >= total) {
						break;
					}
					++cnt;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "ClientMetricErrorWhenWriteKeys").error(e);
			throw;
		}
		std::cout << "writeRandomKeys finish, written=" << cnt << std::endl;
		return Void();
	}

	ACTOR Future<uint64_t> writeKeysAndGetLatencyVersion(Database cx,
	                                                     ClientMetricWorkload* self,
	                                                     int numKeys,
	                                                     uint64_t previousVS) {
		state int retry = 0;
		state int max_retry = 10;
		state int keysLimit = 1;
		loop {
			if (retry > max_retry) {
				// this should not happen, it should succeed after a few retry
				ASSERT(false);
			}
			// write random keys to generate some latency metrics
			wait(self->writeRandomKeys(cx, numKeys));
			// get the latest latency metric and parse its version stamp
			RangeResult r = wait(self->latencyRangeQuery(cx, keysLimit, true));
			if (r.size() == 0) {
				// latency metrics might not be present due to transaction batching, retry a few times
				++retry;
				continue;
			}
			ASSERT(r.size() > 0);
			// [0] is the latest version, as we have reverse = true
			KeyRef latest = r[0].key;
			uint64_t vs = getVersionStamp(latest);
			ASSERT(vs >= previousVS);
			if (vs == previousVS) {
				// it means there is no new latency metrics, retry until we see one
				++retry;
				continue;
			}
			return vs;
		}
	}

	// goal:
	//      write some random keys, check the latency metric and the latest version stamp vs1
	//      write some other random keys, check the latency metric and latest version stamp again vs2
	//      vs2 should be strictly larger than vs1, to verify new latency metrics are added
	ACTOR Future<Void> runner(Database cx, ClientMetricWorkload* self) {
		try {
			state int initialWrites = deterministicRandom()->randomInt(100, 200);
			state int secondWrites = deterministicRandom()->randomInt(100, 200);

			state uint64_t zeroVS = 0;
			state uint64_t vs1 = wait(self->writeKeysAndGetLatencyVersion(cx, self, initialWrites, zeroVS));
			std::cout << "vs1=" << vs1 << std::endl;
			ASSERT(vs1 > zeroVS);

			state uint64_t vs2 = wait(self->writeKeysAndGetLatencyVersion(cx, self, secondWrites, vs1));
			std::cout << "vs2=" << vs2 << std::endl;
			ASSERT(vs2 > vs1);

		} catch (Error& e) {
			TraceEvent("ClientMetricError").error(e);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ClientMetricWorkload> ClientMetricWorkloadFactory;
