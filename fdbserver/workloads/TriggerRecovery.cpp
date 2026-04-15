/*
 * TriggerRecovery.cpp
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

#include "fdbserver/tester/workloads.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/RunRYWTransaction.h"

struct TriggerRecoveryLoopWorkload : TestWorkload {
	static constexpr auto NAME = "TriggerRecoveryLoop";

	double startTime;
	int numRecoveries;
	double delayBetweenRecoveries;
	double killAllProportion;
	Optional<int32_t> originalNumOfResolvers;
	Optional<int32_t> currentNumOfResolvers;

	TriggerRecoveryLoopWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		startTime = getOption(options, "startTime"_sr, 0.0);
		numRecoveries = getOption(options, "numRecoveries"_sr, deterministicRandom()->randomInt(1, 10));
		delayBetweenRecoveries = getOption(options, "delayBetweenRecoveries"_sr, 0.0);
		killAllProportion = getOption(options, "killAllProportion"_sr, 0.1);
		ASSERT((numRecoveries > 0) && (startTime >= 0) && (delayBetweenRecoveries >= 0));
		TraceEvent(SevInfo, "TriggerRecoveryLoopSetup")
		    .detail("StartTime", startTime)
		    .detail("NumRecoveries", numRecoveries)
		    .detail("DelayBetweenRecoveries", delayBetweenRecoveries);
	}

	Future<Void> setOriginalNumOfResolvers(Database cx, TriggerRecoveryLoopWorkload* self) {
		DatabaseConfiguration config = co_await getDatabaseConfiguration(cx);
		self->originalNumOfResolvers = self->currentNumOfResolvers = config.getDesiredResolvers();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return setOriginalNumOfResolvers(cx, this);
		}
		return Void();
	}

	Future<Void> returnIfClusterRecovered(Database cx) {
		while (true) {
			ReadYourWritesTransaction tr(cx);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Version v = co_await tr.getReadVersion();
				tr.makeSelfConflicting();
				co_await tr.commit();
				TraceEvent(SevInfo, "TriggerRecoveryLoop_ClusterVersion").detail("Version", v);
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> changeResolverConfig(Database cx, TriggerRecoveryLoopWorkload* self, bool setToOriginal = false) {
		int32_t numResolversToSet{ 0 };
		if (setToOriginal) {
			numResolversToSet = self->originalNumOfResolvers.get();
		} else {
			numResolversToSet = self->currentNumOfResolvers.get() == self->originalNumOfResolvers.get()
			                        ? self->originalNumOfResolvers.get() + 1
			                        : self->originalNumOfResolvers.get();
		}
		StringRef configStr(format("resolvers=%d", numResolversToSet));
		while (true) {
			Optional<ConfigureAutoResult> conf;
			ConfigurationResult r = co_await ManagementAPI::changeConfig(cx.getReference(), { configStr }, conf, true);
			if (r == ConfigurationResult::SUCCESS) {
				self->currentNumOfResolvers = numResolversToSet;
				TraceEvent(SevInfo, "TriggerRecoveryLoop_ChangeResolverConfigSuccess")
				    .detail("NumOfResolvers", self->currentNumOfResolvers.get());
				break;
			}
			TraceEvent(SevWarn, "TriggerRecoveryLoop_ChangeResolverConfigFailed").detail("Result", r);
			co_await delay(1.0);
		}
	}

	Future<Void> killAll(Database cx) {
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult kvs = co_await tr.getRange(
				    KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr),
				    CLIENT_KNOBS->TOO_MANY);
				ASSERT(!kvs.more);
				std::map<Key, Value> address_interface;
				for (auto it : kvs) {
					auto ip_port = (it.key.endsWith(":tls"_sr) ? it.key.removeSuffix(":tls"_sr) : it.key)
					                   .removePrefix("\xff\xff/worker_interfaces/"_sr);
					address_interface[ip_port] = it.value;
				}
				for (const auto& it : address_interface) {
					if (cx->apiVersionAtLeast(700))
						BinaryReader::fromStringRef<ClientWorkerInterface>(it.second, IncludeVersion())
						    .reboot.send(RebootRequest());
					else
						tr.set("\xff\xff/reboot_worker"_sr, it.second);
				}
				TraceEvent(SevInfo, "TriggerRecoveryLoop_AttempedKillAll").log();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> _start(Database cx) {
		co_await delay(startTime);
		int numRecoveriesDone = 0;
		try {
			while (true) {
				if (deterministicRandom()->random01() < killAllProportion) {
					co_await killAll(cx);
				} else {
					co_await changeResolverConfig(cx, this);
				}
				numRecoveriesDone++;
				TraceEvent(SevInfo, "TriggerRecoveryLoop_AttempedRecovery").detail("RecoveryNum", numRecoveriesDone);
				if (numRecoveriesDone == numRecoveries) {
					break;
				}
				co_await delay(delayBetweenRecoveries);
				co_await returnIfClusterRecovered(cx);
			}
		} catch (Error& e) {
			// Dummy catch here to give a chance to reset number of resolvers to its original value
		}
		co_await changeResolverConfig(cx, this, true);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TriggerRecoveryLoopWorkload> TriggerRecoveryLoopWorkloadFactory;
