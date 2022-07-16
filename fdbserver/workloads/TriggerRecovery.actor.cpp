/*
 * TriggerRecovery.actor.cpp
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
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "flow/actorcompiler.h" // has to be last include

struct TriggerRecoveryLoopWorkload : TestWorkload {
	double startTime;
	int numRecoveries;
	double delayBetweenRecoveries;
	double killAllProportion;
	Optional<int32_t> originalNumOfResolvers;
	Optional<int32_t> currentNumOfResolvers;

	TriggerRecoveryLoopWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		startTime = getOption(options, LiteralStringRef("startTime"), 0.0);
		numRecoveries = getOption(options, LiteralStringRef("numRecoveries"), deterministicRandom()->randomInt(1, 10));
		delayBetweenRecoveries = getOption(options, LiteralStringRef("delayBetweenRecoveries"), 0.0);
		killAllProportion = getOption(options, LiteralStringRef("killAllProportion"), 0.1);
		ASSERT((numRecoveries > 0) && (startTime >= 0) && (delayBetweenRecoveries >= 0));
		TraceEvent(SevInfo, "TriggerRecoveryLoopSetup")
		    .detail("StartTime", startTime)
		    .detail("NumRecoveries", numRecoveries)
		    .detail("DelayBetweenRecoveries", delayBetweenRecoveries);
	}

	std::string description() const override { return "TriggerRecoveryLoop"; }

	ACTOR Future<Void> setOriginalNumOfResolvers(Database cx, TriggerRecoveryLoopWorkload* self) {
		DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		self->originalNumOfResolvers = self->currentNumOfResolvers = config.getDesiredResolvers();
		return Void();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return setOriginalNumOfResolvers(cx, this);
		}
		return Void();
	}

	ACTOR Future<Void> returnIfClusterRecovered(Database cx) {
		loop {
			state ReadYourWritesTransaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				state Version v = wait(tr.getReadVersion());
				tr.makeSelfConflicting();
				wait(tr.commit());
				TraceEvent(SevInfo, "TriggerRecoveryLoop_ClusterVersion").detail("Version", v);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> changeResolverConfig(Database cx,
	                                        TriggerRecoveryLoopWorkload* self,
	                                        bool setToOriginal = false) {
		state int32_t numResolversToSet;
		if (setToOriginal) {
			numResolversToSet = self->originalNumOfResolvers.get();
		} else {
			numResolversToSet = self->currentNumOfResolvers.get() == self->originalNumOfResolvers.get()
			                        ? self->originalNumOfResolvers.get() + 1
			                        : self->originalNumOfResolvers.get();
		}
		state StringRef configStr(format("resolvers=%d", numResolversToSet));
		loop {
			Optional<ConfigureAutoResult> conf;
			ConfigurationResult r = wait(ManagementAPI::changeConfig(cx.getReference(), { configStr }, conf, true));
			if (r == ConfigurationResult::SUCCESS) {
				self->currentNumOfResolvers = numResolversToSet;
				TraceEvent(SevInfo, "TriggerRecoveryLoop_ChangeResolverConfigSuccess")
				    .detail("NumOfResolvers", self->currentNumOfResolvers.get());
				break;
			}
			TraceEvent(SevWarn, "TriggerRecoveryLoop_ChangeResolverConfigFailed").detail("Result", r);
			wait(delay(1.0));
		}
		return Void();
	}

	ACTOR Future<Void> killAll(Database cx) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				RangeResult kvs = wait(tr.getRange(KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"),
				                                               LiteralStringRef("\xff\xff/worker_interfaces0")),
				                                   CLIENT_KNOBS->TOO_MANY));
				ASSERT(!kvs.more);
				std::map<Key, Value> address_interface;
				for (auto it : kvs) {
					auto ip_port =
					    (it.key.endsWith(LiteralStringRef(":tls")) ? it.key.removeSuffix(LiteralStringRef(":tls"))
					                                               : it.key)
					        .removePrefix(LiteralStringRef("\xff\xff/worker_interfaces/"));
					address_interface[ip_port] = it.value;
				}
				for (auto it : address_interface) {
					if (cx->apiVersionAtLeast(700))
						BinaryReader::fromStringRef<ClientWorkerInterface>(it.second, IncludeVersion())
						    .reboot.send(RebootRequest());
					else
						tr.set(LiteralStringRef("\xff\xff/reboot_worker"), it.second);
				}
				TraceEvent(SevInfo, "TriggerRecoveryLoop_AttempedKillAll").log();
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> _start(Database cx, TriggerRecoveryLoopWorkload* self) {
		wait(delay(self->startTime));
		state int numRecoveriesDone = 0;
		try {
			loop {
				if (deterministicRandom()->random01() < self->killAllProportion) {
					wait(self->killAll(cx));
				} else {
					wait(self->changeResolverConfig(cx, self));
				}
				numRecoveriesDone++;
				TraceEvent(SevInfo, "TriggerRecoveryLoop_AttempedRecovery").detail("RecoveryNum", numRecoveriesDone);
				if (numRecoveriesDone == self->numRecoveries) {
					break;
				}
				wait(delay(self->delayBetweenRecoveries));
				wait(self->returnIfClusterRecovered(cx));
			}
		} catch (Error& e) {
			// Dummy catch here to give a chance to reset number of resolvers to its original value
		}
		wait(self->changeResolverConfig(cx, self, true));
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TriggerRecoveryLoopWorkload> TriggerRecoveryLoopWorkloadFactory("TriggerRecoveryLoop");
