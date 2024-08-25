/*
 * DiskFailureInjection.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/Status.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DiskFailureInjectionWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "DiskFailureInjection";
	bool enabled;
	double testDuration = 60.0;
	double startDelay = 0.0;
	bool throttleDisk = false;
	int workersToThrottle = 3;
	double stallInterval = 0.0;
	double stallPeriod = 60.0;
	double throttlePeriod = 60.0;
	bool corruptFile = false;
	int workersToCorrupt = 1;
	double percentBitFlips = 10;
	double periodicBroadcastInterval = 5.0;
	std::vector<NetworkAddress> chosenWorkers;
	// Verification Mode: We run the workload indefinitely in this mode.
	// The idea is to keep going until we get a non-zero chaosMetric to ensure
	// that we haven't lost the chaos event. testDuration is ignored in this mode
	bool verificationMode = false;

	DiskFailureInjectionWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {}

	DiskFailureInjectionWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		startDelay = getOption(options, "startDelay"_sr, startDelay);
		testDuration = getOption(options, "testDuration"_sr, testDuration);
		verificationMode = getOption(options, "verificationMode"_sr, verificationMode);
		throttleDisk = getOption(options, "throttleDisk"_sr, throttleDisk);
		workersToThrottle = getOption(options, "workersToThrottle"_sr, workersToThrottle);
		stallInterval = getOption(options, "stallInterval"_sr, stallInterval);
		stallPeriod = getOption(options, "stallPeriod"_sr, stallPeriod);
		throttlePeriod = getOption(options, "throttlePeriod"_sr, throttlePeriod);
		corruptFile = getOption(options, "corruptFile"_sr, corruptFile);
		workersToCorrupt = getOption(options, "workersToCorrupt"_sr, workersToCorrupt);
		percentBitFlips = getOption(options, "percentBitFlips"_sr, percentBitFlips);
		periodicBroadcastInterval = getOption(options, "periodicBroadcastInterval"_sr, periodicBroadcastInterval);
	}

	// TODO: Currently this workload doesn't play well with MachineAttrition.
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

	void initFailureInjectionMode(DeterministicRandom& random) override { enabled = clientId == 0; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	// Starts the workload by -
	// 1. Starting the actor to periodically check chaosMetrics and re-broadcast chaos events, and
	// 2. Starting the actor that injects failures on chosen storage servers
	Future<Void> start(Database const& cx) override {
		if (enabled) {
			auto result = diskFailureInjectionClient<WorkerInterface>(cx, this);
			//  In verification mode, we want to wait until periodicEventBroadcast actor returns which indicates that
			//  a non-zero chaosMetric was found.
			if (verificationMode) {
				return (periodicEventBroadcast(this) && delay(testDuration)) || result;
			} else {
				// Else we honor the testDuration
				return timeout(periodicEventBroadcast(this) && result, testDuration, Void());
			}
		} else
			return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static void checkDiskFailureInjectionResult(Future<Void> res, WorkerInterface worker) {
		if (res.isError()) {
			auto err = res.getError();
			if (err.code() == error_code_client_invalid_operation) {
				TraceEvent(SevError, "ChaosDisabled")
				    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString());
			} else {
				TraceEvent(SevError, "DiskFailureInjectionFailed")
				    .error(err)
				    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString());
			}
		}
	}

	// Sets the disk delay request
	ACTOR void injectDiskDelays(WorkerInterface worker,
	                            double stallInterval,
	                            double stallPeriod,
	                            double throttlePeriod) {
		state Future<Void> res;
		SetFailureInjection::DiskFailureCommand diskFailure;
		diskFailure.stallInterval = stallInterval;
		diskFailure.stallPeriod = stallPeriod;
		diskFailure.throttlePeriod = throttlePeriod;
		SetFailureInjection req;
		req.diskFailure = diskFailure;
		res = worker.clientInterface.setFailureInjection.getReply(req);
		wait(ready(res));
		checkDiskFailureInjectionResult(res, worker);
	}

	// Sets the disk corruption request
	ACTOR void injectBitFlips(WorkerInterface worker, double percentage) {
		state Future<Void> res;
		SetFailureInjection::FlipBitsCommand flipBits;
		flipBits.percentBitFlips = percentage;
		SetFailureInjection req;
		req.flipBits = flipBits;
		res = worker.clientInterface.setFailureInjection.getReply(req);
		wait(ready(res));
		checkDiskFailureInjectionResult(res, worker);
	}

	// Choose random storage servers to inject disk failures.
	// We currently only inject disk failure on storage servers. Can be expanded to include
	// other worker types in future
	ACTOR template <class W>
	Future<Void> diskFailureInjectionClient(Database cx, DiskFailureInjectionWorkload* self) {
		wait(::delay(self->startDelay));
		state double lastTime = now();
		state std::vector<W> machines;
		state int throttledWorkers = 0;
		state int corruptedWorkers = 0;
		loop {
			wait(poisson(&lastTime, 1));
			try {
				std::pair<std::vector<W>, int> m = wait(getStorageWorkers(cx, self->dbInfo, false));
				if (m.second > 0) {
					throw operation_failed();
				}
				machines = std::move(m.first);
			} catch (Error& e) {
				// If we failed to get a complete list of storage servers, we can't inject failure events
				// But don't throw the error in that case
				TraceEvent("ChaosCouldNotGetStorages").error(e);
				continue;
			}
			auto machine = deterministicRandom()->randomChoice(machines);

			// If we have already chosen this worker, then just continue
			if (find(self->chosenWorkers.begin(), self->chosenWorkers.end(), machine.address()) !=
			    self->chosenWorkers.end()) {
				continue;
			}

			// Keep track of chosen workers for verification purpose
			self->chosenWorkers.emplace_back(machine.address());
			if (self->throttleDisk && (throttledWorkers++ < self->workersToThrottle))
				self->injectDiskDelays(machine, self->stallInterval, self->stallPeriod, self->throttlePeriod);
			if (self->corruptFile && (corruptedWorkers++ < self->workersToCorrupt)) {
				if (g_simulator == g_network)
					g_simulator->corruptWorkerMap[machine.address()] = true;
				self->injectBitFlips(machine, self->percentBitFlips);
			}
		}
	}

	// Resend the chaos event to previously chosen workers, in case some workers got restarted and lost their chaos
	// config
	ACTOR static Future<Void> reSendChaos(DiskFailureInjectionWorkload* self) {
		state int throttledWorkers = 0;
		state int corruptedWorkers = 0;
		state std::map<NetworkAddress, WorkerInterface> workersMap;
		state std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
		for (auto worker : workers) {
			workersMap[worker.interf.address()] = worker.interf;
		}
		TraceEvent("ResendChaos")
		    .detail("ChosenWorkersSize", self->chosenWorkers.size())
		    .detail("FoundWorkers", workersMap.size())
		    .detail(
		        "ResendToNumber",
		        std::count_if(self->chosenWorkers.begin(),
		                      self->chosenWorkers.end(),
		                      [&map = std::as_const(workersMap)](auto const& addr) { return map.count(addr) > 0; }));
		for (auto& workerAddress : self->chosenWorkers) {
			auto itr = workersMap.find(workerAddress);
			if (itr != workersMap.end()) {
				if (self->throttleDisk && (throttledWorkers++ < self->workersToThrottle)) {
					self->injectDiskDelays(itr->second, self->stallInterval, self->stallPeriod, self->throttlePeriod);
				}
				if (self->corruptFile && (corruptedWorkers++ < self->workersToCorrupt)) {
					if (g_simulator == g_network)
						g_simulator->corruptWorkerMap[workerAddress] = true;
					self->injectBitFlips(itr->second, self->percentBitFlips);
				}
			}
		}
		return Void();
	}

	// Fetches chaosMetrics and verifies that chaos events are happening for enabled workers
	ACTOR static Future<int> chaosGetStatus(DiskFailureInjectionWorkload* self) {
		state int foundChaosMetrics = 0;
		state std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));

		Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventsFuture;
		latestEventsFuture = latestEventOnWorkers(workers, "ChaosMetrics");
		state Optional<std::pair<WorkerEvents, std::set<std::string>>> workerEvents = wait(latestEventsFuture);

		state WorkerEvents cMetrics = workerEvents.present() ? workerEvents.get().first : WorkerEvents();

		// Check if any of the chosen workers for chaos events have non-zero chaosMetrics
		try {
			for (auto& workerAddress : self->chosenWorkers) {
				auto chaosMetrics = cMetrics.find(workerAddress);
				if (chaosMetrics != cMetrics.end()) {
					// we expect diskDelays to be non-zero for chosenWorkers for throttleDisk event
					if (self->throttleDisk) {
						int diskDelays = chaosMetrics->second.getInt("DiskDelays");
						if (diskDelays > 0) {
							foundChaosMetrics += diskDelays;
						}
					}

					// we expect bitFlips to be non-zero for chosenWorkers for corruptFile event
					if (self->corruptFile) {
						int bitFlips = chaosMetrics->second.getInt("BitFlips");
						if (bitFlips > 0) {
							foundChaosMetrics += bitFlips;
						}
					}
				}
			}
		} catch (Error& e) {
			// it's possible to get an empty event, it's okay to ignore
			if (e.code() != error_code_attribute_not_found) {
				TraceEvent(SevError, "ChaosGetStatus").error(e);
				throw e;
			}
		}

		return foundChaosMetrics;
	}

	// Periodically re-send the chaos event in case of a process restart
	ACTOR static Future<Void> periodicEventBroadcast(DiskFailureInjectionWorkload* self) {
		wait(::delay(self->startDelay));
		state double start = now();
		state double elapsed = 0.0;

		loop {
			wait(delayUntil(start + elapsed));
			wait(reSendChaos(self));
			elapsed += self->periodicBroadcastInterval;
			wait(delayUntil(start + elapsed));
			int foundChaosMetrics = wait(chaosGetStatus(self));
			if (foundChaosMetrics > 0) {
				TraceEvent("FoundChaos")
				    .detail("ChaosMetricCount", foundChaosMetrics)
				    .detail("ClientID", self->clientId);
				return Void();
			}
		}
	}
};
WorkloadFactory<DiskFailureInjectionWorkload> DiskFailureInjectionWorkloadFactory;
FailureInjectorFactory<DiskFailureInjectionWorkload> DiskFailureInjectionWorkloadFailureInjectionFactory;
