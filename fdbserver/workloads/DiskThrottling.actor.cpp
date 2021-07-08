<<<<<<< HEAD
/*
 * DiskThrottling.actor.cpp
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

=======
>>>>>>> 96bde8919... Adding the Disk throttle workload file that I forgot earlier.
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DiskThrottlingWorkload : TestWorkload {
<<<<<<< HEAD
	bool enabled;
	double testDuration;
	double startDelay;
	double stallInterval;
	double stallPeriod;
	double throttlePeriod;
	double periodicCheckInterval;
	std::vector<NetworkAddress> chosenWorkers;
	std::vector<Future<Void>> clients;

	DiskThrottlingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		startDelay = getOption(options, LiteralStringRef("startDelay"), 0.0);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 60.0);
		stallInterval = getOption(options, LiteralStringRef("stallInterval"), 0.0);
		stallPeriod = getOption(options, LiteralStringRef("stallPeriod"), 60.0);
		throttlePeriod = getOption(options, LiteralStringRef("throttlePeriod"), 60.0);
		periodicCheckInterval = getOption(options, LiteralStringRef("periodicCheckInterval"), 10.0);
	}

	std::string description() const override {
		if (&g_simulator == g_network)
			return "DiskThrottling";
		else
			return "NoSimDiskThrolling";
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	// Starts the workload by -
	// 1. Starting the actor to periodically check chaosMetrics, and
	// 2. Starting the actor that injects failures on chosen storage servers
	Future<Void> start(Database const& cx) override {
		if (enabled) {
			clients.push_back(periodicMetricCheck(this));
			clients.push_back(throttleDiskClient<WorkerInterface>(cx, this));
			return timeout(waitForAll(clients), testDuration, Void());
		} else
			return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(vector<PerfMetric>& m) override {}

	static void checkDiskThrottleResult(Future<Void> res, WorkerInterface worker) {
		if (res.isError()) {
			auto err = res.getError();
			if (err.code() == error_code_client_invalid_operation) {
				TraceEvent(SevError, "ChaosDisabled")
				    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString());
			} else {
				TraceEvent(SevError, "DiskThrottlingFailed")
				    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString())
				    .error(err);
			}
		}
	}

	// Sets the disk failure request
	ACTOR void doThrottle(WorkerInterface worker,
	                      double stallInterval,
	                      double stallPeriod,
	                      double throttlePeriod,
	                      double startDelay) {
		state Future<Void> res;
		wait(::delay(startDelay));
		SetFailureInjection::DiskFailureCommand diskFailure;
		diskFailure.stallInterval = stallInterval;
		diskFailure.stallPeriod = stallPeriod;
		diskFailure.throttlePeriod = throttlePeriod;
		SetFailureInjection req;
		req.diskFailure = diskFailure;
		res = worker.clientInterface.setFailureInjection.getReply(req);
		wait(ready(res));
		checkDiskThrottleResult(res, worker);
	}

	// Currently unused, because we only inject disk failures on storage servers
	ACTOR static Future<Void> getAllWorkers(DiskThrottlingWorkload* self, std::vector<WorkerInterface>* result) {
		result->clear();
		std::vector<WorkerDetails> res =
		    wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest{}));
		for (auto& worker : res) {
			result->emplace_back(worker.interf);
		}
		return Void();
	}

	ACTOR static Future<Void> getAllStorageWorkers(Database cx,
	                                               DiskThrottlingWorkload* self,
	                                               std::vector<WorkerInterface>* result) {
		result->clear();
		state std::vector<WorkerInterface> res = wait(getStorageWorkers(cx, self->dbInfo, false));
		for (auto& worker : res) {
			result->emplace_back(worker);
		}
		return Void();
	}

	// Choose random storage servers to inject disk failures
	ACTOR template <class W>
	Future<Void> throttleDiskClient(Database cx, DiskThrottlingWorkload* self) {
		state double lastTime = now();
		state std::vector<W> machines;
		loop {
			wait(poisson(&lastTime, 1));
			wait(DiskThrottlingWorkload::getAllStorageWorkers(cx, self, &machines));
			auto machine = deterministicRandom()->randomChoice(machines);

			// If we have already chosen this worker, then just continue
			if (find(self->chosenWorkers.begin(), self->chosenWorkers.end(), machine.address()) !=
			    self->chosenWorkers.end())
				continue;

			// Keep track of chosen workers for verification purpose
			self->chosenWorkers.emplace_back(machine.address());
			self->doThrottle(machine, self->stallInterval, self->stallPeriod, self->throttlePeriod, self->startDelay);
		}
	}

	// Resend the chaos event to previosuly chosen workers, in case some workers got restarted and lost their chaos
	// config
	ACTOR static Future<Void> reSendChaos(DiskThrottlingWorkload* self) {
		std::vector<WorkerDetails> workers =
		    wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest{}));
		std::map<NetworkAddress, WorkerInterface> workersMap;
		for (auto worker : workers) {
			workersMap[worker.interf.address()] = worker.interf;
		}
		for (auto& workerAddress : self->chosenWorkers) {
			auto itr = workersMap.find(workerAddress);
			if (itr != workersMap.end())
				self->doThrottle(
				    itr->second, self->stallInterval, self->stallPeriod, self->throttlePeriod, self->startDelay);
		}
		return Void();
	}

	// For fetching chaosMetrics to ensure chaos events are happening
	// This is borrowed code from Status.actor.cpp
	struct WorkerEvents : std::map<NetworkAddress, TraceEventFields> {};

	ACTOR static Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventOnWorkers(
	    std::vector<WorkerDetails> workers,
	    std::string eventName) {
		try {
			state vector<Future<ErrorOr<TraceEventFields>>> eventTraces;
			for (int c = 0; c < workers.size(); c++) {
				EventLogRequest req =
				    eventName.size() > 0 ? EventLogRequest(Standalone<StringRef>(eventName)) : EventLogRequest();
				eventTraces.push_back(errorOr(timeoutError(workers[c].interf.eventLogRequest.getReply(req), 2.0)));
			}

			wait(waitForAll(eventTraces));

			std::set<std::string> failed;
			WorkerEvents results;

			for (int i = 0; i < eventTraces.size(); i++) {
				const ErrorOr<TraceEventFields>& v = eventTraces[i].get();
				if (v.isError()) {
					failed.insert(workers[i].interf.address().toString());
					results[workers[i].interf.address()] = TraceEventFields();
				} else {
					results[workers[i].interf.address()] = v.get();
				}
			}

			std::pair<WorkerEvents, std::set<std::string>> val;
			val.first = results;
			val.second = failed;

			return val;
		} catch (Error& e) {
			ASSERT(e.code() ==
			       error_code_actor_cancelled); // All errors should be filtering through the errorOr actor above
			throw;
		}
	}

	// Fetches chaosMetrics and verifies that chaos events are happening for enabled workers
	ACTOR static Future<Void> chaosGetStatus(DiskThrottlingWorkload* self) {
		std::vector<WorkerDetails> workers =
		    wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest{}));

		Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>> latestEventsFuture;
		latestEventsFuture = latestEventOnWorkers(workers, "ChaosMetrics");
		state Optional<std::pair<WorkerEvents, std::set<std::string>>> workerEvents = wait(latestEventsFuture);

		state WorkerEvents cMetrics = workerEvents.present() ? workerEvents.get().first : WorkerEvents();

		// Now verify that all chosen workers for chaos events have non-zero chaosMetrics
		std::vector<Future<Optional<std::pair<WorkerEvents, std::set<std::string>>>>> futures;

		for (auto& workerAddress : self->chosenWorkers) {
			auto chaosMetrics = cMetrics.find(workerAddress);
			if (chaosMetrics != cMetrics.end()) {
				int diskDelays = chaosMetrics->second.getInt("DiskDelays");

				// we expect diskDelays to be non-zero for chosenWorkers
				if (diskDelays == 0) {
					TraceEvent(SevError, "ChaosGetStatus")
					    .detail("OnEndpoint", workerAddress.toString())
					    .detail("DiskDelays", diskDelays);
				}
			}
		}

		return Void();
	}

	// Periodically fetches chaosMetrics to ensure that chaas events are taking place
	ACTOR static Future<Void> periodicMetricCheck(DiskThrottlingWorkload* self) {
		state double start = now();
		state double elapsed = 0.0;

		loop {
			// re-send the chaos event in case of a process restart
			wait(reSendChaos(self));
			elapsed += self->periodicCheckInterval;
			wait(delayUntil(start + elapsed));
			wait(chaosGetStatus(self));
		}
	}
=======
    bool enabled;
    double testDuration;
    double throttleFor;
    DiskThrottlingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
        enabled = !clientId; // only do this on the "first" client
        testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
        throttleFor = getOption(options, LiteralStringRef("throttleDelay"), 2.0);
        TraceEvent("DiskThrottlingWorkload").detail("TestDuration", testDuration).detail("For", throttleFor);
    }

    std::string description() const override {
        if (&g_simulator == g_network)
            return "DiskThrottling";
        else
            return "NoSimDiskThrolling";
    }

    Future<Void> setup(Database const& cx) override { return Void(); }

    Future<Void> start(Database const& cx) override {
        //if (&g_simulator == g_network && enabled) {
        //  TraceEvent("DiskThrottlingStart").detail("For", throttleFor);
        //  return timeout(reportErrors(throttleDiskClient<ISimulator::ProcessInfo*>(cx, this), "DiskThrottlingError"),
        //                 testDuration,
        //                 Void());
        //} else
        if (enabled) {
            return timeout(reportErrors(throttleDiskClient<WorkerInterface>(cx, this), "DiskThrottlingError"),
                           testDuration,
                           Void());
        } else
            return Void();
    }

    Future<bool> check(Database const& cx) override { return true; }

    void getMetrics(vector<PerfMetric>& m) override {}

    ACTOR void doThrottle_unused(ISimulator::ProcessInfo* machine, double t, double delay = 0.0) {
        wait(::delay(delay));
        TraceEvent("ThrottleDisk").detail("For", t);
        g_simulator.throttleDisk(machine, t);
        TraceEvent("ThrottleDiskSet").detail("For", t);
    }

    static void checkDiskThrottleResult(Future<Void> res, WorkerInterface worker) {
        if (res.isError()) {
            auto err = res.getError();
            if (err.code() == error_code_client_invalid_operation) {
                TraceEvent(SevError, "ChaosDisabled")
                    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString());
            } else {
                TraceEvent(SevError, "DiskThrottlingFailed")
                    .detail("OnEndpoint", worker.waitFailure.getEndpoint().addresses.address.toString())
                    .error(err);
            }
        }
    }

    ACTOR void doThrottle(WorkerInterface worker, double t, double delay = 0.0) {
        state Future<Void> res;
        wait(::delay(delay));
        SetFailureInjection::ThrottleDiskCommand throttleDisk;
        throttleDisk.time = t;
        SetFailureInjection req;
        req.throttleDisk = throttleDisk;
        TraceEvent("ThrottleDisk").detail("For", t);
        res = worker.clientInterface.setFailureInjection.getReply(req);
        wait(ready(res));
        checkDiskThrottleResult(res, worker);
    }

    static Future<Void> getAllWorkers_unused(DiskThrottlingWorkload* self, std::vector<ISimulator::ProcessInfo*>* result) {
        result->clear();
        *result = g_simulator.getAllProcesses();
        return Void();
    }

    static Future<Void> getAllStorageWorkers_unused(Database cx, DiskThrottlingWorkload* self, std::vector<ISimulator::ProcessInfo*>* result) {
        vector<ISimulator::ProcessInfo*> all = g_simulator.getAllProcesses();
        for (int i = 0; i < all.size(); i++)
            if (!all[i]->failed &&
                all[i]->name == std::string("Server") &&
                ((all[i]->startingClass == ProcessClass::StorageClass) ||
                 (all[i]->startingClass == ProcessClass::UnsetClass)))
                result->emplace_back(all[i]);
        return Void();
    }

    ACTOR static Future<Void> getAllWorkers(DiskThrottlingWorkload* self, std::vector<WorkerInterface>* result) {
        result->clear();
        std::vector<WorkerDetails> res =
            wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest{}));
        for (auto& worker : res) {
            result->emplace_back(worker.interf);
        }
        return Void();
    }

    ACTOR static Future<Void> getAllStorageWorkers(Database cx, DiskThrottlingWorkload* self, std::vector<WorkerInterface>* result) {
        result->clear();
        state std::vector<WorkerInterface> res = wait(getStorageWorkers(cx, self->dbInfo, false));
        for (auto& worker : res) {
            result->emplace_back(worker);
        }
        return Void();
    }

    ACTOR template <class W>
    Future<Void> throttleDiskClient(Database cx, DiskThrottlingWorkload* self) {
        state double lastTime = now();
        state double workloadEnd = now() + self->testDuration;
        state std::vector<W> machines;
        loop {
            wait(poisson(&lastTime, 1));
            wait(DiskThrottlingWorkload::getAllStorageWorkers(cx, self, &machines));
            auto machine = deterministicRandom()->randomChoice(machines);
            TraceEvent("DoThrottleDisk").detail("For", self->throttleFor);
            self->doThrottle(machine, self->throttleFor);
        }
    }
>>>>>>> 96bde8919... Adding the Disk throttle workload file that I forgot earlier.
};
WorkloadFactory<DiskThrottlingWorkload> DiskThrottlingWorkloadFactory("DiskThrottling");
