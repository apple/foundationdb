#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DiskThrottlingWorkload : TestWorkload {
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
};
WorkloadFactory<DiskThrottlingWorkload> DiskThrottlingWorkloadFactory("DiskThrottling");
