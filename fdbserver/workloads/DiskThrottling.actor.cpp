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
    double throttleFrequency;
    double throttleMin;
    double throttleMax;
    DiskThrottlingWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
        enabled = !clientId; // only do this on the "first" client
        testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
        throttleFrequency = getOption(options, LiteralStringRef("throttleFrequency"), 0.0);
        throttleMin = getOption(options, LiteralStringRef("throttleMin"), 2.0);
        throttleMax = getOption(options, LiteralStringRef("throttleMax"), 2.0);
        TraceEvent("DiskThrottlingWorkload")
            .detail("TestDuration", testDuration).detail("Frequency", throttleFrequency)
            .detail("Min", throttleMin).detail("Max", throttleMax);
    }

    std::string description() const override {
        if (&g_simulator == g_network)
            return "DiskThrottling";
        else
            return "NoSimDiskThrolling";
    }

    Future<Void> setup(Database const& cx) override { return Void(); }

    Future<Void> start(Database const& cx) override {
        if (enabled) {
            return timeout(reportErrors(throttleDiskClient<WorkerInterface>(cx, this), "DiskThrottlingError"),
                           testDuration,
                           Void());
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

    ACTOR void doThrottle(WorkerInterface worker, double frequency, double minDelay, double maxDelay, double startDelay = 0.0) {
        state Future<Void> res;
        wait(::delay(startDelay));
        SetFailureInjection::ThrottleDiskCommand throttleDisk;
        throttleDisk.delayFrequency = frequency;
        throttleDisk.delayMin = minDelay;
        throttleDisk.delayMax = maxDelay;
        SetFailureInjection req;
        req.throttleDisk = throttleDisk;
        res = worker.clientInterface.setFailureInjection.getReply(req);
        wait(ready(res));
        checkDiskThrottleResult(res, worker);
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
            self->doThrottle(machine, self->throttleFrequency, self->throttleMin, self->throttleMax);
        }
    }
};
WorkloadFactory<DiskThrottlingWorkload> DiskThrottlingWorkloadFactory("DiskThrottling");
