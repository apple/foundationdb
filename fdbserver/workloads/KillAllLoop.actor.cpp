#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "flow/actorcompiler.h" // has to be last include


struct KillAllLoopWorkload : TestWorkload {
    double startTime;
    int numKills;
    double delayBetweenKills;

    KillAllLoopWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		startTime = getOption(options, LiteralStringRef("startTime"), 0.0);
		numKills = getOption(options, LiteralStringRef("numKills"), g_random->randomInt(1,10));
        delayBetweenKills = getOption(options, LiteralStringRef("delayBetweenKills"), 0.0);
        ASSERT(numKills > 0 && startTime >= 0 and delayBetweenKills >= 0);
		TraceEvent(SevInfo, "KillAllLoopSetup").detail("StartTime", startTime).detail("NumKills", numKills).detail("DelayBetweenKills", delayBetweenKills);
	}

    virtual std::string description() { return "KillAllLoop"; }

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

    ACTOR Future<Void> returnIfClusterRecovered(Database cx) {
        loop {
            state ReadYourWritesTransaction tr(cx);
            try {
                Version v = wait(tr.getReadVersion());
                TraceEvent(SevInfo, "KillAllLoop_ClusterVersion").detail("Version", v);
                break;
            } catch( Error &e ) {
				wait( tr.onError(e) );
			}
        }
        return Void();
    }

    ACTOR Future<Void> _start(Database cx, KillAllLoopWorkload *self) {
        wait(delay(self->startTime));
        state int numKillsDone = 0;
        state ReadYourWritesTransaction tr(cx);
        loop {
            try {
                tr.reset();
                tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		        tr.setOption(FDBTransactionOptions::LOCK_AWARE);
                Standalone<RangeResultRef> kvs = wait(tr.getRange(KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces"), LiteralStringRef("\xff\xff\xff")), 1) );
                std::map<Key,Value> address_interface;
		        for( auto it : kvs ) {
			        auto ip_port = it.key.endsWith(LiteralStringRef(":tls")) ? it.key.removeSuffix(LiteralStringRef(":tls")) : it.key;
			        address_interface[ip_port] = it.value;
		        }
                for( auto it : address_interface ) {
					tr.set(LiteralStringRef("\xff\xff/reboot_worker"), it.second);
				}
                TraceEvent(SevInfo, "KillAllLoop_AttempedKillAll").detail("KillNum", numKillsDone);
                numKillsDone++;
                if(numKillsDone == self->numKills) {
                    break;
                }
                wait(delay(self->delayBetweenKills));
                wait(self->returnIfClusterRecovered(cx));
            } catch( Error &e ) {
				wait( tr.onError(e) );
			}
        }
        return Void();
    }

    virtual Future<Void> start(Database const& cx) {
        if(clientId != 0)
            return Void();
		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}
};

WorkloadFactory<KillAllLoopWorkload> KillAllLoopWorkloadFactory("KillAllLoop");
