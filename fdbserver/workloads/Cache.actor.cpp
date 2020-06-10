#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct CacheWorkload : TestWorkload {
	Key keyPrefix;

	CacheWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		keyPrefix = unprintable( getOption(options, LiteralStringRef("keyPrefix"), LiteralStringRef("")).toString() );
	}

	virtual std::string description() { return "CacheWorkload"; }
	virtual Future<Void> setup( Database const& cx ) {
        if (clientId == 0) {
            //Call management API to cache keys under the given prefix
            return addCachedRange(cx, prefixRange(keyPrefix));
        }
        return Void();
	}
	virtual Future<Void> start( Database const& cx ) {
        return Void();
	}
	virtual Future<bool> check( Database const& cx ) {
        return true;
	}
	virtual void getMetrics( vector<PerfMetric>& m ) {
	}
};

WorkloadFactory<CacheWorkload> CacheWorkloadFactory("Cache");
