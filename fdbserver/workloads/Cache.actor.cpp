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

	std::string description() const override { return "CacheWorkload"; }
	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			// Call management API to cache keys under the given prefix
			return addCachedRange(cx, prefixRange(keyPrefix));
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<CacheWorkload> CacheWorkloadFactory("Cache");
