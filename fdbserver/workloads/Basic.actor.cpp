#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// Basic workload which runs a single transaction in simulation.
struct BasicWorkload : TestWorkload {
	BasicWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	std::string description() override {
		return "Basic";
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			TraceEvent("ABC_start");
			return testKVStore(cx->clone());
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		return true;
	}

	void getMetrics(vector<PerfMetric>& m) override {}

	ACTOR Future<Void> testKVStore(Database cx) {
		state Reference<ReadYourWritesTransaction> tr =
				Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
		tr->set(KeyRef("foo2"), ValueRef("bar"));
		wait(tr->commit());
		// auto ver = tr->getCommittedVersion();

		/*
		UID id = deterministicRandom()->randomUniqueID();
		std::string fn = id.toString();
		state IKeyValueStore* store =  keyValueStoreMemory(fn, id, 500e6);

		wait(store->init());

		KeyValueRef kv = KeyValueRef(StringRef("foo"), StringRef("bar"));
		store->set(kv);
		*/

		return Void();
	}
};

WorkloadFactory<BasicWorkload> BasicWorkloadFactory("Basic");
