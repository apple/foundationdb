#define FDB_API_VERSION 610
#include "foundationdb/fdb_c.h"
#undef DLLEXPORT
#include "workloads.h"
#include <polevault/SCLoadGenerator.h>
#include <polevault/ConsumerClient.h>

namespace {

struct SimpleWorkload : FDBWorkload {
	static const std::string name;
	SCLoadGenerator snowcannonGen;
	std::shared_ptr<ConsumerClientIF> consumerClient;

	std::string description() const override { return name; }
	bool init(FDBWorkloadContext* context) override {}
	void setup(FDBDatabase* db, GenericPromise<bool> done) override {}

	void start(FDBDatabase* db, GenericPromise<bool> done) override {}

	void check(FDBDatabase* db, GenericPromise<bool> done) override { done.send(success); }
	void getMetrics(std::vector<FDBPerfMetric>& out) const override {}
};

const std::string SimpleWorkload::name = "SimpleWorkload";

} // namespace

FDBWorkloadFactoryT<SimpleWorkload> simpleWorkload(SimpleWorkload::name);
