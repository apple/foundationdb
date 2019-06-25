#define FDB_API_VERSION 610
#include "foundationdb/fdb_c.h"
#undef DLLEXPORT
#include "workloads.h"
#include "IngestAdapter/EndpointLoadGenerator.h"
#include "IngestAdapter/ConsumerClient.h"

namespace {

struct IngestAdapterWorkload : FDBWorkload {
	static const std::string name;
	bool success = true;
	EndpointLoadGenerator requestGen;
	std::shared_ptr<ConsumerClientIF> consumerClient;

	std::string description() const override { return name; }
	bool init(FDBWorkloadContext* context) override { return true; }
	void setup(FDBDatabase* db, GenericPromise<bool> done) override {}

	void start(FDBDatabase* db, GenericPromise<bool> done) override {}

	void check(FDBDatabase* db, GenericPromise<bool> done) override { done.send(success); }
	void getMetrics(std::vector<FDBPerfMetric>& out) const override {}
};

const std::string IngestAdapterWorkload::name = "IngestAdapterWorkload";

} // namespace

FDBWorkloadFactoryT<IngestAdapterWorkload> ingestAdapterWorkload(IngestAdapterWorkload::name);
