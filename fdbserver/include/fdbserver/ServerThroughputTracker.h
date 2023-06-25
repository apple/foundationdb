/**
 * ServerThroughputTracker.h
 */

#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKMetricsTracker.h"

enum class OpType {
	READ,
	WRITE,
};

// The ServerThroughputTracker class is responsible for tracking
// every throttlingId's reported throughput across all storage servers
class ServerThroughputTracker {
	class ThroughputCounters {
		Smoother readThroughput;
		Smoother writeThroughput;

	public:
		ThroughputCounters();
		void updateThroughput(double newThroughput, OpType opType);
		double getThroughput() const;
	};

	std::unordered_map<UID, ThrottlingIdMap<ThroughputCounters>> throughput;

public:
	// Returns all throttling IDs running significant workload on the specified storage server.
	std::vector<ThrottlingId> getThrottlingIdsAffectingStorageServer(UID storageServerId) const;

	// Updates throughput statistics based on new storage queue info
	void update(StorageQueueInfo const&);

	// Returns the current throughput for the provided throttling ID on the
	// provided storage server
	Optional<double> getThroughput(UID storageServerId, ThrottlingId const&) const;

	// Returns the current cluster-wide throughput for the provided throttling ID
	double getThroughput(ThrottlingId const&) const;

	// Returns the current throughput on the provided storage server, summed
	// across all throttling IDs
	Optional<double> getThroughput(UID storageServerId) const;

	// Used to remove a throttling ID which has expired
	void removeThrottlingId(ThrottlingId const&);

	// Returns the number of storage servers currently being tracked
	int storageServersTracked() const;
};
