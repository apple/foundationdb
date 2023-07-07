/**
 * IRKThroughputTracker.h
 */

#pragma once

#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKMetricsTracker.h"

// Tracks the cluster-wide throughput (in bytes/second) of each throttlingId
class IRKThroughputTracker {
public:
	virtual ~IRKThroughputTracker() = default;

	// Returns the current cluster-wide throughput for the provided throttling ID
	virtual double getThroughput(ThrottlingId const&) const = 0;
};

// The ServerThroughputTracker class is responsible for tracking
// every throttlingId's reported throughput across all storage servers
class ServerThroughputTracker : public IRKThroughputTracker {
	enum class OpType {
		READ,
		WRITE,
	};

	class ThroughputCounters {
		Smoother readThroughput;
		Smoother writeThroughput;

	public:
		ThroughputCounters();
		void updateThroughput(double newThroughput, OpType);
		double getThroughput() const;
	};

	std::unordered_map<UID, ThrottlingIdMap<ThroughputCounters>> throughput;

	void cleanupUnseenStorageServers(std::unordered_set<UID> const& seen);

	static void cleanupUnseenThrottlingIds(ThrottlingIdMap<ThroughputCounters>&,
	                                       std::unordered_set<ThrottlingId> const& seenReadThrottlingId,
	                                       std::unordered_set<ThrottlingId> const& seenWriteThrottlingId);

public:
	~ServerThroughputTracker();

	// Returns all throttling IDs running significant workload on the specified storage server.
	std::vector<ThrottlingId> getThrottlingIdsAffectingStorageServer(UID storageServerId) const;

	// Updates throughput statistics based on new storage queue info
	void update(Map<UID, StorageQueueInfo> const&);

	// Returns the current throughput for the provided throttling ID on the
	// provided storage server
	Optional<double> getThroughput(UID storageServerId, ThrottlingId const&) const;

	double getThroughput(ThrottlingId const&) const override;

	// Returns the current throughput on the provided storage server, summed
	// across all throttling IDs
	Optional<double> getThroughput(UID storageServerId) const;

	// Used to remove a throttling ID which has expired
	void removeThrottlingId(ThrottlingId const&);

	// Returns the number of storage servers currently being tracked
	int storageServersTracked() const;
};

// The ClientThroughputTracker class is responsible for tracking
// every throttlingId's cluster-wide throughput from statistics
// reported originally from clients
class ClientThroughputTracker : public IRKThroughputTracker {
	struct ThroughputSmoother {
		HoltLinearSmoother smoother;
		ThroughputSmoother();
	};

	ThrottlingIdMap<ThroughputSmoother> throughput;

public:
	~ClientThroughputTracker();

	double getThroughput(ThrottlingId const&) const override;

	// Updates per-throttlingId throughput statistics based on newly
	// reported throughput metrics from a GRV proxy
	void update(ThrottlingIdMap<uint64_t> const& newThroughput);

	// Used to remove a throttling ID which has expired
	void removeThrottlingId(ThrottlingId const&);
};
