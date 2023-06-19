/**
 * IRKMetricsTracker.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/RatekeeperLimits.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"
#include "flow/IndexedSet.h"
#include "flow/IRandom.h"

// Stores statistics for an individual storage server that are relevant for ratekeeper throttling
class StorageQueueInfo {
	uint64_t totalWriteCosts{ 0 };
	int totalWriteOps{ 0 };

	// refresh periodically
	TransactionTagMap<TransactionCommitCostEstimation> tagCostEst;

	UID ratekeeperID;
	Smoother smoothFreeSpace, smoothTotalSpace;
	HoltLinearSmoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	HoltLinearSmoother smoothDurableVersion, smoothLatestVersion;

public:
	bool valid;
	UID id;
	LocalityData locality;
	StorageQueuingMetricsReply lastReply;
	bool acceptingRequests;
	limitReason_t limitReason;
	std::vector<BusyThrottlingIdInfo> busiestReadTags, busiestWriteTags;

	StorageQueueInfo(const UID& id, const LocalityData& locality);
	StorageQueueInfo(const UID& rateKeeperID, const UID& id, const LocalityData& locality);
	// Summarizes up the commit cost per storage server. Returns the UpdateCommitCostRequest for corresponding SS.
	UpdateCommitCostRequest refreshCommitCost(double elapsed);
	int64_t getStorageQueueBytes() const { return lastReply.bytesInput - smoothDurableBytes.smoothTotal(); }
	int64_t getDurabilityLag() const { return smoothLatestVersion.smoothTotal() - smoothDurableVersion.smoothTotal(); }
	void update(StorageQueuingMetricsReply const&, Smoother& smoothTotalDurableBytes);
	void addCommitCost(TransactionTagRef tagName, TransactionCommitCostEstimation const& cost);

	// Accessor methods for Smoothers
	double getSmoothFreeSpace() const { return smoothFreeSpace.smoothTotal(); }
	double getSmoothTotalSpace() const { return smoothTotalSpace.smoothTotal(); }
	double getSmoothDurableBytes() const { return smoothDurableBytes.smoothTotal(); }
	double getSmoothInputBytesRate() const { return smoothInputBytes.smoothRate(); }
	double getVerySmoothDurableBytesRate() const { return verySmoothDurableBytes.smoothRate(); }

	// Determine the ratio (limit / current throughput) for throttling based on write queue size
	Optional<double> getTagThrottlingRatio(int64_t storageTargetBytes, int64_t storageSpringBytes) const;
};

// Stores statistics for an individual tlog that are relevant for ratekeeper throttling
class TLogQueueInfo {
	HoltLinearSmoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothFreeSpace;
	Smoother smoothTotalSpace;

public:
	TLogQueuingMetricsReply lastReply;
	bool valid;
	UID id;

	// Accessor methods for Smoothers
	double getSmoothFreeSpace() const { return smoothFreeSpace.smoothTotal(); }
	double getSmoothTotalSpace() const { return smoothTotalSpace.smoothTotal(); }
	double getSmoothDurableBytes() const { return smoothDurableBytes.smoothTotal(); }
	double getSmoothInputBytesRate() const { return smoothInputBytes.smoothRate(); }
	double getVerySmoothDurableBytesRate() const { return verySmoothDurableBytes.smoothRate(); }

	explicit TLogQueueInfo(UID id);
	Version getLastCommittedVersion() const { return lastReply.v; }
	void update(TLogQueuingMetricsReply const& reply, Smoother& smoothTotalDurableBytes);
};

// Responsible for tracking the current throttling-relevant statistics
// for all storage servers and tlogs in a database.
class IRKMetricsTracker {
public:
	virtual ~IRKMetricsTracker() = default;

	// Returns a map of storage server id to throttling-relevant statistics
	// for all storage servers in the cluster.
	virtual Map<UID, StorageQueueInfo> const& getStorageQueueInfo() const& = 0;

	// Returns true iff the list of storage servers is too stale.
	virtual bool ssListFetchTimedOut() const = 0;

	// Returns a map of tlog id to throttling-relevant statistics for all tlogs
	// in the cluster.
	virtual Map<UID, TLogQueueInfo> const& getTlogQueueInfo() const& = 0;

	// Returns the smoothed rate at which bytes are being made durable
	// on the whole cluster
	virtual double getSmoothTotalDurableBytesRate() const = 0;

	// Run actors to periodically refresh throttling-relevant statistics
	// Returned Future should never be ready, but can be used to propagate errors
	virtual Future<Void> run() = 0;
};

// Tracks the current set of storage servers and tlogs in a database and periodically
// pulls throttling-relevant statistics from these storage servers.
//
// Also periodically receives write cost estimations for tags from commit proxies.
class RKMetricsTracker : public IRKMetricsTracker {
	friend class RKMetricsTrackerImpl;
	ActorCollection actors;
	UID ratekeeperId;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Database db;
	FutureStream<ReportCommitCostEstimationRequest> reportCommitCostEstimation;
	double lastSSListFetchedTimestamp;
	PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges;
	// Maps storage server ID to storage server interface
	std::unordered_map<UID, StorageServerInterface> storageServerInterfaces;
	Map<UID, StorageQueueInfo> storageQueueInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;
	Smoother smoothTotalDurableBytes;

	void updateCommitCostEstimation(UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation);

public:
	RKMetricsTracker(UID ratekeeperId,
	                 Database,
	                 FutureStream<ReportCommitCostEstimationRequest>,
	                 Reference<AsyncVar<ServerDBInfo> const>);
	~RKMetricsTracker();
	Map<UID, StorageQueueInfo> const& getStorageQueueInfo() const& override;
	bool ssListFetchTimedOut() const override;
	Map<UID, TLogQueueInfo> const& getTlogQueueInfo() const& override;
	double getSmoothTotalDurableBytesRate() const override;
	Future<Void> run() override;
};

class MockRKMetricsTracker : public IRKMetricsTracker {
	Map<UID, StorageQueueInfo> storageQueueInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;
	bool failedSSListFetch{ false };

public:
	Map<UID, StorageQueueInfo> const& getStorageQueueInfo() const& override { return storageQueueInfo; }
	Map<UID, TLogQueueInfo> const& getTlogQueueInfo() const& override { return tlogQueueInfo; }
	void failSSListFetch() { failedSSListFetch = true; }
	bool ssListFetchTimedOut() const override { return failedSSListFetch; }
	double getSmoothTotalDurableBytesRate() const override { return 0; }
	Future<Void> run() override { return Never(); }
	void updateStorageQueueInfo(StorageQueueInfo const& ss) { storageQueueInfo.insert(mapPair(ss.id, ss)); }
	void updateTLogQueueInfo(TLogQueueInfo const& tl) { tlogQueueInfo.insert(mapPair(tl.id, tl)); }
};
