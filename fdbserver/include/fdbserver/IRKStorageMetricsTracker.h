/**
 * IRKStorageMetricsTracker.h
 */

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/RatekeeperInterface.h"
#include "flow/IndexedSet.h"
#include "flow/IRandom.h"

enum limitReason_t {
	unlimited, // TODO: rename to workload?
	storage_server_write_queue_size, // 1
	storage_server_write_bandwidth_mvcc,
	storage_server_readable_behind,
	log_server_mvcc_write_bandwidth,
	log_server_write_queue, // 5
	storage_server_min_free_space, // a storage server's normal limits are being reduced by low free space
	storage_server_min_free_space_ratio, // a storage server's normal limits are being reduced by a low free space ratio
	log_server_min_free_space,
	log_server_min_free_space_ratio,
	storage_server_durability_lag, // 10
	storage_server_list_fetch_failed,
	blob_worker_lag,
	blob_worker_missing,
	limitReason_t_end
};

// Storages statistics for an individual storage server that are relevant for ratekeeper throttling
class StorageQueueInfo {
	uint64_t totalWriteCosts{ 0 };
	int totalWriteOps{ 0 };

	// refresh periodically
	TransactionTagMap<TransactionCommitCostEstimation> tagCostEst;

	UID ratekeeperID;
	Smoother smoothFreeSpace, smoothTotalSpace;
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;

	// Currently unused
	Smoother smoothDurableVersion, smoothLatestVersion;

public:
	bool valid;
	UID id;
	LocalityData locality;
	StorageQueuingMetricsReply lastReply;
	bool acceptingRequests;
	limitReason_t limitReason;
	std::vector<StorageQueuingMetricsReply::TagInfo> busiestReadTags, busiestWriteTags;

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

// Responsible for tracking the current throttling-relevant statistics
// for all storage servers in a database.
class IRKStorageMetricsTracker {
public:
	virtual ~IRKStorageMetricsTracker() = default;
	virtual Map<UID, StorageQueueInfo> const& getStorageQueueInfo() const = 0;
	virtual Future<Void> run(Smoother& smoothTotalDurableBytes) = 0;
	virtual bool ssListFetchTimedOut() const = 0;
};

// Tracks the current set of storage servers in a database and periodically
// pulls throttling-relevant statistics from these storage servers.
class RKStorageMetricsTracker : public IRKStorageMetricsTracker {
	friend class RKStorageMetricsTrackerImpl;
	ActorCollection actors;
	UID ratekeeperId;
	Database db;
	RatekeeperInterface rkInterf; // TODO: Only hold needed RequestStream?
	double lastSSListFetchedTimestamp;
	PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges;
	// Maps storage server ID to storage server interface
	std::unordered_map<UID, StorageServerInterface> storageServerInterfaces;
	Map<UID, StorageQueueInfo> storageQueueInfo;

	void updateCommitCostEstimation(UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation);

public:
	explicit RKStorageMetricsTracker(UID ratekeeperId, Database, RatekeeperInterface);
	~RKStorageMetricsTracker();
	Map<UID, StorageQueueInfo> const& getStorageQueueInfo() const override;
	Future<Void> run(Smoother& smoothTotalDurableBytes) override;
	bool ssListFetchTimedOut() const override;
};
