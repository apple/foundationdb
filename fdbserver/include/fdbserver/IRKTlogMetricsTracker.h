/**
 * IRKTlogMetricsTracker.h
 */

#include "fdbrpc/Smoother.h"
#include "flow/flow.h"
#include "flow/IndexedSet.h"
#include "flow/IRandom.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"

// Stores statistics for an individual tlog that are relevant for ratekeeper throttling
class TLogQueueInfo {
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
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
// for all tlogs in a database.
class IRKTlogMetricsTracker {
public:
	virtual ~IRKTlogMetricsTracker() = default;
	virtual Map<UID, TLogQueueInfo> const& getTlogQueueInfo() const = 0;
	virtual Future<Void> run(Smoother& smoothTotalDurableBytes) = 0;
};

// Tracks the current set of tlogs in a database and periodically
// pulls throttling-relevant statistics from these tlogs.
class RKTlogMetricsTracker : public IRKTlogMetricsTracker {
	friend class RKTlogMetricsTrackerImpl;
	UID ratekeeperId;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;

public:
	RKTlogMetricsTracker(UID ratekeeperId, Reference<AsyncVar<ServerDBInfo> const>);
	~RKTlogMetricsTracker();
	Map<UID, TLogQueueInfo> const& getTlogQueueInfo() const override;
	Future<Void> run(Smoother& smoothTotalDurableBytes) override;
};
