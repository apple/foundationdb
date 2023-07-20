/**
 * IRKBlobMonitor.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/IRKConfigurationMonitor.h"
#include "fdbserver/IRKRecoveryTracker.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Deque.h"

// Responsible for tracking the current throttling-relevant statistics
// for blob workers
class IRKBlobMonitor {
public:
	virtual ~IRKBlobMonitor() = default;

	// Returns a deque of timestamp, version pairs, where the version is the minimum blob version
	// across all blob workers at the corresponding timestamp.
	virtual Deque<std::pair<double, Version>> const& getVersionHistory() const& = 0;

	// Returns true iff any blob ranges exist in the database
	virtual bool hasAnyRanges() const = 0;

	// Returns the last time that the blob manager reported zero
	// blocked assignments
	virtual double getUnblockedAssignmentTime() const = 0;

	// Update the last unblocked assignment time to the current time
	// TODO: Refactor to remove mutable access here?
	virtual void setUnblockedAssignmentTimeNow() = 0;

	// Runs actors to periodically refresh throttling-relevant statistics
	// Returned Future should never be ready, but can be used to propagate errors
	virtual Future<Void> run(IRKConfigurationMonitor const&, IRKRecoveryTracker const&) = 0;
};

class RKBlobMonitor : public IRKBlobMonitor {
	friend class RKBlobMonitorImpl;
	Database db;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Deque<std::pair<double, Version>> versionHistory;
	bool anyBlobRanges;
	double unblockedAssignmentTime;

public:
	RKBlobMonitor(Database, Reference<AsyncVar<ServerDBInfo> const>);
	~RKBlobMonitor();
	Deque<std::pair<double, Version>> const& getVersionHistory() const& override;
	bool hasAnyRanges() const override;
	double getUnblockedAssignmentTime() const override;
	void setUnblockedAssignmentTimeNow() override;
	Future<Void> run(IRKConfigurationMonitor const&, IRKRecoveryTracker const&) override;
};

class MockRKBlobMonitor : public IRKBlobMonitor {
	Deque<std::pair<double, Version>> versionHistory;
	bool anyBlobRanges{ false };

public:
	~MockRKBlobMonitor() = default;
	Deque<std::pair<double, Version>> const& getVersionHistory() const& override { return versionHistory; }
	bool hasAnyRanges() const override { return anyBlobRanges; }
	double getUnblockedAssignmentTime() const override { return now(); }
	void setUnblockedAssignmentTimeNow() override {}
	Future<Void> run(IRKConfigurationMonitor const&, IRKRecoveryTracker const&) override { return Never(); }

	void setCurrentVersion(Version v);
	void addRange() { anyBlobRanges = true; }
};
