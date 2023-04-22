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

class IRKBlobMonitor {
public:
	virtual ~IRKBlobMonitor() = default;
	virtual Deque<std::pair<double, Version>> const& getBlobWorkerVersionHistory() const& = 0;
	virtual bool hasAnyRanges() const = 0;
	virtual double getUnblockedAssignmentTime() const = 0;

	// TODO: Refactor to remove mutable access here?
	virtual void setUnblockedAssignmentTimeNow() = 0;

	virtual Future<Void> run(IRKConfigurationMonitor const&, IRKRecoveryTracker const&) = 0;
};

class RKBlobMonitor : public IRKBlobMonitor {
	friend class RKBlobMonitorImpl;
	Database db;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Deque<std::pair<double, Version>> blobWorkerVersionHistory;
	bool anyBlobRanges;
	double unblockedAssignmentTime;

public:
	RKBlobMonitor(Database, Reference<AsyncVar<ServerDBInfo> const>);
	~RKBlobMonitor();
	Deque<std::pair<double, Version>> const& getBlobWorkerVersionHistory() const& override;
	bool hasAnyRanges() const override;
	double getUnblockedAssignmentTime() const override;
	void setUnblockedAssignmentTimeNow() override;
	Future<Void> run(IRKConfigurationMonitor const&, IRKRecoveryTracker const&) override;
};

class MockRKBlobMonitor : public IRKBlobMonitor {
	Deque<std::pair<double, Version>> blobWorkerVersionHistory;

public:
	~MockRKBlobMonitor() = default;
	Deque<std::pair<double, Version>> const& getBlobWorkerVersionHistory() const& override {
		return blobWorkerVersionHistory;
	}
	bool hasAnyRanges() const override { return false; }
	double getUnblockedAssignmentTime() const override { return now(); }
	void setUnblockedAssignmentTimeNow() override {}
	Future<Void> run(IRKConfigurationMonitor const&, IRKRecoveryTracker const&) override { return Never(); }
};
