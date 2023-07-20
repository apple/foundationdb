/**
 * IRKRecoveryTracker.h
 */

#pragma once

#include "fdbclient/FDBTypes.h"
#include "flow/genericactors.actor.h"

class IRKRecoveryTracker {
public:
	virtual ~IRKRecoveryTracker() = default;

	// Returns the sum of the durations of all recoveries since the
	// the specified version.
	virtual double getRecoveryDuration(Version) const = 0;

	// Cleanup internal state of oldest recoveries so that only
	// CLIENT_KNOBS->MAX_GENERATIONS remain.
	virtual void cleanupOldRecoveries() = 0;

	// Update the maximum seen version based on a new version
	// seen by a GRV proxy.
	virtual void updateMaxVersion(Version version) = 0;

	// Returns the largest version received from a GRV proxy
	// (or 0 if no version has yet been received).
	virtual Version getMaxVersion() const = 0;

	// Run actors to periodically refresh recovery statistics.
	// Returned Future should never be ready, but can be used to propagate errors.
	virtual Future<Void> run() = 0;
};

class RKRecoveryTracker : public IRKRecoveryTracker {
	friend class RKRecoveryTrackerImpl;

	// Listens to updates on whether or not the cluster is in recovery
	Reference<IAsyncListener<bool>> inRecovery;

	// Maps recovery versions to recovery start and end times
	std::map<Version, std::pair<double, Optional<double>>> version_recovery;

	// Maximum version seen from a GRV proxy
	// (or 0 if no version has yet been received).
	Version maxVersion;

	// Version of the current ongoing recovery
	Version recoveryVersion;

	// Tracks whether the cluster was in recovery the last
	// time inRecovery was read
	bool recovering;

public:
	explicit RKRecoveryTracker(Reference<IAsyncListener<bool>> inRecovery);
	~RKRecoveryTracker();
	double getRecoveryDuration(Version) const override;
	void cleanupOldRecoveries() override;
	void updateMaxVersion(Version updateVersion) override;
	Version getMaxVersion() const override;
	Future<Void> run() override;
};

class MockRKRecoveryTracker : public IRKRecoveryTracker {
public:
	double getRecoveryDuration(Version) const override { return 0.0; }
	void cleanupOldRecoveries() override {}
	void updateMaxVersion(Version updateVersion) override {}
	Version getMaxVersion() const override { return 0; }
	Future<Void> run() override { return Never(); }
};
