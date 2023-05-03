/**
 * IRKConfigurationMonitor.h
 */

#pragma once

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/flow.h"

// Responsible for monitoring the throttling-relevant
// components of the database configuration
class IRKConfigurationMonitor {
public:
	virtual ~IRKConfigurationMonitor() = default;
	virtual bool areBlobGranulesEnabled() const = 0;
	virtual int getStorageTeamSize() const = 0;
	virtual Optional<Key> getRemoteDC() const = 0;

	// Run actors to periodically refresh throttling-relevant statistics
	// Returned Future should never be ready, but can be used to propagate errors
	virtual Future<Void> run() = 0;
};

class RKConfigurationMonitor : public IRKConfigurationMonitor {
	friend class RKConfigurationMonitorImpl;
	Database db;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Optional<Key> remoteDC;
	DatabaseConfiguration configuration;

public:
	RKConfigurationMonitor(Database, Reference<AsyncVar<ServerDBInfo> const>);
	~RKConfigurationMonitor();
	bool areBlobGranulesEnabled() const override;
	int getStorageTeamSize() const override;
	Optional<Key> getRemoteDC() const override;
	Future<Void> run() override;
};

class MockRKConfigurationMonitor : public IRKConfigurationMonitor {
	int storageTeamSize;
	bool blobGranulesEnabled{ false };

public:
	explicit MockRKConfigurationMonitor(int storageTeamSize) : storageTeamSize(storageTeamSize) {}
	bool areBlobGranulesEnabled() const override { return blobGranulesEnabled; }
	int getStorageTeamSize() const override { return storageTeamSize; }
	Optional<Key> getRemoteDC() const override { return {}; }
	Future<Void> run() override { return Never(); }
	void enableBlobGranules() { blobGranulesEnabled = true; }
};
