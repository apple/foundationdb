/**
 * RKConfigurationMonitor.actor.cpp
 */

#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/IRKConfigurationMonitor.h"
#include "flow/actorcompiler.h" // must be last include

class RKConfigurationMonitorImpl {
public:
	ACTOR static Future<Void> monitorDBInfo(RKConfigurationMonitor* self) {
		loop {
			wait(self->dbInfo->onChange());
			self->remoteDC = self->dbInfo->get().logSystemConfig.getRemoteDcId();
		}
	}

	ACTOR static Future<Void> monitorDB(RKConfigurationMonitor* self) {
		loop {
			state ReadYourWritesTransaction tr(self->db);

			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					RangeReadResult results = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

					self->configuration.fromKeyValues((VectorRef<KeyValueRef>)results);

					state Future<Void> watchFuture =
					    tr.watch(moveKeysLockOwnerKey) || tr.watch(excludedServersVersionKey) ||
					    tr.watch(failedServersVersionKey) || tr.watch(excludedLocalityVersionKey) ||
					    tr.watch(failedLocalityVersionKey);
					wait(tr.commit());
					wait(watchFuture);
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}
}; // class RKConfigurationMonitorImpl

RKConfigurationMonitor::RKConfigurationMonitor(Database db, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
  : db(db), dbInfo(dbInfo) {
	remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();
}

RKConfigurationMonitor::~RKConfigurationMonitor() = default;

bool RKConfigurationMonitor::areBlobGranulesEnabled() const {
	return configuration.blobGranulesEnabled;
}

int RKConfigurationMonitor::getStorageTeamSize() const {
	return configuration.storageTeamSize;
}

Optional<Key> RKConfigurationMonitor::getRemoteDC() const {
	return remoteDC;
}

Future<Void> RKConfigurationMonitor::run() {
	return RKConfigurationMonitorImpl::monitorDB(this) && RKConfigurationMonitorImpl::monitorDBInfo(this);
}
