/**
 * RKBlobMonitor.actor.cpp
 */

#include "fdbserver/IRKBlobMonitor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/QuietDatabase.h"
#include "flow/actorcompiler.h" // must be last include

class RKBlobMonitorImpl {
	ACTOR static Future<bool> checkAnyBlobRanges(Database db) {
		state Transaction tr(db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				// FIXME: check if any active ranges. This still returns true if there are inactive ranges, but it
				// mostly serves its purpose to allow setting blob_granules_enabled=1 on a cluster that has no blob
				// workers currently.
				RangeReadResult anyData = wait(tr.getRange(blobRangeKeys, 1));
				return !anyData.empty();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

public:
	ACTOR static Future<Void> run(RKBlobMonitor* self,
	                              IRKConfigurationMonitor const* configurationMonitor,
	                              IRKRecoveryTracker const* recoveryTracker) {
		state std::vector<BlobWorkerInterface> blobWorkers;
		state int workerFetchCount = 0;
		state double lastStartTime = 0;
		state double startTime = 0;
		state bool blobWorkerDead = false;
		state double lastLoggedTime = 0;

		loop {
			while (!configurationMonitor->areBlobGranulesEnabled()) {
				// FIXME: clear blob worker state if granules were previously enabled?
				wait(delay(SERVER_KNOBS->SERVER_LIST_DELAY));
			}

			state Version grv;
			state Future<Void> blobWorkerDelay =
			    delay(SERVER_KNOBS->METRIC_UPDATE_RATE * FLOW_KNOBS->DELAY_JITTER_OFFSET);
			int fetchAmount = SERVER_KNOBS->BW_FETCH_WORKERS_INTERVAL /
			                  (SERVER_KNOBS->METRIC_UPDATE_RATE * FLOW_KNOBS->DELAY_JITTER_OFFSET);
			if (++workerFetchCount == fetchAmount || blobWorkerDead) {
				workerFetchCount = 0;
				state Future<bool> anyBlobRangesCheck = checkAnyBlobRanges(self->db);
				wait(store(blobWorkers, getBlobWorkers(self->db, true, &grv)));
				wait(store(self->anyBlobRanges, anyBlobRangesCheck));
			} else {
				grv = recoveryTracker->getMaxVersion();
			}

			lastStartTime = startTime;
			startTime = now();

			if (blobWorkers.size() > 0) {
				state Future<Optional<BlobManagerBlockedReply>> blockedAssignments;
				if (self->dbInfo->get().blobManager.present()) {
					blockedAssignments = timeout(
					    brokenPromiseToNever(self->dbInfo->get().blobManager.get().blobManagerBlockedReq.getReply(
					        BlobManagerBlockedRequest())),
					    SERVER_KNOBS->BLOB_WORKER_TIMEOUT);
				}
				state std::vector<Future<Optional<MinBlobVersionReply>>> aliveVersions;
				aliveVersions.reserve(blobWorkers.size());
				for (auto& it : blobWorkers) {
					MinBlobVersionRequest req;
					req.grv = grv;
					aliveVersions.push_back(timeout(brokenPromiseToNever(it.minBlobVersionRequest.getReply(req)),
					                                SERVER_KNOBS->BLOB_WORKER_TIMEOUT));
				}
				if (blockedAssignments.isValid()) {
					wait(success(blockedAssignments));
					if (blockedAssignments.get().present() && blockedAssignments.get().get().blockedAssignments == 0) {
						self->unblockedAssignmentTime = now();
					}
				}
				wait(waitForAll(aliveVersions));
				Version minVer = grv;
				blobWorkerDead = false;
				int minIdx = 0;
				for (int i = 0; i < blobWorkers.size(); i++) {
					if (aliveVersions[i].get().present()) {
						if (aliveVersions[i].get().get().version < minVer) {
							minVer = aliveVersions[i].get().get().version;
							minIdx = i;
						}
					} else {
						blobWorkerDead = true;
						minVer = 0;
						minIdx = i;
						break;
					}
				}
				if (minVer > 0 && blobWorkers.size() > 0 &&
				    now() - self->unblockedAssignmentTime < SERVER_KNOBS->BW_MAX_BLOCKED_INTERVAL) {
					while (!self->versionHistory.empty() && minVer < self->versionHistory.back().second) {
						self->versionHistory.pop_back();
					}
					self->versionHistory.push_back(std::make_pair(now(), minVer));
				}
				while (self->versionHistory.size() > SERVER_KNOBS->MIN_BW_HISTORY &&
				       self->versionHistory[1].first <
				           self->versionHistory.back().first - SERVER_KNOBS->BW_ESTIMATION_INTERVAL) {
					self->versionHistory.pop_front();
				}
				if (now() - lastLoggedTime > SERVER_KNOBS->BW_RW_LOGGING_INTERVAL) {
					lastLoggedTime = now();
					TraceEvent("RkMinBlobWorkerVersion")
					    .detail("BWVersion", minVer)
					    .detail("MaxVer", recoveryTracker->getMaxVersion())
					    .detail("MinId", blobWorkers.size() > 0 ? blobWorkers[minIdx].id() : UID())
					    .detail("BMBlocked",
					            now() - self->unblockedAssignmentTime >= SERVER_KNOBS->BW_MAX_BLOCKED_INTERVAL);
				}
			}
			wait(blobWorkerDelay);
		}
	}

}; // class RKBlobMonitorImpl

RKBlobMonitor::RKBlobMonitor(Database db, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
  : db(db), dbInfo(dbInfo), anyBlobRanges(false), unblockedAssignmentTime(now()) {}

RKBlobMonitor::~RKBlobMonitor() = default;

Deque<std::pair<double, Version>> const& RKBlobMonitor::getVersionHistory() const& {
	return versionHistory;
}

bool RKBlobMonitor::hasAnyRanges() const {
	return anyBlobRanges;
}

double RKBlobMonitor::getUnblockedAssignmentTime() const {
	return unblockedAssignmentTime;
}

void RKBlobMonitor::setUnblockedAssignmentTimeNow() {
	unblockedAssignmentTime = now();
}

Future<Void> RKBlobMonitor::run(IRKConfigurationMonitor const& configurationMonitor,
                                IRKRecoveryTracker const& recoveryTracker) {
	return RKBlobMonitorImpl::run(this, &configurationMonitor, &recoveryTracker);
}

void MockRKBlobMonitor::setCurrentVersion(Version v) {
	versionHistory.emplace_back(now(), v);
	while (versionHistory.size() > SERVER_KNOBS->MIN_BW_HISTORY &&
	       versionHistory[1].first < now() - SERVER_KNOBS->BW_ESTIMATION_INTERVAL) {
		versionHistory.pop_front();
	}
}
