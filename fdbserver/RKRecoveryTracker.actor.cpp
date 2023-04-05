/**
 * RKRecoveryTracker.actor.cpp
 */

#include "fdbclient/Knobs.h"
#include "fdbserver/IRKRecoveryTracker.h"
#include "flow/actorcompiler.h" // must be last include

class RKRecoveryTrackerImpl {
public:
	ACTOR static Future<Void> run(RKRecoveryTracker* self) {
		loop {
			wait(self->dbInfo->onChange());

			// Entering recovery
			if (!self->recovering && self->dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				self->recovering = true;
				self->recoveryVersion = self->maxVersion;
				if (self->recoveryVersion == 0) {
					self->recoveryVersion = std::numeric_limits<Version>::max();
				}
				if (self->version_recovery.count(self->recoveryVersion)) {
					auto& it = self->version_recovery[self->recoveryVersion];
					double existingEnd = it.second.present() ? it.second.get() : now();
					double existingDuration = existingEnd - it.first;
					self->version_recovery[self->recoveryVersion] =
					    std::make_pair(now() - existingDuration, Optional<double>());
				} else {
					self->version_recovery[self->recoveryVersion] = std::make_pair(now(), Optional<double>());
				}
			}

			// Exiting recovery
			if (self->recovering && self->dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
				self->recovering = false;
				self->version_recovery[self->recoveryVersion].second = now();
			}
		}
	}
}; // class RKRecoveryTrackerImpl

RKRecoveryTracker::RKRecoveryTracker(Reference<AsyncVar<ServerDBInfo> const> dbInfo) : dbInfo(dbInfo), maxVersion(0) {
	recovering = dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS;
	recoveryVersion = std::numeric_limits<Version>::max();
	if (recovering) {
		version_recovery[recoveryVersion] = std::make_pair(now(), Optional<double>());
	}
}

RKRecoveryTracker::~RKRecoveryTracker() = default;

double RKRecoveryTracker::getRecoveryDuration(Version ver) const {
	auto it = version_recovery.lower_bound(ver);
	double recoveryDuration = 0;
	while (it != version_recovery.end()) {
		if (it->second.second.present()) {
			recoveryDuration += it->second.second.get() - it->second.first;
		} else {
			recoveryDuration += now() - it->second.first;
		}
		++it;
	}
	return recoveryDuration;
}

void RKRecoveryTracker::updateMaxVersion(Version version) {
	maxVersion = std::max(maxVersion, version);
	if (recoveryVersion == std::numeric_limits<Version>::max() && version_recovery.count(recoveryVersion)) {
		recoveryVersion = maxVersion;
		version_recovery[recoveryVersion] = version_recovery[std::numeric_limits<Version>::max()];
		version_recovery.erase(std::numeric_limits<Version>::max());
	}
}

void RKRecoveryTracker::cleanupOldRecoveries() {
	while (version_recovery.size() > CLIENT_KNOBS->MAX_GENERATIONS) {
		version_recovery.erase(version_recovery.begin());
	}
}

Version RKRecoveryTracker::getMaxVersion() const {
	return maxVersion;
}

Future<Void> RKRecoveryTracker::run() {
	return RKRecoveryTrackerImpl::run(this);
}
