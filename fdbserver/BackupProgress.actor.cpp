#include "fdbserver/BackupProgress.actor.h"

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/actorcompiler.h" // This must be the last #include.

void BackupProgress::addBackupStatus(const WorkerBackupStatus& status) {
	auto& it = progress[status.epoch];
	auto lb = it.lower_bound(status.tag);
	if (lb != it.end() && status.tag == lb->first) {
		if (lb->second < status.version) {
			lb->second = status.version;
		}
	} else {
		it.insert(lb, { status.tag, status.version });
	}
}

std::map<std::pair<LogEpoch, Version>, std::map<Tag, Version>> BackupProgress::getUnfinishedBackup() {
	std::map<std::pair<LogEpoch, Version>, std::map<Tag, Version>> toRecruit;

	Version lastEpochEndVersion = invalidVersion;
	for (const auto& [epoch, tagsAndEndVersion] : epochTagsEndVersions) {
		if (lastEpochEndVersion == invalidVersion) {
			lastEpochEndVersion = getLastEpochEndVersion(epoch);
			TraceEvent("BW", dbgid).detail("Epoch", epoch).detail("LastEndVersion", lastEpochEndVersion);
		}
		std::set<Tag> tags = enumerateLogRouterTags(tagsAndEndVersion.first);
		const Version& endVersion = tagsAndEndVersion.second;
		std::map<Tag, Version> tagVersions;
		auto progressIt = progress.find(epoch);
		if (progressIt != progress.end()) {
			for (const auto& [tag, savedVersion] : progressIt->second) {
				tags.erase(tag);
				if (savedVersion < endVersion - 1) {
					tagVersions.insert({ tag, savedVersion });
					TraceEvent("BW", dbgid)
					    .detail("OldEpoch", epoch)
					    .detail("Tag", tag.toString())
					    .detail("Version", savedVersion)
					    .detail("EpochEndVersion", endVersion);
				}
			}
		}
		for (const Tag tag : tags) { // tags without progress data
			tagVersions.insert({ tag, lastEpochEndVersion });
			TraceEvent("BW", dbgid)
			    .detail("OldEpoch", epoch)
			    .detail("Tag", tag.toString())
			    .detail("Version", lastEpochEndVersion)
			    .detail("EpochEndVersion", endVersion);
		}
		if (!tagVersions.empty()) {
			toRecruit[{ epoch, endVersion }] = tagVersions;
		}
		lastEpochEndVersion = tagsAndEndVersion.second;
	}
	return toRecruit;
}

Version BackupProgress::getLastEpochEndVersion(LogEpoch epoch) {
	auto it = progress.lower_bound(epoch);
	if (it != progress.end()) {
		if (it == progress.begin()) {
			it = progress.end();
		} else {
			it--;
		}
	} else if (!progress.empty()) {
		it = --progress.end();
	}
	if (it == progress.end()) return 1;

	Version v = 0;
	for (const auto& [tag, savedVersion] : it->second) {
		v = std::max(v, savedVersion);
	}
	return v + 1;
}

// Returns each tag's savedVersion for all epochs.
ACTOR Future<Void> getBackupProgress(Database cx, UID dbgid, Reference<BackupProgress> bStatus) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Standalone<RangeResultRef> results = wait(tr.getRange(backupProgressKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

			for (auto& it : results) {
				const UID workerID = decodeBackupProgressKey(it.key);
				const WorkerBackupStatus status = decodeBackupProgressValue(it.value);
				bStatus->addBackupStatus(status);
				TraceEvent("GotBackupProgress", dbgid)
				    .detail("W", workerID)
				    .detail("Epoch", status.epoch)
				    .detail("Version", status.version)
				    .detail("Tag", status.tag.toString());
			}
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Set the progress to "startVersion - 1" so that if backup worker later
// doesn't update the progress, the next master can know. Otherwise, the next
// master didn't see the progress and assumes the work is done, thus losing
// some mutations in the backup.
/*
ACTOR Future<Void> setInitialBackupProgress(Reference<MasterData> self, Database cx, std::vector<std::pair<UID, Tag>>
idsTags) { state Transaction tr(cx);

    loop {
        try {
            tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
            tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
            tr.setOption(FDBTransactionOptions::LOCK_AWARE);

            const Version startVersion = self->logSystem->getStartVersion() - 1;
            const LogEpoch epoch = self->cstate.myDBState.recoveryCount;
            for (int i = 0; i < idsTags.size(); i++) {
                Key key = backupProgressKeyFor(idsTags[i].first);
                WorkerBackupStatus status(epoch, startVersion, idsTags[i].second);
                tr.set(key, backupProgressValue(status));
                tr.addReadConflictRange(singleKeyRange(key));
            }
            wait(tr.commit());
            return Void();
        } catch (Error& e) {
            wait(tr.onError(e));
        }
    }
}
*/