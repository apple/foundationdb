#include "fdbserver/BackupProgress.actor.h"

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"
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

	for (const auto& [epoch, info] : epochInfos) {
		std::set<Tag> tags = enumerateLogRouterTags(info.logRouterTags);
		std::map<Tag, Version> tagVersions;
		auto progressIt = progress.find(epoch);
		if (progressIt != progress.end()) {
			for (const auto& [tag, savedVersion] : progressIt->second) {
				tags.erase(tag);
				if (savedVersion < info.epochEnd - 1) {
					tagVersions.insert({ tag, savedVersion });
					TraceEvent("BW", dbgid)
					    .detail("OldEpoch", epoch)
					    .detail("Tag", tag.toString())
					    .detail("BeginVersion", savedVersion)
					    .detail("EndVersion", info.epochEnd);
				}
			}
		}
		for (const Tag tag : tags) { // tags without progress data
			tagVersions.insert({ tag, info.epochBegin - 1 });
			TraceEvent("BW", dbgid)
			    .detail("OldEpoch", epoch)
			    .detail("Tag", tag.toString())
			    .detail("BeginVersion", info.epochBegin - 1)
			    .detail("EndVersion", info.epochEnd);
		}
		if (!tagVersions.empty()) {
			toRecruit[{ epoch, info.epochEnd }] = tagVersions;
		}
	}
	return toRecruit;
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

TEST_CASE("/BackupProgress/Unfinished") {
	std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo> epochInfos;

	const int epoch1 = 2, begin1 = 1, end1 = 100;
	const Tag tag1(tagLocalityLogRouter, 0);
	epochInfos.insert({ epoch1, ILogSystem::EpochTagsVersionsInfo(1, begin1, end1) });
	BackupProgress progress(UID(0, 0), epochInfos);

	std::map<std::pair<LogEpoch, Version>, std::map<Tag, Version>> unfinished = progress.getUnfinishedBackup();

	ASSERT(unfinished.size() == 1);
	for (const auto [epochVersion, tagVersion] : unfinished) {
		ASSERT(epochVersion.first == epoch1 && epochVersion.second == end1);
		ASSERT(tagVersion.size() == 1 && tagVersion.begin()->first == tag1 && tagVersion.begin()->second == begin1 - 1);
	}

	const int saved1 = 50;
	WorkerBackupStatus status1(epoch1, 50, tag1);
	progress.addBackupStatus(status1);
	unfinished = progress.getUnfinishedBackup();
	ASSERT(unfinished.size() == 1);
	for (const auto [epochVersion, tagVersion] : unfinished) {
		ASSERT(epochVersion.first == epoch1 && epochVersion.second == end1);
		ASSERT(tagVersion.size() == 1 && tagVersion.begin()->first == tag1 && tagVersion.begin()->second == saved1);
	}

	return Void();
}