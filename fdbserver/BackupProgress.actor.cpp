/*
 * BackupProgress.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

	auto tagIt = epochTags.find(status.epoch);
	if (tagIt == epochTags.end()) {
		epochTags.insert({ status.epoch, status.totalTags });
	} else {
		ASSERT(status.totalTags == tagIt->second);
	}
}

void BackupProgress::updateTagVersions(std::map<Tag, Version>* tagVersions, std::set<Tag>* tags,
                                       const std::map<Tag, Version>& progress, Version endVersion, LogEpoch epoch) {
	for (const auto& [tag, savedVersion] : progress) {
		// If tag is not in "tags", it means the old epoch has more tags than
		// new epoch's tags. Just ignore the tag here.
		auto n = tags->erase(tag);
		if (n > 0 && savedVersion < endVersion - 1) {
			tagVersions->insert({ tag, savedVersion + 1 });
			TraceEvent("BackupVersionRange", dbgid)
			    .detail("OldEpoch", epoch)
			    .detail("Tag", tag.toString())
			    .detail("BeginVersion", savedVersion + 1)
			    .detail("EndVersion", endVersion);
		}
	}
}

std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> BackupProgress::getUnfinishedBackup() {
	std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> toRecruit;

	if (!backupStartedValue.present()) return toRecruit; // No active backups

	for (const auto& [epoch, info] : epochInfos) {
		std::set<Tag> tags = enumerateLogRouterTags(info.logRouterTags);
		std::map<Tag, Version> tagVersions;
		auto progressIt = progress.lower_bound(epoch);
		if (progressIt != progress.end() && progressIt->first == epoch) {
			updateTagVersions(&tagVersions, &tags, progressIt->second, info.epochEnd, epoch);
		} else {
			auto rit = std::find_if(
			    progress.rbegin(), progress.rend(),
			    [epoch = epoch](const std::pair<LogEpoch, std::map<Tag, Version>>& p) { return p.first < epoch; });
			if (!(rit == progress.rend())) {
				// A partial recovery can result in empty epoch that copies previous
				// epoch's version range. In this case, we should check previous
				// epoch's savedVersion.
				int savedMore = 0;
				for (auto [tag, version] : rit->second) {
					if (version >= info.epochBegin) {
						savedMore++;
					}
				}
				if (savedMore > 0) {
					// The logRouterTags are the same
					// ASSERT(info.logRouterTags == epochTags[rit->first]);

					updateTagVersions(&tagVersions, &tags, rit->second, info.epochEnd, epoch);
				}
			}
		}

		for (const Tag tag : tags) { // tags without progress data
			tagVersions.insert({ tag, info.epochBegin });
			TraceEvent("BackupVersionRange", dbgid)
			    .detail("OldEpoch", epoch)
			    .detail("Tag", tag.toString())
			    .detail("BeginVersion", info.epochBegin)
			    .detail("EndVersion", info.epochEnd);
		}
		if (!tagVersions.empty()) {
			toRecruit[{ epoch, info.epochEnd, info.logRouterTags }] = tagVersions;
		}
	}
	return toRecruit;
}

// Save each tag's savedVersion for all epochs into "bStatus".
ACTOR Future<Void> getBackupProgress(Database cx, UID dbgid, Reference<BackupProgress> bStatus) {
	state Transaction tr(cx);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Future<Optional<Value>> fValue = tr.get(backupStartedKey);
			state Standalone<RangeResultRef> results = wait(tr.getRange(backupProgressKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

			Optional<Value> value = wait(fValue);
			bStatus->setBackupStartedValue(value);
			for (auto& it : results) {
				const UID workerID = decodeBackupProgressKey(it.key);
				const WorkerBackupStatus status = decodeBackupProgressValue(it.value);
				bStatus->addBackupStatus(status);
				TraceEvent("GotBackupProgress", dbgid)
				    .detail("BackupWorker", workerID)
				    .detail("Epoch", status.epoch)
				    .detail("Version", status.version)
				    .detail("Tag", status.tag.toString())
				    .detail("TotalTags", status.totalTags);
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
	progress.setBackupStartedValue(Optional<Value>(LiteralStringRef("1")));

	std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> unfinished = progress.getUnfinishedBackup();

	ASSERT(unfinished.size() == 1);
	for (const auto [epochVersionCount, tagVersion] : unfinished) {
		ASSERT(std::get<0>(epochVersionCount) == epoch1 && std::get<1>(epochVersionCount) == end1 &&
		       std::get<2>(epochVersionCount) == 1);
		ASSERT(tagVersion.size() == 1 && tagVersion.begin()->first == tag1 && tagVersion.begin()->second == begin1);
	}

	const int saved1 = 50, totalTags = 1;
	WorkerBackupStatus status1(epoch1, saved1, tag1, totalTags);
	progress.addBackupStatus(status1);
	unfinished = progress.getUnfinishedBackup();
	ASSERT(unfinished.size() == 1);
	for (const auto [epochVersionCount, tagVersion] : unfinished) {
		ASSERT(std::get<0>(epochVersionCount) == epoch1 && std::get<1>(epochVersionCount) == end1 &&
		       std::get<2>(epochVersionCount) == 1);
		ASSERT(tagVersion.size() == 1 && tagVersion.begin()->first == tag1 && tagVersion.begin()->second == saved1 + 1);
	}

	return Void();
}