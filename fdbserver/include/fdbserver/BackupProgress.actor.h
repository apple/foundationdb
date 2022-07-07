/*
 * BackupProgress.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BACKUPPROGRESS_ACTOR_G_H)
#define FDBSERVER_BACKUPPROGRESS_ACTOR_G_H
#include "fdbserver/BackupProgress.actor.g.h"
#elif !defined(FDBSERVER_BACKUPPROGRESS_ACTOR_H)
#define FDBSERVER_BACKUPPROGRESS_ACTOR_H

#include <map>
#include <tuple>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystem.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class BackupProgress : NonCopyable, ReferenceCounted<BackupProgress> {
public:
	BackupProgress(UID id, const std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo>& infos)
	  : dbgid(id), epochInfos(infos) {}
	~BackupProgress() {}

	// Adds a backup status. If the tag already has an entry, then the max of
	// savedVersion is used.
	void addBackupStatus(const WorkerBackupStatus& status);

	// Returns a map of tuple<Epoch, endVersion, logRouterTags> : std::map<tag, savedVersion>, so that
	// the backup range should be [savedVersion + 1, endVersion) for the "tag" of the "Epoch".
	//
	// Specifically, the backup ranges for each old epoch are:
	//    if tag in tags_without_backup_progress:
	//        backup [epochBegin, endVersion)
	//    else if savedVersion < endVersion - 1 = knownCommittedVersion
	//        backup [savedVersion + 1, endVersion)
	std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> getUnfinishedBackup();

	// Set the value for "backupStartedKey"
	void setBackupStartedValue(Optional<Value> value) { backupStartedValue = value; }

	// Returns progress for an epoch.
	std::map<Tag, Version> getEpochStatus(LogEpoch epoch) const {
		const auto it = progress.find(epoch);
		if (it == progress.end())
			return {};
		return it->second;
	}

	void addref() { ReferenceCounted<BackupProgress>::addref(); }

	void delref() { ReferenceCounted<BackupProgress>::delref(); }

private:
	std::set<Tag> enumerateLogRouterTags(int logRouterTags) const {
		std::set<Tag> tags;
		for (int i = 0; i < logRouterTags; i++) {
			tags.insert(Tag(tagLocalityLogRouter, i));
		}
		return tags;
	}

	// For each tag in progress, the saved version is smaller than endVersion - 1,
	// add {tag, savedVersion+1} to tagVersions and remove the tag from "tags".
	void updateTagVersions(std::map<Tag, Version>* tagVersions,
	                       std::set<Tag>* tags,
	                       const std::map<Tag, Version>& progress,
	                       Version endVersion,
	                       Version adjustedBeginVersion,
	                       LogEpoch epoch);

	const UID dbgid;

	// Note this MUST be iterated in ascending order.
	const std::map<LogEpoch, ILogSystem::EpochTagsVersionsInfo> epochInfos;

	// Backup progress saved in the system keyspace. Note there can be multiple
	// progress status for a tag in an epoch due to later epoch trying to fill
	// the gap. "progress" MUST be iterated in ascending order.
	std::map<LogEpoch, std::map<Tag, Version>> progress;

	// LogRouterTags for each epoch obtained by decoding backup progress from
	// the system keyspace.
	std::map<LogEpoch, int32_t> epochTags;

	// Value of the "backupStartedKey".
	Optional<Value> backupStartedValue;
};

ACTOR Future<Void> getBackupProgress(Database cx, UID dbgid, Reference<BackupProgress> bStatus, bool logging);

#include "flow/unactorcompiler.h"
#endif
