/*
 * BackupProgress.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include <map>
#include <set>
#include <tuple>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/core/BackupProgressTypes.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"

class BackupProgress : NonCopyable, ReferenceCounted<BackupProgress> {
public:
	BackupProgress(UID id, const std::map<LogEpoch, EpochTagsVersionsInfo>& infos) : dbgid(id), epochInfos(infos) {}
	~BackupProgress() {}

	void addBackupStatus(const WorkerBackupStatus& status);

	std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> getUnfinishedBackup();

	void setBackupStartedValue(Optional<Value> value) { backupStartedValue = value; }

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

	void updateTagVersions(std::map<Tag, Version>* tagVersions,
	                       std::set<Tag>* tags,
	                       const std::map<Tag, Version>& progress,
	                       Version endVersion,
	                       Version adjustedBeginVersion,
	                       LogEpoch epoch);

	const UID dbgid;
	const std::map<LogEpoch, EpochTagsVersionsInfo> epochInfos;
	std::map<LogEpoch, std::map<Tag, Version>> progress;
	std::map<LogEpoch, int32_t> epochTags;
	Optional<Value> backupStartedValue;
};

Future<Void> getBackupProgress(Database cx, UID dbgid, Reference<BackupProgress> bStatus, Severity severity);
