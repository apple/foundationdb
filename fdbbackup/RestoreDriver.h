/*
 * RestoreDriver.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include <string>

void printRestoreUsage(std::string const& programName, bool devhelp);

enum class RestoreType { UNKNOWN, START, STATUS, ABORT, WAIT };

RestoreType getRestoreType(std::string const& name);

extern CSimpleOpt::SOption const rgRestoreOptions[];

class RestoreDriverState {
	WaitForComplete waitForDone{ false };
	std::string restoreClusterFileOrig;
	std::string restoreClusterFileDest;
	bool dryRun{ false };
	RestoreType restoreType{ RestoreType::UNKNOWN };
	Version targetVersion{ ::invalidVersion };
	Version beginVersion{ ::invalidVersion };
	std::string targetTimestamp;
	Optional<std::string> tagName;
	std::string restoreContainer;
	std::string addPrefix;
	std::string removePrefix;
	OnlyApplyMutationLogs onlyApplyMutationLogs{ false };
	InconsistentSnapshotOnly inconsistentSnapshotOnly{ false };
	Database db;
	Standalone<VectorRef<KeyRangeRef>> backupKeys;
	Optional<std::string> encryptionKeyFile;

	void createDatabase() { db = Database::createDatabase(restoreClusterFileDest, Database::API_VERSION_LATEST); }
	void initializeBackupKeys() {
		if (backupKeys.size() == 0) {
			backupKeys.push_back(backupKeys.arena(), normalKeys);
		}
	}

public:
	void processArg(std::string const& programName, CSimpleOpt const& args);
	WaitForComplete shouldWaitForDone() const { return waitForDone; }
	std::string const& getOrigClusterFile() const { return restoreClusterFileOrig; }
	std::string const& getDestClusterFile() const { return restoreClusterFileDest; }
	bool isDryRun() const { return dryRun; }
	RestoreType getRestoreType() const { return restoreType; }
	Version getTargetVersion() const { return targetVersion; }
	Version getBeginVersion() const { return beginVersion; }
	bool tagNameProvided() const { return tagName.present(); }
	std::string getTagName() const { return tagName.orDefault(BackupAgentBase::getDefaultTag().toString()); }
	std::string const& getTargetTimestamp() const { return targetTimestamp; }
	std::string const& getRestoreContainer() const { return restoreContainer; }
	std::string const& getAddPrefix() const { return addPrefix; }
	std::string const& getRemovePrefix() const { return removePrefix; }
	OnlyApplyMutationLogs shouldOnlyApplyMutationLogs() const { return onlyApplyMutationLogs; }
	InconsistentSnapshotOnly restoreInconsistentSnapshotOnly() const { return inconsistentSnapshotOnly; }
	Optional<std::string> const &getEncryptionKeyFile() const { return encryptionKeyFile; }
	Database const& getDatabase() const { return db; }
	Standalone<VectorRef<KeyRangeRef>> const& getBackupKeys() const { return backupKeys; }
	bool setup();
	void parseCommandLineArgs(int argc, char** argv);
};
