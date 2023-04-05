/*
 * BackupContainerLocalDirectory.h
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

#ifndef FDBCLIENT_BACKUP_CONTAINER_LOCAL_DIRECTORY_H
#define FDBCLIENT_BACKUP_CONTAINER_LOCAL_DIRECTORY_H
#pragma once

#include "fdbclient/BackupContainerFileSystem.h"
#include "flow/flow.h"

class BackupContainerLocalDirectory : public BackupContainerFileSystem,
                                      ReferenceCounted<BackupContainerLocalDirectory> {
public:
	void addref() final;
	void delref() final;

	static std::string getURLFormat();

	BackupContainerLocalDirectory(const std::string& url, Optional<std::string> const& encryptionKeyFileName);

	static Future<std::vector<std::string>> listURLs(const std::string& url);

	Future<Void> create() final;

	// The container exists if the folder it resides in exists
	Future<bool> exists() final;

	Future<Reference<IAsyncFile>> readFile(const std::string& path) final;

	Future<Reference<IBackupFile>> writeFile(const std::string& path) final;

	Future<Void> deleteFile(const std::string& path) final;

	Future<FilesAndSizesT> listFiles(const std::string& path, std::function<bool(std::string const&)>) final;

	Future<Void> deleteContainer(int* pNumDeleted) final;

private:
	std::string m_path;
};

#endif
