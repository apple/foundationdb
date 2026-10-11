/*
 * BackupContainerBlobStore.h
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

#ifndef FDBCLIENT_BACKUP_CONTAINER_BLOBSTORE_H
#define FDBCLIENT_BACKUP_CONTAINER_BLOBSTORE_H
#pragma once

#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/IBlobStore.h"

class BackupContainerBlobStore final : public BackupContainerFileSystem, ReferenceCounted<BackupContainerBlobStore> {
	Reference<IBlobStoreEndpoint> m_bstore;
	std::string m_name;

	// All backup data goes into a single bucket
	std::string m_bucket;

	// Optional object key prefix under which the data and index folder trees are placed.
	// Normalized to contain no leading or trailing slash.  Empty means the default bucket-root layout.
	std::string m_prefix;

	// if not used for backup, don't prefix paths with fdbbackup-specific logic
	bool isBackup;

	std::string dataPath(const std::string& path);

	// Get the path of the backups's index entry
	std::string indexEntry();

	friend class BackupContainerBlobStoreImpl;

public:
	BackupContainerBlobStore(Reference<IBlobStoreEndpoint> bstore,
	                         const std::string& name,
	                         const IBlobStoreEndpoint::ParametersT& params,
	                         const Optional<std::string>& encryptionKeyFileName,
	                         int encryptionBlockSize,
	                         bool isBackup);

	void addref() override;
	void delref() override;

	static std::string getURLFormat(bool withResource = true);

	static void validateBackupUrl(const std::string& resource);

	// Normalize and validate the value of the "prefix" URL parameter.  Strips leading and trailing
	// slashes; an empty result selects the default bucket-root layout.  Throws backup_invalid_url
	// if a path segment contains characters outside [A-Za-z0-9._-], a segment is empty, "." or "..",
	// or the first segment is the reserved data or backups folder.  If error is non-null, it is
	// populated before throwing.
	static std::string normalizePrefix(std::string prefix, std::string* error = nullptr) {
		// Strip leading and trailing slashes; an empty result selects the default bucket-root layout.
		size_t begin = prefix.find_first_not_of('/');
		if (begin == std::string::npos) {
			return "";
		}
		size_t end = prefix.find_last_not_of('/');
		prefix = prefix.substr(begin, end - begin + 1);

		auto invalidPrefix = [&]() {
			if (error != nullptr) {
				*error = "Invalid 'prefix' parameter value: '" + prefix + "'";
			}
			throw backup_invalid_url();
		};
		auto validSegment = [](const std::string& segment) {
			return !segment.empty() && segment != "." && segment != "..";
		};
		auto isAsciiAlphanumeric = [](char c) {
			return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
		};

		// Validate character set and path segments.  Reject empty ("//"), "." and ".." segments so the
		// prefix always denotes a well-formed key path that cannot traverse upward.
		std::string segment;
		for (auto c : prefix) {
			if (c == '/') {
				if (!validSegment(segment)) {
					invalidPrefix();
				}
				segment.clear();
				continue;
			}
			if (!isAsciiAlphanumeric(c) && c != '_' && c != '-' && c != '.') {
				invalidPrefix();
			}
			segment += c;
		}
		if (!validSegment(segment)) {
			invalidPrefix();
		}
		std::string firstSegment = prefix.substr(0, prefix.find('/'));
		if (firstSegment == "data" || firstSegment == "backups") {
			invalidPrefix();
		}
		return prefix;
	}

	Future<Reference<IAsyncFile>> readFile(const std::string& path) final;

	static Future<std::vector<std::string>> listURLs(Reference<IBlobStoreEndpoint> bstore,
	                                                 const std::string& bucket,
	                                                 const std::string& prefix);

	Future<Reference<IBackupFile>> writeFile(const std::string& path) final;

	Future<Void> writeEntireFile(const std::string& path, const std::string& contents) final;

	Future<Void> deleteFile(const std::string& path) final;

	Future<FilesAndSizesT> listFiles(const std::string& path, std::function<bool(std::string const&)> pathFilter) final;

	Future<Void> create() final;

	// The container exists if the index entry in the blob bucket exists
	Future<bool> exists() final;

	Future<Void> deleteContainer(int* pNumDeleted) final;

	std::string getBucket() const;

	std::string getPrefix() const;
};

#endif
