/*
 * BlobConnectionProvider.h
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

#ifndef BLOB_CONNECTION_PROVIDER_H
#define BLOB_CONNECTION_PROVIDER_H

#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobMetadataUtils.h"

struct BlobConnectionProvider : NonCopyable, ReferenceCounted<BlobConnectionProvider> {
	// chooses a partition and prepends the necessary prefix to the filename (if necessary) for writing a file, and
	// returns it and the backup container
	virtual std::pair<Reference<BackupContainerFileSystem>, std::string> createForWrite(std::string newFileName) = 0;

	// given a file, return the backup container and full file path necessary to access it. File path should be
	// something returned from createForWrite
	virtual Reference<BackupContainerFileSystem> getForRead(std::string filePath) = 0;

	virtual bool isExpired() const = 0;
	virtual bool needsRefresh() const = 0;
	virtual void update(Standalone<BlobMetadataDetailsRef> newBlobMetadata) = 0;

	virtual ~BlobConnectionProvider() {}

	static Reference<BlobConnectionProvider> newBlobConnectionProvider(std::string blobUrl);

	// FIXME: make this function dedupe location connections/providers across location ids
	static Reference<BlobConnectionProvider> newBlobConnectionProvider(Standalone<BlobMetadataDetailsRef> blobMetadata);
};

#endif