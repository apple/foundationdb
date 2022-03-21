/*
 * BlobStoreWorkload.h
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

#ifndef FDBSERVER_BLOB_STORE_WORKLOAD_H
#define FDBSERVER_BLOB_STORE_WORKLOAD_H
#pragma once

#include <cstdlib>
#include <string>

inline void updateBackupURL(std::string& backupURL,
                            const std::string& accessKeyEnvVar,
                            const std::string& accessKeyPlaceholder,
                            const std::string& secretKeyEnvVar,
                            const std::string& secretKeyPlaceholder) {
	std::string accessKey, secretKey;
	ASSERT(platform::getEnvironmentVar(accessKeyEnvVar.c_str(), accessKey));
	ASSERT(platform::getEnvironmentVar(secretKeyEnvVar.c_str(), secretKey));
	{
		auto pos = backupURL.find(accessKeyPlaceholder.c_str());
		backupURL.replace(pos, accessKeyPlaceholder.size(), accessKey);
	}
	{
		auto pos = backupURL.find(secretKeyPlaceholder.c_str());
		backupURL.replace(pos, secretKeyPlaceholder.size(), secretKey);
	}
}

#endif
