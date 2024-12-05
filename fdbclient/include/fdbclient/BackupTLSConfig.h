/*
 * BackupTLSConfig.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBBACKUP_BACKUPTLSCONFIG_H
#define FDBBACKUP_BACKUPTLSCONFIG_H
#pragma once

#include <string>
#include <vector>

// TLS and blob credentials for backups and setup for these credentials.
struct BackupTLSConfig {
	std::string tlsCertPath, tlsKeyPath, tlsCAPath, tlsPassword, tlsVerifyPeers;
	std::vector<std::string> blobCredentials;

	// Returns if TLS setup is successful
	bool setupTLS();

	// Sets up blob crentials. Add the file specified by FDB_BLOB_CREDENTIALS as well.
	// Note this must be called after g_network is set up.
	void setupBlobCredentials();
};

#endif // FDBBACKUP_BACKUPTLSCONFIG_H
