/*
 * BlobTLSConfig.h
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

#ifndef FDBCLIENT_BLOBTLSCONFIG_H
#define FDBCLIENT_BLOBTLSCONFIG_H

// TLS and blob credential setup.
// Copied from fdbbackup/BackupTLSConfig.* and renamed BlobTLSConfig.
struct BlobTLSConfig {
	std::string tlsCertPath, tlsKeyPath, tlsCAPath, tlsPassword, tlsVerifyPeers;
	std::vector<std::string> blobCredentials;

	// Returns if TLS setup is successful
	bool setupTLS();

	// Sets up blob crentials. Add the file specified by FDB_BLOB_CREDENTIALS as well.
	// Note this must be called after g_network is set up.
	void setupBlobCredentials();
};
#endif