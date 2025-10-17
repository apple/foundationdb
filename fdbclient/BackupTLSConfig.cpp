/*
 * BackupTLSConfig.cpp
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

#include <iostream>

#include "fdbclient/NativeAPI.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/network.h"

#include "fdbclient/BackupTLSConfig.h"

void BackupTLSConfig::setupBlobCredentials() {
	// Add blob credentials files from the environment to the list collected from the command line.
	const char* blobCredsFromENV = getenv("FDB_BLOB_CREDENTIALS");
	if (blobCredsFromENV != nullptr) {
		StringRef t((uint8_t*)blobCredsFromENV, strlen(blobCredsFromENV));
		do {
			StringRef file = t.eat(":");
			if (file.size() != 0)
				blobCredentials.push_back(file.toString());
		} while (t.size() != 0);
	}

	// Update the global blob credential files list
	std::vector<std::string>* pFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
	if (pFiles != nullptr) {
		for (auto& f : blobCredentials) {
			pFiles->push_back(f);
		}
	}
}

bool BackupTLSConfig::setupTLS() {
	if (tlsCertPath.size()) {
		try {
			setNetworkOption(FDBNetworkOptions::TLS_CERT_PATH, tlsCertPath);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS certificate path to " << tlsCertPath << " (" << e.what() << ")\n";
			return false;
		}
	}

	if (tlsCAPath.size()) {
		try {
			setNetworkOption(FDBNetworkOptions::TLS_CA_PATH, tlsCAPath);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS CA path to " << tlsCAPath << " (" << e.what() << ")\n";
			return false;
		}
	}
	if (tlsKeyPath.size()) {
		try {
			if (tlsPassword.size())
				setNetworkOption(FDBNetworkOptions::TLS_PASSWORD, tlsPassword);

			setNetworkOption(FDBNetworkOptions::TLS_KEY_PATH, tlsKeyPath);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS key path to " << tlsKeyPath << " (" << e.what() << ")\n";
			return false;
		}
	}
	if (tlsVerifyPeers.size()) {
		try {
			setNetworkOption(FDBNetworkOptions::TLS_VERIFY_PEERS, tlsVerifyPeers);
		} catch (Error& e) {
			std::cerr << "ERROR: cannot set TLS peer verification to " << tlsVerifyPeers << " (" << e.what() << ")\n";
			return false;
		}
	}
	return true;
}
