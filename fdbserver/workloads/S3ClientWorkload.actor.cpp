/*
 * S3ClientWorkload.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/MockS3Server.h"

#include "fdbrpc/HTTP.h"
#include "fdbrpc/simulator.h"
#include "flow/Trace.h"
#include "flow/ActorCollection.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/S3Client.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "flow/Platform.h"

#include <string>
#include <vector>
#include <libgen.h>

#include "flow/actorcompiler.h" // This must be the last #include.

// Test s3client operations against s3.
// Run this workload with ../build_output/bin/fdbserver -r simulation -f
// ../src/foundationdb/tests/slow/S3ClientWorkload.toml
struct S3ClientWorkload : TestWorkload {
	static constexpr auto NAME = "S3ClientWorkload";
	const bool enabled;
	bool pass;
	std::string s3Url;
	std::string credentials;
	std::string simfdbDir;
	S3ClientWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {
		s3Url = getOption(options, "s3Url"_sr, ""_sr).toString();
		if (s3Url.empty()) {
			// Default location for s3 instance.
			s3Url = "blobstore://127.0.0.1:8333";
		}
		simfdbDir = getOption(options, "simfdb"_sr, "simfdb"_sr).toString();
		// Place credentials file in the simulation root, NOT inside the server's data dir (simfdbDir)
		credentials = "S3ClientWorkload.blob-credentials.json";
	}
	~S3ClientWorkload() {
		if (pass) {
			TraceEvent("S3ClientWorkloadPass");
		} else {
			TraceEvent("S3ClientWorkloadFail");
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (!enabled)
			return Void();
		return _setup(this, cx);
	}

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

private:
	void setupCredentialsFile() {
		// Write the credentials file content -- hardcoded and nonsense for now. It just needs to be present.
		writeFile(
		    credentials,
		    "{\"accounts\":{\"@host\":{\"api_key\":\"seaweedfs\",\"secret\":\"tot4llys3cure\",\"token\":\"TOKEN\"}}}");

		// Set the credentials file path into the global network configuration
		auto* blobCredFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
		if (!blobCredFiles) {
			blobCredFiles = new std::vector<std::string>();
			g_network->setGlobal(INetwork::enBlobCredentialFiles, blobCredFiles);
		}
		blobCredFiles->push_back(credentials);
	}

	// Add the basename of a file to the URL path
	static std::string addFileToUrl(std::string filePath, std::string baseUrl) {
		std::string basename = ::basename(const_cast<char*>(filePath.c_str()));

		// Parse the URL and append the basename to the path
		try {
			// Find the position after the host:port part
			size_t hostEnd = baseUrl.find('/', baseUrl.find("://") + 3);
			if (hostEnd == std::string::npos) {
				hostEnd = baseUrl.length();
			}

			// Find the query string start
			size_t queryStart = baseUrl.find('?', hostEnd);
			if (queryStart == std::string::npos) {
				queryStart = baseUrl.length();
			}

			// Get the current path
			std::string currentPath = baseUrl.substr(hostEnd, queryStart - hostEnd);
			// Ensure there's always a path separator
			if (currentPath.empty() || currentPath.back() != '/') {
				currentPath += '/';
			}

			// Construct the new URL
			return baseUrl.substr(0, hostEnd) + currentPath + basename + baseUrl.substr(queryStart);
		} catch (Error& e) {
			TraceEvent(SevError, "S3ClientWorkloadURLParseError")
			    .error(e)
			    .detail("URL", baseUrl)
			    .detail("Path", filePath);
			throw;
		}
	}

	ACTOR Future<Void> _start(S3ClientWorkload* self, Database cx) {
		if (self->clientId != 0) {
			// Our simulation test can trigger multiple same workloads at the same time
			// Only run one time workload in the simulation
			return Void();
		}

		if (g_network->isSimulated()) {
			// Network partition between CC and DD can cause DD no longer existing,
			// which results in the bulk loading task cannot complete
			// So, this workload disable the network partition
			disableConnectionFailures("BulkLoading");
		}

		// --- BEGIN PRE-TEST CLEANUP ---
		state std::string download_path_to_clean = "downloaded_credentials";
		try {
			// Attempt to delete the credentials file if it exists
			if (fileExists(self->credentials)) {
				deleteFile(self->credentials);
				TraceEvent(SevDebug, "S3ClientWorkloadCleanedPreExistingFile").detail("File", self->credentials);
			}
			// Attempt to delete the download target as a directory or file unconditionally
			// Use eraseDirectoryRecursive as it might exist as a directory
			platform::eraseDirectoryRecursive(download_path_to_clean);
			TraceEvent(SevDebug, "S3ClientWorkloadAttemptedCleanPreExistingDownload")
			    .detail("Path", download_path_to_clean);

		} catch (Error& e) {
			TraceEvent(SevWarn, "S3ClientWorkloadPreCleanupError").errorUnsuppressed(e);
			// Continue even if pre-cleanup fails
		}
		// --- END PRE-TEST CLEANUP ---

		// --- BEGIN PER-RUN ISOLATION & CLEANUP ---
		// Create a unique directory for this workload instance
		// Use deterministic directory name instead of random UID to ensure deterministic behavior
		state std::string uniqueRunDir =
		    format("s3_workload_run_%08x_%08x", self->clientId, deterministicRandom()->randomInt(0, 1000000));
		try {
			platform::createDirectory(uniqueRunDir);
			TraceEvent(SevDebug, "S3ClientWorkloadCreatedRunDir").detail("Dir", uniqueRunDir);
		} catch (Error& e) {
			TraceEvent(SevError, "S3ClientWorkloadCreateRunDirError").errorUnsuppressed(e).detail("Dir", uniqueRunDir);
			throw; // Fail fast if we can't create our working directory
		}
		// --- END PER-RUN ISOLATION & CLEANUP ---

		// Modify paths to be within the unique directory
		self->credentials = joinPath(uniqueRunDir, "S3ClientWorkload.blob-credentials.json");
		state std::string download = joinPath(uniqueRunDir, "downloaded_credentials");

		// Setup the credentials file inside the unique directory.
		self->setupCredentialsFile();

		// Create a unique object key for S3 (using only the base filename)
		std::string baseFilename =
		    ::basename(const_cast<char*>(self->credentials.c_str())); // Gets filename from the *new* path
		// Use deterministic ID based on client ID and test context instead of random UID
		// This ensures identical behavior across determinism check runs
		std::string deterministicId = format("%08x_%08x", self->clientId, deterministicRandom()->randomInt(0, 1000000));
		std::string uniqueObjectKey = baseFilename + "_" + deterministicId;
		state std::string file_url = self->addFileToUrl(uniqueObjectKey, self->s3Url);
		state bool uploaded = false; // Track if upload started/succeeded
		state Optional<Error> errorToThrow; // State variable to hold error

		try {
			// Use original local path (now inside unique dir) for source, unique URL for destination
			wait(copyUpFile(self->credentials, file_url));
			uploaded = true; // Mark as uploaded only after wait() succeeds
			wait(copyDownFile(file_url, download));
			wait(deleteResource(file_url)); // Attempt deletion on success path
		} catch (Error& e) {
			TraceEvent(SevError, "S3ClientWorkloadError") // Log original error
			    .error(e)
			    .detail("S3URL", file_url)
			    .detail("Path", self->credentials)
			    .detail("Download", download);

			// Store the original error BEFORE attempting cleanup
			errorToThrow = e;

			// --- Attempt S3 cleanup even on failure ---
			if (uploaded) { // Only try to delete if we think it was uploaded
				try {
					wait(deleteResource(file_url));
					TraceEvent(SevWarn, "S3ClientWorkloadCleanedS3AfterError").detail("S3URL", file_url);
				} catch (Error& cleanup_e) {
					// Log cleanup error but don't overwrite original error
					TraceEvent(SevWarn, "S3ClientWorkloadS3CleanupError")
					    .errorUnsuppressed(cleanup_e)
					    .detail("S3URL", file_url);
				}
			}
			// --- End S3 cleanup attempt ---
		}

		// Check if an error occurred and throw it now
		if (errorToThrow.present()) {
			throw errorToThrow.get();
		}

		// Compare the contents of the original and downloaded files
		// Paths are now inside uniqueRunDir
		std::string originalContent = readFileBytes(self->credentials, 1024 * 1024); // 1MB max size
		std::string downloadedContent = readFileBytes(download, 1024 * 1024); // 1MB max size
		if (originalContent != downloadedContent) {
			TraceEvent(SevError, "S3ClientWorkloadContentMismatch")
			    .detailf("OriginalSize", "%zu", originalContent.size())
			    .detailf("DownloadedSize", "%zu", downloadedContent.size());
			throw file_not_found();
		}

		// Cleanup local files (now inside uniqueRunDir)
		try {
			deleteFile(self->credentials);
			deleteFile(download);
			TraceEvent(SevDebug, "S3ClientWorkloadCleanedLocalFiles")
			    .detail("CredentialsFile", self->credentials)
			    .detail("DownloadFile", download);
			// Attempt to cleanup the unique run directory itself
			platform::eraseDirectoryRecursive(uniqueRunDir);
			TraceEvent(SevDebug, "S3ClientWorkloadCleanedRunDir").detail("Dir", uniqueRunDir);
		} catch (Error& e) {
			// Log if cleanup fails, but don't fail the test just for this
			TraceEvent(SevWarn, "S3ClientWorkloadCleanupError").errorUnsuppressed(e);
		}

		return Void();
	}

	ACTOR Future<Void> _setup(S3ClientWorkload* self, Database cx) {
		// Only client 0 registers the MockS3Server to avoid duplicates
		if (self->clientId == 0) {
			// Check if we're using a local mock server URL pattern
			bool useMockS3 = self->s3Url.find("127.0.0.1") != std::string::npos ||
			                 self->s3Url.find("localhost") != std::string::npos ||
			                 self->s3Url.find("mock-s3-server") != std::string::npos;

			if (useMockS3 && g_network->isSimulated()) {
				TraceEvent("S3ClientWorkload").detail("Phase", "Registering MockS3Server").detail("URL", self->s3Url);

				// Register MockS3Server with IP address - simulation environment doesn't support hostname resolution.
				// See in HTTPServer.actor.cpp how the MockS3RequestHandler is implemented. Client connects to
				// connect("127.0.0.1", "8080") and then simulation network routes it to MockS3Server.
				wait(g_simulator->registerSimHTTPServer("127.0.0.1", "8080", makeReference<MockS3RequestHandler>()));

				TraceEvent("S3ClientWorkload")
				    .detail("Phase", "MockS3Server Registered")
				    .detail("Address", "127.0.0.1:8080");
			}
		}
		return Void();
	}
};

WorkloadFactory<S3ClientWorkload> S3ClientWorkloadFactory;
