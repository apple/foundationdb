/*
 * S3ClientWorkload.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/S3Client.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/Platform.h"

// Run this workload with ../build_output/bin/fdbserver -r simulation -f
// ../src/foundationdb/tests/fast/S3ClientWorkload.toml
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
			// Default location for local seaweedfs instance.
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
		return Void();
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
		std::string basename = ::basename(filePath);

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
			if (!currentPath.empty() && currentPath.back() != '/') {
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
		state std::string uniqueRunDir = "s3_workload_run_" + deterministicRandom()->randomUniqueID().toString();
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
		std::string baseFilename = ::basename(self->credentials); // Gets filename from the *new* path
		std::string uniqueObjectKey = baseFilename + "_" + deterministicRandom()->randomUniqueID().toString();
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
};

WorkloadFactory<S3ClientWorkload> S3ClientWorkloadFactory;
