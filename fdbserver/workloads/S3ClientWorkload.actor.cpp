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
#include "fdbserver/MockS3ServerChaos.h"

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

	// Chaos injection options
	bool enableChaos;
	double errorRate;
	double throttleRate;
	double delayRate;
	double corruptionRate;
	double maxDelay;

	S3ClientWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {
		s3Url = getOption(options, "s3Url"_sr, ""_sr).toString();
		if (s3Url.empty()) {
			// Default location for s3 instance.
			s3Url = "blobstore://127.0.0.1:8333";
		}
		simfdbDir = getOption(options, "simfdb"_sr, "simfdb"_sr).toString();
		// Place credentials file in the simulation root, NOT inside the server's data dir (simfdbDir)
		credentials = "S3ClientWorkload.blob-credentials.json";

		// Initialize chaos options
		enableChaos = getOption(options, "enableChaos"_sr, false);
		errorRate = getOption(options, "errorRate"_sr, 0.1);
		throttleRate = getOption(options, "throttleRate"_sr, 0.05);
		delayRate = getOption(options, "delayRate"_sr, 0.1);
		corruptionRate = getOption(options, "corruptionRate"_sr, 0.01);
		maxDelay = getOption(options, "maxDelay"_sr, 2.0);
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
	// Uses S3BlobStoreEndpoint::fromString() for robust URL parsing (similar to
	// BlobMetadataUtils::getBlobMetadataPartitionedURL)
	static std::string addFileToUrl(std::string filePath, std::string baseUrl) {
		std::string basename = ::basename(const_cast<char*>(filePath.c_str()));

		try {
			std::string resource;
			std::string error;
			S3BlobStoreEndpoint::ParametersT parameters;
			Reference<S3BlobStoreEndpoint> endpoint =
			    S3BlobStoreEndpoint::fromString(baseUrl, {}, &resource, &error, &parameters);

			if (!error.empty() || !endpoint) {
				TraceEvent(SevError, "S3ClientWorkloadURLParseError").detail("URL", baseUrl).detail("Error", error);
				throw backup_invalid_url();
			}

			// If there's an existing resource in the URL, find it and append the basename after it
			if (!resource.empty()) {
				size_t resourceStart = baseUrl.find(resource);
				if (resourceStart == std::string::npos) {
					throw backup_invalid_url();
				}
				// Insert "/basename" after the existing resource
				std::string separator = (resource.back() == '/') ? "" : "/";
				return baseUrl.insert(resourceStart + resource.size(), separator + basename);
			} else {
				// No resource in URL, need to insert before query string
				size_t queryStart = baseUrl.find('?');
				if (queryStart != std::string::npos) {
					return baseUrl.insert(queryStart, "/" + basename);
				} else {
					return baseUrl + "/" + basename;
				}
			}
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
		// Create a unique directory for this workload instance inside simfdb
		// This keeps test artifacts in the simulation directory like other workloads
		// Use deterministic directory name instead of random UID to ensure deterministic behavior
		state std::string uniqueRunDir =
		    joinPath(self->simfdbDir,
		             format("s3_workload_run_%08x_%08x", self->clientId, deterministicRandom()->randomInt(0, 1000000)));
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

		// Cleanup local files - each operation handles its own errors non-fatally
		// Delete credentials file
		try {
			if (fileExists(self->credentials)) {
				deleteFile(self->credentials);
				TraceEvent(SevDebug, "S3ClientWorkloadDeletedCredentials").detail("File", self->credentials);
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "S3ClientWorkloadCredentialsCleanupFailed").error(e).detail("File", self->credentials);
		}

		// Delete download file
		try {
			if (fileExists(download)) {
				deleteFile(download);
				TraceEvent(SevDebug, "S3ClientWorkloadDeletedDownload").detail("File", download);
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "S3ClientWorkloadDownloadCleanupFailed").error(e).detail("File", download);
		}

		// Delete run directory - may fail in simulation due to timing/locking
		try {
			platform::eraseDirectoryRecursive(uniqueRunDir);
			TraceEvent(SevInfo, "S3ClientWorkloadCleanedRunDir").detail("Dir", uniqueRunDir);
		} catch (Error& e) {
			TraceEvent(SevWarn, "S3ClientWorkloadRunDirCleanupFailed")
			    .error(e)
			    .detail("Dir", uniqueRunDir)
			    .detail("Reason", "Non-fatal in simulation");
		}

		return Void();
	}

	ACTOR Future<Void> _setup(S3ClientWorkload* self, Database cx) {
		// Only client 0 registers the MockS3Server to avoid unnecessary duplicate trace events
		// Note: Both startMockS3ServerChaos() and registerSimHTTPServer() have internal duplicate detection
		if (self->clientId == 0) {
			// Check if we're using a local mock server URL pattern
			bool useMockS3 = self->s3Url.find("127.0.0.1") != std::string::npos ||
			                 self->s3Url.find("localhost") != std::string::npos ||
			                 self->s3Url.find("mock-s3-server") != std::string::npos;

			if (useMockS3 && g_network->isSimulated()) {
				// Check if 127.0.0.1:8080 is already registered in simulator's httpHandlers
				std::string serverKey = "127.0.0.1:8080";
				bool alreadyRegistered = g_simulator->httpHandlers.count(serverKey) > 0;

				if (alreadyRegistered) {
					TraceEvent("S3ClientWorkload")
					    .detail("Phase", "MockS3Server Already Registered")
					    .detail("Address", serverKey)
					    .detail("ChaosRequested", self->enableChaos)
					    .detail("Reason", "Reusing existing HTTP handler from previous test");
				} else if (self->enableChaos) {
					TraceEvent("S3ClientWorkload")
					    .detail("Phase", "Starting MockS3ServerChaos")
					    .detail("URL", self->s3Url);

					// Start MockS3ServerChaos - has internal duplicate detection
					NetworkAddress listenAddress(IPAddress(0x7f000001), 8080);
					wait(startMockS3ServerChaos(listenAddress));

					TraceEvent("S3ClientWorkload")
					    .detail("Phase", "MockS3ServerChaos Started")
					    .detail("Address", "127.0.0.1:8080");
				} else {
					TraceEvent("S3ClientWorkload")
					    .detail("Phase", "Registering MockS3Server")
					    .detail("URL", self->s3Url);

					// Register regular MockS3Server
					wait(
					    g_simulator->registerSimHTTPServer("127.0.0.1", "8080", makeReference<MockS3RequestHandler>()));

					TraceEvent("S3ClientWorkload")
					    .detail("Phase", "MockS3Server Registered")
					    .detail("Address", "127.0.0.1:8080");
				}
			}
		}

		// Configure chaos rates for all clients if chaos is enabled
		// This allows each test to have different chaos rates
		if (self->enableChaos && g_network->isSimulated()) {
			auto injector = S3FaultInjector::injector();
			injector->setErrorRate(self->errorRate);
			injector->setThrottleRate(self->throttleRate);
			injector->setDelayRate(self->delayRate);
			injector->setCorruptionRate(self->corruptionRate);
			injector->setMaxDelay(self->maxDelay);

			TraceEvent("S3ClientWorkload")
			    .detail("Phase", "Chaos Configured")
			    .detail("ClientID", self->clientId)
			    .detail("ErrorRate", self->errorRate)
			    .detail("ThrottleRate", self->throttleRate)
			    .detail("DelayRate", self->delayRate)
			    .detail("CorruptionRate", self->corruptionRate);
		}

		return Void();
	}
};

WorkloadFactory<S3ClientWorkload> S3ClientWorkloadFactory;
