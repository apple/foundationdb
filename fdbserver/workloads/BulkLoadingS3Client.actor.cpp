/*
 * BulkLoadingS3Client.actor.cpp
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
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/Platform.h"

const std::string simulationBulkLoadS3ClientFolder = joinPath("simfdb", "bulkloads3client");

// Run this workload with ../build_output/bin/fdbserver -r simulation -f
// ../src/foundationdb/tests/fast/BulkLoadingS3Client.toml
struct BulkLoadingS3Client : TestWorkload {
	static constexpr auto NAME = "BulkLoadingS3ClientWorkload";
	const bool enabled;
	bool pass;
	std::string s3Url; // Added field for S3 URL
	std::string credentials;

	BulkLoadingS3Client(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {
		s3Url = getOption(options, "s3Url"_sr, ""_sr).toString();
		if (s3Url.empty()) {
			// Default location for local seaweedfs instance.
			s3Url = "blobstore://127.0.0.1:8333";
		}
		credentials = getOption(options, "credentials"_sr, ""_sr).toString();
		if (credentials.empty()) {
			credentials = "BulkLoadingS3Client.blob-credentials.jsonxx";
		}
		// If credentials path is not absolute, use the TOML file directory as prefix
		if (!credentials.empty() && credentials[0] != '/' && credentials[0] != '\\') {
			std::string tomlfile = getOption(options, "testfile"_sr, ""_sr).toString();
			if (!tomlfile.empty()) {
				auto tomlDir = tomlfile.substr(0, tomlfile.find_last_of("/\\"));
				credentials = joinPath(tomlDir, credentials);
			}
		}
	}
	~BulkLoadingS3Client() {
		if (pass) {
			TraceEvent("BulkLoadingS3ClientWorkloadPass");
		} else {
			TraceEvent("BulkLoadingS3ClientWorkloadFail");
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
			TraceEvent(SevError, "BulkLoadingS3ClientURLParseError")
			    .error(e)
			    .detail("URL", baseUrl)
			    .detail("Path", filePath);
			throw;
		}
	}

	ACTOR Future<Void> _start(BulkLoadingS3Client* self, Database cx) {
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

		// Set the credentials file path into the global network configuration
		auto* blobCredFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
		if (!blobCredFiles) {
			blobCredFiles = new std::vector<std::string>();
			g_network->setGlobal(INetwork::enBlobCredentialFiles, blobCredFiles);
		}
		blobCredFiles->push_back(self->credentials);

		// Add the credentials file to the URL path
		state std::string s3_url = addFileToUrl(self->credentials, self->s3Url);
		try {
			TraceEvent("BulkLoadingS3ClientWorkloadUploadCredentials")
			    .detail("S3URL", s3_url)
			    .detail("Path", self->credentials);

			// Upload a file. Make it the credentials file.
			wait(copyUpFile(self->credentials, s3_url));
		} catch (Error& e) {
			TraceEvent(SevWarn, "BulkLoadingS3ClientWorkloadError")
			    .error(e)
			    .detail("S3URL", s3_url)
			    .detail("Path", self->credentials);
			throw;
		}

		return Void();
	}
};

WorkloadFactory<BulkLoadingS3Client> BulkLoadingS3ClientFactory;
