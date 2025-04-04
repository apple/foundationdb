/*
 * S3TransferManagerWrapper.actor.cpp
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

// Foundational includes first
#include "flow/flow.h"
#include "flow/network.h" // For g_network
#include "flow/actorcompiler.h" // Keep this last among Flow includes?

// Platform and FDB Client includes
#include "fdbclient/FDBAWSCredentialsProvider.h"
#include "fdbclient/S3TransferManagerWrapper.actor.h" // Include the actor header
#include "fdbclient/Knobs.h" // Include CLIENT_KNOBS
#include "flow/IAsyncFile.h"
#include "flow/Platform.h"

// AWS SDK includes (Ensure these are before usage of AWS types)
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/transfer/TransferManager.h> // Include for TransferManager and TransferHandle
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/StringUtils.h> // For Aws::Utils::StringUtils::Split
// Additional includes for async wrappers
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/core/outcome/Outcome.h>

// Standard library includes
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <stdexcept>
#include <thread>
#include <vector>
#include <condition_variable>
#include <deque>
#include <atomic>
#include <variant> // For variant work queue

namespace {

// Global variables for AWS SDK and S3 client/transfer manager
Aws::SDKOptions g_options;
std::shared_ptr<Aws::S3::S3Client> g_s3Client;
std::shared_ptr<Aws::Transfer::TransferManager> g_transferManager;
std::once_flag g_initFlag;
std::atomic<bool> g_initializationSucceeded = false;

// --- Blobstore URL Parsing Logic ---
// Example URL: s3://mybucket/backup/path?region=us-west-2&endpoint_override=http://localhost:9000
struct BlobstoreUrlInfo {
	std::string bucket;
	std::string region;
	std::string endpoint;
	Aws::Http::Scheme scheme = Aws::Http::Scheme::HTTPS;
};

void parseQueryParams(const std::string& queryPart, BlobstoreUrlInfo& info) {
	if (queryPart.empty())
		return;

	auto params = Aws::Utils::StringUtils::Split(queryPart, '&');
	for (const auto& param : params) {
		auto kv = Aws::Utils::StringUtils::Split(param, '=');
		if (kv.size() == 2) {
			std::string key = kv[0];
			std::string value = kv[1]; // TODO: URL Decode value if necessary
			if (key == "region") {
				info.region = value;
			} else if (key == "endpoint_override") {
				if (value.rfind("http://", 0) == 0) {
					info.scheme = Aws::Http::Scheme::HTTP;
					info.endpoint = value.substr(strlen("http://"));
				} else if (value.rfind("https://", 0) == 0) {
					info.scheme = Aws::Http::Scheme::HTTPS;
					info.endpoint = value.substr(strlen("https://"));
				} else {
					info.scheme = Aws::Http::Scheme::HTTPS;
					info.endpoint = value;
					std::cerr << "Warning: Assuming HTTPS for endpoint_override without explicit scheme: " << value
					          << std::endl;
				}
			}
			// Add other query params as needed
		}
	}
}

bool parseS3Url(const std::string& url, BlobstoreUrlInfo& info) {
	const std::string prefix = "s3://";
	if (url.compare(0, prefix.length(), prefix) != 0) {
		std::cerr << "Error: Blobstore URL does not start with " << prefix << ": " << url << std::endl;
		return false;
	}

	std::string urlWithoutPrefix = url.substr(prefix.length());
	size_t queryPos = urlWithoutPrefix.find('?');
	std::string pathPart = urlWithoutPrefix;
	std::string queryPart = "";

	if (queryPos != std::string::npos) {
		pathPart = urlWithoutPrefix.substr(0, queryPos);
		queryPart = urlWithoutPrefix.substr(queryPos + 1);
	}

	size_t firstSlashPos = pathPart.find('/');
	if (firstSlashPos == std::string::npos) {
		info.bucket = pathPart;
	} else {
		info.bucket = pathPart.substr(0, firstSlashPos);
	}

	if (info.bucket.empty()) {
		std::cerr << "Error: Could not extract bucket name from Blobstore URL: " << url << std::endl;
		return false;
	}

	parseQueryParams(queryPart, info);
	return true;
}
// --- End URL Parsing ---

// --- Thread Pool for Monitoring Transfer Handles ---

struct TransferMonitorWorkItem {
	std::shared_ptr<Aws::Transfer::TransferHandle> handle;
	Promise<Void> promise;
};

std::deque<TransferMonitorWorkItem> g_transferQueue;
std::mutex g_queueMutex;
std::condition_variable g_queueCondVar;
std::vector<std::thread> g_monitorThreads;
std::atomic<bool> g_stopMonitorThreads = false;

void transferMonitorLoop() {
	std::unique_lock<std::mutex> lock(g_queueMutex);
	while (true) {
		g_queueCondVar.wait(lock, [] { return g_stopMonitorThreads.load() || !g_transferQueue.empty(); });

		if (g_stopMonitorThreads.load() && g_transferQueue.empty()) {
			break; // Exit thread
		}

		if (g_transferQueue.empty()) {
			continue; // Spurious wakeup
		}

		TransferMonitorWorkItem item = std::move(g_transferQueue.front());
		g_transferQueue.pop_front();
		lock.unlock(); // Unlock while processing

		item.handle->WaitUntilFinished(); // Blocking wait on helper thread
		Aws::Transfer::TransferStatus status = item.handle->GetStatus();
		bool success = (status == Aws::Transfer::TransferStatus::COMPLETED);
		std::string errorMessage;
		if (!success) {
			const auto& errors = item.handle->GetCoreErrors();
			if (!errors.empty()) {
				errorMessage = errors.front().GetMessage();
			} else {
				errorMessage = "Transfer failed with status: " +
				               std::string(Aws::Transfer::TransferStatusMapper::GetNameForTransferStatus(status));
			}
		}

		// Use g_network->onMainThreadVoid to send result back to Flow thread
		if (g_network) {
			g_network->onMainThreadVoid(
			    [p = std::move(item.promise), success, errMsg = std::move(errorMessage)]() mutable {
				    if (success) {
					    p.send(Void());
				    } else {
					    std::cerr << "Transfer Error (signaled to Flow): " << errMsg << std::endl;
					    // Send the error object directly
					    p.sendError(backup_error());
				    }
			    },
			    nullptr); // nullptr for TaskPriority
		} else {
			std::cerr << "Error: g_network is null, cannot signal completion for transfer." << std::endl;
			if (item.promise.canBeSet()) {
				// Use connection_failed error
				item.promise.sendError(connection_failed());
			}
		}

		lock.lock(); // Re-lock
	}
	std::cout << "Transfer monitor thread exiting." << std::endl;
}

// Use placeholder/default values for Knobs until correct names are found
const int DEFAULT_MONITOR_THREADS = 4;
const int DEFAULT_CONNECT_TIMEOUT_MS = 1000;
const int DEFAULT_REQUEST_TIMEOUT_MS = 3000;
const int DEFAULT_TRANSFER_BUFFER_SIZE_MB = 128;
const int DEFAULT_TRANSFER_MANAGER_THREADS = 4;

void startTransferMonitors(int numThreads = DEFAULT_MONITOR_THREADS) {
	if (numThreads <= 0)
		numThreads = 1; // Ensure at least one thread
	std::cout << "Starting " << numThreads << " S3 transfer monitor threads." << std::endl;
	g_stopMonitorThreads = false; // Reset stop flag
	for (int i = 0; i < numThreads; ++i) {
		g_monitorThreads.emplace_back(transferMonitorLoop);
	}
}

void stopTransferMonitors() {
	std::cout << "Stopping S3 transfer monitor threads..." << std::endl;
	{
		std::lock_guard<std::mutex> lock(g_queueMutex);
		g_stopMonitorThreads = true;
		// Clear the queue? Or let threads finish existing items? Clearing might lose promises.
		// Let's let them finish for now.
		// g_transferQueue.clear();
	}
	g_queueCondVar.notify_all(); // Wake up all threads
	for (auto& t : g_monitorThreads) {
		if (t.joinable()) {
			t.join();
		}
	}
	g_monitorThreads.clear();
	std::cout << "S3 transfer monitor threads stopped." << std::endl;
}

// --- End Thread Pool ---

// --- Thread Pool for Direct S3 Client Calls (List, Delete) ---

// Define Outcome types for Promises
using ListOutcome = Aws::S3::Model::ListObjectsV2Outcome;
using DeleteObjectOutcome = Aws::S3::Model::DeleteObjectOutcome;
using DeleteObjectsOutcome = Aws::S3::Model::DeleteObjectsOutcome;

// Define Work Item structs
struct ListWorkItem {
	Aws::S3::Model::ListObjectsV2Request request;
	Promise<ListOutcome> promise;
};

struct DeleteObjectWorkItem {
	Aws::S3::Model::DeleteObjectRequest request;
	Promise<DeleteObjectOutcome> promise;
};

struct DeleteObjectsWorkItem {
	Aws::S3::Model::DeleteObjectsRequest request;
	Promise<DeleteObjectsOutcome> promise;
};

// Define the variant type for the queue
using S3ClientWorkItem = std::variant<ListWorkItem, DeleteObjectWorkItem, DeleteObjectsWorkItem>;

// Globals for the S3 client monitor pool
std::deque<S3ClientWorkItem> g_s3ClientQueue;
std::mutex g_s3ClientMutex;
std::condition_variable g_s3ClientCondVar;
std::vector<std::thread> g_s3ClientMonitorThreads;
std::atomic<bool> g_stopS3ClientMonitorThreads = false;

// The monitor loop function for S3 client calls
void s3ClientMonitorLoop() {
	std::unique_lock<std::mutex> lock(g_s3ClientMutex);
	while (true) {
		g_s3ClientCondVar.wait(lock, [] { return g_stopS3ClientMonitorThreads.load() || !g_s3ClientQueue.empty(); });

		if (g_stopS3ClientMonitorThreads.load() && g_s3ClientQueue.empty()) {
			break; // Exit thread
		}
		if (g_s3ClientQueue.empty()) {
			continue; // Spurious wakeup
		}

		S3ClientWorkItem itemVariant = std::move(g_s3ClientQueue.front());
		g_s3ClientQueue.pop_front();
		lock.unlock(); // Unlock while processing

		// Use std::visit to handle the different work item types
		std::visit(
		    [&](auto&& item) {
			    using T = std::decay_t<decltype(item)>;
			    using OutcomeType = decltype(item.promise)::ResultType;

			    OutcomeType outcome; // Variable to hold the result
			    std::string operationName = "UnknownS3Op";

			    // Ensure the client is valid before making calls
			    if (!g_s3Client) {
				    TraceEvent(SevError, "S3ClientMonitorLoop")
				        .detail("Error", "S3 Client not initialized before call");
				    // Create an error outcome - Adjust S3Error type if needed
				    outcome = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::INTERNAL_FAILURE,
				                                                       "ClientNotInitialized",
				                                                       "S3 Client not initialized",
				                                                       false); // not retryable
			    } else {
				    // Execute the blocking SDK call based on the item type
				    try {
					    if constexpr (std::is_same_v<T, ListWorkItem>) {
						    operationName = "ListObjectsV2";
						    outcome = g_s3Client->ListObjectsV2(item.request);
					    } else if constexpr (std::is_same_v<T, DeleteObjectWorkItem>) {
						    operationName = "DeleteObject";
						    outcome = g_s3Client->DeleteObject(item.request);
					    } else if constexpr (std::is_same_v<T, DeleteObjectsWorkItem>) {
						    operationName = "DeleteObjects";
						    outcome = g_s3Client->DeleteObjects(item.request);
					    }
				    } catch (const std::exception& e) {
					    // Catch potential exceptions during the SDK call itself
					    TraceEvent(SevError, "S3ClientMonitorLoopException")
					        .detail("Operation", operationName)
					        .detail("Exception", e.what());
					    outcome = Aws::Client::AWSError<Aws::S3::S3Errors>(
					        Aws::S3::S3Errors::INTERNAL_FAILURE, "SdkCallException", e.what(), false);
				    } catch (...) {
					    TraceEvent(SevError, "S3ClientMonitorLoopException")
					        .detail("Operation", operationName)
					        .detail("Exception", "Unknown");
					    outcome = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::INTERNAL_FAILURE,
					                                                       "SdkCallUnknownException",
					                                                       "Unknown exception during SDK call",
					                                                       false);
				    }
			    }

			    // Use g_network->onMainThreadVoid to send the Outcome back
			    if (g_network) {
				    g_network->onMainThreadVoid(
				        [p = std::move(item.promise), out = std::move(outcome)]() mutable { p.send(std::move(out)); },
				        nullptr);
			    } else {
				    std::cerr << "Error: g_network is null, cannot signal completion for S3 client op: "
				              << operationName << std::endl;
				    if (item.promise.canBeSet()) {
					    // Send error directly if promise still valid
					    // Create a generic network error outcome
					    OutcomeType errorOutcome =
					        Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NETWORK_CONNECTION,
					                                                 "NetworkShutdown",
					                                                 "g_network was null during signal",
					                                                 true); // May be retryable if network comes back?
					    item.promise.send(std::move(errorOutcome));
				    }
			    }
		    },
		    itemVariant); // End std::visit

		lock.lock(); // Re-lock before next iteration
	}
	std::cout << "S3 client monitor thread exiting." << std::endl;
}

// Start/Stop functions for the S3 client monitor pool
void startS3ClientMonitors(int numThreads = DEFAULT_MONITOR_THREADS) {
	std::cout << "Starting " << numThreads << " S3 client monitor threads." << std::endl;
	g_stopS3ClientMonitorThreads = false;
	for (int i = 0; i < numThreads; ++i) {
		g_s3ClientMonitorThreads.emplace_back(s3ClientMonitorLoop);
	}
}

void stopS3ClientMonitors() {
	std::cout << "Stopping S3 client monitor threads..." << std::endl;
	{
		std::lock_guard<std::mutex> lock(g_s3ClientMutex);
		g_stopS3ClientMonitorThreads = true;
	}
	g_s3ClientCondVar.notify_all();
	for (auto& t : g_s3ClientMonitorThreads) {
		if (t.joinable()) {
			t.join();
		}
	}
	g_s3ClientMonitorThreads.clear();
	std::cout << "S3 client monitor threads stopped." << std::endl;
}

// Enqueue function for S3 client work
void enqueueS3ClientWork(S3ClientWorkItem workItemVariant) {
	{
		std::lock_guard<std::mutex> lock(g_s3ClientMutex);
		if (g_stopS3ClientMonitorThreads.load()) {
			TraceEvent(SevWarn, "S3ClientEnqueue").detail("Status", "MonitorsStopped");
			// Need to extract the promise and send an error
		     std::visit([](auto&& item) {
				if (item.promise.canBeSet()) {
					// Modify initializeAwsSdk to start monitors
					void initializeAwsSdkInternal() {
						try {
							Aws::Utils::Logging::InitializeAWSLogging(
							    Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
							        "FDBAWSSDK", Aws::Utils::Logging::LogLevel::Info, "aws_sdk_"));
							Aws::InitAPI(g_options);

							// TODO: Find correct Knob names for these configuration values.
							std::string blobstoreUrl = ""; // Placeholder - Must be fetched
							std::string credentialsPath = ""; // Placeholder - Can be empty, must be fetched
							TraceEvent(SevWarnAlways, "S3TransferInit")
							    .detail("TODO", "Fetch actual Blobstore URL and Credentials Path");
							// Example using *potential* knobs (commented out):
							// blobstoreUrl = CLIENT_KNOBS->BACKUP_AWS_ENDPOINT;
							// credentialsPath = CLIENT_KNOBS->BACKUP_AWS_SECRET_ACCESS_KEY_FILE;

							if (blobstoreUrl.empty()) {
								// Cannot proceed without URL
								throw std::runtime_error("Blobstore URL is not configured (TODO: Fetch from Knobs).");
							}

							BlobstoreUrlInfo urlInfo;
							if (!parseS3Url(blobstoreUrl, urlInfo)) {
								throw std::runtime_error("Could not parse blobstore URL: " + blobstoreUrl);
							}

							Aws::Client::ClientConfiguration clientConfig;
							if (!urlInfo.region.empty()) {
								clientConfig.region = urlInfo.region;
								TraceEvent(SevInfo, "S3TransferInit").detail("Region", urlInfo.region);
							} else {
								TraceEvent(SevWarn, "S3TransferInit").detail("Region", "NotSpecified");
							}
							if (!urlInfo.endpoint.empty()) {
								clientConfig.endpointOverride = urlInfo.endpoint;
								clientConfig.scheme = urlInfo.scheme;
								TraceEvent(SevInfo, "S3TransferInit")
								    .detail("EndpointOverride", urlInfo.endpoint)
								    .detail("Scheme", urlInfo.scheme == Aws::Http::Scheme::HTTPS ? "HTTPS" : "HTTP");
							}

							// Use default/placeholder values for timeouts/threads
							TraceEvent(SevWarnAlways, "S3TransferInit")
							    .detail("TODO", "Fetch actual S3 timeout/buffer/thread Knobs");
							clientConfig.connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS; // Placeholder
							clientConfig.requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS; // Placeholder

							auto credentialsProvider = std::make_shared<FDBAWSCredentialsProvider>(
							    "FDBAWSCredentialsProvider", credentialsPath);

							g_s3Client =
							    Aws::MakeShared<Aws::S3::S3Client>("S3Client", credentialsProvider, clientConfig);

							Aws::Transfer::TransferManagerConfiguration transferConfig;
							transferConfig.s3Client = g_s3Client;
							transferConfig.transferBufferMaxHeapSize =
							    DEFAULT_TRANSFER_BUFFER_SIZE_MB * 1024 * 1024; // Placeholder
							transferConfig.executor = Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
							    "TransferManagerExecutor", DEFAULT_TRANSFER_MANAGER_THREADS); // Placeholder

							g_transferManager = Aws::Transfer::TransferManager::Create(transferConfig);

							if (!g_s3Client || !g_transferManager) {
								throw std::runtime_error("Failed to initialize S3Client or TransferManager.");
							}

							g_initializationSucceeded = true;
							startTransferMonitors(); // Start our monitor threads
							TraceEvent(SevInfo, "S3TransferInit").detail("Status", "Success");

						} catch (const std::exception& e) {
							TraceEvent(SevError, "S3TransferInit").detail("Status", "Failed").detail("Error", e.what());
							g_initializationSucceeded = false;
						} catch (...) {
							TraceEvent(SevError, "S3TransferInit")
							    .detail("Status", "Failed")
							    .detail("Error", "Unknown");
							g_initializationSucceeded = false;
						}
					}

					// Ensures initialization is attempted once and checks for success.
					void ensureInitialized() {
						std::call_once(g_initFlag, initializeAwsSdkInternal);
						if (!g_initializationSucceeded.load()) {
							throw backup_error(); // Throw FDB error if init failed
						}
					}

					// Function to add work to the queue
					void enqueueTransfer(std::shared_ptr<Aws::Transfer::TransferHandle> handle, Promise<Void> promise) {
						{
							std::lock_guard<std::mutex> lock(g_queueMutex);
							if (g_stopMonitorThreads.load()) {
								TraceEvent(SevWarn, "S3TransferEnqueue").detail("Status", "MonitorsStopped");
								promise.sendError(backup_error().asError()); // Or another error indicating shutdown
								return;
							}
							g_transferQueue.emplace_back(
							    TransferMonitorWorkItem{ std::move(handle), std::move(promise) });
							TraceEvent(SevVerbose, "S3TransferEnqueue").detail("QueueSize", g_transferQueue.size());
						}
						g_queueCondVar.notify_one(); // Signal one waiting thread
					}

				} // namespace

				ACTOR Future<Void> uploadFileWithTransferManager(
				    std::string localFile, std::string bucketName, std::string objectKey) {
					state Promise<Void> p;
					state Future<Void> f = p.getFuture();
					state std::string file = localFile;
					state std::string bucket = bucketName;
					state std::string key = objectKey;

					// Ensure AWS SDK, manager, and monitor threads are ready
					// This might throw, propagating the error back to the caller
					ensureInitialized();

					TraceEvent(SevInfo, "S3UploadStart")
					    .detail("LocalFile", file)
					    .detail("Bucket", bucket)
					    .detail("ObjectKey", key);

					try {
						// Check if file exists and is readable before attempting upload?
						// auto fileAttr = platform::getFileAttributes(file);
						// if (!fileAttr.present() || !fileAttr.size() >= 0) { ... throw ... }

						// Use default content-type or allow customization if needed
						auto uploadHandle =
						    g_transferManager->UploadFile(file, bucket, key, "application/octet-stream", {});
						enqueueTransfer(std::move(uploadHandle),
						                std::move(p)); // Pass handle and promise to monitor queue

						wait(f); // Wait on the promise fulfilled by the monitor thread via g_network->signal

						TraceEvent(SevInfo, "S3UploadSuccess")
						    .detail("LocalFile", file)
						    .detail("Bucket", bucket)
						    .detail("ObjectKey", key);
						return Void();
					} catch (Error& e) {
						TraceEvent(SevError, "S3UploadFailed")
						    .detail("LocalFile", file)
						    .detail("Bucket", bucket)
						    .detail("ObjectKey", key)
						    .error(e);
						throw; // Re-throw the error caught by wait(f) or thrown by ensureInitialized
					} catch (...) {
						// Catch any other potential exceptions (though ensureInitialized should handle std::exception)
						TraceEvent(SevError, "S3UploadFailed")
						    .detail("LocalFile", file)
						    .detail("Bucket", bucket)
						    .detail("ObjectKey", key)
						    .error(unknown_error());
						throw unknown_error();
					}
				}

				ACTOR Future<Void> downloadFileWithTransferManager(
				    std::string bucketName, std::string objectKey, std::string localFile) {
					state Promise<Void> p;
					state Future<Void> f = p.getFuture();
					state std::string bucket = bucketName;
					state std::string key = objectKey;
					state std::string file = localFile;

					// Ensure AWS SDK, manager, and monitor threads are ready
					ensureInitialized();

					TraceEvent(SevInfo, "S3DownloadStart")
					    .detail("Bucket", bucket)
					    .detail("ObjectKey", key)
					    .detail("LocalFile", file);

					try {
						// Ensure target directory exists before calling DownloadFile
						// platform::createDirectory(parentDirectoryOf(file)); // Add helper if needed

						auto downloadHandle = g_transferManager->DownloadFile(bucket, key, file);
						enqueueTransfer(std::move(downloadHandle),
						                std::move(p)); // Pass handle and promise to monitor queue

						wait(f); // Wait on the promise fulfilled by the monitor thread via g_network->signal

						TraceEvent(SevInfo, "S3DownloadSuccess")
						    .detail("Bucket", bucket)
						    .detail("ObjectKey", key)
						    .detail("LocalFile", file);
						return Void();
					} catch (Error& e) {
						TraceEvent(SevError, "S3DownloadFailed")
						    .detail("Bucket", bucket)
						    .detail("ObjectKey", key)
						    .detail("LocalFile", file)
						    .error(e);
						throw;
					} catch (...) {
						TraceEvent(SevError, "S3DownloadFailed")
						    .detail("Bucket", bucket)
						    .detail("ObjectKey", key)
						    .detail("LocalFile", file)
						    .error(unknown_error());
						throw unknown_error();
					}
				}

				// Function to be called during shutdown (e.g., from stopNetwork or similar)
				// Needs to be integrated into FDB's shutdown process if required.
				void shutdownS3TransferManager() {
					TraceEvent(SevInfo, "S3TransferShutdown").detail("Status", "Starting");
					stopTransferMonitors(); // Stop monitors first

					// Reset shared pointers. This should trigger cleanup if ref count hits zero.
					// Order matters: Reset TransferManager before S3Client.
					if (g_transferManager) {
						g_transferManager.reset();
						TraceEvent(SevInfo, "S3TransferShutdown").detail("TransferManager", "Reset");
					}
					if (g_s3Client) {
						g_s3Client.reset();
						TraceEvent(SevInfo, "S3TransferShutdown").detail("S3Client", "Reset");
					}

					// Only shutdown API if we successfully initialized
					if (g_initializationSucceeded.load()) {
						Aws::ShutdownAPI(g_options);
						Aws::Utils::Logging::ShutdownAWSLogging();
						g_initializationSucceeded = false; // Reset flag
						TraceEvent(SevInfo, "S3TransferShutdown").detail("AWSSDK", "ShutdownComplete");
					} else {
						TraceEvent(SevInfo, "S3TransferShutdown").detail("AWSSDK", "Skipped (Not Initialized)");
					}

					// Reset the once_flag? Not directly possible.
					// If re-initialization is needed later, the application structure would need to handle it.
				}
