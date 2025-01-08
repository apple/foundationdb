/*
 * S3BlobStore.h
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

#pragma once

#include <map>
#include <unordered_map>
#include <functional>
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/Net2Packet.h"
#include "fdbclient/Knobs.h"
#include "flow/IRateControl.h"
#include "fdbrpc/HTTP.h"
#include "fdbrpc/Stats.h"
#include "fdbclient/JSONDoc.h"
#include "flow/IConnection.h"

#include <boost/functional/hash.hpp>

// unique key that identifies interchangeable connections for the same settings and destination
// FIXME: can we define std::hash as a struct member of a S3BlobStoreEndpoint?
struct BlobStoreConnectionPoolKey {
	std::string host;
	std::string service;
	std::string region;
	bool isTLS;

	BlobStoreConnectionPoolKey(const std::string& host,
	                           const std::string& service,
	                           const std::string& region,
	                           bool isTLS)
	  : host(host), service(service), region(region), isTLS(isTLS) {}

	bool operator==(const BlobStoreConnectionPoolKey& other) const {
		return isTLS == other.isTLS && host == other.host && service == other.service && region == other.region;
	}
};

namespace std {
template <>
struct hash<BlobStoreConnectionPoolKey> {
	std::size_t operator()(const BlobStoreConnectionPoolKey& key) const {
		std::size_t seed = 0;
		boost::hash_combine(seed, std::hash<std::string>{}(key.host));
		boost::hash_combine(seed, std::hash<std::string>{}(key.service));
		boost::hash_combine(seed, std::hash<std::string>{}(key.region));
		boost::hash_combine(seed, std::hash<bool>{}(key.isTLS));
		return seed;
	}
};
} // namespace std

// Representation of all the things you need to connect to a blob store instance with some credentials.
// Reference counted because a very large number of them could be needed.
class S3BlobStoreEndpoint : public ReferenceCounted<S3BlobStoreEndpoint> {
public:
	struct Stats {
		Stats() : requests_successful(0), requests_failed(0), bytes_sent(0) {}
		Stats operator-(const Stats& rhs);
		void clear() { memset(this, 0, sizeof(*this)); }
		json_spirit::mObject getJSON();

		int64_t requests_successful;
		int64_t requests_failed;
		int64_t bytes_sent;
	};

	static Stats s_stats;

	struct BlobStats {
		UID id;
		CounterCollection cc;
		Counter requestsSuccessful;
		Counter requestsFailed;
		Counter newConnections;
		Counter expiredConnections;
		Counter reusedConnections;
		Counter fastRetries;

		LatencySample requestLatency;

		// init not in static codepath, to avoid initialization race issues and so no blob connections means no
		// unnecessary blob stats traces
		BlobStats()
		  : id(deterministicRandom()->randomUniqueID()), cc("BlobStoreStats", id.toString()),
		    requestsSuccessful("RequestsSuccessful", cc), requestsFailed("RequestsFailed", cc),
		    newConnections("NewConnections", cc), expiredConnections("ExpiredConnections", cc),
		    reusedConnections("ReusedConnections", cc), fastRetries("FastRetries", cc),
		    requestLatency("BlobStoreRequestLatency",
		                   id,
		                   CLIENT_KNOBS->BLOBSTORE_LATENCY_LOGGING_INTERVAL,
		                   CLIENT_KNOBS->BLOBSTORE_LATENCY_LOGGING_ACCURACY) {}
	};
	// null when initialized, so no blob stats until a blob connection is used
	static std::unique_ptr<BlobStats> blobStats;
	static Future<Void> statsLogger;

	void maybeStartStatsLogger() {
		if (!blobStats && CLIENT_KNOBS->BLOBSTORE_ENABLE_LOGGING) {
			blobStats = std::make_unique<BlobStats>();
			specialCounter(
			    blobStats->cc, "GlobalConnectionPoolCount", [this]() { return this->globalConnectionPool.size(); });
			specialCounter(blobStats->cc, "GlobalConnectionPoolSize", [this]() {
				// FIXME: could track this explicitly via an int variable with extra logic, but this should be small and
				// infrequent
				int totalConnections = 0;
				for (auto& it : this->globalConnectionPool) {
					totalConnections += it.second->pool.size();
				}
				return totalConnections;
			});

			statsLogger = blobStats->cc.traceCounters(
			    "BlobStoreMetrics", blobStats->id, CLIENT_KNOBS->BLOBSTORE_STATS_LOGGING_INTERVAL, "BlobStoreMetrics");
		}
	}

	struct Credentials {
		std::string key;
		std::string secret;
		std::string securityToken;
	};

	struct BlobKnobs {
		BlobKnobs();
		int secure_connection, connect_tries, connect_timeout, max_connection_life, request_tries, request_timeout_min,
		    requests_per_second, list_requests_per_second, write_requests_per_second, read_requests_per_second,
		    delete_requests_per_second, multipart_max_part_size, multipart_min_part_size, concurrent_requests,
		    concurrent_uploads, concurrent_lists, concurrent_reads_per_file, concurrent_writes_per_file,
		    enable_read_cache, read_block_size, read_ahead_blocks, read_cache_blocks_per_file,
		    max_send_bytes_per_second, max_recv_bytes_per_second, sdk_auth, enable_object_integrity_check,
		    global_connection_pool, max_delay_retryable_error, max_delay_connection_failed;

		bool set(StringRef name, int value);
		std::string getURLParameters() const;
		static std::vector<std::string> getKnobDescriptions() {
			return {
				"secure_connection (or sc)             Set 1 for secure connection and 0 for insecure connection.",
				"connect_tries (or ct)                 Number of times to try to connect for each request.",
				"connect_timeout (or cto)              Number of seconds to wait for a connect request to succeed.",
				"max_connection_life (or mcl)          Maximum number of seconds to use a single TCP connection.",
				"request_tries (or rt)                 Number of times to try each request until a parsable HTTP "
				"response other than 429 is received.",
				"request_timeout_min (or rtom)         Number of seconds to wait for a request to succeed after a "
				"connection is established.",
				"requests_per_second (or rps)          Max number of requests to start per second.",
				"list_requests_per_second (or lrps)    Max number of list requests to start per second.",
				"write_requests_per_second (or wrps)   Max number of write requests to start per second.",
				"read_requests_per_second (or rrps)    Max number of read requests to start per second.",
				"delete_requests_per_second (or drps)  Max number of delete requests to start per second.",
				"multipart_max_part_size (or maxps)    Max part size for multipart uploads.",
				"multipart_min_part_size (or minps)    Min part size for multipart uploads.",
				"concurrent_requests (or cr)           Max number of total requests in progress at once, regardless of "
				"operation-specific concurrency limits.",
				"concurrent_uploads (or cu)            Max concurrent uploads (part or whole) that can be in progress "
				"at once.",
				"concurrent_lists (or cl)              Max concurrent list operations that can be in progress at once.",
				"concurrent_reads_per_file (or crps)   Max concurrent reads in progress for any one file.",
				"concurrent_writes_per_file (or cwps)  Max concurrent uploads in progress for any one file.",
				"enable_read_cache (or erc)            Whether read block caching is enabled.",
				"read_block_size (or rbs)              Block size in bytes to be used for reads.",
				"read_ahead_blocks (or rab)            Number of blocks to read ahead of requested offset.",
				"read_cache_blocks_per_file (or rcb)   Size of the read cache for a file in blocks.",
				"max_send_bytes_per_second (or sbps)   Max send bytes per second for all requests combined.",
				"max_recv_bytes_per_second (or rbps)   Max receive bytes per second for all requests combined (NOT YET "
				"USED).",
				"max_delay_retryable_error (or dre)    Max seconds to delay before retry when see a retryable error.",
				"max_delay_connection_failed (or dcf)  Max seconds to delay before retry when see a connection "
				"failure.",
				"sdk_auth (or sa)                      Use AWS SDK to resolve credentials. Only valid if "
				"BUILD_AWS_BACKUP is enabled.",
				"enable_object_integrity_check (or eoic) Enable integrity check on GET requests (Default: false).",
				"global_connection_pool (or gcp)       Enable shared connection pool between all blobstore instances."
			};
		}

		bool isTLS() const { return secure_connection == 1; }
	};

	struct ReusableConnection {
		Reference<IConnection> conn;
		double expirationTime;
	};

	// basically, reference counted queue with option to add other fields
	struct ConnectionPoolData : NonCopyable, ReferenceCounted<ConnectionPoolData> {
		std::queue<ReusableConnection> pool;
	};

	// global connection pool for multiple blobstore endpoints with same connection settings and request destination
	static std::unordered_map<BlobStoreConnectionPoolKey, Reference<ConnectionPoolData>> globalConnectionPool;

	S3BlobStoreEndpoint(std::string const& host,
	                    std::string const& service,
	                    std::string region,
	                    Optional<std::string> const& proxyHost,
	                    Optional<std::string> const& proxyPort,
	                    Optional<Credentials> const& creds,
	                    BlobKnobs const& knobs = BlobKnobs(),
	                    HTTP::Headers extraHeaders = HTTP::Headers())
	  : host(host), service(service), region(region), proxyHost(proxyHost), proxyPort(proxyPort),
	    useProxy(proxyHost.present() && proxyPort.present()), credentials(creds),
	    lookupKey(creds.present() && creds.get().key.empty()),
	    lookupSecret(creds.present() && creds.get().secret.empty()), knobs(knobs), extraHeaders(extraHeaders),
	    requestRate(new SpeedLimit(knobs.requests_per_second, 1)),
	    requestRateList(new SpeedLimit(knobs.list_requests_per_second, 1)),
	    requestRateWrite(new SpeedLimit(knobs.write_requests_per_second, 1)),
	    requestRateRead(new SpeedLimit(knobs.read_requests_per_second, 1)),
	    requestRateDelete(new SpeedLimit(knobs.delete_requests_per_second, 1)),
	    sendRate(new SpeedLimit(knobs.max_send_bytes_per_second, 1)),
	    recvRate(new SpeedLimit(knobs.max_recv_bytes_per_second, 1)), concurrentRequests(knobs.concurrent_requests),
	    concurrentUploads(knobs.concurrent_uploads), concurrentLists(knobs.concurrent_lists) {

		if (host.empty() || (proxyHost.present() != proxyPort.present()))
			throw connection_string_invalid();

		// set connection pool instance
		if (useProxy || !knobs.global_connection_pool) {
			// don't use global connection pool if there's a proxy, as it complicates the logic
			// FIXME: handle proxies?
			connectionPool = makeReference<ConnectionPoolData>();
		} else {
			BlobStoreConnectionPoolKey key(host, service, region, knobs.isTLS());
			auto it = globalConnectionPool.find(key);
			if (it != globalConnectionPool.end()) {
				connectionPool = it->second;
			} else {
				connectionPool = makeReference<ConnectionPoolData>();
				globalConnectionPool.insert({ key, connectionPool });
			}
		}
		ASSERT(connectionPool.isValid());

		maybeStartStatsLogger();
	}

	static std::string getURLFormat(bool withResource = false) {
		const char* resource = "";
		if (withResource)
			resource = "<name>";
		return format("blobstore://<api_key>:<secret>:<security_token>@<host>[:<port>]/"
		              "%s[?<param>=<value>[&<param>=<value>]...]",
		              resource);
	}

	typedef std::map<std::string, std::string> ParametersT;

	// Parse url and return a S3BlobStoreEndpoint
	// If the url has parameters that S3BlobStoreEndpoint can't consume then an error will be thrown unless
	// ignored_parameters is given in which case the unconsumed parameters will be added to it.
	static Reference<S3BlobStoreEndpoint> fromString(const std::string& url,
	                                                 const Optional<std::string>& proxy,
	                                                 std::string* resourceFromURL,
	                                                 std::string* error,
	                                                 ParametersT* ignored_parameters);

	// Get a normalized version of this URL with the given resource and any non-default BlobKnob values as URL
	// parameters in addition to the passed params string
	std::string getResourceURL(std::string resource, std::string params) const;

	// FIXME: add periodic connection reaper to pool
	// local connection pool for this blobstore
	Reference<ConnectionPoolData> connectionPool;
	Future<ReusableConnection> connect(bool* reusingConn);
	void returnConnection(ReusableConnection& conn);

	std::string host;
	std::string service;
	std::string region;
	Optional<std::string> proxyHost;
	Optional<std::string> proxyPort;
	bool useProxy;

	Optional<Credentials> credentials;
	bool lookupKey;
	bool lookupSecret;
	BlobKnobs knobs;
	HTTP::Headers extraHeaders;

	// Speed and concurrency limits
	Reference<IRateControl> requestRate;
	Reference<IRateControl> requestRateList;
	Reference<IRateControl> requestRateWrite;
	Reference<IRateControl> requestRateRead;
	Reference<IRateControl> requestRateDelete;
	Reference<IRateControl> sendRate;
	Reference<IRateControl> recvRate;
	FlowLock concurrentRequests;
	FlowLock concurrentUploads;
	FlowLock concurrentLists;

	Future<Void> updateSecret();

	// Calculates the authentication string from the secret key
	static std::string hmac_sha1(Credentials const& creds, std::string const& msg);

	// Sets headers needed for Authorization (including Date which will be overwritten if present)
	void setAuthHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers);

	// Set headers in the AWS V4 authorization format. $date and $datestamp are used for unit testing
	void setV4AuthHeaders(const std::string& verb,
	                      const std::string& resource,
	                      HTTP::Headers& headers,
	                      std::string date = "",
	                      std::string datestamp = "");

	std::string getHost() const { return host; }

	std::string getRegion() const { return region; }

	// Do an HTTP request to the Blob Store, read the response.  Handles authentication.
	// Every blob store interaction should ultimately go through this function

	Future<Reference<HTTP::IncomingResponse>> doRequest(std::string const& verb,
	                                                    std::string const& resource,
	                                                    const HTTP::Headers& headers,
	                                                    UnsentPacketQueue* pContent,
	                                                    int contentLen,
	                                                    std::set<unsigned int> successCodes);

	struct ObjectInfo {
		std::string name;
		int64_t size;
	};

	struct ListResult {
		std::vector<std::string> commonPrefixes;
		std::vector<ObjectInfo> objects;
	};

	// Get bucket contents via a stream, since listing large buckets will take many serial blob requests
	// If a delimiter is passed then common prefixes will be read in parallel, recursively, depending on recurseFilter.
	// Recursefilter is a must be a function that takes a string and returns true if it passes.  The default behavior is
	// to assume true.
	Future<Void> listObjectsStream(std::string const& bucket,
	                               PromiseStream<ListResult> results,
	                               Optional<std::string> prefix = {},
	                               Optional<char> delimiter = {},
	                               int maxDepth = 0,
	                               std::function<bool(std::string const&)> recurseFilter = nullptr);

	// Get a list of the files in a bucket, see listObjectsStream for more argument detail.
	Future<ListResult> listObjects(std::string const& bucket,
	                               Optional<std::string> prefix = {},
	                               Optional<char> delimiter = {},
	                               int maxDepth = 0,
	                               std::function<bool(std::string const&)> recurseFilter = nullptr);

	// Get a list of all buckets
	Future<std::vector<std::string>> listBuckets();

	// Check if a bucket exists
	Future<bool> bucketExists(std::string const& bucket);

	// Check if an object exists in a bucket
	Future<bool> objectExists(std::string const& bucket, std::string const& object);

	// Get the size of an object in a bucket
	Future<int64_t> objectSize(std::string const& bucket, std::string const& object);

	// Read an arbitrary segment of an object
	Future<int> readObject(std::string const& bucket,
	                       std::string const& object,
	                       void* data,
	                       int length,
	                       int64_t offset);

	// Delete an object in a bucket
	Future<Void> deleteObject(std::string const& bucket, std::string const& object);

	// Delete all objects in a bucket under a prefix.  Note this is not atomic as blob store does not
	// support this operation directly. This method is just a convenience method that lists and deletes
	// all of the objects in the bucket under the given prefix.
	// Since it can take a while, if a pNumDeleted and/or pBytesDeleted are provided they will be incremented every time
	// a deletion of an object completes.
	Future<Void> deleteRecursively(std::string const& bucket,
	                               std::string prefix = "",
	                               int* pNumDeleted = nullptr,
	                               int64_t* pBytesDeleted = nullptr);

	// Create a bucket if it does not already exists.
	Future<Void> createBucket(std::string const& bucket);

	// Useful methods for working with tiny files
	Future<std::string> readEntireFile(std::string const& bucket, std::string const& object);
	Future<Void> writeEntireFile(std::string const& bucket, std::string const& object, std::string const& content);
	Future<Void> writeEntireFileFromBuffer(std::string const& bucket,
	                                       std::string const& object,
	                                       UnsentPacketQueue* pContent,
	                                       int contentLen,
	                                       std::string const& contentHash);

	// MultiPart upload methods
	// Returns UploadID
	Future<std::string> beginMultiPartUpload(std::string const& bucket, std::string const& object);
	// Returns eTag
	Future<std::string> uploadPart(std::string const& bucket,
	                               std::string const& object,
	                               std::string const& uploadID,
	                               unsigned int partNumber,
	                               UnsentPacketQueue* pContent,
	                               int contentLen,
	                               std::string const& contentHash);
	typedef std::map<int, std::string> MultiPartSetT;
	Future<Void> finishMultiPartUpload(std::string const& bucket,
	                                   std::string const& object,
	                                   std::string const& uploadID,
	                                   MultiPartSetT const& parts);
};
