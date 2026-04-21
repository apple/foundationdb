/*
 * BlobStoreCommon.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/IBlobStore.h"
#include "fdbclient/S3BlobStore.h"
#include "fdbclient/GCSBlobStore.h"
#include "fdbclient/Knobs.h"
#include "flow/Hostname.h"
#include "flow/IAsyncFile.h"
#include "flow/IConnection.h"
#include "flow/Trace.h"
#include "flow/CoroUtils.h"
#include "flow/genericactors.actor.h"

#include "md5/md5.h"
#include "libb64/encode.h"
#include <openssl/sha.h>

#include <boost/algorithm/string.hpp>
#include <climits>
#include <limits>

IBlobStoreEndpoint::ConnectionPoolData::~ConnectionPoolData() {
	// In simulation, explicitly close all pooled connections before destruction.
	// This satisfies Sim2Conn's assertion: !opened || closedByCaller
	// Without this, connections would be destroyed without being closed, causing assertion failures.
	if (g_network && g_network->isSimulated()) {
		while (!pool.empty()) {
			ReusableConnection& rconn = pool.front();
			if (rconn.conn.isValid()) {
				rconn.conn->close();
			}
			pool.pop();
		}
	}
}

json_spirit::mObject IBlobStoreEndpoint::Stats::getJSON() {
	json_spirit::mObject o;

	o["requests_failed"] = requests_failed;
	o["requests_successful"] = requests_successful;
	o["bytes_sent"] = bytes_sent;

	return o;
}

IBlobStoreEndpoint::Stats IBlobStoreEndpoint::Stats::operator-(const Stats& rhs) {
	Stats r;
	r.requests_failed = requests_failed - rhs.requests_failed;
	r.requests_successful = requests_successful - rhs.requests_successful;
	r.bytes_sent = bytes_sent - rhs.bytes_sent;
	return r;
}

IBlobStoreEndpoint::Stats IBlobStoreEndpoint::s_stats;

BlobKnobs::BlobKnobs() {
	secure_connection = 1;
	connect_tries = CLIENT_KNOBS->BLOBSTORE_CONNECT_TRIES;
	connect_timeout = CLIENT_KNOBS->BLOBSTORE_CONNECT_TIMEOUT;
	max_connection_life = CLIENT_KNOBS->BLOBSTORE_MAX_CONNECTION_LIFE;
	request_tries = CLIENT_KNOBS->BLOBSTORE_REQUEST_TRIES;
	request_timeout_min = CLIENT_KNOBS->BLOBSTORE_REQUEST_TIMEOUT_MIN;
	requests_per_second = CLIENT_KNOBS->BLOBSTORE_REQUESTS_PER_SECOND;
	concurrent_requests = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS;
	list_requests_per_second = CLIENT_KNOBS->BLOBSTORE_LIST_REQUESTS_PER_SECOND;
	write_requests_per_second = CLIENT_KNOBS->BLOBSTORE_WRITE_REQUESTS_PER_SECOND;
	read_requests_per_second = CLIENT_KNOBS->BLOBSTORE_READ_REQUESTS_PER_SECOND;
	delete_requests_per_second = CLIENT_KNOBS->BLOBSTORE_DELETE_REQUESTS_PER_SECOND;
	multipart_max_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MAX_PART_SIZE;
	multipart_min_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	concurrent_uploads = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_UPLOADS;
	concurrent_lists = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_LISTS;
	concurrent_reads_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_READS_PER_FILE;
	concurrent_writes_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
	enable_read_cache = CLIENT_KNOBS->BLOBSTORE_ENABLE_READ_CACHE;
	read_block_size = CLIENT_KNOBS->BLOBSTORE_READ_BLOCK_SIZE;
	read_ahead_blocks = CLIENT_KNOBS->BLOBSTORE_READ_AHEAD_BLOCKS;
	read_cache_blocks_per_file = CLIENT_KNOBS->BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	max_send_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	max_recv_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;
	max_delay_retryable_error = CLIENT_KNOBS->BLOBSTORE_MAX_DELAY_RETRYABLE_ERROR;
	max_delay_connection_failed = CLIENT_KNOBS->BLOBSTORE_MAX_DELAY_CONNECTION_FAILED;
	multipart_retry_delay_ms = CLIENT_KNOBS->BLOBSTORE_MULTIPART_RETRY_DELAY_MS;
	sdk_auth = false;
	enable_object_integrity_check = CLIENT_KNOBS->BLOBSTORE_ENABLE_OBJECT_INTEGRITY_CHECK;
	global_connection_pool = CLIENT_KNOBS->BLOBSTORE_GLOBAL_CONNECTION_POOL;
}

bool BlobKnobs::set(StringRef name, int value) {
#define TRY_PARAM(n, sn)                                                                                               \
	if (name == #n || name == #sn) {                                                                                   \
		n = value;                                                                                                     \
		return true;                                                                                                   \
	}
	TRY_PARAM(secure_connection, sc)
	TRY_PARAM(connect_tries, ct);
	TRY_PARAM(connect_timeout, cto);
	TRY_PARAM(max_connection_life, mcl);
	TRY_PARAM(request_tries, rt);
	TRY_PARAM(request_timeout_min, rtom);
	// TODO: For backward compatibility because request_timeout was renamed to request_timeout_min
	if (name == "request_timeout"_sr || name == "rto"_sr) {
		request_timeout_min = value;
		return true;
	}
	TRY_PARAM(requests_per_second, rps);
	TRY_PARAM(list_requests_per_second, lrps);
	TRY_PARAM(write_requests_per_second, wrps);
	TRY_PARAM(read_requests_per_second, rrps);
	TRY_PARAM(delete_requests_per_second, drps);
	TRY_PARAM(concurrent_requests, cr);
	TRY_PARAM(multipart_max_part_size, maxps);
	TRY_PARAM(multipart_min_part_size, minps);
	TRY_PARAM(multipart_retry_delay_ms, mrd);
	TRY_PARAM(concurrent_uploads, cu);
	TRY_PARAM(concurrent_lists, cl);
	TRY_PARAM(concurrent_reads_per_file, crpf);
	TRY_PARAM(concurrent_writes_per_file, cwpf);
	TRY_PARAM(enable_read_cache, erc);
	TRY_PARAM(read_block_size, rbs);
	TRY_PARAM(read_ahead_blocks, rab);
	TRY_PARAM(read_cache_blocks_per_file, rcb);
	TRY_PARAM(max_send_bytes_per_second, sbps);
	TRY_PARAM(max_recv_bytes_per_second, rbps);
	TRY_PARAM(max_delay_retryable_error, dre);
	TRY_PARAM(max_delay_connection_failed, dcf);
	TRY_PARAM(sdk_auth, sa);
	TRY_PARAM(enable_object_integrity_check, eoic);
	TRY_PARAM(global_connection_pool, gcp);
#undef TRY_PARAM
	return false;
}

// Returns a Blob URL parameter string that specifies all of the non-default options using short names.
std::string BlobKnobs::getURLParameters() const {
	static BlobKnobs defaults;
	std::string r;
#define _CHECK_PARAM(n, sn)                                                                                            \
	if (n != defaults.n) {                                                                                             \
		r += format("%s%s=%d", r.empty() ? "" : "&", #sn, n);                                                          \
	}
	_CHECK_PARAM(secure_connection, sc);
	_CHECK_PARAM(connect_tries, ct);
	_CHECK_PARAM(connect_timeout, cto);
	_CHECK_PARAM(max_connection_life, mcl);
	_CHECK_PARAM(request_tries, rt);
	_CHECK_PARAM(request_timeout_min, rto);
	_CHECK_PARAM(requests_per_second, rps);
	_CHECK_PARAM(list_requests_per_second, lrps);
	_CHECK_PARAM(write_requests_per_second, wrps);
	_CHECK_PARAM(read_requests_per_second, rrps);
	_CHECK_PARAM(delete_requests_per_second, drps);
	_CHECK_PARAM(concurrent_requests, cr);
	_CHECK_PARAM(multipart_max_part_size, maxps);
	_CHECK_PARAM(multipart_min_part_size, minps);
	_CHECK_PARAM(concurrent_uploads, cu);
	_CHECK_PARAM(concurrent_lists, cl);
	_CHECK_PARAM(concurrent_reads_per_file, crpf);
	_CHECK_PARAM(concurrent_writes_per_file, cwpf);
	_CHECK_PARAM(enable_read_cache, erc);
	_CHECK_PARAM(read_block_size, rbs);
	_CHECK_PARAM(read_ahead_blocks, rab);
	_CHECK_PARAM(read_cache_blocks_per_file, rcb);
	_CHECK_PARAM(max_send_bytes_per_second, sbps);
	_CHECK_PARAM(max_recv_bytes_per_second, rbps);
	_CHECK_PARAM(sdk_auth, sa);
	_CHECK_PARAM(enable_object_integrity_check, eoic);
	_CHECK_PARAM(global_connection_pool, gcp);
	_CHECK_PARAM(max_delay_retryable_error, dre);
	_CHECK_PARAM(max_delay_connection_failed, dcf);
#undef _CHECK_PARAM
	return r;
}

Future<Optional<json_spirit::mObject>> IBlobStoreEndpoint::tryReadJSONFile(std::string path) {
	std::string content;

	// Event type to be logged in the event of an exception
	const char* errorEventType = "BlobCredentialFileError";

	try {
		Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
		    path, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0);
		int64_t size = co_await f->size();
		Standalone<StringRef> buf = makeString(size);
		int r = co_await f->read(mutateString(buf), size, 0);
		ASSERT(r == size);
		content = buf.toString();

		// Any exceptions from here forward are parse failures
		errorEventType = "BlobCredentialFileParseFailed";
		json_spirit::mValue json;
		json_spirit::read_string(content, json);
		if (json.type() == json_spirit::obj_type)
			co_return json.get_obj();
		else
			TraceEvent(SevWarn, "BlobCredentialFileNotJSONObject").suppressFor(60).detail("File", path);

	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled)
			TraceEvent(SevWarn, errorEventType).errorUnsuppressed(e).suppressFor(60).detail("File", path);
	}

	co_return Optional<json_spirit::mObject>();
}

std::string IBlobStoreEndpoint::getResourceURL(std::string resource, std::string params) const {
	std::string hostPort = host;
	if (!service.empty()) {
		hostPort.append(":");
		hostPort.append(service);
	}

	std::string r = format("blobstore://@%s/%s", hostPort.c_str(), resource.c_str());

	// Get params that are deviations from knob defaults
	std::string knobParams = knobs.getURLParameters();
	if (!knobParams.empty()) {
		if (!params.empty()) {
			params.append("&");
		}
		params.append(knobParams);
	}

	for (const auto& [k, v] : extraHeaders) {
		if (!params.empty()) {
			params.append("&");
		}
		params.append("header=");
		params.append(k);
		params.append(":");
		params.append(v);
	}

	if (!params.empty())
		r.append("?").append(params);

	return r;
}

Reference<IBlobStoreEndpoint> IBlobStoreEndpoint::fromString(const std::string& url,
                                                             const Optional<std::string>& proxy,
                                                             std::string* resourceFromURL,
                                                             std::string* error,
                                                             ParametersT* ignored_parameters) {
	if (resourceFromURL)
		resourceFromURL->clear();

	try {
		// Replace HTML-encoded ampersands with raw ampersands
		std::string decoded_url = url;
		size_t pos = 0;
		while ((pos = decoded_url.find("&amp;", pos)) != std::string::npos) {
			decoded_url.replace(pos, 5, "&");
			pos += 1;
		}

		// Also handle double-encoded ampersands
		pos = 0;
		while ((pos = decoded_url.find("&amp;amp;", pos)) != std::string::npos) {
			decoded_url.replace(pos, 9, "&");
			pos += 1;
		}

		StringRef t(decoded_url);
		StringRef prefix = t.eat("://");
		if (prefix != "blobstore"_sr)
			throw format("Invalid blobstore URL prefix '%s'", prefix.toString().c_str());

		Optional<std::string> proxyHost, proxyPort;
		if (proxy.present()) {
			StringRef proxyRef(proxy.get());
			if (proxy.get().find("://") != std::string::npos) {
				StringRef proxyPrefix = proxyRef.eat("://");
				if (proxyPrefix != "http"_sr) {
					throw format("Invalid proxy URL prefix '%s'. Either don't use a prefix, or use http://",
					             proxyPrefix.toString().c_str());
				}
			}
			std::string proxyBody = proxyRef.eat().toString();
			if (!Hostname::isHostname(proxyBody) && !NetworkAddress::parseOptional(proxyBody).present()) {
				throw format("'%s' is not a valid value for proxy. Format should be either IP:port or host:port.",
				             proxyBody.c_str());
			}
			StringRef p(proxyBody);
			proxyHost = p.eat(":").toString();
			proxyPort = p.eat().toString();
		}

		Optional<StringRef> cred;
		if (url.find("@") != std::string::npos) {
			cred = t.eat("@");
		}
		uint8_t foundSeparator = 0;
		StringRef hostPort = t.eatAny("/?", &foundSeparator);
		StringRef resource;
		if (foundSeparator == '/') {
			resource = t.eat("?");
		}

		// hostPort is at least a host or IP address, optionally followed by :portNumber or :serviceName
		StringRef h(hostPort);
		StringRef host = h.eat(":");
		if (host.size() == 0)
			throw std::string("host cannot be empty");

		StringRef service = h.eat();

		std::string region;
		std::string gcsProjectId;
		std::string provider = "s3";

		BlobKnobs knobs;
		HTTP::Headers extraHeaders;
		while (1) {
			StringRef name = t.eat("=");
			if (name.size() == 0)
				break;
			StringRef value = t.eat("&");

			// Special case for header
			if (name == "header"_sr) {
				StringRef originalValue = value;
				StringRef headerFieldName = value.eat(":");
				StringRef headerFieldValue = value;
				if (headerFieldName.size() == 0 || headerFieldValue.size() == 0) {
					throw format("'%s' is not a valid value for '%s' parameter.  Format is <FieldName>:<FieldValue> "
					             "where strings are not empty.",
					             originalValue.toString().c_str(),
					             name.toString().c_str());
				}
				std::string& fieldValue = extraHeaders[headerFieldName.toString()];
				// RFC 2616 section 4.2 says header field names can repeat but only if it is valid to concatenate
				// their values with comma separation
				if (!fieldValue.empty()) {
					fieldValue.append(",");
				}
				fieldValue.append(headerFieldValue.toString());
				continue;
			}

			// overwrite region from parameter
			if (name == "region"_sr) {
				region = value.toString();
				continue;
			}

			if (name == "gcs_project_id"_sr || name == "gcspid"_sr) {
				gcsProjectId = value.toString();
				continue;
			}

			if (name == "provider"_sr || name == "p"_sr) {
				provider = value.toString();
				continue;
			}

			// See if the parameter is a knob
			// First try setting a dummy value (all knobs are currently numeric) just to see if this parameter is
			// known. If it is, then we will set it to a good value or throw below, so the dummy set has no bad
			// side effects.
			bool known = knobs.set(name, 0);

			// If the parameter is not known then throw unless there is an ignored_parameters set to add it to
			if (!known) {
				if (ignored_parameters == nullptr) {
					throw format("%s is not a valid parameter name", name.toString().c_str());
				}
				(*ignored_parameters)[name.toString()] = value.toString();
				continue;
			}

			// The parameter is known so it must be numeric and valid.
			char* valueEnd = nullptr;
			std::string s = value.toString();
			long int ivalue = strtol(s.c_str(), &valueEnd, 10);
			if (*valueEnd || (ivalue == 0 && s != "0") ||
			    (((ivalue == LONG_MAX) || (ivalue == LONG_MIN)) && errno == ERANGE))
				throw format("%s is not a valid value for %s", s.c_str(), name.toString().c_str());

			// It should not be possible for this set to fail now since the dummy set above had to have worked.
			ASSERT(knobs.set(name, ivalue));
		}

		if (resourceFromURL != nullptr)
			*resourceFromURL = resource.toString();

		if (provider == "gcs") {
			return makeReference<GCSBlobStoreEndpoint>(
			    host.toString(), service.toString(), proxyHost, proxyPort, cred, gcsProjectId, knobs, extraHeaders);
		}

		return makeReference<S3BlobStoreEndpoint>(
		    host.toString(), service.toString(), region, proxyHost, proxyPort, cred, knobs, extraHeaders);

	} catch (std::string& err) {
		if (error != nullptr)
			*error = err;
		TraceEvent(SevWarnAlways, "BlobStoreEndpointBadURL")
		    .suppressFor(60)
		    .detail("Description", err)
		    .detail("Format", getURLFormat())
		    .detail("URL", url);
		throw backup_invalid_url();
	}
}

Future<Void> updateSecret_impl(Reference<IBlobStoreEndpoint> b) {
	auto* pFiles = (std::vector<std::string>*)g_network->global(INetwork::enBlobCredentialFiles);
	if (pFiles == nullptr)
		co_return;

	std::vector<Future<Optional<json_spirit::mObject>>> reads;
	for (auto& f : *pFiles)
		reads.push_back(IBlobStoreEndpoint::tryReadJSONFile(f));

	co_await waitForAll(reads);

	std::string credentialsFileKey = b->credentialFileKey();

	int invalid = 0;

	for (auto& f : reads) {
		// If value not present then the credentials file wasn't readable or valid. Continue to check other results.
		if (!f.get().present()) {
			TraceEvent(SevWarn, "BlobStoreCredentialFileNotReadable")
			    .detail("Endpoint", b->host)
			    .detail("Service", b->service);
			++invalid;
			continue;
		}

		JSONDoc doc(f.get().get());
		if (doc.has("accounts") && doc.last().type() == json_spirit::obj_type) {
			JSONDoc accounts(doc.last().get_obj());
			if (accounts.has(credentialsFileKey, false) && accounts.last().type() == json_spirit::obj_type) {
				JSONDoc account(accounts.last());
				if (b->extractCredentialFields(account)) {
					co_return;
				}
			}
		}
	}

	// If any sources were invalid
	if (invalid > 0) {
		TraceEvent(SevWarn, "BlobStoreCredentialFileInvalid")
		    .detail("Endpoint", b->host)
		    .detail("Service", b->service)
		    .detail("Invalid", invalid);
		throw backup_auth_unreadable();
	}

	// All sources were valid but didn't contain the desired info
	TraceEvent(SevWarn, "BlobStoreCredentialsMissing")
	    .detail("Endpoint", b->host)
	    .detail("Service", b->service)
	    .detail("Reason", "No valid credentials found");
	throw backup_auth_missing();
}

Future<Void> IBlobStoreEndpoint::updateSecret() {
	return updateSecret_impl(Reference<IBlobStoreEndpoint>::addRef(this));
}

AsyncResult<IBlobStoreEndpoint::ListResult> listObjects_impl(
    Reference<IBlobStoreEndpoint> bstore,
    std::string bucket,
    Optional<std::string> prefix,
    Optional<char> delimiter,
    int maxDepth,
    std::function<bool(std::string const&)> recurseFilterUnsafe) {
	// C++20 coroutine safety: copy const& params to survive across suspension.
	auto recurseFilter = recurseFilterUnsafe;

	IBlobStoreEndpoint::ListResult results;
	PromiseStream<IBlobStoreEndpoint::ListResult> resultStream;
	Future<Void> done = bstore->listObjectsStream(bucket, resultStream, prefix, delimiter, maxDepth, recurseFilter);
	// Wrap done in an actor which sends end_of_stream because list does not so that many lists can write to the same
	// stream
	done = map(done, [=](Void) mutable {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	try {
		while (true) {
			// Preserve actor choose{} semantics: if the stream result and completion are both ready,
			// drain the stream item before honoring done. Using race(done, stream) drops the final
			// stream item because race() breaks ties by argument order.
			auto res = co_await race(resultStream.getFuture(), done);
			if (res.index() == 0) {
				IBlobStoreEndpoint::ListResult info = std::get<0>(std::move(res));

				results.commonPrefixes.insert(
				    results.commonPrefixes.end(), info.commonPrefixes.begin(), info.commonPrefixes.end());
				results.objects.insert(results.objects.end(), info.objects.begin(), info.objects.end());
			} else if (res.index() == 1) {
				done = Never();
			} else {
				UNREACHABLE();
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	co_return results;
}

AsyncResult<IBlobStoreEndpoint::ListResult> IBlobStoreEndpoint::listObjects(
    std::string const& bucket,
    Optional<std::string> prefix,
    Optional<char> delimiter,
    int maxDepth,
    std::function<bool(std::string const&)> recurseFilter) {
	return listObjects_impl(
	    Reference<IBlobStoreEndpoint>::addRef(this), bucket, prefix, delimiter, maxDepth, recurseFilter);
}

Future<Void> deleteRecursively_impl(Reference<IBlobStoreEndpoint> b,
                                    std::string bucket,
                                    std::string prefix,
                                    int* pNumDeleted,
                                    int64_t* pBytesDeleted) {
	PromiseStream<IBlobStoreEndpoint::ListResult> resultStream;
	// Start a recursive parallel listing which will send results to resultStream as they are received
	Future<Void> done = b->listObjectsStream(bucket, resultStream, prefix, '/', std::numeric_limits<int>::max());
	// Wrap done in an actor which will send end_of_stream since listObjectsStream() does not (so that many calls can
	// write to the same stream)
	done = map(done, [=](Void) mutable {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	std::list<Future<Void>> deleteFutures;
	try {
		while (true) {
			// Preserve actor choose{} semantics: if the stream result and completion are both ready,
			// drain the stream item before honoring done. Using race(done, stream) drops the final
			// stream item because race() breaks ties by argument order.
			auto res = co_await race(resultStream.getFuture(), done);
			if (res.index() == 0) {
				IBlobStoreEndpoint::ListResult list = std::get<0>(std::move(res));

				for (auto& object : list.objects) {
					deleteFutures.push_back(map(b->deleteObject(bucket, object.name), [=](Void) -> Void {
						if (pNumDeleted != nullptr) {
							++*pNumDeleted;
						}
						if (pBytesDeleted != nullptr) {
							*pBytesDeleted += object.size;
						}
						return Void();
					}));
				}
			} else if (res.index() == 1) {
				done = Never();
			} else {
				UNREACHABLE();
			}

			// This is just a precaution to avoid having too many outstanding delete actors waiting to run
			while (deleteFutures.size() > CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS) {
				co_await deleteFutures.front();
				deleteFutures.pop_front();
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream)
			throw;
	}

	while (!deleteFutures.empty()) {
		co_await deleteFutures.front();
		deleteFutures.pop_front();
	}
}

Future<Void> IBlobStoreEndpoint::deleteRecursively(std::string const& bucket,
                                                   std::string prefix,
                                                   int* pNumDeleted,
                                                   int64_t* pBytesDeleted) {
	return deleteRecursively_impl(
	    Reference<IBlobStoreEndpoint>::addRef(this), bucket, prefix, pNumDeleted, pBytesDeleted);
}

Future<Void> writeEntireFile_impl(Reference<IBlobStoreEndpoint> bstore,
                                  std::string bucket,
                                  std::string object,
                                  std::string content) {
	UnsentPacketQueue packets;
	if (content.size() > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	PacketWriter pw(packets.getWriteBuffer(content.size()), nullptr, Unversioned());
	pw.serializeBytes(content);

	// Yield because we may have just had to copy several MB's into packet buffer chain and next we have to calculate a
	// hash of it.
	// TODO: If this actor is used to send large files then combine the summing and packetization into a loop
	// with a yield() every 20k or so.
	co_await yield();

	// If enable_object_integrity_check is true, calculate the sha256 sum of the content.
	// Otherwise, do md5. The hash type must match what the provider's writeEntireFileFromBuffer expects.
	std::string contentHash;
	if (bstore->knobs.enable_object_integrity_check) {
		unsigned char hash[SHA256_DIGEST_LENGTH];
		SHA256_CTX sha256;
		SHA256_Init(&sha256);
		SHA256_Update(&sha256, content.data(), content.size());
		SHA256_Final(hash, &sha256);
		contentHash = base64::encoder::from_string(std::string(reinterpret_cast<char*>(hash), SHA256_DIGEST_LENGTH));
		contentHash.resize(contentHash.size() - 1);
	} else {
		MD5_CTX sum;
		::MD5_Init(&sum);
		::MD5_Update(&sum, content.data(), content.size());
		std::string sumBytes;
		sumBytes.resize(16);
		::MD5_Final((unsigned char*)sumBytes.data(), &sum);
		contentHash = base64::encoder::from_string(sumBytes);
		contentHash.resize(contentHash.size() - 1);
	}

	co_await bstore->writeEntireFileFromBuffer(bucket, object, &packets, content.size(), contentHash);
}

Future<Void> IBlobStoreEndpoint::writeEntireFile(std::string const& bucket,
                                                 std::string const& object,
                                                 std::string const& content) {
	return writeEntireFile_impl(Reference<IBlobStoreEndpoint>::addRef(this), bucket, object, content);
}

Future<IBlobStoreEndpoint::ReusableConnection> connect_impl(Reference<IBlobStoreEndpoint> b, bool* reusingConn) {
	// First try to get a connection from the pool
	*reusingConn = false;
	while (!b->connectionPool->pool.empty()) {
		IBlobStoreEndpoint::ReusableConnection rconn = b->connectionPool->pool.front();
		b->connectionPool->pool.pop();

		// If the connection expires in the future then return it
		if (rconn.expirationTime > now()) {
			*reusingConn = true;
			++b->blobStats->reusedConnections;
			TraceEvent("IBlobStoreEndpointReusingConnected")
			    .suppressFor(60)
			    .detail("RemoteEndpoint", rconn.conn->getPeerAddress())
			    .detail("ExpiresIn", rconn.expirationTime - now())
			    .detail("Proxy", b->proxyHost.orDefault(""));
			co_return rconn;
		}
		++b->blobStats->expiredConnections;
	}
	++b->blobStats->newConnections;
	std::string host = b->host, service = b->service;
	TraceEvent(SevDebug, "IBlobStoreEndpointBuildingNewConnection")
	    .detail("UseProxy", b->useProxy)
	    .detail("TLS", b->knobs.secure_connection == 1)
	    .detail("Host", host)
	    .detail("Service", service)
	    .log();
	if (service.empty()) {
		if (b->useProxy) {
			fprintf(stderr, "ERROR: Port can't be empty when using HTTP proxy.\n");
			throw connection_failed();
		}
		service = b->knobs.secure_connection ? "https" : "http";
	}
	bool isTLS = b->knobs.isTLS();
	Reference<IConnection> conn;
	if (b->useProxy) {
		if (isTLS) {
			conn = co_await HTTP::proxyConnect(host, service, b->proxyHost.get(), b->proxyPort.get());
		} else {
			host = b->proxyHost.get();
			service = b->proxyPort.get();
			conn = co_await INetworkConnections::net()->connect(host, service, false);
		}
	} else {
		conn = co_await INetworkConnections::net()->connect(host, service, isTLS);
	}
	ASSERT(conn.isValid());
	co_await conn->connectHandshake();

	TraceEvent("IBlobStoreEndpointNewConnectionSuccess")
	    .suppressFor(60)
	    .detail("RemoteEndpoint", conn->getPeerAddress())
	    .detail("ExpiresIn", b->knobs.max_connection_life)
	    .detail("Proxy", b->proxyHost.orDefault(""));

	if (b->lookupSecretOnEachRequest()) {
		co_await b->updateSecret();
	}

	co_return IBlobStoreEndpoint::ReusableConnection({ conn, now() + b->knobs.max_connection_life });
}

Future<IBlobStoreEndpoint::ReusableConnection> IBlobStoreEndpoint::connect(bool* reusingConn) {
	return connect_impl(Reference<IBlobStoreEndpoint>::addRef(this), reusingConn);
}

void IBlobStoreEndpoint::returnConnection(ReusableConnection& rconn) {
	// If it expires in the future then add it to the pool in the front
	if (rconn.expirationTime > now()) {
		connectionPool->pool.push(rconn);
	} else {
		++blobStats->expiredConnections;
	}
	rconn.conn = Reference<IConnection>();
}

// Do a request, get a Response.
// Request content is provided as UnsentPacketQueue *pContent which will be depleted as bytes are sent but the queue
// itself must live for the life of this actor and be destroyed by the caller.
Future<Reference<HTTP::IncomingResponse>> doRequest_impl(Reference<IBlobStoreEndpoint> bstore,
                                                         std::string verb,
                                                         std::string resource,
                                                         HTTP::Headers headers,
                                                         UnsentPacketQueue* pContent,
                                                         int contentLen,
                                                         std::set<unsigned int> successCodes) {
	UnsentPacketQueue contentCopy;
	auto req = makeReference<HTTP::OutgoingRequest>();
	req->verb = verb;
	req->data.content = &contentCopy;
	req->data.contentLen = contentLen;

	if (resource.empty()) {
		resource = "/";
	}

	// For requests with content to upload, the request timeout should be at least twice the amount of time
	// it would take to upload the content given the upload bandwidth and concurrency limits.
	int bandwidthThisRequest = 1 + bstore->knobs.max_send_bytes_per_second / bstore->knobs.concurrent_uploads;
	int contentUploadSeconds = contentLen / bandwidthThisRequest;
	int requestTimeout = std::max(bstore->knobs.request_timeout_min, 3 * contentUploadSeconds);

	co_await bstore->concurrentRequests.take();
	FlowLock::Releaser globalReleaser(bstore->concurrentRequests, 1);

	int maxTries = std::min(bstore->knobs.request_tries, bstore->knobs.connect_tries);
	int thisTry = 1;
	double nextRetryDelay = 2.0;
	bool retryExtended = false;

	while (true) {
		Optional<Error> err;
		Optional<NetworkAddress> remoteAddress;
		bool connectionEstablished = false;
		Reference<HTTP::IncomingResponse> r;
		std::string canonicalURI = resource;
		UID connID = UID();
		double reqStartTimer{ 0 };
		double connectStartTimer = g_network->timer();
		bool reusingConn = false;
		bool fastRetry = false;
		IBlobStoreEndpoint::ReusableConnection rconn;

		// Reset headers to initial state for this retry attempt to prevent header accumulation
		req->data.headers = headers;
		req->data.headers["Host"] = bstore->host;

		// Merge extraHeaders into headers
		for (const auto& [k, v] : bstore->extraHeaders) {
			std::string& fieldValue = req->data.headers[k];
			if (!fieldValue.empty()) {
				fieldValue.append(",");
			}
			fieldValue.append(v);
		}

		try {
			Future<IBlobStoreEndpoint::ReusableConnection> frconn = bstore->connect(&reusingConn);

			// Make a shallow copy of the queue
			req->data.content->discardAll();
			if (pContent != nullptr) {
				PacketBuffer* pFirst = pContent->getUnsent();
				PacketBuffer* pLast = nullptr;
				for (PacketBuffer* p = pFirst; p != nullptr; p = p->nextPacketBuffer()) {
					p->addref();
					p->bytes_sent = 0; // Reset sent count on each buffer
					pLast = p;
				}
				req->data.content->prependWriteBuffer(pFirst, pLast);
			}

			rconn = co_await timeoutError(frconn, bstore->knobs.connect_timeout);
			connectionEstablished = true;
			connID = rconn.conn->getDebugID();
			reqStartTimer = g_network->timer();

			// Provider hook: pre-retry check
			if (retryExtended) {
				bool proceed = co_await bstore->preRetryCheck(verb, resource, rconn, requestTimeout, retryExtended);
				if (!proceed) {
					bstore->returnConnection(rconn);
					continue;
				}
			}

			// Finish/update the request headers (which includes auth headers).
			// This must be done AFTER the connection is ready because credentials may be
			// refreshed when a new connection is established.
			bstore->setRequestHeaders(verb, resource, req->data.headers);
			canonicalURI = bstore->normalizeResourceForRequest(resource);

			// Has to be in absolute-form.
			if (bstore->useProxy && bstore->knobs.secure_connection == 0) {
				canonicalURI = "http://" + bstore->host + ":" + bstore->service + canonicalURI;
			}

			req->resource = canonicalURI;

			remoteAddress = rconn.conn->getPeerAddress();
			co_await bstore->requestRate->getAllowance(1);

			Future<Reference<HTTP::IncomingResponse>> reqF =
			    HTTP::doRequest(rconn.conn, req, bstore->sendRate, &bstore->s_stats.bytes_sent, bstore->recvRate);

			if (reqF.isReady() && reusingConn) {
				fastRetry = true;
			}

			Reference<HTTP::IncomingResponse> _r = co_await timeoutError(reqF, requestTimeout);
			r = _r;

			bstore->simulateRequestFailure(verb, resource, r);

			// Since the response was parsed successfully reuse the connection unless we
			// received the "Connection: close" header.
			if (r->data.headers["Connection"] != "close") {
				bstore->returnConnection(rconn);
			} else {
				++bstore->blobStats->expiredConnections;
			}
			rconn.conn.clear();

		} catch (Error& e) {
			TraceEvent(SevWarn, "BlobStoreDoRequestError")
			    .errorUnsuppressed(e)
			    .detail("Verb", verb)
			    .detail("Resource", resource);
			// Close the connection on error to satisfy Sim2Conn's assertion in simulation
			if (connectionEstablished && rconn.conn.isValid()) {
				rconn.conn->close();
				rconn.conn.clear();
			}
			if (e.code() == error_code_actor_cancelled)
				throw;
			err = e;
		}

		double end = g_network->timer();
		double connectDuration = reqStartTimer - connectStartTimer;
		double reqDuration = end - reqStartTimer;
		bstore->blobStats->requestLatency.addMeasurement(reqDuration);

		// If err is not present then r is valid. If r->code is in successCodes then record the successful request and
		// return r.
		if (!err.present() && successCodes.contains(r->code)) {
			bstore->s_stats.requests_successful++;
			++bstore->blobStats->requestsSuccessful;
			co_return r;
		}

		bstore->s_stats.requests_failed++;
		++bstore->blobStats->requestsFailed;

		// All errors in err are potentially retryable as well as certain HTTP response codes...
		// But only if our previous attempt was not the last allowable try.
		bool retryable = err.present() || r->code == 500 || r->code == 502 || r->code == 503 || r->code == 429;
		retryable = retryable && (thisTry < maxTries);

		if (!retryable || !err.present()) {
			fastRetry = false;
		}

		TraceEvent event(SevWarn,
		                 (retryable || retryExtended) ? (fastRetry ? "BlobStoreEndpointRequestFailedFastRetryable"
		                                                           : "BlobStoreEndpointRequestFailedRetryable")
		                                              : "BlobStoreEndpointRequestFailed");

		bool connectionFailed = false;
		if (err.present()) {
			event.errorUnsuppressed(err.get());
			if (err.get().code() == error_code_connection_failed) {
				connectionFailed = true;
			}
		}
		event.suppressFor(60);
		if (!err.present()) {
			event.detail("ResponseCode", r->code);
		}

		// Provider hook: process failure
		if (!err.present()) {
			bstore->processRequestFailure(r, event, retryExtended);
		}

		event.detail("ConnectionEstablished", connectionEstablished);
		event.detail("ReusingConn", reusingConn);
		if (connectionEstablished) {
			event.detail("ConnID", connID);
			event.detail("ConnectDuration", connectDuration);
			event.detail("ReqDuration", reqDuration);
		}

		if (remoteAddress.present())
			event.detail("RemoteEndpoint", remoteAddress.get());
		else
			event.detail("RemoteHost", bstore->host);

		event.detail("Verb", verb)
		    .detail("Resource", resource)
		    .detail("ThisTry", thisTry)
		    .detail("URI", canonicalURI)
		    .detail("Proxy", bstore->proxyHost.orDefault(""));

		// If r is not valid or not code 429 then increment the try count.
		// 429's will not count against the attempt limit. Also skip incrementing the retry count for fast retries.
		if (!fastRetry && (!r || r->code != 429))
			++thisTry;

		if (fastRetry) {
			++bstore->blobStats->fastRetries;
			co_await delay(0);
		} else if (retryable || retryExtended) {
			// We will wait delay seconds before the next retry, start with nextRetryDelay.
			double delaySeconds = nextRetryDelay;
			// connectionFailed is treated specially
			double limit =
			    connectionFailed ? bstore->knobs.max_delay_connection_failed : bstore->knobs.max_delay_retryable_error;
			// Double but limit the *next* nextRetryDelay.
			nextRetryDelay = std::min(nextRetryDelay * 2, limit);
			// If r is valid then obey the Retry-After response header if present.
			if (r) {
				auto iRetryAfter = r->data.headers.find("Retry-After");
				if (iRetryAfter != r->data.headers.end()) {
					event.detail("RetryAfterHeader", iRetryAfter->second);
					char* pEnd;
					double retryAfter = strtod(iRetryAfter->second.c_str(), &pEnd);
					if (*pEnd)
						retryAfter = 300;
					delaySeconds = std::max(delaySeconds, retryAfter);
				}
			}

			event.detail("RetryDelay", delaySeconds);
			co_await ::delay(delaySeconds);
		} else {
			// We can't retry, so throw something.
			if (r && r->code == 406)
				throw http_not_accepted();

			// This error code means the authentication header was not accepted, likely the account or key is wrong.
			if (r && r->code == 401)
				throw http_auth_failed();

			// Recognize and throw specific errors
			if (err.present()) {
				int code = err.get().code();
				// If we get a timed_out error during the connect() phase, we'll call that connection_failed
				// despite the fact that there was technically never a 'connection' to begin with. It differentiates
				// between an active connection timing out vs a connection timing out, though not between an active
				// connection failing vs connection attempt failing.
				if (code == error_code_timed_out && !connectionEstablished) {
					TraceEvent(SevWarn, "BlobStoreEndpointConnectTimeout")
					    .suppressFor(60)
					    .detail("Timeout", requestTimeout);
					throw connection_failed();
				}

				// TODO: Add more error types?
				if (code == error_code_timed_out || code == error_code_connection_failed ||
				    code == error_code_lookup_failed)
					throw err.get();
			}

			throw http_request_failed();
		}
	}
}

Future<Reference<HTTP::IncomingResponse>> IBlobStoreEndpoint::doRequest(std::string const& verb,
                                                                        std::string const& resource,
                                                                        const HTTP::Headers& headers,
                                                                        UnsentPacketQueue* pContent,
                                                                        int contentLen,
                                                                        std::set<unsigned int> successCodes) {
	return doRequest_impl(
	    Reference<IBlobStoreEndpoint>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
}
