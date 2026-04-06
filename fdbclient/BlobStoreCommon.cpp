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
#include "fdbclient/ClientKnobs.h"
#include "flow/Hostname.h"
#include "flow/IAsyncFile.h"
#include "flow/IConnection.h"
#include "flow/CoroUtils.h"

#include <boost/algorithm/string.hpp>

// --- ConnectionPoolData ---

// IBlobStoreEndpoint::ConnectionPoolData destructor implementation
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

// --- Stats ---

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

// --- BlobKnobs ---

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

// --- IBlobStoreEndpoint::fromString ---

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

		Optional<S3BlobStoreEndpoint::Credentials> creds;
		if (cred.present()) {
			StringRef c(cred.get());
			StringRef key = c.eat(":");
			StringRef secret = c.eat(":");
			StringRef securityToken = c.eat();
			creds = S3BlobStoreEndpoint::Credentials{ key.toString(), secret.toString(), securityToken.toString() };
		}

		return makeReference<S3BlobStoreEndpoint>(
		    host.toString(), service.toString(), region, proxyHost, proxyPort, creds, knobs, extraHeaders);

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
