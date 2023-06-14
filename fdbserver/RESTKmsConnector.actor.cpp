/*
 * RESTKmsConnector.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/RESTUtils.h"
#include "fdbserver/RESTKmsConnector.h"

#include "fdbclient/BlobCipher.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/RESTClient.h"

#include "fdbrpc/HTTP.h"

#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RESTKmsConnectorUtils.h"

#include "flow/Arena.h"
#include "flow/ActorCollection.h"
#include "flow/BooleanParam.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IAsyncFile.h"
#include "flow/IConnection.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

#include <algorithm>
#include <limits>
#include <boost/algorithm/string.hpp>
#include <cstring>
#include <stack>
#include <memory>
#include <queue>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include

using namespace RESTKmsConnectorUtils;

namespace {
bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_encrypt_invalid_kms_config:
	case error_code_encrypt_keys_fetch_failed:
	case error_code_file_not_found:
	case error_code_file_too_large:
	case error_code_http_request_failed:
	case error_code_io_error:
	case error_code_operation_failed:
	case error_code_value_too_large:
	case error_code_timed_out:
	case error_code_connection_failed:
	case error_code_rest_malformed_response:
		return true;
	default:
		return false;
	}
}

bool isKmsNotReachable(const int errCode) {
	return errCode == error_code_timed_out || errCode == error_code_connection_failed;
}

void removeTrailingChar(std::string& str, char c) {
	while (!str.empty() && str[str.length() - 1] == c) {
		str.erase(str.length() - 1);
	}
}

} // namespace

template <class Params>
struct KmsUrlCtx {
	enum class PenaltyType { TIMEOUT = 1, MALFORMED_RESPONSE = 2 };

	std::string url;
	uint64_t nRequests;
	uint64_t nFailedResponses;
	uint64_t nResponseParseFailures;
	double unresponsivenessPenalty;
	double unresponsivenessPenaltyTS;

	KmsUrlCtx()
	  : url(""), nRequests(0), nFailedResponses(0), nResponseParseFailures(0), unresponsivenessPenalty(0.0),
	    unresponsivenessPenaltyTS(0) {}
	explicit KmsUrlCtx(const std::string& u)
	  : url(u), nRequests(0), nFailedResponses(0), nResponseParseFailures(0), unresponsivenessPenalty(0.0),
	    unresponsivenessPenaltyTS(0) {}

	bool operator==(const KmsUrlCtx& toCompare) const { return url.compare(toCompare.url) == 0; }

	void refreshUnresponsivenessPenalty() {
		if (unresponsivenessPenaltyTS == 0) {
			return;
		}
		int64_t timeSinceLastPenalty = now() - unresponsivenessPenaltyTS;
		unresponsivenessPenalty = Params::penalty(timeSinceLastPenalty);
	}

	void penalize(const PenaltyType type) {
		if (type == PenaltyType::TIMEOUT) {
			nFailedResponses++;
			unresponsivenessPenaltyTS = now();
		} else {
			ASSERT_EQ(type, PenaltyType::MALFORMED_RESPONSE);
			nResponseParseFailures++;
		}
	}

	std::string toString() const {
		return fmt::format(
		    "{} {} {} {} {}", url, nRequests, nFailedResponses, nResponseParseFailures, unresponsivenessPenalty);
	}
};

// Current implementation is designed to favor the most-preferable KMS for all outbound requests allowing leveraging KMS
// implemented caching if supported
//
// TODO: Implement load-balancing requests to available KMS servers maintaining prioritized KMS server list based on
// observed errors/connection failures/timeouts etc.

template <class Params>
struct KmsUrlStore {
	void sort() {
		std::sort(kmsUrls.begin(), kmsUrls.end(), [](const KmsUrlCtx<Params>& l, const KmsUrlCtx<Params>& r) {
			// Sort the available URLs based on following rules:
			// 1. URL with higher unresponsiveness-penalty are least preferred
			// 2. Among URLs with same unresponsiveness-penalty weight, URLs with more number of failed-respones are
			// less preferrred
			// 3. Lastly, URLs with more malformed response messages are less preferred

			if (l.unresponsivenessPenalty != r.unresponsivenessPenalty) {
				return l.unresponsivenessPenalty < r.unresponsivenessPenalty;
			}
			if (l.nFailedResponses != r.nFailedResponses) {
				return l.nFailedResponses < r.nFailedResponses;
			}
			return l.nResponseParseFailures < r.nResponseParseFailures;
		});
	}

	void penalize(const KmsUrlCtx<Params>& toPenalize, const typename KmsUrlCtx<Params>::PenaltyType type) {
		bool found = false;
		for (KmsUrlCtx<Params>& urlCtx : kmsUrls) {
			if (urlCtx == toPenalize) {
				urlCtx.penalize(type);
				found = true;
				break;
			}
		}
		ASSERT(found);

		// update the penalties
		for (auto& url : kmsUrls) {
			url.refreshUnresponsivenessPenalty();
		}

		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
			std::string details;
			for (const auto& url : kmsUrls) {
				details.append(fmt::format("[ {} ], ", url.toString()));
			}
			TraceEvent("RESTKmsUrlStoreBeforeSort")
			    .detail("Details", details)
			    .detail("Penalize", toPenalize.toString());
		}

		// Reshuffle the URLs
		sort();

		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
			std::string details;
			for (const auto& url : kmsUrls) {
				details.append(fmt::format("[ {} ], ", url.toString()));
			}
			TraceEvent("RESTKmsUrlStoreAfterSort").detail("Details", details);
		}
	}

	std::vector<KmsUrlCtx<Params>> kmsUrls;
};

FDB_BOOLEAN_PARAM(RefreshPersistedUrls);
FDB_BOOLEAN_PARAM(IsCipherType);

// Routine to determine penalty for cached KMSUrl based on unresponsive KMS behavior observed in recent past. The
// routine is desgined to assign a maximum penalty if KMS responses are unacceptable in very recent past, with time the
// the penalty weight deteorates (matches real world outage OR server overload scenario)

struct KmsUrlPenaltyParams {
	static double penalty(int64_t timeSinceLastPenalty) { return continuousTimeDecay(1.0, 0.1, timeSinceLastPenalty); }
};

struct RESTKmsConnectorCtx : public ReferenceCounted<RESTKmsConnectorCtx> {
	UID uid;
	KmsUrlStore<KmsUrlPenaltyParams> kmsUrlStore;
	double lastKmsUrlsRefreshTs;
	double lastKmsUrlDiscoverTS;
	RESTClient restClient;
	ValidationTokenMap validationTokenMap;
	PromiseStream<Future<Void>> addActor;

	RESTKmsConnectorCtx()
	  : uid(deterministicRandom()->randomUniqueID()), lastKmsUrlsRefreshTs(0), lastKmsUrlDiscoverTS(0.0) {}
	explicit RESTKmsConnectorCtx(const UID& id) : uid(id), lastKmsUrlsRefreshTs(0), lastKmsUrlDiscoverTS(0.0) {}
};

std::string getFullRequestUrl(Reference<RESTKmsConnectorCtx> ctx, const std::string& url, const std::string& suffix) {
	if (suffix.empty()) {
		TraceEvent(SevWarn, "RESTGetFullUrlEmptyEndpoint", ctx->uid);
		throw encrypt_invalid_kms_config();
	}
	std::string fullUrl(url);
	return (suffix[0] == '/') ? fullUrl.append(suffix) : fullUrl.append("/").append(suffix);
}

void dropCachedKmsUrls(Reference<RESTKmsConnectorCtx> ctx,
                       std::unordered_map<std::string, KmsUrlCtx<KmsUrlPenaltyParams>>* urlMap) {
	for (const auto& url : ctx->kmsUrlStore.kmsUrls) {
		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::VERBOSE) {
			TraceEvent("RESTDropCachedKmsUrls", ctx->uid)
			    .detail("Url", url.url)
			    .detail("NumRequests", url.nRequests)
			    .detail("NumFailedResponses", url.nFailedResponses)
			    .detail("NumRespParseFailures", url.nResponseParseFailures);
		}
		urlMap->insert(std::make_pair(url.url, url));
	}
	ctx->kmsUrlStore.kmsUrls.clear();
}

bool shouldRefreshKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	if (!SERVER_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS) {
		return false;
	}

	return (now() - ctx->lastKmsUrlsRefreshTs) > SERVER_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS_INTERVAL_SEC;
}

void extractKmsUrls(Reference<RESTKmsConnectorCtx> ctx,
                    const rapidjson::Document& doc,
                    Reference<HTTP::IncomingResponse> httpResp) {
	// Refresh KmsUrls cache
	std::unordered_map<std::string, KmsUrlCtx<KmsUrlPenaltyParams>> urlMap;
	dropCachedKmsUrls(ctx, &urlMap);
	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls.size(), 0);

	for (const auto& url : doc[KMS_URLS_TAG].GetArray()) {
		if (!url.IsString()) {
			// TODO: We need to log only the kms section of the document
			TraceEvent(SevWarnAlways, "RESTDiscoverKmsUrlsMalformedResp", ctx->uid).detail("UrlType", url.GetType());
			throw operation_failed();
		}

		std::string urlStr;
		urlStr.resize(url.GetStringLength());
		memcpy(urlStr.data(), url.GetString(), url.GetStringLength());

		// preserve the KmsUrl stats while (re)discovering KMS URLs, preferable to select the servers with lesser count
		// of unexpected events in the past

		auto itr = urlMap.find(urlStr);
		if (itr != urlMap.end()) {
			if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::INFO) {
				TraceEvent("RESTDiscoverExistingKmsUrl", ctx->uid).detail("UrlCtx", itr->second.toString());
			}
			ctx->kmsUrlStore.kmsUrls.emplace_back(itr->second);
		} else {
			auto urlCtx = KmsUrlCtx<KmsUrlPenaltyParams>(urlStr);
			if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::INFO) {
				TraceEvent("RESTDiscoverNewKmsUrl", ctx->uid).detail("UrlCtx", urlCtx.toString());
			}
			ctx->kmsUrlStore.kmsUrls.emplace_back(urlCtx);
		}
	}

	// Reshuffle URLs to re-arrange them in appropriate priority
	ctx->kmsUrlStore.sort();

	// Update Kms URLs refresh timestamp
	ctx->lastKmsUrlsRefreshTs = now();
}

ACTOR Future<Void> parseDiscoverKmsUrlFile(Reference<RESTKmsConnectorCtx> ctx, std::string filename) {
	if (filename.empty() || !fileExists(filename)) {
		TraceEvent(SevWarnAlways, "RESTDiscoverKmsUrlsFileNotFound", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	state Reference<IAsyncFile> dFile = wait(IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));
	state int64_t fSize = wait(dFile->size());
	state Standalone<StringRef> buff = makeString(fSize);
	int bytesRead = wait(dFile->read(mutateString(buff), fSize, 0));
	if (bytesRead != fSize) {
		TraceEvent(SevWarnAlways, "RESTDiscoveryKmsUrlFileReadShort", ctx->uid)
		    .detail("Filename", filename)
		    .detail("Expected", fSize)
		    .detail("Actual", bytesRead);
		throw io_error();
	}

	// Acceptable file format (new line character separated URLs):
	// <url1>\n
	// <url2>\n

	std::unordered_map<std::string, KmsUrlCtx<KmsUrlPenaltyParams>> urlMap;
	dropCachedKmsUrls(ctx, &urlMap);
	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls.size(), 0);

	std::stringstream ss(buff.toString());
	std::string url;
	while (std::getline(ss, url, DISCOVER_URL_FILE_URL_SEP)) {
		std::string trimedUrl = boost::trim_copy(url);
		// Remove the trailing '/'(s)
		while (!trimedUrl.empty() && trimedUrl.ends_with('/')) {
			trimedUrl.pop_back();
		}
		if (trimedUrl.empty()) {
			// Empty URL, ignore and continue
			continue;
		}
		auto itr = urlMap.find(trimedUrl);
		if (itr != urlMap.end()) {
			if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::INFO) {
				TraceEvent("RESTParseDiscoverKmsUrlsExistingUrl", ctx->uid).detail("UrlCtx", itr->second.toString());
			}
			ctx->kmsUrlStore.kmsUrls.emplace_back(itr->second);
		} else {
			auto urlCtx = KmsUrlCtx<KmsUrlPenaltyParams>(trimedUrl);
			if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::INFO) {
				TraceEvent("RESTParseDiscoverKmsUrlsAddUrl", ctx->uid).detail("UrlCtx", urlCtx.toString());
			}
			ctx->kmsUrlStore.kmsUrls.emplace_back(urlCtx);
		}
	}

	// Reshuffle URLs to re-arrange them in appropriate priority
	ctx->kmsUrlStore.sort();

	return Void();
}

ACTOR Future<Void> discoverKmsUrls(Reference<RESTKmsConnectorCtx> ctx, RefreshPersistedUrls refreshPersistedUrls) {
	// KMS discovery needs to be done in two scenarios:
	// 1) Initial cluster bootstrap - first boot.
	// 2) Requests to all cached KMS URLs is failing for some reason.
	//
	// Following steps are followed as part of KMS discovery:
	// 1) Based on the configured KMS URL discovery mode, the KMS URLs are extracted and persited in a DynamicKnob
	// enabled configuration knob. Approach allows relying on the parsing configuration supplied discovery URL mode
	// only during afte the initial boot, from then on, the URLs can periodically refreshed along with encryption
	// key fetch requests (SERVER_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS needs to be enabled). 2) Cluster will
	// continue using cached KMS URLs (and refreshing them if needed); however, if for some reason, all cached URLs
	// aren't working, then code re-discovers the URL following step#1 and refresh persisted state as well.

	if (!refreshPersistedUrls) {
		// TODO: request must be satisfied accessing KMS URLs persited using DynamicKnobs. Will be implemented once
		// feature is available
	}

	std::string_view mode{ SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MODE };

	if (mode.compare("file") == 0) {
		wait(parseDiscoverKmsUrlFile(ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_DISCOVER_KMS_URL_FILE));
	} else {
		throw not_implemented();
	}

	ctx->lastKmsUrlDiscoverTS = now();

	return Void();
}

void checkResponseForError(Reference<RESTKmsConnectorCtx> ctx,
                           const rapidjson::Document& doc,
                           IsCipherType isCipherType) {
	// check version tag sanity
	if (!doc.HasMember(REQUEST_VERSION_TAG) || !doc[REQUEST_VERSION_TAG].IsInt()) {
		TraceEvent(SevWarnAlways, "RESTKMSResponseMissingVersion", ctx->uid).log();
		CODE_PROBE(true, "KMS response missing version");
		throw rest_malformed_response();
	}

	const int version = doc[REQUEST_VERSION_TAG].GetInt();
	const int maxSupportedVersion = isCipherType ? SERVER_KNOBS->REST_KMS_MAX_CIPHER_REQUEST_VERSION
	                                             : SERVER_KNOBS->REST_KMS_MAX_BLOB_METADATA_REQUEST_VERSION;
	if (version == INVALID_REQUEST_VERSION || version > maxSupportedVersion) {
		TraceEvent(SevWarnAlways, "RESTKMSResponseInvalidVersion", ctx->uid)
		    .detail("Version", version)
		    .detail("MaxSupportedVersion", maxSupportedVersion);
		CODE_PROBE(true, "KMS response invalid version");
		throw rest_malformed_response();
	}

	// Check if response has error
	Optional<ErrorDetail> errorDetails = RESTKmsConnectorUtils::getError(doc);
	if (errorDetails.present()) {
		TraceEvent("RESTKMSErrorResponse", ctx->uid)
		    .detail("ErrorMsg", errorDetails->errorMsg)
		    .detail("ErrorCode", errorDetails->errorCode);
		throw encrypt_keys_fetch_failed();
	}
}

void checkDocForNewKmsUrls(Reference<RESTKmsConnectorCtx> ctx,
                           Reference<HTTP::IncomingResponse> resp,
                           const rapidjson::Document& doc) {
	if (doc.HasMember(KMS_URLS_TAG) && !doc[KMS_URLS_TAG].IsNull()) {
		try {
			extractKmsUrls(ctx, doc, resp);
		} catch (Error& e) {
			TraceEvent("RESTRefreshKmsUrlsFailed", ctx->uid).error(e);
			// Given cipherKeyDetails extraction was done successfully, ignore KmsUrls parsing error
		}
	}
}

Standalone<VectorRef<EncryptCipherKeyDetailsRef>> parseEncryptCipherResponse(Reference<RESTKmsConnectorCtx> ctx,
                                                                             Reference<HTTP::IncomingResponse> resp) {
	// Acceptable response payload json format:
	//
	// response_json_payload {
	//   "version" = <version>
	//   "cipher_key_details" : [
	//     {
	//        "base_cipher_id"    : <cipherKeyId>,
	//        "encrypt_domain_id" : <domainId>,
	//        "base_cipher"       : <baseCipher>,
	//        "refresh_after_sec"   : <refreshTimeInterval>, (Optional)
	//        "expire_after_sec"    : <expireTimeInterval>  (Optional)
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "kms_urls" : [
	//         "url1", "url2", ...
	//   ],
	//	 "error" : {					// Optional, populated by the KMS, if present, rest of payload is ignored.
	//		"errMsg" : <message>,
	//		"errCode": <code>
	// 	  }
	// }

	if (!resp.isValid() || resp->code != HTTP::HTTP_STATUS_CODE_OK) {
		// STATUS_OK is gating factor for REST request success
		throw http_request_failed();
	}

	if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::VERBOSE) {
		TraceEvent("RESTParseEncryptCipherResponseStart", ctx->uid);
	}

	rapidjson::Document doc;
	doc.Parse(resp->data.content.data());

	checkResponseForError(ctx, doc, IsCipherType::True);

	Standalone<VectorRef<EncryptCipherKeyDetailsRef>> result;

	// Extract CipherKeyDetails
	if (!doc.HasMember(CIPHER_KEY_DETAILS_TAG) || !doc[CIPHER_KEY_DETAILS_TAG].IsArray()) {
		TraceEvent(SevWarn, "RESTParseEncryptCipherResponseFailed", ctx->uid)
		    .detail("Reason", "MissingCipherKeyDetails");
		CODE_PROBE(true, "REST CipherKeyDetails not array");
		throw rest_malformed_response();
	}

	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		if (!cipherDetail.IsObject()) {
			TraceEvent(SevWarn, "RESTParseEncryptCipherResponseFailed", ctx->uid)
			    .detail("CipherDetailType", cipherDetail.GetType())
			    .detail("Reason", "EncryptKeyDetailsNotObject");
			CODE_PROBE(true, "REST CipherKeyDetail not object");
			throw rest_malformed_response();
		}

		const bool isBaseCipherIdPresent = cipherDetail.HasMember(BASE_CIPHER_ID_TAG);
		const bool isBaseCipherPresent = cipherDetail.HasMember(BASE_CIPHER_TAG);
		const bool isEncryptDomainIdPresent = cipherDetail.HasMember(ENCRYPT_DOMAIN_ID_TAG);
		if (!isBaseCipherIdPresent || !isBaseCipherPresent || !isEncryptDomainIdPresent) {
			TraceEvent(SevWarn, "RESTParseEncryptCipherResponseFailed", ctx->uid)
			    .detail("Reason", "MalformedKeyDetail")
			    .detail("BaseCipherIdPresent", isBaseCipherIdPresent)
			    .detail("BaseCipherPresent", isBaseCipherPresent)
			    .detail("EncryptDomainIdPresent", isEncryptDomainIdPresent);
			CODE_PROBE(true, "REST CipherKeyDetail malformed");
			throw rest_malformed_response();
		}

		const int cipherKeyLen = cipherDetail[BASE_CIPHER_TAG].GetStringLength();
		std::unique_ptr<uint8_t[]> cipherKey = std::make_unique<uint8_t[]>(cipherKeyLen);
		memcpy(cipherKey.get(), cipherDetail[BASE_CIPHER_TAG].GetString(), cipherKeyLen);

		// Extract cipher refresh and/or expiry interval if supplied
		Optional<int64_t> refreshAfterSec =
		    cipherDetail.HasMember(REFRESH_AFTER_SEC) && cipherDetail[REFRESH_AFTER_SEC].GetInt64() > 0
		        ? cipherDetail[REFRESH_AFTER_SEC].GetInt64()
		        : Optional<int64_t>();
		Optional<int64_t> expireAfterSec =
		    cipherDetail.HasMember(EXPIRE_AFTER_SEC) ? cipherDetail[EXPIRE_AFTER_SEC].GetInt64() : Optional<int64_t>();

		EncryptCipherDomainId domainId = cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64();
		EncryptCipherBaseKeyId baseCipherId = cipherDetail[BASE_CIPHER_ID_TAG].GetUint64();
		StringRef cipher = StringRef(cipherKey.get(), cipherKeyLen);

		// https://en.wikipedia.org/wiki/Key_checksum_value
		// Key Check Value (KCV) is the checksum of a cryptographic key, it is used to validate integrity of the
		// 'baseCipher' key supplied by the external KMS. The checksum computed is eventually persisted as part of
		// EncryptionHeader and assists in following scenarios: a) 'baseCipher' corruption happen external to FDB b)
		// 'baseCipher' corruption within FDB processes
		//
		// Approach compute KCV after reading it from the network buffer, HTTP checksum protects against potential
		// on-wire corruption
		if (cipher.size() > MAX_BASE_CIPHER_LEN) {
			// HMAC_SHA digest generation accepts upto MAX_BASE_CIPHER_LEN key-buffer, longer keys are truncated and
			// weakens the security guarantees.
			TraceEvent(SevWarnAlways, "RESTKmsConnectorMaxBaseCipherKeyLimit")
			    .detail("MaxAllowed", MAX_BASE_CIPHER_LEN)
			    .detail("BaseCipherLen", cipher.size());
			throw rest_max_base_cipher_len();
		}

		EncryptCipherKeyCheckValue cipherKCV = Sha256KCV().computeKCV(cipher.begin(), cipher.size());

		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
			TraceEvent event("RESTParseEncryptCipherResponse", ctx->uid);
			event.detail("DomainId", domainId);
			event.detail("BaseCipherId", baseCipherId);
			event.detail("BaseCipherLen", cipher.size());
			event.detail("BaseCipherKCV", cipherKCV);
			if (refreshAfterSec.present()) {
				event.detail("RefreshAt", refreshAfterSec.get());
			}
			if (expireAfterSec.present()) {
				event.detail("ExpireAt", expireAfterSec.get());
			}
		}

		result.emplace_back_deep(
		    result.arena(), domainId, baseCipherId, cipher, cipherKCV, refreshAfterSec, expireAfterSec);
	}

	checkDocForNewKmsUrls(ctx, resp, doc);

	return result;
}

Standalone<VectorRef<BlobMetadataDetailsRef>> parseBlobMetadataResponse(Reference<RESTKmsConnectorCtx> ctx,
                                                                        Reference<HTTP::IncomingResponse> resp) {
	// Acceptable response payload json format:
	// (baseLocation and partitions follow the same properties as described in BlobMetadataUtils.h)
	//
	// response_json_payload {
	//   "version" = <version>
	//   "blob_metadata_details" : [
	//     {
	//        "domain_id" : <domainId>,
	//        "locations" : [
	//			  { id: 1, path: "fdbblob://partition1"} , {id: 2, path: "fdbblob://partition2"}, ...
	//		  ],
	//        "refresh_after_sec"   : <refreshTimeInterval>, (Optional)
	//        "expire_after_sec"    : <expireTimeInterval>  (Optional)
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "kms_urls" : [
	//         "url1", "url2", ...
	//   ],
	//	 "error" : {					// Optional, populated by the KMS, if present, rest of payload is ignored.
	//		"errMsg" : <message>,
	//		"errCode": <code>
	// 	  }
	// }

	if (resp->code != HTTP::HTTP_STATUS_CODE_OK) {
		// STATUS_OK is gating factor for REST request success
		throw http_request_failed();
	}

	rapidjson::Document doc;
	doc.Parse(resp->data.content.data());

	checkResponseForError(ctx, doc, IsCipherType::False);

	Standalone<VectorRef<BlobMetadataDetailsRef>> result;

	// Extract CipherKeyDetails
	if (!doc.HasMember(BLOB_METADATA_DETAILS_TAG) || !doc[BLOB_METADATA_DETAILS_TAG].IsArray()) {
		TraceEvent(SevWarn, "ParseBlobMetadataResponseFailureMissingDetails", ctx->uid).log();
		CODE_PROBE(true, "REST BlobMetadata details missing or not-array");
		throw rest_malformed_response();
	}

	for (const auto& detail : doc[BLOB_METADATA_DETAILS_TAG].GetArray()) {
		if (!detail.IsObject()) {
			TraceEvent(SevWarn, "ParseBlobMetadataResponseFailureDetailsNotObject", ctx->uid)
			    .detail("CipherDetailType", detail.GetType());
			CODE_PROBE(true, "REST BlobMetadata detail not-object");
			throw rest_malformed_response();
		}

		const bool isDomainIdPresent = detail.HasMember(BLOB_METADATA_DOMAIN_ID_TAG);
		const bool isLocationsPresent =
		    detail.HasMember(BLOB_METADATA_LOCATIONS_TAG) && detail[BLOB_METADATA_LOCATIONS_TAG].IsArray();
		if (!isDomainIdPresent || !isLocationsPresent) {
			TraceEvent(SevWarn, "ParseBlobMetadataResponseMalformedDetail", ctx->uid)
			    .detail("DomainIdPresent", isDomainIdPresent)
			    .detail("LocationsPresent", isLocationsPresent);
			CODE_PROBE(true, "REST BlobMetadata detail malformed");
			throw rest_malformed_response();
		}

		BlobMetadataDomainId domainId = detail[BLOB_METADATA_DOMAIN_ID_TAG].GetInt64();

		// just do extra memory copy for simplicity here
		Standalone<VectorRef<BlobMetadataLocationRef>> locations;
		for (const auto& location : detail[BLOB_METADATA_LOCATIONS_TAG].GetArray()) {
			if (!location.IsObject()) {
				TraceEvent("ParseBlobMetadataResponseFailureLocationNotObject", ctx->uid)
				    .detail("LocationType", location.GetType());
				throw rest_malformed_response();
			}
			const bool isLocationIdPresent = location.HasMember(BLOB_METADATA_LOCATION_ID_TAG);
			const bool isPathPresent = location.HasMember(BLOB_METADATA_LOCATION_PATH_TAG);
			if (!isLocationIdPresent || !isPathPresent) {
				TraceEvent(SevWarn, "ParseBlobMetadataResponseMalformedLocation", ctx->uid)
				    .detail("LocationIdPresent", isLocationIdPresent)
				    .detail("PathPresent", isPathPresent);
				CODE_PROBE(true, "REST BlobMetadata location malformed");
				throw rest_malformed_response();
			}

			BlobMetadataLocationId locationId = location[BLOB_METADATA_LOCATION_ID_TAG].GetInt64();

			const int pathLen = location[BLOB_METADATA_LOCATION_PATH_TAG].GetStringLength();
			std::unique_ptr<uint8_t[]> pathStr = std::make_unique<uint8_t[]>(pathLen);
			memcpy(pathStr.get(), location[BLOB_METADATA_LOCATION_PATH_TAG].GetString(), pathLen);
			locations.emplace_back_deep(locations.arena(), locationId, StringRef(pathStr.get(), pathLen));
		}

		// Extract refresh and/or expiry interval if supplied
		double refreshAt = detail.HasMember(REFRESH_AFTER_SEC) && detail[REFRESH_AFTER_SEC].GetInt64() > 0
		                       ? now() + detail[REFRESH_AFTER_SEC].GetInt64()
		                       : std::numeric_limits<double>::max();
		double expireAt = detail.HasMember(EXPIRE_AFTER_SEC) ? now() + detail[EXPIRE_AFTER_SEC].GetInt64()
		                                                     : std::numeric_limits<double>::max();
		result.emplace_back_deep(result.arena(), domainId, locations, refreshAt, expireAt);
	}

	checkDocForNewKmsUrls(ctx, resp, doc);

	return result;
}

StringRef getEncryptKeysByKeyIdsRequestBody(Reference<RESTKmsConnectorCtx> ctx,
                                            const KmsConnLookupEKsByKeyIdsReq& req,
                                            const bool refreshKmsUrls,
                                            Arena& arena) {
	// Acceptable request payload json format:
	//
	// request_json_payload {
	//   "version" = <version>
	//   "cipher_key_details" = [
	//     {
	//        "base_cipher_id"      : <cipherKeyId>
	//        "encrypt_domain_id"   : <domainId>		// Optional
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "validation_tokens" = [
	//     {
	//        "token_name" : <name>,
	//        "token_value": <value>
	//     },
	//     {
	//         ....
	//     }
	//   ]
	//   "refresh_kms_urls" = 1/0
	//   "debug_uid" = <uid-string>   // Optional debug info to trace requests across FDB <--> KMS
	// }

	rapidjson::Document doc;
	doc.SetObject();

	// Append 'request version'
	addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_CURRENT_BLOB_METADATA_REQUEST_VERSION);

	// Append 'cipher_key_details' as json array
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto& detail : req.encryptKeyInfos) {
		addBaseCipherIdDomIdToDoc(doc, keyIdDetails, detail.baseCipherId, detail.domainId);
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());

	// Append 'validation_tokens' as json array
	addValidationTokensSectionToJsonDoc(doc, ctx->validationTokenMap);

	// Append 'refresh_kms_urls'
	addRefreshKmsUrlsSectionToJsonDoc(doc, refreshKmsUrls);

	// Append 'debug_uid' section if needed
	addDebugUidSectionToJsonDoc(doc, req.debugId);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	StringRef ref = makeString(sb.GetSize(), arena);
	memcpy(mutateString(ref), sb.GetString(), sb.GetSize());
	return ref;
}

ACTOR template <class T>
Future<T> kmsRequestImpl(
    Reference<RESTKmsConnectorCtx> ctx,
    std::string urlSuffix,
    StringRef requestBodyRef,
    std::function<T(Reference<RESTKmsConnectorCtx>, Reference<HTTP::IncomingResponse>)> parseFunc) {
	state UID requestID = deterministicRandom()->randomUniqueID();

	// Follow multi-phase approach:
	// Step-1: Enumerate KmsUrlStore cached URLs in the defined order of preference, if URL fails with an acceptable
	// error (time-out or connection-failed), then continue enumeration. Otherwise, bubble up the error.
	// Step-2: Refresh KmsUlrStore cached URLs by re-discovering KMS URLs and loop Step-1

	state int pass = 0;
	state KmsUrlCtx<KmsUrlPenaltyParams>* urlCtx;
	loop {
		state int idx = 0;
		state double start = now();

		pass++;
		while (idx < ctx->kmsUrlStore.kmsUrls.size()) {
			urlCtx = &ctx->kmsUrlStore.kmsUrls[idx++];
			try {
				std::string kmsEncryptionFullUrl = getFullRequestUrl(ctx, urlCtx->url, urlSuffix);

				if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
					TraceEvent("RESTKmsRequestImpl", ctx->uid)
					    .detail("Pass", pass)
					    .detail("RequestID", requestID)
					    .detail("FullUrl", kmsEncryptionFullUrl)
					    .detail("StartIdx", start)
					    .detail("CurIdx", idx)
					    .detail("LastKmsUrlDiscoverTS", ctx->lastKmsUrlDiscoverTS);
				}

				Reference<HTTP::IncomingResponse> resp = wait(ctx->restClient.doPost(
				    kmsEncryptionFullUrl, requestBodyRef.toString(), RESTKmsConnectorUtils::getHTTPHeaders()));
				urlCtx->nRequests++;

				try {
					T parsedResp = parseFunc(ctx, resp);
					return parsedResp;
				} catch (Error& e) {
					TraceEvent(SevWarn, "KmsRequestRespParseFailure").error(e).detail("RequestID", requestID);
					ctx->kmsUrlStore.penalize(*urlCtx, KmsUrlCtx<KmsUrlPenaltyParams>::PenaltyType::MALFORMED_RESPONSE);
					// attempt to do request from next KmsUrl
				}
			} catch (Error& e) {
				ctx->kmsUrlStore.penalize(*urlCtx, KmsUrlCtx<KmsUrlPenaltyParams>::PenaltyType::TIMEOUT);
				// Keep re-trying if KMS request time-out OR is server unreachable; otherwise, bubble up the error
				if (!isKmsNotReachable(e.code())) {
					if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {
						TraceEvent("KmsRequestFailedUnreachable", ctx->uid).error(e).detail("RequestID", requestID);
					}
					throw e;
				}
				TraceEvent(SevDebug, "KmsRequestError", ctx->uid).error(e).detail("RequestID", requestID);
				// attempt to do request from next KmsUrl
			}

			// Possible scenarios:
			// 1. URLs got reshuffled since the start of the enumeration.
			// 2. All cached URLs aren't working, KMS URLs got re-discovered since start of enumeration.
			// For #1, let the code continue enumerating cached URLs, an attempt to reset enumeration order could
			// cause deadlock when: all cached URLs aren't working and multiple requests keep updating penalities
			// and reshuffling the order. For #2, reset the enumeration order to re-attempt operation after
			// re-discovery for KMS URL is done (stale cached KMS URLs)

			if (start < ctx->lastKmsUrlDiscoverTS) {
				idx = 0;
			}
		}
		// Re-discover KMS urls and re-attempt request using newer KMS URLs
		wait(discoverKmsUrls(ctx, RefreshPersistedUrls::True));
	}
}

ACTOR Future<Void> fetchEncryptionKeysByKeyIds(Reference<RESTKmsConnectorCtx> ctx, KmsConnLookupEKsByKeyIdsReq req) {
	state KmsConnLookupEKsByKeyIdsRep reply;

	try {
		bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
		StringRef requestBodyRef = getEncryptKeysByKeyIdsRequestBody(ctx, req, refreshKmsUrls, req.arena);
		std::function<Standalone<VectorRef<EncryptCipherKeyDetailsRef>>(Reference<RESTKmsConnectorCtx>,
		                                                                Reference<HTTP::IncomingResponse>)>
		    f = &parseEncryptCipherResponse;
		wait(store(
		    reply.cipherKeyDetails,
		    kmsRequestImpl(
		        ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT, requestBodyRef, std::move(f))));
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("RESTLookupEKsByKeyIdsFailed", ctx->uid).error(e);
		if (!canReplyWith(e)) {
			throw e;
		}
		req.reply.sendError(e);
	}
	return Void();
}

StringRef getEncryptKeysByDomainIdsRequestBody(Reference<RESTKmsConnectorCtx> ctx,
                                               const KmsConnLookupEKsByDomainIdsReq& req,
                                               const bool refreshKmsUrls,
                                               Arena& arena) {
	// Acceptable request payload json format:
	//
	// request_json_payload {
	//   "version" = <version>
	//   "cipher_key_details" = [
	//     {
	//        "encrypt_domain_id"   : <domainId>
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "validation_tokens" = [
	//     {
	//        "token_name" : <name>,
	//        "token_value": <value>
	//     },
	//     {
	//         ....
	//     }
	//   ]
	//   "refresh_kms_urls" = 1/0
	//   "debug_uid" = <uid-string>     // Optional debug info to trace requests across FDB <--> KMS
	// }

	rapidjson::Document doc;
	doc.SetObject();

	// Append 'request version'
	addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_CURRENT_CIPHER_REQUEST_VERSION);

	// Append 'cipher_key_details' as json array
	addLatestDomainDetailsToDoc(doc, CIPHER_KEY_DETAILS_TAG, ENCRYPT_DOMAIN_ID_TAG, req.encryptDomainIds);

	// Append 'validation_tokens' as json array
	addValidationTokensSectionToJsonDoc(doc, ctx->validationTokenMap);

	// Append 'refresh_kms_urls'
	addRefreshKmsUrlsSectionToJsonDoc(doc, refreshKmsUrls);

	// Append 'debug_uid' section if needed
	addDebugUidSectionToJsonDoc(doc, req.debugId);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	StringRef ref = makeString(sb.GetSize(), arena);
	memcpy(mutateString(ref), sb.GetString(), sb.GetSize());
	return ref;
}

ACTOR Future<Void> fetchEncryptionKeysByDomainIds(Reference<RESTKmsConnectorCtx> ctx,
                                                  KmsConnLookupEKsByDomainIdsReq req) {
	state KmsConnLookupEKsByDomainIdsRep reply;
	try {
		bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
		StringRef requestBodyRef = getEncryptKeysByDomainIdsRequestBody(ctx, req, refreshKmsUrls, req.arena);

		std::function<Standalone<VectorRef<EncryptCipherKeyDetailsRef>>(Reference<RESTKmsConnectorCtx>,
		                                                                Reference<HTTP::IncomingResponse>)>
		    f = &parseEncryptCipherResponse;

		wait(store(reply.cipherKeyDetails,
		           kmsRequestImpl(ctx,
		                          SERVER_KNOBS->REST_KMS_CONNECTOR_GET_LATEST_ENCRYPTION_KEYS_ENDPOINT,
		                          requestBodyRef,
		                          std::move(f))));
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("RESTLookupEKsByDomainIdsFailed", ctx->uid).error(e);
		if (!canReplyWith(e)) {
			throw e;
		}
		req.reply.sendError(e);
	}
	return Void();
}

StringRef getBlobMetadataRequestBody(Reference<RESTKmsConnectorCtx> ctx,
                                     KmsConnBlobMetadataReq& req,
                                     const bool refreshKmsUrls) {
	// Acceptable request payload json format:
	//
	// request_json_payload {
	//   "version" = <version>
	//   "blob_metadata_details" = [
	//     {
	//        "domain_id"   : <domainId>
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "validation_tokens" = [
	//     {
	//        "token_name" : <name>,
	//        "token_value": <value>
	//     },
	//     {
	//         ....
	//     }
	//   ]
	//   "refresh_kms_urls" = 1/0
	//   "debug_uid" = <uid-string>     // Optional debug info to trace requests across FDB <--> KMS
	// }

	rapidjson::Document doc;
	doc.SetObject();

	// Append 'request version'
	addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_CURRENT_BLOB_METADATA_REQUEST_VERSION);

	// Append 'blob_metadata_details' as json array
	addLatestDomainDetailsToDoc(doc, BLOB_METADATA_DETAILS_TAG, BLOB_METADATA_DOMAIN_ID_TAG, req.domainIds);

	// Append 'validation_tokens' as json array
	addValidationTokensSectionToJsonDoc(doc, ctx->validationTokenMap);

	// Append 'refresh_kms_urls'
	addRefreshKmsUrlsSectionToJsonDoc(doc, refreshKmsUrls);

	// Append 'debug_uid' section if needed
	addDebugUidSectionToJsonDoc(doc, req.debugId);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	StringRef ref = makeString(sb.GetSize(), req.arena);
	memcpy(mutateString(ref), sb.GetString(), sb.GetSize());
	return ref;
}

// FIXME: add lookup error stats and suppress error trace events on interval
ACTOR Future<Void> fetchBlobMetadata(Reference<RESTKmsConnectorCtx> ctx, KmsConnBlobMetadataReq req) {
	state KmsConnBlobMetadataRep reply;
	try {
		bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
		StringRef requestBodyRef = getBlobMetadataRequestBody(ctx, req, refreshKmsUrls);

		// for some reason the compiler can't handle just passing &parseBlobMetadata, so you have to explicitly
		// declare its templated return type as part of an std::function first
		std::function<Standalone<VectorRef<BlobMetadataDetailsRef>>(Reference<RESTKmsConnectorCtx>,
		                                                            Reference<HTTP::IncomingResponse>)>
		    f = &parseBlobMetadataResponse;
		wait(
		    store(reply.metadataDetails,
		          kmsRequestImpl(
		              ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_GET_BLOB_METADATA_ENDPOINT, requestBodyRef, std::move(f))));
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("RESTLookupBlobMetadataFailed", ctx->uid).error(e);
		if (!canReplyWith(e)) {
			throw e;
		}
		req.reply.sendError(e);
	}
	return Void();
}

ACTOR Future<Void> procureValidationTokensFromFiles(Reference<RESTKmsConnectorCtx> ctx, std::string details) {
	Standalone<StringRef> detailsRef(details);
	if (details.empty()) {
		TraceEvent("RESTValidationTokenEmptyFileDetails", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	TraceEvent("RESTValidationToken", ctx->uid).detail("DetailsStr", details);

	state std::unordered_map<std::string, std::string> tokenFilePathMap;
	loop {
		StringRef name = detailsRef.eat(TOKEN_NAME_FILE_SEP);
		if (name.empty()) {
			break;
		}
		StringRef path = detailsRef.eat(TOKEN_TUPLE_SEP);
		if (path.empty()) {
			TraceEvent("RESTValidationTokenFileDetailsMalformed", ctx->uid).detail("FileDetails", details);
			throw operation_failed();
		}

		std::string tokenName = boost::trim_copy(name.toString());
		std::string tokenFile = boost::trim_copy(path.toString());
		if (!fileExists(tokenFile)) {
			TraceEvent("RESTValidationTokenFileNotFound", ctx->uid)
			    .detail("TokenName", tokenName)
			    .detail("Filename", tokenFile);
			throw encrypt_invalid_kms_config();
		}

		tokenFilePathMap.emplace(tokenName, tokenFile);
		TraceEvent("RESTValidationToken", ctx->uid).detail("FName", tokenName).detail("Filename", tokenFile);
	}

	// Clear existing cached validation tokens
	ctx->validationTokenMap.clear();

	// Enumerate all token files and extract details
	state uint64_t tokensPayloadSize = 0;
	for (const auto& item : tokenFilePathMap) {
		state std::string tokenName = item.first;
		state std::string tokenFile = item.second;
		state Reference<IAsyncFile> tFile = wait(IAsyncFileSystem::filesystem()->open(
		    tokenFile, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));

		state int64_t fSize = wait(tFile->size());
		if (fSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE) {
			TraceEvent(SevWarnAlways, "RESTValidationTokenFileTooLarge", ctx->uid)
			    .detail("FileName", tokenFile)
			    .detail("Size", fSize)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE);
			throw file_too_large();
		}

		tokensPayloadSize += fSize;
		if (tokensPayloadSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE) {
			TraceEvent(SevWarnAlways, "RESTValidationTokenPayloadTooLarge", ctx->uid)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE);
			throw value_too_large();
		}

		state Standalone<StringRef> buff = makeString(fSize);
		int bytesRead = wait(tFile->read(mutateString(buff), fSize, 0));
		if (bytesRead != fSize) {
			TraceEvent(SevError, "RESTDiscoveryKmsUrlFileReadShort", ctx->uid)
			    .detail("Filename", tokenFile)
			    .detail("Expected", fSize)
			    .detail("Actual", bytesRead);
			throw io_error();
		}

		// Populate validation token details
		ValidationTokenCtx tokenCtx =
		    ValidationTokenCtx(tokenName, ValidationTokenSource::VALIDATION_TOKEN_SOURCE_FILE);
		tokenCtx.value.resize(fSize);
		memcpy(tokenCtx.value.data(), buff.begin(), fSize);
		tokenCtx.filePath = tokenFile;

		if (SERVER_KNOBS->REST_KMS_CONNECTOR_REMOVE_TRAILING_NEWLINE) {
			removeTrailingChar(tokenCtx.value, '\n');
		}

		// NOTE: avoid logging token-value to prevent token leaks in log files..
		TraceEvent("RESTValidationTokenReadFile", ctx->uid)
		    .detail("TokenName", tokenCtx.name)
		    .detail("TokenSize", tokenCtx.value.size())
		    .detail("TokenFilePath", tokenCtx.filePath.get())
		    .detail("TotalPayloadSize", tokensPayloadSize);

		ctx->validationTokenMap.emplace(tokenName, std::move(tokenCtx));
	}

	return Void();
}

ACTOR Future<Void> procureValidationTokens(Reference<RESTKmsConnectorCtx> ctx) {
	std::string_view mode{ SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MODE };

	if (mode.compare("file") == 0) {
		wait(procureValidationTokensFromFiles(ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_DETAILS));
	} else {
		throw not_implemented();
	}

	return Void();
}

ACTOR Future<Void> restConnectorCoreImpl(KmsConnectorInterface interf) {
	state Reference<RESTKmsConnectorCtx> self = makeReference<RESTKmsConnectorCtx>(interf.id());
	state Future<Void> collection = actorCollection(self->addActor.getFuture());

	TraceEvent("RESTKmsConnectorInit", self->uid).log();

	wait(discoverKmsUrls(self, RefreshPersistedUrls::False));
	wait(procureValidationTokens(self));

	loop {
		choose {
			when(KmsConnLookupEKsByKeyIdsReq req = waitNext(interf.ekLookupByIds.getFuture())) {
				self->addActor.send(fetchEncryptionKeysByKeyIds(self, req));
			}
			when(KmsConnLookupEKsByDomainIdsReq req = waitNext(interf.ekLookupByDomainIds.getFuture())) {
				self->addActor.send(fetchEncryptionKeysByDomainIds(self, req));
			}
			when(KmsConnBlobMetadataReq req = waitNext(interf.blobMetadataReq.getFuture())) {
				self->addActor.send(fetchBlobMetadata(self, req));
			}
			when(wait(collection)) {
				// this should throw an error, not complete
				ASSERT(false);
			}
		}
	}
}

Future<Void> RESTKmsConnector::connectorCore(KmsConnectorInterface interf) {
	return restConnectorCoreImpl(interf);
}

// Only used to link unit tests
void forceLinkRESTKmsConnectorTest() {}

namespace {
std::string_view KMS_URL_NAME_TEST = "http://foo/bar";
std::string_view BLOB_METADATA_BASE_LOCATION_TEST = "file://local";
uint8_t BASE_CIPHER_KEY_TEST[32];

std::shared_ptr<platform::TmpFile> prepareTokenFile(const uint8_t* buff, const int len) {
	std::shared_ptr<platform::TmpFile> tmpFile = std::make_shared<platform::TmpFile>("/tmp");
	ASSERT(fileExists(tmpFile->getFileName()));
	tmpFile->write(buff, len);
	return tmpFile;
}

std::shared_ptr<platform::TmpFile> prepareTokenFile(const int tokenLen) {
	Standalone<StringRef> buff = makeString(tokenLen);
	deterministicRandom()->randomBytes(mutateString(buff), tokenLen);

	return prepareTokenFile(buff.begin(), tokenLen);
}

ACTOR Future<Void> testEmptyValidationFileDetails(Reference<RESTKmsConnectorCtx> ctx) {
	try {
		wait(procureValidationTokensFromFiles(ctx, ""));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_invalid_kms_config);
	}
	return Void();
}

ACTOR Future<Void> testMalformedFileValidationTokenDetails(Reference<RESTKmsConnectorCtx> ctx) {
	try {
		wait(procureValidationTokensFromFiles(ctx, "abdc/tmp/foo"));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}

	return Void();
}

ACTOR Future<Void> testValidationTokenFileNotFound(Reference<RESTKmsConnectorCtx> ctx) {
	try {
		wait(procureValidationTokensFromFiles(ctx, "foo$/imaginary-dir/dream/phantom-file"));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_invalid_kms_config);
	}
	return Void();
}

ACTOR Future<Void> testTooLargeValidationTokenFile(Reference<RESTKmsConnectorCtx> ctx) {
	std::string name("foo");
	const int tokenLen = SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE + 1;

	state std::shared_ptr<platform::TmpFile> tmpFile = prepareTokenFile(tokenLen);

	std::string details;
	details.append(name).append(TOKEN_NAME_FILE_SEP).append(tmpFile->getFileName());

	try {
		wait(procureValidationTokensFromFiles(ctx, details));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_file_too_large);
	}

	return Void();
}

ACTOR Future<Void> testValidationFileTokenPayloadTooLarge(Reference<RESTKmsConnectorCtx> ctx) {
	const int tokenLen = SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE;
	const int nTokens = SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE /
	                        SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE +
	                    2;
	Standalone<StringRef> buff = makeString(tokenLen);
	deterministicRandom()->randomBytes(mutateString(buff), tokenLen);

	std::string details;
	state std::vector<std::shared_ptr<platform::TmpFile>> tokenfiles;
	for (int i = 0; i < nTokens; i++) {
		std::shared_ptr<platform::TmpFile> tokenfile = prepareTokenFile(buff.begin(), tokenLen);

		details.append(std::to_string(i)).append(TOKEN_NAME_FILE_SEP).append(tokenfile->getFileName());
		if (i < nTokens)
			details.append(TOKEN_TUPLE_SEP);
		tokenfiles.emplace_back(tokenfile);
	}

	try {
		wait(procureValidationTokensFromFiles(ctx, details));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_value_too_large);
	}

	return Void();
}

ACTOR Future<Void> testMultiValidationFileTokenFiles(Reference<RESTKmsConnectorCtx> ctx) {
	state int numFiles = deterministicRandom()->randomInt(2, 5);
	state int tokenLen = deterministicRandom()->randomInt(26, 75);
	state Standalone<StringRef> buff = makeString(tokenLen);
	state std::unordered_map<std::string, std::shared_ptr<platform::TmpFile>> tokenFiles;
	state std::unordered_map<std::string, std::string> tokenNameValueMap;
	state std::string tokenDetailsStr;
	state bool newLineAppended = BUGGIFY ? true : false;

	std::string token;
	// Construct token-value buffer ensuring it doesn't have trailing new-line character.
	loop {
		deterministicRandom()->randomBytes(mutateString(buff), tokenLen);
		token = std::string((char*)buff.begin(), tokenLen);
		removeTrailingChar(token, '\n');
		if (token.size() > 0) {
			break;
		}
	}
	tokenLen = token.size();
	std::string tokenWithNewLine(token);
	tokenWithNewLine.push_back('\n');

	for (int i = 1; i <= numFiles; i++) {
		std::string tokenName = std::to_string(i);
		std::shared_ptr<platform::TmpFile> tokenfile =
		    newLineAppended ? prepareTokenFile(reinterpret_cast<uint8_t*>(tokenWithNewLine.data()), tokenLen + 1)
		                    : prepareTokenFile(reinterpret_cast<uint8_t*>(token.data()), tokenLen);

		tokenFiles.emplace(tokenName, tokenfile);
		tokenDetailsStr.append(tokenName).append(TOKEN_NAME_FILE_SEP).append(tokenfile->getFileName());
		if (i < numFiles)
			tokenDetailsStr.append(TOKEN_TUPLE_SEP);

		tokenNameValueMap.emplace(std::to_string(i), token);
	}

	wait(procureValidationTokensFromFiles(ctx, tokenDetailsStr));

	ASSERT_EQ(ctx->validationTokenMap.size(), tokenNameValueMap.size());

	for (const auto& token : ctx->validationTokenMap) {
		const auto& itr = tokenNameValueMap.find(token.first);
		const ValidationTokenCtx& tokenCtx = token.second;

		ASSERT(itr != tokenNameValueMap.end());
		ASSERT_EQ(token.first.compare(itr->first), 0);
		ASSERT_EQ(tokenCtx.value.compare(itr->second), 0);
		ASSERT_EQ(tokenCtx.source, ValidationTokenSource::VALIDATION_TOKEN_SOURCE_FILE);
		ASSERT(tokenCtx.filePath.present());
		ASSERT_EQ(tokenCtx.filePath.compare(tokenFiles[tokenCtx.name]->getFileName()), 0);
		ASSERT_NE(tokenCtx.getReadTS(), 0);
	}

	CODE_PROBE(newLineAppended, "RESTKmsConnector remove trailing newline");

	return Void();
}

EncryptCipherDomainId getRandomDomainId() {
	const int lottery = deterministicRandom()->randomInt(0, 100);
	if (lottery < 10) {
		return SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID;
	} else if (lottery >= 10 && lottery < 25) {
		return ENCRYPT_HEADER_DOMAIN_ID;
	} else {
		return lottery;
	}
}

void addFakeRefreshExpire(rapidjson::Document& resDoc, rapidjson::Value& detail, rapidjson::Value& key) {
	if (deterministicRandom()->coinflip()) {
		key.SetString(REFRESH_AFTER_SEC, resDoc.GetAllocator());
		rapidjson::Value refreshInterval;
		refreshInterval.SetInt64(10);
		detail.AddMember(key, refreshInterval, resDoc.GetAllocator());
	}
	if (deterministicRandom()->coinflip()) {
		key.SetString(EXPIRE_AFTER_SEC, resDoc.GetAllocator());
		rapidjson::Value expireInterval;
		deterministicRandom()->coinflip() ? expireInterval.SetInt64(10) : expireInterval.SetInt64(-1);
		detail.AddMember(key, expireInterval, resDoc.GetAllocator());
	}
}

void addFakeKmsUrls(const rapidjson::Document& reqDoc, rapidjson::Document& resDoc) {
	ASSERT(reqDoc.HasMember(REFRESH_KMS_URLS_TAG));
	if (reqDoc[REFRESH_KMS_URLS_TAG].GetBool()) {
		rapidjson::Value kmsUrls(rapidjson::kArrayType);
		for (int i = 0; i < 3; i++) {
			rapidjson::Value url;
			url.SetString(KMS_URL_NAME_TEST.data(), resDoc.GetAllocator());
			kmsUrls.PushBack(url, resDoc.GetAllocator());
		}
		rapidjson::Value memberKey(KMS_URLS_TAG, resDoc.GetAllocator());
		resDoc.AddMember(memberKey, kmsUrls, resDoc.GetAllocator());
	}
}

void getFakeEncryptCipherResponse(StringRef jsonReqRef,
                                  const bool baseCipherIdPresent,
                                  Reference<HTTP::IncomingResponse> httpResponse) {
	rapidjson::Document reqDoc;
	reqDoc.Parse(jsonReqRef.toString().data());

	rapidjson::Document resDoc;
	resDoc.SetObject();

	ASSERT(reqDoc.HasMember(REQUEST_VERSION_TAG) && reqDoc[REQUEST_VERSION_TAG].IsInt());
	ASSERT(reqDoc.HasMember(CIPHER_KEY_DETAILS_TAG) && reqDoc[CIPHER_KEY_DETAILS_TAG].IsArray());

	addVersionToDoc(resDoc, reqDoc[REQUEST_VERSION_TAG].GetInt());

	rapidjson::Value cipherKeyDetails(rapidjson::kArrayType);
	for (const auto& detail : reqDoc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		rapidjson::Value keyDetail(rapidjson::kObjectType);

		ASSERT(detail.HasMember(ENCRYPT_DOMAIN_ID_TAG));

		rapidjson::Value key(ENCRYPT_DOMAIN_ID_TAG, resDoc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail[ENCRYPT_DOMAIN_ID_TAG].GetInt64());
		keyDetail.AddMember(key, domainId, resDoc.GetAllocator());

		key.SetString(BASE_CIPHER_ID_TAG, resDoc.GetAllocator());
		rapidjson::Value baseCipherId;
		if (detail.HasMember(BASE_CIPHER_ID_TAG)) {
			domainId.SetUint64(detail[BASE_CIPHER_ID_TAG].GetUint64());
		} else {
			ASSERT(!baseCipherIdPresent);
			domainId.SetUint(1234);
		}
		keyDetail.AddMember(key, domainId, resDoc.GetAllocator());

		key.SetString(BASE_CIPHER_TAG, resDoc.GetAllocator());
		rapidjson::Value baseCipher;
		baseCipher.SetString((char*)&BASE_CIPHER_KEY_TEST[0], sizeof(BASE_CIPHER_KEY_TEST), resDoc.GetAllocator());
		keyDetail.AddMember(key, baseCipher, resDoc.GetAllocator());

		addFakeRefreshExpire(resDoc, keyDetail, key);

		cipherKeyDetails.PushBack(keyDetail, resDoc.GetAllocator());
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, resDoc.GetAllocator());
	resDoc.AddMember(memberKey, cipherKeyDetails, resDoc.GetAllocator());

	addFakeKmsUrls(reqDoc, resDoc);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	resDoc.Accept(writer);
	httpResponse->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResponse->data.content.data(), sb.GetString(), sb.GetSize());
	httpResponse->data.contentLen = sb.GetSize();
}

void getFakeBlobMetadataResponse(StringRef jsonReqRef,
                                 const bool baseCipherIdPresent,
                                 Reference<HTTP::IncomingResponse> httpResponse) {
	rapidjson::Document reqDoc;
	reqDoc.Parse(jsonReqRef.toString().data());

	rapidjson::Document resDoc;
	resDoc.SetObject();

	ASSERT(reqDoc.HasMember(REQUEST_VERSION_TAG) && reqDoc[REQUEST_VERSION_TAG].IsInt());
	ASSERT(reqDoc.HasMember(BLOB_METADATA_DETAILS_TAG) && reqDoc[BLOB_METADATA_DETAILS_TAG].IsArray());

	addVersionToDoc(resDoc, reqDoc[REQUEST_VERSION_TAG].GetInt());

	rapidjson::Value blobMetadataDetails(rapidjson::kArrayType);
	for (const auto& detail : reqDoc[BLOB_METADATA_DETAILS_TAG].GetArray()) {
		rapidjson::Value keyDetail(rapidjson::kObjectType);

		ASSERT(detail.HasMember(BLOB_METADATA_DOMAIN_ID_TAG));

		rapidjson::Value key(BLOB_METADATA_DOMAIN_ID_TAG, resDoc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail[BLOB_METADATA_DOMAIN_ID_TAG].GetInt64());
		keyDetail.AddMember(key, domainId, resDoc.GetAllocator());

		int locationCount = deterministicRandom()->randomInt(1, 6);
		rapidjson::Value locations(rapidjson::kArrayType);
		for (int i = 0; i < locationCount; i++) {
			rapidjson::Value location(rapidjson::kObjectType);

			rapidjson::Value locId;
			key.SetString(BLOB_METADATA_LOCATION_ID_TAG, resDoc.GetAllocator());
			locId.SetInt64(i);
			location.AddMember(key, locId, resDoc.GetAllocator());

			rapidjson::Value path;
			key.SetString(BLOB_METADATA_LOCATION_PATH_TAG, resDoc.GetAllocator());
			path.SetString(BLOB_METADATA_BASE_LOCATION_TEST.data(), resDoc.GetAllocator());
			location.AddMember(key, path, resDoc.GetAllocator());

			locations.PushBack(location, resDoc.GetAllocator());
		}

		key.SetString(BLOB_METADATA_LOCATIONS_TAG, resDoc.GetAllocator());
		keyDetail.AddMember(key, locations, resDoc.GetAllocator());

		addFakeRefreshExpire(resDoc, keyDetail, key);

		blobMetadataDetails.PushBack(keyDetail, resDoc.GetAllocator());
	}
	rapidjson::Value memberKey(BLOB_METADATA_DETAILS_TAG, resDoc.GetAllocator());
	resDoc.AddMember(memberKey, blobMetadataDetails, resDoc.GetAllocator());

	addFakeKmsUrls(reqDoc, resDoc);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	resDoc.Accept(writer);
	httpResponse->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResponse->data.content.data(), sb.GetString(), sb.GetSize());
}

void validateKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls.size(), 3);
	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls[0].url.compare(KMS_URL_NAME_TEST), 0);
}

void testGetEncryptKeysByKeyIdsRequestBody(Reference<RESTKmsConnectorCtx> ctx, Arena& arena) {
	KmsConnLookupEKsByKeyIdsReq req;
	std::unordered_map<EncryptCipherBaseKeyId, EncryptCipherDomainId> keyMap;
	const int nKeys = deterministicRandom()->randomInt(7, 8);
	for (int i = 1; i < nKeys; i++) {
		EncryptCipherDomainId domainId = getRandomDomainId();
		req.encryptKeyInfos.emplace_back(domainId, i);
		keyMap[i] = domainId;
	}

	bool refreshKmsUrls = deterministicRandom()->coinflip();
	if (deterministicRandom()->coinflip()) {
		req.debugId = deterministicRandom()->randomUniqueID();
	}

	StringRef requestBodyRef = getEncryptKeysByKeyIdsRequestBody(ctx, req, refreshKmsUrls, arena);
	TraceEvent("FetchKeysByKeyIds", ctx->uid).setMaxFieldLength(100000).detail("JsonReqStr", requestBodyRef.toString());
	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeEncryptCipherResponse(requestBodyRef, true, httpResp);
	TraceEvent("FetchKeysByKeyIds", ctx->uid).setMaxFieldLength(100000).detail("HttpRespStr", httpResp->data.content);

	Standalone<VectorRef<EncryptCipherKeyDetailsRef>> cipherDetails = parseEncryptCipherResponse(ctx, httpResp);
	ASSERT_EQ(cipherDetails.size(), keyMap.size());
	for (const auto& detail : cipherDetails) {
		ASSERT(keyMap.find(detail.encryptKeyId) != keyMap.end());
		ASSERT_EQ(keyMap[detail.encryptKeyId], detail.encryptDomainId);
		ASSERT_EQ(detail.encryptKey.size(), sizeof(BASE_CIPHER_KEY_TEST));
		ASSERT_EQ(memcmp(detail.encryptKey.begin(), &BASE_CIPHER_KEY_TEST[0], sizeof(BASE_CIPHER_KEY_TEST)), 0);
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testGetEncryptKeysByDomainIdsRequestBody(Reference<RESTKmsConnectorCtx> ctx, Arena& arena) {
	KmsConnLookupEKsByDomainIdsReq req;
	std::unordered_set<EncryptCipherDomainId> domainIds;
	const int nKeys = deterministicRandom()->randomInt(7, 25);
	for (int i = 1; i < nKeys; i++) {
		EncryptCipherDomainId domainId = getRandomDomainId();
		if (domainIds.insert(domainId).second) {
			req.encryptDomainIds.push_back(domainId);
		}
	}

	bool refreshKmsUrls = deterministicRandom()->coinflip();

	StringRef jsonReqRef = getEncryptKeysByDomainIdsRequestBody(ctx, req, refreshKmsUrls, arena);
	TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("JsonReqStr", jsonReqRef.toString());
	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeEncryptCipherResponse(jsonReqRef, false, httpResp);
	TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("HttpRespStr", httpResp->data.content);

	Standalone<VectorRef<EncryptCipherKeyDetailsRef>> cipherDetails = parseEncryptCipherResponse(ctx, httpResp);
	ASSERT_EQ(domainIds.size(), cipherDetails.size());
	for (const auto& detail : cipherDetails) {
		ASSERT(domainIds.find(detail.encryptDomainId) != domainIds.end());
		ASSERT_EQ(detail.encryptKey.size(), sizeof(BASE_CIPHER_KEY_TEST));
		ASSERT_EQ(memcmp(detail.encryptKey.begin(), &BASE_CIPHER_KEY_TEST[0], sizeof(BASE_CIPHER_KEY_TEST)), 0);
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testGetBlobMetadataRequestBody(Reference<RESTKmsConnectorCtx> ctx) {
	KmsConnBlobMetadataReq req;
	std::unordered_set<BlobMetadataDomainId> domainIds;
	const int nKeys = deterministicRandom()->randomInt(7, 25);
	for (int i = 1; i < nKeys; i++) {
		EncryptCipherDomainId domainId = deterministicRandom()->randomInt(0, 1000);
		if (domainIds.insert(domainId).second) {
			req.domainIds.push_back(domainId);
		}
	}

	bool refreshKmsUrls = deterministicRandom()->coinflip();

	TraceEvent("FetchBlobMetadataStart", ctx->uid);
	StringRef jsonReqRef = getBlobMetadataRequestBody(ctx, req, refreshKmsUrls);
	TraceEvent("FetchBlobMetadataReq", ctx->uid).detail("JsonReqStr", jsonReqRef.toString());
	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeBlobMetadataResponse(jsonReqRef, false, httpResp);
	TraceEvent("FetchBlobMetadataResp", ctx->uid).detail("HttpRespStr", httpResp->data.content);

	Standalone<VectorRef<BlobMetadataDetailsRef>> details = parseBlobMetadataResponse(ctx, httpResp);

	ASSERT_EQ(domainIds.size(), details.size());
	for (const auto& detail : details) {
		auto it = domainIds.find(detail.domainId);
		ASSERT(it != domainIds.end());
		ASSERT(!detail.locations.empty());
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testMissingOrInvalidVersion(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value cDetails(rapidjson::kArrayType);
	rapidjson::Value detail(rapidjson::kObjectType);
	rapidjson::Value key(isCipher ? BASE_CIPHER_ID_TAG : BLOB_METADATA_DOMAIN_ID_TAG, doc.GetAllocator());
	rapidjson::Value id;
	id.SetUint(12345);
	detail.AddMember(key, id, doc.GetAllocator());
	cDetails.PushBack(detail, doc.GetAllocator());
	key.SetString(isCipher ? CIPHER_KEY_DETAILS_TAG : BLOB_METADATA_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(key, cDetails, doc.GetAllocator());

	rapidjson::Value versionKey(REQUEST_VERSION_TAG, doc.GetAllocator());
	rapidjson::Value versionValue;
	int version = INVALID_REQUEST_VERSION;
	if (deterministicRandom()->coinflip()) {
		if (deterministicRandom()->coinflip()) {
			version = -7;
		} else {
			version = (isCipher ? SERVER_KNOBS->REST_KMS_CURRENT_CIPHER_REQUEST_VERSION
			                    : SERVER_KNOBS->REST_KMS_CURRENT_BLOB_METADATA_REQUEST_VERSION) +
			          10;
		}
	} else {
		// set to invalid_version
	}
	versionValue.SetInt(version);
	doc.AddMember(versionKey, versionValue, doc.GetAllocator());

	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	httpResp->data.contentLen = 0;
	httpResp->data.content = "";
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->data.content.data(), sb.GetString(), sb.GetSize());

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
}

void testMissingDetailsTag(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value key(KMS_URLS_TAG, doc.GetAllocator());
	rapidjson::Value refreshUrl;
	refreshUrl.SetBool(true);
	doc.AddMember(key, refreshUrl, doc.GetAllocator());

	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->data.content.data(), sb.GetString(), sb.GetSize());
	httpResp->data.contentLen = sb.GetSize();

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
}

void testMalformedDetails(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	TraceEvent("TestMalformedDetailsStart");
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value key(isCipher ? CIPHER_KEY_DETAILS_TAG : BLOB_METADATA_DETAILS_TAG, doc.GetAllocator());
	rapidjson::Value details;
	details.SetBool(true);
	doc.AddMember(key, details, doc.GetAllocator());

	addVersionToDoc(doc, 1);

	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->data.content.data(), sb.GetString(), sb.GetSize());
	httpResp->data.contentLen = sb.GetSize();

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	TraceEvent("TestMalformedDetailsEnd");
}

void testMalformedDetailNotObj(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	TraceEvent("TestMalformedDetailNotObjStart");
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value cDetails(rapidjson::kArrayType);
	rapidjson::Value detail;
	rapidjson::Value key(isCipher ? BASE_CIPHER_ID_TAG : BLOB_METADATA_DOMAIN_ID_TAG, doc.GetAllocator());
	rapidjson::Value id;
	id.SetUint(12345);
	detail.AddMember(key, id, doc.GetAllocator());
	cDetails.PushBack(detail, doc.GetAllocator());
	key.SetString(isCipher ? CIPHER_KEY_DETAILS_TAG : BLOB_METADATA_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(key, cDetails, doc.GetAllocator());

	addVersionToDoc(doc, 1);

	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->data.content.data(), sb.GetString(), sb.GetSize());
	httpResp->data.contentLen = sb.GetSize();

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	TraceEvent("TestMalformedDetailNotObjEnd");
}

void testMalformedDetailObj(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	TraceEvent("TestMalformedDetailObjStart");
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value cDetails(rapidjson::kArrayType);
	rapidjson::Value detail(rapidjson::kObjectType);
	rapidjson::Value key(isCipher ? BASE_CIPHER_ID_TAG : BLOB_METADATA_DOMAIN_ID_TAG, doc.GetAllocator());
	rapidjson::Value id;
	id.SetUint(12345);
	detail.AddMember(key, id, doc.GetAllocator());
	cDetails.PushBack(detail, doc.GetAllocator());
	key.SetString(isCipher ? CIPHER_KEY_DETAILS_TAG : BLOB_METADATA_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(key, cDetails, doc.GetAllocator());

	addVersionToDoc(doc, 1);

	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->data.content.data(), sb.GetString(), sb.GetSize());
	httpResp->data.contentLen = sb.GetSize();

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	TraceEvent("TestMalformedDetailObjEnd");
}

void testKMSErrorResponse(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	rapidjson::Document doc;
	doc.SetObject();

	addVersionToDoc(doc, 1);

	// Construct fake response, it should get ignored anyways
	rapidjson::Value cDetails(rapidjson::kArrayType);
	rapidjson::Value detail(rapidjson::kObjectType);
	rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
	rapidjson::Value id;
	id.SetUint(12345);
	detail.AddMember(key, id, doc.GetAllocator());
	cDetails.PushBack(detail, doc.GetAllocator());
	key.SetString(isCipher ? CIPHER_KEY_DETAILS_TAG : BLOB_METADATA_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(key, cDetails, doc.GetAllocator());

	// Add error tag
	rapidjson::Value errorTag(rapidjson::kObjectType);

	// Add 'error_detail'
	rapidjson::Value eKey(ERROR_MSG_TAG, doc.GetAllocator());
	rapidjson::Value detailInfo;
	detailInfo.SetString("Foo is always bad", doc.GetAllocator());
	errorTag.AddMember(eKey, detailInfo, doc.GetAllocator());

	key.SetString(ERROR_TAG, doc.GetAllocator());
	doc.AddMember(key, errorTag, doc.GetAllocator());

	Reference<HTTP::IncomingResponse> httpResp = makeReference<HTTP::IncomingResponse>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->data.content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->data.content.data(), sb.GetString(), sb.GetSize());
	httpResp->data.contentLen = sb.GetSize();

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_keys_fetch_failed);
	}
}

ACTOR Future<Void> testParseDiscoverKmsUrlFileNotFound(Reference<RESTKmsConnectorCtx> ctx) {
	try {
		wait(parseDiscoverKmsUrlFile(ctx, "/imaginary-dir/dream/phantom-file"));
		ASSERT(false); // error expected
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_invalid_kms_config);
	}
	return Void();
}

ACTOR Future<Void> testParseDiscoverKmsUrlFile(Reference<RESTKmsConnectorCtx> ctx) {
	state std::shared_ptr<platform::TmpFile> tmpFile = std::make_shared<platform::TmpFile>("/tmp");
	ASSERT(fileExists(tmpFile->getFileName()));

	state std::unordered_set<std::string> urls;
	urls.emplace("https://127.0.0.1/foo  ");
	urls.emplace("  https://127.0.0.1/foo1");
	urls.emplace("  https://127.0.0.1/foo2  ");
	urls.emplace("https://127.0.0.1/foo3/");
	urls.emplace("https://127.0.0.1/foo4///");

	state std::unordered_set<std::string> compareUrls;
	compareUrls.emplace("https://127.0.0.1/foo");
	compareUrls.emplace("https://127.0.0.1/foo1");
	compareUrls.emplace("https://127.0.0.1/foo2");
	compareUrls.emplace("https://127.0.0.1/foo3");
	compareUrls.emplace("https://127.0.0.1/foo4");

	std::string content;
	for (auto& url : urls) {
		content.append(url);
		content.push_back(DISCOVER_URL_FILE_URL_SEP);
	}
	tmpFile->write((const uint8_t*)content.data(), content.size());
	wait(parseDiscoverKmsUrlFile(ctx, tmpFile->getFileName()));

	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls.size(), urls.size());
	for (const auto& url : ctx->kmsUrlStore.kmsUrls) {
		ASSERT(compareUrls.find(url.url) != compareUrls.end());
		ASSERT_EQ(url.nFailedResponses, 0);
		ASSERT_EQ(url.nRequests, 0);
		ASSERT_EQ(url.nResponseParseFailures, 0);
	}

	return Void();
}

ACTOR Future<Void> testParseDiscoverKmsUrlFileAlreadyExisting(Reference<RESTKmsConnectorCtx> ctx) {
	std::unordered_map<std::string, KmsUrlCtx<KmsUrlPenaltyParams>> urlMap;
	dropCachedKmsUrls(ctx, &urlMap);
	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls.size(), 0);

	auto urlCtx = KmsUrlCtx<KmsUrlPenaltyParams>("https://127.0.0.1/foo2");
	urlCtx.nFailedResponses = 1;
	urlCtx.nRequests = 2;
	urlCtx.nResponseParseFailures = 3;
	ctx->kmsUrlStore.kmsUrls.push_back(KmsUrlCtx<KmsUrlPenaltyParams>("https://127.0.0.1/foo4"));
	ctx->kmsUrlStore.kmsUrls.push_back(KmsUrlCtx<KmsUrlPenaltyParams>("https://127.0.0.1/foo5"));
	ctx->kmsUrlStore.kmsUrls.push_back(KmsUrlCtx<KmsUrlPenaltyParams>(urlCtx));

	state std::shared_ptr<platform::TmpFile> tmpFile = std::make_shared<platform::TmpFile>("/tmp");
	ASSERT(fileExists(tmpFile->getFileName()));

	state std::unordered_set<std::string> urls;
	urls.emplace("https://127.0.0.1/foo  ");
	urls.emplace("  https://127.0.0.1/foo1");
	urls.emplace("  https://127.0.0.1/foo2  ");

	state std::unordered_set<std::string> compareUrls;
	compareUrls.emplace("https://127.0.0.1/foo");
	compareUrls.emplace("https://127.0.0.1/foo1");
	compareUrls.emplace("https://127.0.0.1/foo2");

	std::string content;
	for (auto& url : urls) {
		content.append(url);
		content.push_back(DISCOVER_URL_FILE_URL_SEP);
	}
	tmpFile->write((const uint8_t*)content.data(), content.size());
	wait(parseDiscoverKmsUrlFile(ctx, tmpFile->getFileName()));

	ASSERT_EQ(ctx->kmsUrlStore.kmsUrls.size(), urls.size());
	for (const auto& url : ctx->kmsUrlStore.kmsUrls) {
		ASSERT(compareUrls.find(url.url) != compareUrls.end());
		if (url.url == "https://127.0.0.1/foo2") {
			ASSERT_EQ(url.nFailedResponses, 1);
			ASSERT_EQ(url.nRequests, 2);
			ASSERT_EQ(url.nResponseParseFailures, 3);
		} else {
			ASSERT_EQ(url.nFailedResponses, 0);
			ASSERT_EQ(url.nRequests, 0);
			ASSERT_EQ(url.nResponseParseFailures, 0);
		}
	}

	return Void();
}

void setKnobs() {
	auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
	g_knobs.setKnob("rest_kms_current_cipher_request_version", KnobValueRef::create(int{ 1 }));
	g_knobs.setKnob("rest_kms_current_blob_metadata_request_version", KnobValueRef::create(int{ 1 }));
	g_knobs.setKnob("rest_log_level", KnobValueRef::create(int{ 3 }));
	g_knobs.setKnob("rest_kms_connector_remove_trailing_newline", KnobValueRef::create(bool{ true }));
}

} // namespace

TEST_CASE("/KmsConnector/REST/ParseKmsDiscoveryUrls") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	setKnobs();

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	wait(testParseDiscoverKmsUrlFileNotFound(ctx));
	wait(testParseDiscoverKmsUrlFile(ctx));
	wait(testParseDiscoverKmsUrlFileAlreadyExisting(ctx));

	return Void();
}

TEST_CASE("/KmsConnector/REST/ParseValidationTokenFile") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	setKnobs();

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	wait(testEmptyValidationFileDetails(ctx));
	wait(testMalformedFileValidationTokenDetails(ctx));
	wait(testValidationTokenFileNotFound(ctx));
	wait(testTooLargeValidationTokenFile(ctx));
	wait(testValidationFileTokenPayloadTooLarge(ctx));
	wait(testMultiValidationFileTokenFiles(ctx));

	return Void();
}

TEST_CASE("/KmsConnector/REST/ParseEncryptCipherResponse") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	setKnobs();

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	testMissingOrInvalidVersion(ctx, true);
	testMissingDetailsTag(ctx, true);
	testMalformedDetails(ctx, true);
	testMalformedDetailNotObj(ctx, true);
	testMalformedDetailObj(ctx, true);
	testKMSErrorResponse(ctx, true);
	return Void();
}

TEST_CASE("/KmsConnector/REST/ParseBlobMetadataResponse") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	setKnobs();

	testMissingOrInvalidVersion(ctx, true);
	testMissingDetailsTag(ctx, false);
	testMalformedDetails(ctx, false);
	testMalformedDetailNotObj(ctx, false);
	testMalformedDetailObj(ctx, true);
	testKMSErrorResponse(ctx, false);
	return Void();
}

TEST_CASE("/KmsConnector/REST/GetEncryptionKeyOps") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	setKnobs();

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	// Prepare KmsConnector context details
	wait(testParseDiscoverKmsUrlFile(ctx));
	wait(testMultiValidationFileTokenFiles(ctx));

	const int numIterations = deterministicRandom()->randomInt(512, 786);
	for (int i = 0; i < numIterations; i++) {
		testGetEncryptKeysByKeyIdsRequestBody(ctx, arena);
		testGetEncryptKeysByDomainIdsRequestBody(ctx, arena);
		testGetBlobMetadataRequestBody(ctx);
	}
	return Void();
}

namespace {
struct TestUrlPenaltyParam {
	static double penalty(int64_t ignored) {
		int elapsed = deterministicRandom()->randomInt(1, 120);
		return KmsUrlPenaltyParams::penalty(elapsed);
	}
};
} // namespace

TEST_CASE("/KmsConnector/KmsUrlStore") {
	KmsUrlStore<TestUrlPenaltyParam> store;
	const int nUrls = deterministicRandom()->randomInt(2, 10);
	for (int i = 0; i < nUrls; i++) {
		store.kmsUrls.emplace_back("foo" + std::to_string(i));
	}
	ASSERT_EQ(store.kmsUrls.size(), nUrls);
	for (const auto& url : store.kmsUrls) {
		ASSERT_EQ(url.unresponsivenessPenalty, 0.0);
		ASSERT_EQ(url.unresponsivenessPenaltyTS, 0);
		ASSERT_EQ(url.nFailedResponses, 0);
		ASSERT_EQ(url.nResponseParseFailures, 0);
		ASSERT_EQ(url.nRequests, 0);
	}

	const int nIterations = deterministicRandom()->randomInt(100, 500);
	for (int i = 0; i < nIterations; i++) {
		const int idx = deterministicRandom()->randomInt(0, nUrls);

		if (deterministicRandom()->coinflip()) {
			if (deterministicRandom()->coinflip()) {
				store.penalize(store.kmsUrls[idx], KmsUrlCtx<TestUrlPenaltyParam>::PenaltyType::TIMEOUT);
			} else {
				store.penalize(store.kmsUrls[idx], KmsUrlCtx<TestUrlPenaltyParam>::PenaltyType::MALFORMED_RESPONSE);
			}
		} else {
			// perfect world!
		}

		for (int j = 0; j < store.kmsUrls.size() - 1; j++) {
			if (store.kmsUrls[j].unresponsivenessPenalty != store.kmsUrls[j + 1].unresponsivenessPenalty) {
				ASSERT_LE(store.kmsUrls[j].unresponsivenessPenalty, store.kmsUrls[j + 1].unresponsivenessPenalty);
			} else {
				if (store.kmsUrls[j].nFailedResponses != store.kmsUrls[j + 1].nFailedResponses) {
					ASSERT_LE(store.kmsUrls[j].nFailedResponses, store.kmsUrls[j + 1].nFailedResponses);
				} else {
					ASSERT_LE(store.kmsUrls[j].nResponseParseFailures, store.kmsUrls[j + 1].nResponseParseFailures);
				}
			}
		}
	}
	return Void();
}