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

#include "fdbserver/RESTKmsConnector.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/RESTClient.h"

#include "fdbrpc/HTTP.h"

#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"

#include "flow/Arena.h"
#include "flow/ActorCollection.h"
#include "flow/BooleanParam.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IAsyncFile.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

#include <limits>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <boost/algorithm/string.hpp>
#include <cstring>
#include <memory>
#include <queue>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include

namespace {
const char* BASE_CIPHER_ID_TAG = "base_cipher_id";
const char* BASE_CIPHER_TAG = "baseCipher";
const char* CIPHER_KEY_DETAILS_TAG = "cipher_key_details";
const char* ENCRYPT_DOMAIN_ID_TAG = "encrypt_domain_id";
const char* REFRESH_AFTER_SEC = "refresh_after_sec";
const char* EXPIRE_AFTER_SEC = "expire_after_sec";
const char* ERROR_TAG = "error";
const char* ERROR_MSG_TAG = "errMsg";
const char* ERROR_CODE_TAG = "errCode";
const char* KMS_URLS_TAG = "kms_urls";
const char* QUERY_MODE_TAG = "query_mode";
const char* REFRESH_KMS_URLS_TAG = "refresh_kms_urls";
const char* VALIDATION_TOKENS_TAG = "validation_tokens";
const char* VALIDATION_TOKEN_NAME_TAG = "token_name";
const char* VALIDATION_TOKEN_VALUE_TAG = "token_value";
const char* DEBUG_UID_TAG = "debug_uid";

const char* TOKEN_NAME_FILE_SEP = "#";
const char* TOKEN_TUPLE_SEP = ",";
const char DISCOVER_URL_FILE_URL_SEP = '\n';

const char* QUERY_MODE_LOOKUP_BY_DOMAIN_ID = "lookupByDomainId";
const char* QUERY_MODE_LOOKUP_BY_KEY_ID = "lookupByKeyId";

const char* BLOB_METADATA_DETAILS_TAG = "blob_metadata_details";
const char* BLOB_METADATA_DOMAIN_ID_TAG = "domain_id";
const char* BLOB_METADATA_BASE_LOCATION_TAG = "base_location";
const char* BLOB_METADATA_PARTITIONS_TAG = "partitions";

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
		return true;
	default:
		return false;
	}
}

bool isKmsNotReachable(const int errCode) {
	return errCode == error_code_timed_out || errCode == error_code_connection_failed;
}

} // namespace

struct KmsUrlCtx {
	std::string url;
	uint64_t nRequests;
	uint64_t nFailedResponses;
	uint64_t nResponseParseFailures;

	KmsUrlCtx() : url(""), nRequests(0), nFailedResponses(0), nResponseParseFailures(0) {}
	explicit KmsUrlCtx(const std::string& u) : url(u), nRequests(0), nFailedResponses(0), nResponseParseFailures(0) {}

	bool operator<(const KmsUrlCtx& toCompare) const {
		if (nFailedResponses != toCompare.nFailedResponses) {
			return nFailedResponses > toCompare.nFailedResponses;
		}
		return nResponseParseFailures > toCompare.nResponseParseFailures;
	}
};

enum class ValidationTokenSource {
	VALIDATION_TOKEN_SOURCE_FILE = 1,
	VALIDATION_TOKEN_SOURCE_LAST // Always the last element
};

struct ValidationTokenCtx {
	std::string name;
	std::string value;
	ValidationTokenSource source;
	Optional<std::string> filePath;

	explicit ValidationTokenCtx(const std::string& n, ValidationTokenSource s)
	  : name(n), value(""), source(s), filePath(Optional<std::string>()), readTS(now()) {}
	double getReadTS() const { return readTS; }

private:
	double readTS; // Approach assists refreshing token based on time of creation
};

using KmsUrlMinHeap = std::priority_queue<std::shared_ptr<KmsUrlCtx>,
                                          std::vector<std::shared_ptr<KmsUrlCtx>>,
                                          std::less<std::vector<std::shared_ptr<KmsUrlCtx>>::value_type>>;

FDB_BOOLEAN_PARAM(RefreshPersistedUrls);

struct RESTKmsConnectorCtx : public ReferenceCounted<RESTKmsConnectorCtx> {
	UID uid;
	KmsUrlMinHeap kmsUrlHeap;
	double lastKmsUrlsRefreshTs;
	RESTClient restClient;
	std::unordered_map<std::string, ValidationTokenCtx> validationTokens;
	PromiseStream<Future<Void>> addActor;

	RESTKmsConnectorCtx() : uid(deterministicRandom()->randomUniqueID()), lastKmsUrlsRefreshTs(0) {}
	explicit RESTKmsConnectorCtx(const UID& id) : uid(id), lastKmsUrlsRefreshTs(0) {}
};

std::string getFullRequestUrl(Reference<RESTKmsConnectorCtx> ctx, const std::string& url, const std::string& suffix) {
	if (suffix.empty()) {
		TraceEvent("GetFullUrlEmptyEndpoint", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}
	std::string fullUrl(url);
	return fullUrl.append("/").append(suffix);
}

void dropCachedKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	while (!ctx->kmsUrlHeap.empty()) {
		std::shared_ptr<KmsUrlCtx> curUrl = ctx->kmsUrlHeap.top();

		TraceEvent("DropCachedKmsUrls", ctx->uid)
		    .detail("Url", curUrl->url)
		    .detail("NumRequests", curUrl->nRequests)
		    .detail("NumFailedResponses", curUrl->nFailedResponses)
		    .detail("NumRespParseFailures", curUrl->nResponseParseFailures);

		ctx->kmsUrlHeap.pop();
	}
}

bool shouldRefreshKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	if (!SERVER_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS) {
		return false;
	}

	return (now() - ctx->lastKmsUrlsRefreshTs) > SERVER_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS_INTERVAL_SEC;
}

void extractKmsUrls(Reference<RESTKmsConnectorCtx> ctx,
                    const rapidjson::Document& doc,
                    Reference<HTTP::Response> httpResp) {
	// Refresh KmsUrls cache
	dropCachedKmsUrls(ctx);
	ASSERT(ctx->kmsUrlHeap.empty());

	for (const auto& url : doc[KMS_URLS_TAG].GetArray()) {
		if (!url.IsString()) {
			// TODO: We need to log only the kms section of the document
			TraceEvent("DiscoverKmsUrlsMalformedResp", ctx->uid).detail("UrlType", url.GetType());
			throw operation_failed();
		}

		std::string urlStr;
		urlStr.resize(url.GetStringLength());
		memcpy(urlStr.data(), url.GetString(), url.GetStringLength());

		TraceEvent("DiscoverKmsUrlsAddUrl", ctx->uid).detail("Url", urlStr);

		ctx->kmsUrlHeap.emplace(std::make_shared<KmsUrlCtx>(urlStr));
	}

	// Update Kms URLs refresh timestamp
	ctx->lastKmsUrlsRefreshTs = now();
}

ACTOR Future<Void> parseDiscoverKmsUrlFile(Reference<RESTKmsConnectorCtx> ctx, std::string filename) {
	if (filename.empty() || !fileExists(filename)) {
		TraceEvent("DiscoverKmsUrlsFileNotFound", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	state Reference<IAsyncFile> dFile = wait(IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));
	state int64_t fSize = wait(dFile->size());
	state Standalone<StringRef> buff = makeString(fSize);
	int bytesRead = wait(dFile->read(mutateString(buff), fSize, 0));
	if (bytesRead != fSize) {
		TraceEvent("DiscoveryKmsUrlFileReadShort", ctx->uid)
		    .detail("Filename", filename)
		    .detail("Expected", fSize)
		    .detail("Actual", bytesRead);
		throw io_error();
	}

	// Acceptable file format (new line character separated URLs):
	// <url1>\n
	// <url2>\n

	std::stringstream ss(buff.toString());
	std::string url;
	while (std::getline(ss, url, DISCOVER_URL_FILE_URL_SEP)) {
		std::string trimedUrl = boost::trim_copy(url);
		if (trimedUrl.empty()) {
			// Empty URL, ignore and continue
			continue;
		}
		TraceEvent(SevDebug, "DiscoverKmsUrlsAddUrl", ctx->uid).detail("Url", url);
		ctx->kmsUrlHeap.emplace(std::make_shared<KmsUrlCtx>(url));
	}

	return Void();
}

ACTOR Future<Void> discoverKmsUrls(Reference<RESTKmsConnectorCtx> ctx, RefreshPersistedUrls refreshPersistedUrls) {
	// KMS discovery needs to be done in two scenarios:
	// 1) Initial cluster bootstrap - first boot.
	// 2) Requests to all cached KMS URLs is failing for some reason.
	//
	// Following steps are followed as part of KMS discovery:
	// 1) Based on the configured KMS URL discovery mode, the KMS URLs are extracted and persited in a DynamicKnob
	// enabled configuration knob. Approach allows relying on the parsing configuration supplied discovery URL mode only
	// during afte the initial boot, from then on, the URLs can periodically refreshed along with encryption key fetch
	// requests (SERVER_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS needs to be enabled).
	// 2) Cluster will continue using cached KMS URLs (and refreshing them if needed); however, if for some reason, all
	// cached URLs aren't working, then code re-discovers the URL following step#1 and refresh persisted state as well.

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

	return Void();
}

void checkResponseForError(Reference<RESTKmsConnectorCtx> ctx, const rapidjson::Document& doc) {
	// Check if response has error
	if (doc.HasMember(ERROR_TAG)) {
		Standalone<StringRef> errMsgRef;
		Standalone<StringRef> errCodeRef;

		if (doc[ERROR_TAG].HasMember(ERROR_MSG_TAG) && doc[ERROR_TAG][ERROR_MSG_TAG].IsString()) {
			errMsgRef = makeString(doc[ERROR_TAG][ERROR_MSG_TAG].GetStringLength());
			memcpy(mutateString(errMsgRef),
			       doc[ERROR_TAG][ERROR_MSG_TAG].GetString(),
			       doc[ERROR_TAG][ERROR_MSG_TAG].GetStringLength());
		}
		if (doc[ERROR_TAG].HasMember(ERROR_CODE_TAG) && doc[ERROR_TAG][ERROR_CODE_TAG].IsString()) {
			errMsgRef = makeString(doc[ERROR_TAG][ERROR_CODE_TAG].GetStringLength());
			memcpy(mutateString(errMsgRef),
			       doc[ERROR_TAG][ERROR_CODE_TAG].GetString(),
			       doc[ERROR_TAG][ERROR_CODE_TAG].GetStringLength());
		}

		if (!errCodeRef.empty() || !errMsgRef.empty()) {
			TraceEvent("KMSErrorResponse", ctx->uid)
			    .detail("ErrorMsg", errMsgRef.empty() ? "" : errMsgRef.toString())
			    .detail("ErrorCode", errCodeRef.empty() ? "" : errCodeRef.toString());
		} else {
			TraceEvent("KMSErrorResponseEmptyDetails", ctx->uid).log();
		}

		throw encrypt_keys_fetch_failed();
	}
}

void checkDocForNewKmsUrls(Reference<RESTKmsConnectorCtx> ctx,
                           Reference<HTTP::Response> resp,
                           const rapidjson::Document& doc) {
	if (doc.HasMember(KMS_URLS_TAG)) {
		try {
			extractKmsUrls(ctx, doc, resp);
		} catch (Error& e) {
			TraceEvent("RefreshKmsUrlsFailed", ctx->uid).error(e);
			// Given cipherKeyDetails extraction was done successfully, ignore KmsUrls parsing error
		}
	}
}

Standalone<VectorRef<EncryptCipherKeyDetailsRef>> parseEncryptCipherResponse(Reference<RESTKmsConnectorCtx> ctx,
                                                                             Reference<HTTP::Response> resp) {
	// Acceptable response payload json format:
	//
	// response_json_payload {
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

	if (resp->code != HTTP::HTTP_STATUS_CODE_OK) {
		// STATUS_OK is gating factor for REST request success
		throw http_request_failed();
	}

	rapidjson::Document doc;
	doc.Parse(resp->content.c_str());

	checkResponseForError(ctx, doc);

	Standalone<VectorRef<EncryptCipherKeyDetailsRef>> result;

	// Extract CipherKeyDetails
	if (!doc.HasMember(CIPHER_KEY_DETAILS_TAG) || !doc[CIPHER_KEY_DETAILS_TAG].IsArray()) {
		TraceEvent(SevWarn, "ParseEncryptCipherResponseFailureMissingCipherKeyDetails", ctx->uid).log();
		throw operation_failed();
	}

	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		if (!cipherDetail.IsObject()) {
			TraceEvent(SevWarn, "ParseEncryptCipherResponseFailureEncryptKeyDetailsNotObject", ctx->uid)
			    .detail("Type", cipherDetail.GetType());
			throw operation_failed();
		}

		const bool isBaseCipherIdPresent = cipherDetail.HasMember(BASE_CIPHER_ID_TAG);
		const bool isBaseCipherPresent = cipherDetail.HasMember(BASE_CIPHER_TAG);
		const bool isEncryptDomainIdPresent = cipherDetail.HasMember(ENCRYPT_DOMAIN_ID_TAG);
		if (!isBaseCipherIdPresent || !isBaseCipherPresent || !isEncryptDomainIdPresent) {
			TraceEvent(SevWarn, "ParseEncryptCipherResponseMalformedKeyDetail", ctx->uid)
			    .detail("BaseCipherIdPresent", isBaseCipherIdPresent)
			    .detail("BaseCipherPresent", isBaseCipherPresent)
			    .detail("EncryptDomainIdPresent", isEncryptDomainIdPresent);
			throw operation_failed();
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

		result.emplace_back_deep(result.arena(),
		                         cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64(),
		                         cipherDetail[BASE_CIPHER_ID_TAG].GetUint64(),
		                         StringRef(cipherKey.get(), cipherKeyLen),
		                         refreshAfterSec,
		                         expireAfterSec);
	}

	checkDocForNewKmsUrls(ctx, resp, doc);

	return result;
}

Standalone<VectorRef<BlobMetadataDetailsRef>> parseBlobMetadataResponse(Reference<RESTKmsConnectorCtx> ctx,
                                                                        Reference<HTTP::Response> resp) {
	// Acceptable response payload json format:
	// (baseLocation and partitions follow the same properties as described in BlobMetadataUtils.h)
	//
	// response_json_payload {
	//   "blob_metadata_details" : [
	//     {
	//        "domain_id" : <domainId>,
	//        "domain_name" : <baseCipher>,
	//        "baseLocation" : <baseLocation>, (Optional if partitions is present)
	//        "partitions" : [
	//			  "partition1", "partition2", ...
	//		  ], (Optional if baseLocation is present)
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
	doc.Parse(resp->content.c_str());

	checkResponseForError(ctx, doc);

	Standalone<VectorRef<BlobMetadataDetailsRef>> result;

	// Extract CipherKeyDetails
	if (!doc.HasMember(BLOB_METADATA_DETAILS_TAG) || !doc[BLOB_METADATA_DETAILS_TAG].IsArray()) {
		TraceEvent(SevWarn, "ParseBlobMetadataResponseFailureMissingDetails", ctx->uid).log();
		throw operation_failed();
	}

	for (const auto& detail : doc[BLOB_METADATA_DETAILS_TAG].GetArray()) {
		if (!detail.IsObject()) {
			TraceEvent(SevWarn, "ParseBlobMetadataResponseFailureDetailsNotObject", ctx->uid)
			    .detail("Type", detail.GetType());
			throw operation_failed();
		}

		const bool isDomainIdPresent = detail.HasMember(BLOB_METADATA_DOMAIN_ID_TAG);
		const bool isBasePresent = detail.HasMember(BLOB_METADATA_BASE_LOCATION_TAG);
		const bool isPartitionsPresent = detail.HasMember(BLOB_METADATA_PARTITIONS_TAG);
		if (!isDomainIdPresent || (!isBasePresent && !isPartitionsPresent)) {
			TraceEvent(SevWarn, "ParseBlobMetadataResponseMalformedDetail", ctx->uid)
			    .detail("DomainIdPresent", isDomainIdPresent)
			    .detail("BaseLocationPresent", isBasePresent)
			    .detail("PartitionsPresent", isPartitionsPresent);
			throw operation_failed();
		}

		std::unique_ptr<uint8_t[]> baseStr;
		Optional<StringRef> base;
		if (isBasePresent) {
			const int baseLen = detail[BLOB_METADATA_BASE_LOCATION_TAG].GetStringLength();
			baseStr = std::make_unique<uint8_t[]>(baseLen);
			memcpy(baseStr.get(), detail[BLOB_METADATA_BASE_LOCATION_TAG].GetString(), baseLen);
			base = StringRef(baseStr.get(), baseLen);
		}

		// just do extra memory copy for simplicity here
		Standalone<VectorRef<StringRef>> partitions;
		if (isPartitionsPresent) {
			for (const auto& partition : detail[BLOB_METADATA_PARTITIONS_TAG].GetArray()) {
				if (!partition.IsString()) {
					TraceEvent("ParseBlobMetadataResponseFailurePartitionNotString", ctx->uid)
					    .detail("Type", partition.GetType());
					throw operation_failed();
				}
				const int partitionLen = partition.GetStringLength();
				std::unique_ptr<uint8_t[]> partitionStr = std::make_unique<uint8_t[]>(partitionLen);
				memcpy(partitionStr.get(), partition.GetString(), partitionLen);
				partitions.push_back_deep(partitions.arena(), StringRef(partitionStr.get(), partitionLen));
			}
		}

		// Extract refresh and/or expiry interval if supplied
		double refreshAt = detail.HasMember(REFRESH_AFTER_SEC) && detail[REFRESH_AFTER_SEC].GetInt64() > 0
		                       ? now() + detail[REFRESH_AFTER_SEC].GetInt64()
		                       : std::numeric_limits<double>::max();
		double expireAt = detail.HasMember(EXPIRE_AFTER_SEC) ? now() + detail[EXPIRE_AFTER_SEC].GetInt64()
		                                                     : std::numeric_limits<double>::max();
		result.emplace_back_deep(
		    result.arena(), detail[BLOB_METADATA_DOMAIN_ID_TAG].GetInt64(), base, partitions, refreshAt, expireAt);
	}

	checkDocForNewKmsUrls(ctx, resp, doc);

	return result;
}

void addQueryModeSection(Reference<RESTKmsConnectorCtx> ctx, rapidjson::Document& doc, const char* mode) {
	rapidjson::Value key(QUERY_MODE_TAG, doc.GetAllocator());
	rapidjson::Value queryMode;
	queryMode.SetString(mode, doc.GetAllocator());

	// Append 'query_mode' object to the parent document
	doc.AddMember(key, queryMode, doc.GetAllocator());
}

void addLatestDomainDetailsToDoc(rapidjson::Document& doc,
                                 const char* rootTagName,
                                 const char* idTagName,
                                 const std::vector<EncryptCipherDomainId>& domainIds) {
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto domId : domainIds) {
		rapidjson::Value keyIdDetail(rapidjson::kObjectType);

		rapidjson::Value key(idTagName, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(domId);
		keyIdDetail.AddMember(key, domId, doc.GetAllocator());

		keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
	}
	rapidjson::Value memberKey(rootTagName, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());
}

void addValidationTokensSectionToJsonDoc(Reference<RESTKmsConnectorCtx> ctx, rapidjson::Document& doc) {
	// Append "validationTokens" as json array
	rapidjson::Value validationTokens(rapidjson::kArrayType);

	for (const auto& token : ctx->validationTokens) {
		rapidjson::Value validationToken(rapidjson::kObjectType);

		// Add "name" - token name
		rapidjson::Value key(VALIDATION_TOKEN_NAME_TAG, doc.GetAllocator());
		rapidjson::Value tokenName(token.second.name.c_str(), doc.GetAllocator());
		validationToken.AddMember(key, tokenName, doc.GetAllocator());

		// Add "value" - token value
		key.SetString(VALIDATION_TOKEN_VALUE_TAG, doc.GetAllocator());
		rapidjson::Value tokenValue;
		tokenValue.SetString(token.second.value.c_str(), token.second.value.size(), doc.GetAllocator());
		validationToken.AddMember(key, tokenValue, doc.GetAllocator());

		validationTokens.PushBack(validationToken, doc.GetAllocator());
	}

	// Append 'validation_token[]' to the parent document
	rapidjson::Value memberKey(VALIDATION_TOKENS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, validationTokens, doc.GetAllocator());
}

void addRefreshKmsUrlsSectionToJsonDoc(Reference<RESTKmsConnectorCtx> ctx,
                                       rapidjson::Document& doc,
                                       const bool refreshKmsUrls) {
	rapidjson::Value key(REFRESH_KMS_URLS_TAG, doc.GetAllocator());
	rapidjson::Value refreshUrls;
	refreshUrls.SetBool(refreshKmsUrls);

	// Append 'refresh_kms_urls' object to the parent document
	doc.AddMember(key, refreshUrls, doc.GetAllocator());
}

void addDebugUidSectionToJsonDoc(Reference<RESTKmsConnectorCtx> ctx, rapidjson::Document& doc, Optional<UID> dbgId) {
	if (!dbgId.present()) {
		// Debug id not present; do nothing
		return;
	}
	rapidjson::Value key(DEBUG_UID_TAG, doc.GetAllocator());
	rapidjson::Value debugIdVal;
	const std::string dbgIdStr = dbgId.get().toString();
	debugIdVal.SetString(dbgIdStr.c_str(), dbgIdStr.size(), doc.GetAllocator());

	// Append 'debug_uid' object to the parent document
	doc.AddMember(key, debugIdVal, doc.GetAllocator());
}

StringRef getEncryptKeysByKeyIdsRequestBody(Reference<RESTKmsConnectorCtx> ctx,
                                            const KmsConnLookupEKsByKeyIdsReq& req,
                                            const bool refreshKmsUrls,
                                            Arena& arena) {
	// Acceptable request payload json format:
	//
	// request_json_payload {
	//   "query_mode": "lookupByKeyId" / "lookupByDomainId"
	//   "cipher_key_details" = [
	//     {
	//        "base_cipher_id"      : <cipherKeyId>
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
	//   "debug_uid" = <uid-string>   // Optional debug info to trace requests across FDB <--> KMS
	// }

	rapidjson::Document doc;
	doc.SetObject();

	// Append 'query_mode' object
	addQueryModeSection(ctx, doc, QUERY_MODE_LOOKUP_BY_KEY_ID);

	// Append 'cipher_key_details' as json array
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto& detail : req.encryptKeyInfos) {
		rapidjson::Value keyIdDetail(rapidjson::kObjectType);

		// Add 'base_cipher_id'
		rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
		rapidjson::Value baseKeyId;
		baseKeyId.SetUint64(detail.baseCipherId);
		keyIdDetail.AddMember(key, baseKeyId, doc.GetAllocator());

		// Add 'encrypt_domain_id'
		key.SetString(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail.domainId);
		keyIdDetail.AddMember(key, domainId, doc.GetAllocator());

		// push above object to the array
		keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());

	// Append 'validation_tokens' as json array
	addValidationTokensSectionToJsonDoc(ctx, doc);

	// Append 'refresh_kms_urls'
	addRefreshKmsUrlsSectionToJsonDoc(ctx, doc, refreshKmsUrls);

	// Append 'debug_uid' section if needed
	addDebugUidSectionToJsonDoc(ctx, doc, req.debugId);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	StringRef ref = makeString(sb.GetSize(), arena);
	memcpy(mutateString(ref), sb.GetString(), sb.GetSize());
	return ref;
}

ACTOR template <class T>
Future<T> kmsRequestImpl(Reference<RESTKmsConnectorCtx> ctx,
                         std::string urlSuffix,
                         StringRef requestBodyRef,
                         std::function<T(Reference<RESTKmsConnectorCtx>, Reference<HTTP::Response>)> parseFunc) {
	state UID requestID = deterministicRandom()->randomUniqueID();

	// Follow 2-phase scheme:
	// Phase-1: Attempt to do request reaching out to cached KmsUrls in the order of
	//          past success requests success counts.
	// Phase-2: For some reason if none of the cached KmsUrls worked, re-discover the KmsUrls and
	//          repeat phase-1.

	state int pass = 1;
	for (; pass <= 2; pass++) {
		state std::stack<std::shared_ptr<KmsUrlCtx>> tempStack;

		// Iterate over Kms URLs
		while (!ctx->kmsUrlHeap.empty()) {
			state std::shared_ptr<KmsUrlCtx> curUrl = ctx->kmsUrlHeap.top();
			ctx->kmsUrlHeap.pop();
			tempStack.push(curUrl);

			try {
				std::string kmsEncryptionFullUrl = getFullRequestUrl(ctx, curUrl->url, urlSuffix);
				TraceEvent(SevDebug, "KmsRequestStart", ctx->uid)
				    .detail("RequestID", requestID)
				    .detail("FullUrl", kmsEncryptionFullUrl);
				Reference<HTTP::Response> resp =
				    wait(ctx->restClient.doPost(kmsEncryptionFullUrl, requestBodyRef.toString()));
				curUrl->nRequests++;

				try {
					T parsedResp = parseFunc(ctx, resp);

					// Push urlCtx back on the ctx->urlHeap
					while (!tempStack.empty()) {
						ctx->kmsUrlHeap.emplace(tempStack.top());
						tempStack.pop();
					}

					TraceEvent(SevDebug, "KmsRequestSuccess", ctx->uid).detail("RequestID", requestID);
					return parsedResp;
				} catch (Error& e) {
					TraceEvent(SevWarn, "KmsRequestRespParseFailure").error(e).detail("RequestID", requestID);
					curUrl->nResponseParseFailures++;
					// attempt to do request from next KmsUrl
				}
			} catch (Error& e) {
				curUrl->nFailedResponses++;
				if (pass > 1 && isKmsNotReachable(e.code())) {
					TraceEvent(SevDebug, "KmsRequestFailedUnreachable", ctx->uid)
					    .error(e)
					    .detail("RequestID", requestID);
					throw e;
				} else {
					TraceEvent(SevDebug, "KmsRequestError", ctx->uid).error(e).detail("RequestID", requestID);
					// attempt to do request from next KmsUrl
				}
			}
		}

		if (pass == 1) {
			// Re-discover KMS urls and re-attempt request using newer KMS URLs
			wait(discoverKmsUrls(ctx, RefreshPersistedUrls::True));
		}
	}
	TraceEvent(SevDebug, "KmsRequestFailed", ctx->uid).detail("RequestID", requestID);

	// Failed to do request from the remote KMS
	// TODO: generic KMS error types
	throw encrypt_keys_fetch_failed();
}

ACTOR Future<Void> fetchEncryptionKeysByKeyIds(Reference<RESTKmsConnectorCtx> ctx, KmsConnLookupEKsByKeyIdsReq req) {
	state KmsConnLookupEKsByKeyIdsRep reply;
	try {
		bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
		StringRef requestBodyRef = getEncryptKeysByKeyIdsRequestBody(ctx, req, refreshKmsUrls, req.arena);

		std::function<Standalone<VectorRef<EncryptCipherKeyDetailsRef>>(Reference<RESTKmsConnectorCtx>,
		                                                                Reference<HTTP::Response>)>
		    f = &parseEncryptCipherResponse;
		Standalone<VectorRef<EncryptCipherKeyDetailsRef>> result = wait(kmsRequestImpl(
		    ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT, requestBodyRef, std::move(f)));
		reply.cipherKeyDetails = result;
		reply.arena.dependsOn(result.arena());
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("LookupEKsByKeyIdsFailed", ctx->uid).error(e);
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
	//   "query_mode": "lookupByKeyId" / "lookupByDomainId"
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

	// Append 'query_mode' object
	addQueryModeSection(ctx, doc, QUERY_MODE_LOOKUP_BY_DOMAIN_ID);

	// Append 'cipher_key_details' as json array
	addLatestDomainDetailsToDoc(doc, CIPHER_KEY_DETAILS_TAG, ENCRYPT_DOMAIN_ID_TAG, req.encryptDomainIds);

	// Append 'validation_tokens' as json array
	addValidationTokensSectionToJsonDoc(ctx, doc);

	// Append 'refresh_kms_urls'
	addRefreshKmsUrlsSectionToJsonDoc(ctx, doc, refreshKmsUrls);

	// Append 'debug_uid' section if needed
	addDebugUidSectionToJsonDoc(ctx, doc, req.debugId);

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
		                                                                Reference<HTTP::Response>)>
		    f = &parseEncryptCipherResponse;

		Standalone<VectorRef<EncryptCipherKeyDetailsRef>> result = wait(kmsRequestImpl(
		    ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT, requestBodyRef, std::move(f)));
		reply.cipherKeyDetails = result;
		reply.arena.dependsOn(result.arena());
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("LookupEKsByDomainIdsFailed", ctx->uid).error(e);
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

	// Append 'blob_metadata_details' as json array
	addLatestDomainDetailsToDoc(doc, BLOB_METADATA_DETAILS_TAG, BLOB_METADATA_DOMAIN_ID_TAG, req.domainIds);

	// Append 'validation_tokens' as json array
	addValidationTokensSectionToJsonDoc(ctx, doc);

	// Append 'refresh_kms_urls'
	addRefreshKmsUrlsSectionToJsonDoc(ctx, doc, refreshKmsUrls);

	// Append 'debug_uid' section if needed
	addDebugUidSectionToJsonDoc(ctx, doc, req.debugId);

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
		                                                            Reference<HTTP::Response>)>
		    f = &parseBlobMetadataResponse;
		wait(
		    store(reply.metadataDetails,
		          kmsRequestImpl(
		              ctx, SERVER_KNOBS->REST_KMS_CONNECTOR_GET_BLOB_METADATA_ENDPOINT, requestBodyRef, std::move(f))));
		req.reply.send(reply);
	} catch (Error& e) {
		TraceEvent("LookupBlobMetadataFailed", ctx->uid).error(e);
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
		TraceEvent("ValidationTokenEmptyFileDetails", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	TraceEvent("ValidationToken", ctx->uid).detail("DetailsStr", details);

	state std::unordered_map<std::string, std::string> tokenFilePathMap;
	while (!details.empty()) {
		StringRef name = detailsRef.eat(TOKEN_NAME_FILE_SEP);
		if (name.empty()) {
			break;
		}
		StringRef path = detailsRef.eat(TOKEN_TUPLE_SEP);
		if (path.empty()) {
			TraceEvent("ValidationTokenFileDetailsMalformed", ctx->uid).detail("FileDetails", details);
			throw operation_failed();
		}

		std::string tokenName = boost::trim_copy(name.toString());
		std::string tokenFile = boost::trim_copy(path.toString());
		if (!fileExists(tokenFile)) {
			TraceEvent("ValidationTokenFileNotFound", ctx->uid)
			    .detail("TokenName", tokenName)
			    .detail("Filename", tokenFile);
			throw encrypt_invalid_kms_config();
		}

		tokenFilePathMap.emplace(tokenName, tokenFile);
		TraceEvent("ValidationToken", ctx->uid).detail("FName", tokenName).detail("Filename", tokenFile);
	}

	// Clear existing cached validation tokens
	ctx->validationTokens.clear();

	// Enumerate all token files and extract details
	state uint64_t tokensPayloadSize = 0;
	for (const auto& item : tokenFilePathMap) {
		state std::string tokenName = item.first;
		state std::string tokenFile = item.second;
		state Reference<IAsyncFile> tFile = wait(IAsyncFileSystem::filesystem()->open(
		    tokenFile, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));

		state int64_t fSize = wait(tFile->size());
		if (fSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE) {
			TraceEvent("ValidationTokenFileTooLarge", ctx->uid)
			    .detail("FileName", tokenFile)
			    .detail("Size", fSize)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE);
			throw file_too_large();
		}

		tokensPayloadSize += fSize;
		if (tokensPayloadSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE) {
			TraceEvent("ValidationTokenPayloadTooLarge", ctx->uid)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE);
			throw value_too_large();
		}

		state Standalone<StringRef> buff = makeString(fSize);
		int bytesRead = wait(tFile->read(mutateString(buff), fSize, 0));
		if (bytesRead != fSize) {
			TraceEvent("DiscoveryKmsUrlFileReadShort", ctx->uid)
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

		// NOTE: avoid logging token-value to prevent token leaks in log files..
		TraceEvent("ValidationTokenReadFile", ctx->uid)
		    .detail("TokenName", tokenCtx.name)
		    .detail("TokenSize", tokenCtx.value.size())
		    .detail("TokenFilePath", tokenCtx.filePath.get())
		    .detail("TotalPayloadSize", tokensPayloadSize);

		ctx->validationTokens.emplace(tokenName, std::move(tokenCtx));
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
std::string_view BLOB_METADATA_PARTITION_TEST = "part";
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
		wait(procureValidationTokensFromFiles(ctx, "foo#/imaginary-dir/dream/phantom-file"));
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

	deterministicRandom()->randomBytes(mutateString(buff), tokenLen);

	for (int i = 1; i <= numFiles; i++) {
		std::string tokenName = std::to_string(i);
		std::shared_ptr<platform::TmpFile> tokenfile = prepareTokenFile(buff.begin(), tokenLen);

		std::string token((char*)buff.begin(), tokenLen);
		tokenFiles.emplace(tokenName, tokenfile);
		tokenNameValueMap.emplace(std::to_string(i), token);
		tokenDetailsStr.append(tokenName).append(TOKEN_NAME_FILE_SEP).append(tokenfile->getFileName());
		if (i < numFiles)
			tokenDetailsStr.append(TOKEN_TUPLE_SEP);
	}

	wait(procureValidationTokensFromFiles(ctx, tokenDetailsStr));

	ASSERT_EQ(ctx->validationTokens.size(), tokenNameValueMap.size());

	for (const auto& token : ctx->validationTokens) {
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
                                  Reference<HTTP::Response> httpResponse) {
	rapidjson::Document reqDoc;
	reqDoc.Parse(jsonReqRef.toString().c_str());

	rapidjson::Document resDoc;
	resDoc.SetObject();

	ASSERT(reqDoc.HasMember(CIPHER_KEY_DETAILS_TAG) && reqDoc[CIPHER_KEY_DETAILS_TAG].IsArray());

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
	httpResponse->content.resize(sb.GetSize(), '\0');
	memcpy(httpResponse->content.data(), sb.GetString(), sb.GetSize());
}

void getFakeBlobMetadataResponse(StringRef jsonReqRef,
                                 const bool baseCipherIdPresent,
                                 Reference<HTTP::Response> httpResponse) {
	rapidjson::Document reqDoc;
	reqDoc.Parse(jsonReqRef.toString().c_str());

	rapidjson::Document resDoc;
	resDoc.SetObject();

	ASSERT(reqDoc.HasMember(BLOB_METADATA_DETAILS_TAG) && reqDoc[BLOB_METADATA_DETAILS_TAG].IsArray());

	rapidjson::Value blobMetadataDetails(rapidjson::kArrayType);
	for (const auto& detail : reqDoc[BLOB_METADATA_DETAILS_TAG].GetArray()) {
		rapidjson::Value keyDetail(rapidjson::kObjectType);

		ASSERT(detail.HasMember(BLOB_METADATA_DOMAIN_ID_TAG));

		rapidjson::Value key(BLOB_METADATA_DOMAIN_ID_TAG, resDoc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail[BLOB_METADATA_DOMAIN_ID_TAG].GetInt64());
		keyDetail.AddMember(key, domainId, resDoc.GetAllocator());

		int type = deterministicRandom()->randomInt(0, 3);
		if (type == 0 || type == 1) {
			key.SetString(BLOB_METADATA_BASE_LOCATION_TAG, resDoc.GetAllocator());
			rapidjson::Value baseLocation;
			baseLocation.SetString(BLOB_METADATA_BASE_LOCATION_TEST.data(), resDoc.GetAllocator());
			keyDetail.AddMember(key, baseLocation, resDoc.GetAllocator());
		}
		if (type == 1 || type == 2) {
			int partitionCount = deterministicRandom()->randomInt(2, 6);
			rapidjson::Value partitions(rapidjson::kArrayType);
			for (int i = 0; i < partitionCount; i++) {
				rapidjson::Value p;
				p.SetString(((type == 1) ? BLOB_METADATA_PARTITION_TEST : BLOB_METADATA_BASE_LOCATION_TEST).data(),
				            resDoc.GetAllocator());
				partitions.PushBack(p, resDoc.GetAllocator());
			}
			key.SetString(BLOB_METADATA_PARTITIONS_TAG, resDoc.GetAllocator());
			keyDetail.AddMember(key, partitions, resDoc.GetAllocator());
		}

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
	httpResponse->content.resize(sb.GetSize(), '\0');
	memcpy(httpResponse->content.data(), sb.GetString(), sb.GetSize());
}

void validateKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	ASSERT_EQ(ctx->kmsUrlHeap.size(), 3);
	std::shared_ptr<KmsUrlCtx> urlCtx = ctx->kmsUrlHeap.top();
	ASSERT_EQ(urlCtx->url.compare(KMS_URL_NAME_TEST), 0);
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
	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeEncryptCipherResponse(requestBodyRef, true, httpResp);
	TraceEvent("FetchKeysByKeyIds", ctx->uid).setMaxFieldLength(100000).detail("HttpRespStr", httpResp->content);

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
	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeEncryptCipherResponse(jsonReqRef, false, httpResp);
	TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("HttpRespStr", httpResp->content);

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
	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeBlobMetadataResponse(jsonReqRef, false, httpResp);
	TraceEvent("FetchBlobMetadataResp", ctx->uid).detail("HttpRespStr", httpResp->content);

	Standalone<VectorRef<BlobMetadataDetailsRef>> details = parseBlobMetadataResponse(ctx, httpResp);

	ASSERT_EQ(domainIds.size(), details.size());
	for (const auto& detail : details) {
		auto it = domainIds.find(detail.domainId);
		ASSERT(it != domainIds.end());
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testMissingDetailsTag(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value key(KMS_URLS_TAG, doc.GetAllocator());
	rapidjson::Value refreshUrl;
	refreshUrl.SetBool(true);
	doc.AddMember(key, refreshUrl, doc.GetAllocator());

	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->content.data(), sb.GetString(), sb.GetSize());

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

void testMalformedDetails(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value key(isCipher ? CIPHER_KEY_DETAILS_TAG : BLOB_METADATA_DETAILS_TAG, doc.GetAllocator());
	rapidjson::Value details;
	details.SetBool(true);
	doc.AddMember(key, details, doc.GetAllocator());

	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->content.data(), sb.GetString(), sb.GetSize());

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

void testMalformedDetailObj(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
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

	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->content.data(), sb.GetString(), sb.GetSize());

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

void testKMSErrorResponse(Reference<RESTKmsConnectorCtx> ctx, bool isCipher) {
	rapidjson::Document doc;
	doc.SetObject();

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

	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->content.data(), sb.GetString(), sb.GetSize());

	try {
		if (isCipher) {
			parseEncryptCipherResponse(ctx, httpResp);
		} else {
			parseBlobMetadataResponse(ctx, httpResp);
		}
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_keys_fetch_failed);
	}
}

ACTOR Future<Void> testParseDiscoverKmsUrlFileNotFound(Reference<RESTKmsConnectorCtx> ctx) {
	try {
		wait(parseDiscoverKmsUrlFile(ctx, "/imaginary-dir/dream/phantom-file"));
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_invalid_kms_config);
	}
	return Void();
}

ACTOR Future<Void> testParseDiscoverKmsUrlFile(Reference<RESTKmsConnectorCtx> ctx) {
	state std::shared_ptr<platform::TmpFile> tmpFile = std::make_shared<platform::TmpFile>("/tmp");
	ASSERT(fileExists(tmpFile->getFileName()));

	state std::unordered_set<std::string> urls;
	urls.emplace("https://127.0.0.1/foo");
	urls.emplace("https://127.0.0.1/foo1");
	urls.emplace("https://127.0.0.1/foo2");

	std::string content;
	for (auto& url : urls) {
		content.append(url);
		content.push_back(DISCOVER_URL_FILE_URL_SEP);
	}
	tmpFile->write((const uint8_t*)content.c_str(), content.size());
	wait(parseDiscoverKmsUrlFile(ctx, tmpFile->getFileName()));

	ASSERT_EQ(ctx->kmsUrlHeap.size(), urls.size());
	while (!ctx->kmsUrlHeap.empty()) {
		std::shared_ptr<KmsUrlCtx> urlCtx = ctx->kmsUrlHeap.top();
		ctx->kmsUrlHeap.pop();

		ASSERT(urls.find(urlCtx->url) != urls.end());
		ASSERT_EQ(urlCtx->nFailedResponses, 0);
		ASSERT_EQ(urlCtx->nRequests, 0);
		ASSERT_EQ(urlCtx->nResponseParseFailures, 0);
	}

	return Void();
}

} // namespace

TEST_CASE("/KmsConnector/REST/ParseKmsDiscoveryUrls") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	wait(testParseDiscoverKmsUrlFileNotFound(ctx));
	wait(testParseDiscoverKmsUrlFile(ctx));

	return Void();
}

TEST_CASE("/KmsConnector/REST/ParseValidationTokenFile") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

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

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	testMissingDetailsTag(ctx, true);
	testMalformedDetails(ctx, true);
	testMalformedDetailObj(ctx, true);
	testKMSErrorResponse(ctx, true);
	return Void();
}

TEST_CASE("/KmsConnector/REST/ParseBlobMetadataResponse") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	testMissingDetailsTag(ctx, false);
	testMalformedDetails(ctx, false);
	testMalformedDetailObj(ctx, true);
	testKMSErrorResponse(ctx, false);
	return Void();
}

TEST_CASE("/KmsConnector/REST/GetEncryptionKeyOps") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

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
