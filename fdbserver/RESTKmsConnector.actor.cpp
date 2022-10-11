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
#include "fdbrpc/HTTP.h"
#include "flow/IAsyncFile.h"
#include "fdbserver/KmsConnectorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/RESTClient.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

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
const char* ENCRYPT_DOMAIN_NAME_TAG = "encrypt_domain_name";
const char* CIPHER_KEY_REFRESH_AFTER_SEC = "refresh_after_sec";
const char* CIPHER_KEY_EXPIRE_AFTER_SEC = "expire_after_sec";
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

struct RESTKmsConnectorCtx : public ReferenceCounted<RESTKmsConnectorCtx> {
	UID uid;
	KmsUrlMinHeap kmsUrlHeap;
	double lastKmsUrlsRefreshTs;
	RESTClient restClient;
	std::unordered_map<std::string, ValidationTokenCtx> validationTokens;

	RESTKmsConnectorCtx() : uid(deterministicRandom()->randomUniqueID()), lastKmsUrlsRefreshTs(0) {}
	explicit RESTKmsConnectorCtx(const UID& id) : uid(id), lastKmsUrlsRefreshTs(0) {}
};

std::string getEncryptionKeysFullUrl(Reference<RESTKmsConnectorCtx> ctx, const std::string& url) {
	if (SERVER_KNOBS->REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT.empty()) {
		TraceEvent("GetEncryptionKeysFullUrl_EmptyEndpoint", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	std::string fullUrl(url);
	return fullUrl.append("/").append(SERVER_KNOBS->REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT);
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

void extractKmsUrls(Reference<RESTKmsConnectorCtx> ctx, rapidjson::Document& doc, Reference<HTTP::Response> httpResp) {
	// Refresh KmsUrls cache
	dropCachedKmsUrls(ctx);
	ASSERT(ctx->kmsUrlHeap.empty());

	for (const auto& url : doc[KMS_URLS_TAG].GetArray()) {
		if (!url.IsString()) {
			TraceEvent("DiscoverKmsUrls_MalformedResp", ctx->uid).detail("ResponseContent", httpResp->content);
			throw operation_failed();
		}

		std::string urlStr;
		urlStr.resize(url.GetStringLength());
		memcpy(urlStr.data(), url.GetString(), url.GetStringLength());

		TraceEvent("DiscoverKmsUrls_AddUrl", ctx->uid).detail("Url", urlStr);

		ctx->kmsUrlHeap.emplace(std::make_shared<KmsUrlCtx>(urlStr));
	}

	// Update Kms URLs refresh timestamp
	ctx->lastKmsUrlsRefreshTs = now();
}

ACTOR Future<Void> parseDiscoverKmsUrlFile(Reference<RESTKmsConnectorCtx> ctx, std::string filename) {
	if (filename.empty() || !fileExists(filename)) {
		TraceEvent("DiscoverKmsUrls_FileNotFound", ctx->uid).log();
		throw encrypt_invalid_kms_config();
	}

	state Reference<IAsyncFile> dFile = wait(IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));
	state int64_t fSize = wait(dFile->size());
	state Standalone<StringRef> buff = makeString(fSize);
	int bytesRead = wait(dFile->read(mutateString(buff), fSize, 0));
	if (bytesRead != fSize) {
		TraceEvent("DiscoveryKmsUrl_FileReadShort", ctx->uid)
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
		TraceEvent(SevDebug, "DiscoverKmsUrls_AddUrl", ctx->uid).detail("Url", url);
		ctx->kmsUrlHeap.emplace(std::make_shared<KmsUrlCtx>(url));
	}

	return Void();
}

ACTOR Future<Void> discoverKmsUrls(Reference<RESTKmsConnectorCtx> ctx, bool refreshPersistedUrls) {
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

void parseKmsResponse(Reference<RESTKmsConnectorCtx> ctx,
                      Reference<HTTP::Response> resp,
                      Arena* arena,
                      VectorRef<EncryptCipherKeyDetailsRef>* outCipherKeyDetails) {
	// Acceptable response payload json format:
	//
	// response_json_payload {
	//   "cipher_key_details" : [
	//     {
	//        "base_cipher_id"    : <cipherKeyId>,
	//        "encrypt_domain_id" : <domainId>,
	//        "base_cipher"       : <baseCipher>
	//        "refresh_after_sec"   : <refreshCipherTimeInterval> (Optional)
	//        "expire_after_sec"    : <expireCipherTimeInterval>  (Optional)
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "kms_urls" : [
	//         "url1", "url2", ...
	//   ],
	//	 "error" : {					// Optional, populated by the KMS, if present, rest of payload is ignored.
	//		"errMsg" : <message>
	//		"errCode": <code>
	// 	  }
	// }

	if (resp->code != HTTP::HTTP_STATUS_CODE_OK) {
		// STATUS_OK is gating factor for REST request success
		throw http_request_failed();
	}

	rapidjson::Document doc;
	doc.Parse(resp->content.c_str());

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
			TraceEvent("KMSErrorResponse_EmptyDetails", ctx->uid).log();
		}

		throw encrypt_keys_fetch_failed();
	}

	// Extract CipherKeyDetails
	if (!doc.HasMember(CIPHER_KEY_DETAILS_TAG) || !doc[CIPHER_KEY_DETAILS_TAG].IsArray()) {
		TraceEvent(SevWarn, "ParseKmsResponse_FailureMissingCipherKeyDetails", ctx->uid).log();
		throw operation_failed();
	}

	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		if (!cipherDetail.IsObject()) {
			TraceEvent(SevWarn, "ParseKmsResponse_FailureEncryptKeyDetailsNotObject", ctx->uid)
			    .detail("Type", cipherDetail.GetType());
			throw operation_failed();
		}

		const bool isBaseCipherIdPresent = cipherDetail.HasMember(BASE_CIPHER_ID_TAG);
		const bool isBaseCipherPresent = cipherDetail.HasMember(BASE_CIPHER_TAG);
		const bool isEncryptDomainIdPresent = cipherDetail.HasMember(ENCRYPT_DOMAIN_ID_TAG);
		if (!isBaseCipherIdPresent || !isBaseCipherPresent || !isEncryptDomainIdPresent) {
			TraceEvent(SevWarn, "ParseKmsResponse_MalformedKeyDetail", ctx->uid)
			    .detail("BaseCipherIdPresent", isBaseCipherIdPresent)
			    .detail("BaseCipherPresent", isBaseCipherPresent)
			    .detail("EncryptDomainIdPresent", isEncryptDomainIdPresent);
			throw operation_failed();
		}

		const int cipherKeyLen = cipherDetail[BASE_CIPHER_TAG].GetStringLength();
		std::unique_ptr<uint8_t[]> cipherKey = std::make_unique<uint8_t[]>(cipherKeyLen);
		memcpy(cipherKey.get(), cipherDetail[BASE_CIPHER_TAG].GetString(), cipherKeyLen);

		// Extract cipher refresh and/or expiry interval if supplied
		Optional<int64_t> refreshAfterSec = cipherDetail.HasMember(CIPHER_KEY_REFRESH_AFTER_SEC) &&
		                                            cipherDetail[CIPHER_KEY_REFRESH_AFTER_SEC].GetInt64() > 0
		                                        ? cipherDetail[CIPHER_KEY_REFRESH_AFTER_SEC].GetInt64()
		                                        : Optional<int64_t>();
		Optional<int64_t> expireAfterSec = cipherDetail.HasMember(CIPHER_KEY_EXPIRE_AFTER_SEC)
		                                       ? cipherDetail[CIPHER_KEY_EXPIRE_AFTER_SEC].GetInt64()
		                                       : Optional<int64_t>();

		outCipherKeyDetails->emplace_back_deep(*arena,
		                                       cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64(),
		                                       cipherDetail[BASE_CIPHER_ID_TAG].GetUint64(),
		                                       StringRef(cipherKey.get(), cipherKeyLen),
		                                       refreshAfterSec,
		                                       expireAfterSec);
	}

	if (doc.HasMember(KMS_URLS_TAG)) {
		try {
			extractKmsUrls(ctx, doc, resp);
		} catch (Error& e) {
			TraceEvent("RefreshKmsUrls_Failed", ctx->uid).error(e);
			// Given cipherKeyDetails extraction was done successfully, ignore KmsUrls parsing error
		}
	}
}

void addQueryModeSection(Reference<RESTKmsConnectorCtx> ctx, rapidjson::Document& doc, const char* mode) {
	rapidjson::Value key(QUERY_MODE_TAG, doc.GetAllocator());
	rapidjson::Value queryMode;
	queryMode.SetString(mode, doc.GetAllocator());

	// Append 'query_mode' object to the parent document
	doc.AddMember(key, queryMode, doc.GetAllocator());
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
	//        "encrypt_domain_name" : <domainName>
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

		// Add 'encrypt_domain_name'
		key.SetString(ENCRYPT_DOMAIN_NAME_TAG, doc.GetAllocator());
		rapidjson::Value domainName;
		domainName.SetString(detail.domainName.toString().c_str(), detail.domainName.size(), doc.GetAllocator());
		keyIdDetail.AddMember(key, domainName, doc.GetAllocator());

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

ACTOR
Future<Void> fetchEncryptionKeys_impl(Reference<RESTKmsConnectorCtx> ctx,
                                      StringRef requestBodyRef,
                                      Arena* arena,
                                      VectorRef<EncryptCipherKeyDetailsRef>* outCipherKeyDetails) {
	state Reference<HTTP::Response> resp;

	// Follow 2-phase scheme:
	// Phase-1: Attempt to fetch encryption keys by reaching out to cached KmsUrls in the order of
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
				std::string kmsEncryptionFullUrl = getEncryptionKeysFullUrl(ctx, curUrl->url);
				TraceEvent("FetchEncryptionKeys_Start", ctx->uid).detail("KmsEncryptionFullUrl", kmsEncryptionFullUrl);
				Reference<HTTP::Response> _resp =
				    wait(ctx->restClient.doPost(kmsEncryptionFullUrl, requestBodyRef.toString()));
				resp = _resp;
				curUrl->nRequests++;

				try {
					parseKmsResponse(ctx, resp, arena, outCipherKeyDetails);

					// Push urlCtx back on the ctx->urlHeap
					while (!tempStack.empty()) {
						ctx->kmsUrlHeap.emplace(tempStack.top());
						tempStack.pop();
					}

					TraceEvent("FetchEncryptionKeys_Success", ctx->uid).detail("KmsUrl", curUrl->url);
					return Void();
				} catch (Error& e) {
					TraceEvent(SevWarn, "FetchEncryptionKeys_RespParseFailure").error(e);
					curUrl->nResponseParseFailures++;
					// attempt to fetch encryption details from next KmsUrl
				}
			} catch (Error& e) {
				TraceEvent("FetchEncryptionKeys_Failed", ctx->uid).error(e);
				curUrl->nFailedResponses++;
				if (pass > 1 && isKmsNotReachable(e.code())) {
					throw e;
				} else {
					// attempt to fetch encryption details from next KmsUrl
				}
			}
		}

		if (pass == 1) {
			// Re-discover KMS urls and re-attempt to fetch the encryption key details using newer KMS URLs
			wait(discoverKmsUrls(ctx, true));
		}
	}

	// Failed to fetch encryption keys from the remote KMS
	throw encrypt_keys_fetch_failed();
}

ACTOR Future<KmsConnLookupEKsByKeyIdsRep> fetchEncryptionKeysByKeyIds(Reference<RESTKmsConnectorCtx> ctx,
                                                                      KmsConnLookupEKsByKeyIdsReq req) {
	state KmsConnLookupEKsByKeyIdsRep reply;
	bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
	std::string requestBody;

	StringRef requestBodyRef = getEncryptKeysByKeyIdsRequestBody(ctx, req, refreshKmsUrls, req.arena);

	wait(fetchEncryptionKeys_impl(ctx, requestBodyRef, &reply.arena, &reply.cipherKeyDetails));

	return reply;
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
	//        "encrypt_domain_name" : <domainName>
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
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto& detail : req.encryptDomainInfos) {
		rapidjson::Value keyIdDetail(rapidjson::kObjectType);

		rapidjson::Value key(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail.domainId);
		keyIdDetail.AddMember(key, domainId, doc.GetAllocator());

		// Add 'encrypt_domain_name'
		key.SetString(ENCRYPT_DOMAIN_NAME_TAG, doc.GetAllocator());
		rapidjson::Value domainName;
		domainName.SetString(detail.domainName.toString().c_str(), detail.domainName.size(), doc.GetAllocator());
		keyIdDetail.AddMember(key, domainName, doc.GetAllocator());

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

ACTOR Future<KmsConnLookupEKsByDomainIdsRep> fetchEncryptionKeysByDomainIds(Reference<RESTKmsConnectorCtx> ctx,
                                                                            KmsConnLookupEKsByDomainIdsReq req) {
	state KmsConnLookupEKsByDomainIdsRep reply;
	bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
	StringRef requestBodyRef = getEncryptKeysByDomainIdsRequestBody(ctx, req, refreshKmsUrls, req.arena);

	wait(fetchEncryptionKeys_impl(ctx, requestBodyRef, &reply.arena, &reply.cipherKeyDetails));

	return reply;
}

ACTOR Future<Void> procureValidationTokensFromFiles(Reference<RESTKmsConnectorCtx> ctx, std::string details) {
	Standalone<StringRef> detailsRef(details);
	if (details.empty()) {
		TraceEvent("ValidationToken_EmptyFileDetails", ctx->uid).log();
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
			TraceEvent("ValidationToken_FileDetailsMalformed", ctx->uid).detail("FileDetails", details);
			throw operation_failed();
		}

		std::string tokenName = boost::trim_copy(name.toString());
		std::string tokenFile = boost::trim_copy(path.toString());
		if (!fileExists(tokenFile)) {
			TraceEvent("ValidationToken_FileNotFound", ctx->uid)
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
			TraceEvent("ValidationToken_FileTooLarge", ctx->uid)
			    .detail("FileName", tokenFile)
			    .detail("Size", fSize)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE);
			throw file_too_large();
		}

		tokensPayloadSize += fSize;
		if (tokensPayloadSize > SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE) {
			TraceEvent("ValidationToken_PayloadTooLarge", ctx->uid)
			    .detail("MaxAllowedSize", SERVER_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE);
			throw value_too_large();
		}

		state Standalone<StringRef> buff = makeString(fSize);
		int bytesRead = wait(tFile->read(mutateString(buff), fSize, 0));
		if (bytesRead != fSize) {
			TraceEvent("DiscoveryKmsUrl_FileReadShort", ctx->uid)
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
		TraceEvent("ValidationToken_ReadFile", ctx->uid)
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

ACTOR Future<Void> connectorCore_impl(KmsConnectorInterface interf) {
	state Reference<RESTKmsConnectorCtx> self = makeReference<RESTKmsConnectorCtx>(interf.id());

	TraceEvent("RESTKmsConnector_Init", self->uid).log();

	wait(discoverKmsUrls(self, false /* refreshPersistedUrls */));
	wait(procureValidationTokens(self));

	loop {
		choose {
			when(KmsConnLookupEKsByKeyIdsReq req = waitNext(interf.ekLookupByIds.getFuture())) {
				state KmsConnLookupEKsByKeyIdsReq byKeyIdReq = req;
				state KmsConnLookupEKsByKeyIdsRep byKeyIdResp;
				try {
					KmsConnLookupEKsByKeyIdsRep _rByKeyId = wait(fetchEncryptionKeysByKeyIds(self, byKeyIdReq));
					byKeyIdResp = _rByKeyId;
					byKeyIdReq.reply.send(byKeyIdResp);
				} catch (Error& e) {
					TraceEvent("LookupEKsByKeyIds_Failed", self->uid).error(e);
					if (!canReplyWith(e)) {
						throw e;
					}
					byKeyIdReq.reply.sendError(e);
				}
			}
			when(KmsConnLookupEKsByDomainIdsReq req = waitNext(interf.ekLookupByDomainIds.getFuture())) {
				state KmsConnLookupEKsByDomainIdsReq byDomainIdReq = req;
				state KmsConnLookupEKsByDomainIdsRep byDomainIdResp;
				try {
					KmsConnLookupEKsByDomainIdsRep _rByDomainId =
					    wait(fetchEncryptionKeysByDomainIds(self, byDomainIdReq));
					byDomainIdResp = _rByDomainId;
					byDomainIdReq.reply.send(byDomainIdResp);
				} catch (Error& e) {
					TraceEvent("LookupEKsByDomainIds_Failed", self->uid).error(e);
					if (!canReplyWith(e)) {
						throw e;
					}
					byDomainIdReq.reply.sendError(e);
				}
			}
			when(KmsConnBlobMetadataReq req = waitNext(interf.blobMetadataReq.getFuture())) {
				// TODO: implement!
				TraceEvent(SevWarn, "RESTKMSBlobMetadataNotImplemented!", interf.id());
				req.reply.sendError(not_implemented());
			}
		}
	}
}

Future<Void> RESTKmsConnector::connectorCore(KmsConnectorInterface interf) {
	return connectorCore_impl(interf);
}

// Only used to link unit tests
void forceLinkRESTKmsConnectorTest() {}

namespace {
std::string_view KMS_URL_NAME_TEST = "http://foo/bar";
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

void getFakeKmsResponse(StringRef jsonReqRef, const bool baseCipherIdPresent, Reference<HTTP::Response> httpResponse) {
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

		if (deterministicRandom()->coinflip()) {
			key.SetString(CIPHER_KEY_REFRESH_AFTER_SEC, resDoc.GetAllocator());
			rapidjson::Value refreshInterval;
			refreshInterval.SetInt64(10);
			keyDetail.AddMember(key, refreshInterval, resDoc.GetAllocator());
		}
		if (deterministicRandom()->coinflip()) {
			key.SetString(CIPHER_KEY_EXPIRE_AFTER_SEC, resDoc.GetAllocator());
			rapidjson::Value expireInterval;
			deterministicRandom()->coinflip() ? expireInterval.SetInt64(10) : expireInterval.SetInt64(-1);
			keyDetail.AddMember(key, expireInterval, resDoc.GetAllocator());
		}

		cipherKeyDetails.PushBack(keyDetail, resDoc.GetAllocator());
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, resDoc.GetAllocator());
	resDoc.AddMember(memberKey, cipherKeyDetails, resDoc.GetAllocator());

	ASSERT(reqDoc.HasMember(REFRESH_KMS_URLS_TAG));
	if (reqDoc[REFRESH_KMS_URLS_TAG].GetBool()) {
		rapidjson::Value kmsUrls(rapidjson::kArrayType);
		for (int i = 0; i < 3; i++) {
			rapidjson::Value url;
			url.SetString(KMS_URL_NAME_TEST.data(), resDoc.GetAllocator());
			kmsUrls.PushBack(url, resDoc.GetAllocator());
		}
		memberKey.SetString(KMS_URLS_TAG, resDoc.GetAllocator());
		resDoc.AddMember(memberKey, kmsUrls, resDoc.GetAllocator());
	}

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
		EncryptCipherDomainNameRef domainName = domainId < 0 ? StringRef(arena, FDB_DEFAULT_ENCRYPT_DOMAIN_NAME)
		                                                     : StringRef(arena, std::to_string(domainId));
		req.encryptKeyInfos.emplace_back_deep(req.arena, domainId, i, domainName);
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
	getFakeKmsResponse(requestBodyRef, true, httpResp);
	TraceEvent("FetchKeysByKeyIds", ctx->uid).setMaxFieldLength(100000).detail("HttpRespStr", httpResp->content);

	VectorRef<EncryptCipherKeyDetailsRef> cipherDetails;
	parseKmsResponse(ctx, httpResp, &arena, &cipherDetails);
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
	std::unordered_map<EncryptCipherDomainId, KmsConnLookupDomainIdsReqInfoRef> domainInfoMap;
	const int nKeys = deterministicRandom()->randomInt(7, 25);
	for (int i = 1; i < nKeys; i++) {
		EncryptCipherDomainId domainId = getRandomDomainId();
		EncryptCipherDomainNameRef domainName = domainId < 0 ? StringRef(arena, FDB_DEFAULT_ENCRYPT_DOMAIN_NAME)
		                                                     : StringRef(arena, std::to_string(domainId));
		KmsConnLookupDomainIdsReqInfoRef reqInfo(req.arena, domainId, domainName);
		if (domainInfoMap.insert({ domainId, reqInfo }).second) {
			req.encryptDomainInfos.push_back(req.arena, reqInfo);
		}
	}

	bool refreshKmsUrls = deterministicRandom()->coinflip();

	StringRef jsonReqRef = getEncryptKeysByDomainIdsRequestBody(ctx, req, refreshKmsUrls, arena);
	TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("JsonReqStr", jsonReqRef.toString());
	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	getFakeKmsResponse(jsonReqRef, false, httpResp);
	TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("HttpRespStr", httpResp->content);

	VectorRef<EncryptCipherKeyDetailsRef> cipherDetails;
	parseKmsResponse(ctx, httpResp, &arena, &cipherDetails);
	ASSERT_EQ(domainInfoMap.size(), cipherDetails.size());
	for (const auto& detail : cipherDetails) {
		ASSERT(domainInfoMap.find(detail.encryptDomainId) != domainInfoMap.end());
		ASSERT_EQ(detail.encryptKey.size(), sizeof(BASE_CIPHER_KEY_TEST));
		ASSERT_EQ(memcmp(detail.encryptKey.begin(), &BASE_CIPHER_KEY_TEST[0], sizeof(BASE_CIPHER_KEY_TEST)), 0);
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testMissingCipherDetailsTag(Reference<RESTKmsConnectorCtx> ctx) {
	Arena arena;
	VectorRef<EncryptCipherKeyDetailsRef> cipherDetails;

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
		parseKmsResponse(ctx, httpResp, &arena, &cipherDetails);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

void testMalformedCipherDetails(Reference<RESTKmsConnectorCtx> ctx) {
	Arena arena;
	VectorRef<EncryptCipherKeyDetailsRef> cipherDetails;

	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value key(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
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
		parseKmsResponse(ctx, httpResp, &arena, &cipherDetails);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

void testMalfromedCipherDetailObj(Reference<RESTKmsConnectorCtx> ctx) {
	Arena arena;
	VectorRef<EncryptCipherKeyDetailsRef> cipherDetails;

	rapidjson::Document doc;
	doc.SetObject();

	rapidjson::Value cDetails(rapidjson::kArrayType);
	rapidjson::Value detail(rapidjson::kObjectType);
	rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
	rapidjson::Value id;
	id.SetUint(12345);
	detail.AddMember(key, id, doc.GetAllocator());
	cDetails.PushBack(detail, doc.GetAllocator());
	key.SetString(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(key, cDetails, doc.GetAllocator());

	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	httpResp->content.resize(sb.GetSize(), '\0');
	memcpy(httpResp->content.data(), sb.GetString(), sb.GetSize());

	try {
		parseKmsResponse(ctx, httpResp, &arena, &cipherDetails);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
	}
}

void testKMSErrorResponse(Reference<RESTKmsConnectorCtx> ctx) {
	Arena arena;
	VectorRef<EncryptCipherKeyDetailsRef> cipherDetails;

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
	key.SetString(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
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
		parseKmsResponse(ctx, httpResp, &arena, &cipherDetails);
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

TEST_CASE("/KmsConnector/REST/ParseKmsResponse") {
	state Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	state Arena arena;

	// initialize cipher key used for testing
	deterministicRandom()->randomBytes(&BASE_CIPHER_KEY_TEST[0], 32);

	testMissingCipherDetailsTag(ctx);
	testMalformedCipherDetails(ctx);
	testMalfromedCipherDetailObj(ctx);
	testKMSErrorResponse(ctx);
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
	}
	return Void();
}
