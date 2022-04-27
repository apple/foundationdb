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

#include "fdbserver/RESTKmsConnector.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/rapidjson/document.h"
#include "fdbclient/rapidjson/rapidjson.h"
#include "fdbclient/rapidjson/stringbuffer.h"
#include "fdbclient/rapidjson/writer.h"
#include "fdbrpc/HTTP.h"
#include "fdbserver/KmsConnectorInterface.h"
#include "fdbrpc/RESTClient.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <queue>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "flow/actorcompiler.h" // This must be the last #include

namespace {
const char* KMS_URLS_TAG = "kmsUrls";
const char* BASE_CIPHER_ID_TAG = "baseCipherId";
const char* BASE_CIPHER_TAG = "baseCipher";
const char* ENCRYPT_DOMAIN_ID_TAG = "encryptDomainId";
const char* REFRESH_KMS_URLS_TAG = "refreshKmsUrls";
const char* CIPHER_KEY_DETAILS_TAG = "cipherKeyDetails";
const char* VALIDATION_TOKENS_TAG = "validationTokens";
const char* VALIDATION_TOKEN_NAME_TAG = "tokenName";
const char* VALIDATION_TOKEN_VALUE_TAG = "tokenValue";
} // namespace

struct KmsUrlCtx {
	std::string url;
	uint64_t nRequests;
	uint64_t nFailedResponses;
	uint64_t nResponseParseFailures;

	KmsUrlCtx(const std::string u) : url(u), nRequests(0), nFailedResponses(0), nResponseParseFailures(0) {}
	bool operator<(const KmsUrlCtx& toCompare) const {
		if (nFailedResponses != toCompare.nFailedResponses) {
			return nFailedResponses > toCompare.nFailedResponses;
		}
		return nResponseParseFailures > toCompare.nResponseParseFailures;
	}
};

typedef enum {
	VALIDATION_TOKEN_SOURCE_FILE = 1,
	VALIDATION_TOKEN_SOURCE_LAST // Always the last element
} ValidationTokenSource;

struct ValidationToken {
	std::string name;
	std::string value;
	ValidationTokenSource source;
	Optional<std::string> filePath;
};

using KmsUrlMinHeap = std::priority_queue<std::shared_ptr<KmsUrlCtx>,
                                          std::vector<std::shared_ptr<KmsUrlCtx>>,
                                          std::less<std::vector<std::shared_ptr<KmsUrlCtx>>::value_type>>;

struct RESTKmsConnectorCtx : public ReferenceCounted<RESTKmsConnectorCtx> {
	UID uid;
	KmsUrlMinHeap kmsUrlHeap;
	double lastKmsUrlsRefreshTs;
	RESTClient restClient;
	std::unordered_map<std::string, ValidationToken> validationTokens;

	RESTKmsConnectorCtx() : uid(deterministicRandom()->randomUniqueID()), lastKmsUrlsRefreshTs(0) {}
	explicit RESTKmsConnectorCtx(const UID& id) : uid(id), lastKmsUrlsRefreshTs(0) {}
};

std::string getEncryptionFullUrl(const std::string& url) {
	std::string fullUrl(url);
	return fullUrl.append("/").append(FLOW_KNOBS->REST_KMS_CONNECTOR_GET_ENCRYPTION_KEYS_ENDPOINT);
}

void dropCachedKmsUrlss(Reference<RESTKmsConnectorCtx> ctx) {
	while (!ctx->kmsUrlHeap.empty()) {
		std::shared_ptr<KmsUrlCtx> curUrl = ctx->kmsUrlHeap.top();
		ctx->kmsUrlHeap.pop();

		TraceEvent("DropCachedKmsUrls", ctx->uid)
		    .detail("Url", curUrl->url)
		    .detail("NumRequests", curUrl->nRequests)
		    .detail("NumFailedResponses", curUrl->nFailedResponses)
		    .detail("NumRespParseFailures", curUrl->nResponseParseFailures);
	}
}

bool shouldRefreshKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	if (!FLOW_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS) {
		return false;
	}

	return (now() - ctx->lastKmsUrlsRefreshTs) > FLOW_KNOBS->REST_KMS_CONNECTOR_REFRESH_KMS_URLS_INTERVAL_SEC;
}

void extractKmsUrls(Reference<RESTKmsConnectorCtx> ctx, rapidjson::Document& doc, const std::string& responseContent) {
	// Refresh KmsUrls cache
	dropCachedKmsUrlss(ctx);
	ASSERT(ctx->kmsUrlHeap.empty());

	for (const auto& url : doc[KMS_URLS_TAG].GetArray()) {
		if (!url.IsString()) {
			TraceEvent("DiscoverKmsUrlss_MalformedResp", ctx->uid).detail("ResponseContent", responseContent);
			throw operation_failed();
		}

		ctx->kmsUrlHeap.emplace(std::make_shared<KmsUrlCtx>(url.GetString()));
		TraceEvent("DiscoverKmsUrls_AddUrl", ctx->uid).detail("Url", url.GetString());
	}

	// Update Kms URLs refresh timestamp
	ctx->lastKmsUrlsRefreshTs = now();
}

void parseDiscoverKmsUrlsResp(Reference<RESTKmsConnectorCtx> ctx, Reference<HTTP::Response> resp) {
	ASSERT_EQ(resp->code, HTTP::HTTP_STATUS_CODE_OK);

	// Acceptable REST JSON response schema:
	//
	// response_json_payload {
	//   "kmsUrls" : [ url1, url2, ...]
	// }
	TraceEvent("ParseDiscoverKmsUrls_Response", ctx->uid)
	    .detail("RespCode", resp->code)
	    .detail("RespContent", resp->content);

	rapidjson::Document doc;
	doc.Parse(resp->content.c_str());
	if (!doc.HasMember(KMS_URLS_TAG) || !doc[KMS_URLS_TAG].IsArray()) {
		TraceEvent("DiscoverKmsUrls_MalformedResp", ctx->uid).detail("ResponseContent", resp->content);
		throw operation_failed();
	}

	extractKmsUrls(ctx, doc, resp->content);
}

ACTOR Future<Void> discoverKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	StringRef kmsDiscoveryUrls(FLOW_KNOBS->REST_KMS_CONNECTOR_KMS_DISCOVERY_URLS);
	state std::vector<std::string> urls;

	while (kmsDiscoveryUrls.empty()) {
		StringRef u = kmsDiscoveryUrls.eat(",");
		urls.push_back(u.toString());
	}

	if (urls.empty()) {
		TraceEvent("DiscoverKmsUrls_Empty", ctx->uid).log();
		throw operation_failed();
	}

	state Reference<HTTP::Response> resp;
	state bool done = false;
	state int i = 0;
	for (; i < urls.size(); i++) {
		try {
			TraceEvent("DiscoverKmsUrls", ctx->uid).detail("Url", urls[i]);

			Reference<HTTP::Response> _r = wait(ctx->restClient.doGet(urls[i]));

			resp = _r;
			parseDiscoverKmsUrlsResp(ctx, resp);
			// KmsUrls discovery is complete
			done = true;
			break;
		} catch (Error& e) {
			TraceEvent("DiscoverKmsUrls_Failed", ctx->uid).error(e);
			// continue reaching out to next KmsDiscover URL
		}
	}

	if (!done) {
		TraceEvent("DiscoverKmsUrls_Failed", ctx->uid).log();
		throw operation_failed();
	}
	return Void();
}

void parseKmsResponse(Reference<RESTKmsConnectorCtx> ctx,
                      Reference<HTTP::Response> resp,
                      Arena& arena,
                      std::vector<EncryptCipherKeyDetails>& outCipherKeyDetails) {
	// Acceptable response payload json format:
	//
	// response_json_payload {
	//   "cipherKeyDetails" = [
	//     {
	//        "baseCipherId" : <cipherKeyId>,
	//        "encryptDomainId" : <domainId>,
	//        "baseCipher" : <baseCipher>
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "kmsUrls" = [
	//      {
	//         "url" : <url>
	//      },
	//   ]
	// }

	if (resp->code != HTTP::HTTP_STATUS_CODE_OK) {
		// STATUS_OK is gating factor for REST request success
		throw http_request_failed();
	}

	rapidjson::Document doc;
	doc.Parse(resp->content.c_str());

	// Extract CipherKeyDetails
	if (!doc.HasMember(CIPHER_KEY_DETAILS_TAG) || !doc[CIPHER_KEY_DETAILS_TAG].IsArray()) {
		TraceEvent("ParseKmsResponse_FailureMissingCipherKeyDetails", ctx->uid).log();
		throw operation_failed();
	}

	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		if (!cipherDetail.IsObject()) {
			TraceEvent("ParseKmsResponse_FailureEncryptKeyDetailsNotObject", ctx->uid)
			    .detail("Type", cipherDetail.GetType());
			throw operation_failed();
		}

		const bool isBaseCipherIdPresent = cipherDetail.HasMember(BASE_CIPHER_ID_TAG);
		const bool isBaseCipherPresent = cipherDetail.HasMember(BASE_CIPHER_TAG);
		const bool isEncryptDomainIdPresent = cipherDetail.HasMember(ENCRYPT_DOMAIN_ID_TAG);
		if (!isBaseCipherIdPresent || !isBaseCipherPresent || !isEncryptDomainIdPresent) {
			TraceEvent("ParseKmsResponse_MalformedKeyDetail", ctx->uid)
			    .detail("BaseCipherIdPresent", isBaseCipherIdPresent)
			    .detail("BaseCipherPresent", isBaseCipherPresent)
			    .detail("EncryptDomainIdPresent", isEncryptDomainIdPresent);
			throw operation_failed();
		}

		outCipherKeyDetails.emplace_back(cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64(),
		                                 cipherDetail[BASE_CIPHER_ID_TAG].GetUint64(),
		                                 StringRef(cipherDetail[BASE_CIPHER_TAG].GetString()),
		                                 arena);
	}

	if (doc.HasMember(KMS_URLS_TAG)) {
		try {
			extractKmsUrls(ctx, doc, resp->content);
		} catch (Error& e) {
			TraceEvent("RefreshKmsUrls_Failed", ctx->uid).error(e);
			// Given cipherKeyDetails extraction was done successfully, ignore KmsUrls parsing error
		}
	}
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
		rapidjson::Value tokenValue(token.second.value.c_str(), doc.GetAllocator());
		validationToken.AddMember(key, tokenValue, doc.GetAllocator());

		validationTokens.PushBack(validationToken, doc.GetAllocator());
	}

	// Append validationToken[] to the parent document
	rapidjson::Value memberKey(VALIDATION_TOKENS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, validationTokens, doc.GetAllocator());
}

void addRefreshKmsUrlsSectionToJsonDoc(Reference<RESTKmsConnectorCtx> ctx,
                                       rapidjson::Document& doc,
                                       const bool refreshKmsUrls) {
	rapidjson::Value key(REFRESH_KMS_URLS_TAG, doc.GetAllocator());
	rapidjson::Value refreshUrls;
	refreshUrls.SetBool(refreshKmsUrls);

	// Append refreshKmsUrls object to the parent document
	doc.AddMember(key, refreshUrls, doc.GetAllocator());
}

void constructFetchEncryptKeyByKeyIdJsonReqStr(Reference<RESTKmsConnectorCtx> ctx,
                                               const KmsConnLookupEKsByKeyIdsReq& req,
                                               const bool refreshKmsUrls,
                                               std::string& outJsonStr) {
	// Acceptable request payload json format:
	//
	// request_json_payload {
	//   "cipherKeyDetails" = [
	//     {
	//        "cipherBaseKeyId" : <cipherKeyId>
	//        "encryptDomainId" : <domainId>
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "validationTokens" = [
	//     {
	//        "name" : <name>,
	//        "value": <value>
	//     },
	//     {
	//         ....
	//     }
	//   ]
	//   "refreshKmsUrls" = 1/0
	// }

	rapidjson::Document doc;
	doc.SetObject();

	// Append "keyIdDetails" as json array
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto& detail : req.encryptKeyIds) {
		rapidjson::Value keyIdDetail(rapidjson::kObjectType);

		// Add "baseCipherId"
		rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
		rapidjson::Value baseKeyId;
		baseKeyId.SetUint64(detail.first);
		keyIdDetail.AddMember(key, baseKeyId, doc.GetAllocator());

		// Add "encryptDomainId"
		key.SetString(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail.second);
		keyIdDetail.AddMember(key, domainId, doc.GetAllocator());

		// push above object to the array
		keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());

	// Append "validationTokens" as json array
	addValidationTokensSectionToJsonDoc(ctx, doc);

	// Append "refreshKmsUrls"
	addRefreshKmsUrlsSectionToJsonDoc(ctx, doc, refreshKmsUrls);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	outJsonStr.resize(sb.GetSize());
	outJsonStr.assign(sb.GetString(), sb.GetSize());
}

ACTOR Future<KmsConnLookupEKsByKeyIdsRep> fetchEncryptionKeyByKeyId(Reference<RESTKmsConnectorCtx> ctx,
                                                                    KmsConnLookupEKsByKeyIdsReq req) {
	state Reference<HTTP::Response> resp;
	state KmsConnLookupEKsByKeyIdsRep reply;
	state bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
	state std::string requestBody;

	constructFetchEncryptKeyByKeyIdJsonReqStr(ctx, req, refreshKmsUrls, requestBody);

	state int pass = 1;
	for (; pass <= 2; pass++) {
		state std::stack<std::shared_ptr<KmsUrlCtx>> tempStack;

		// Iterate over Kms URLs
		while (!ctx->kmsUrlHeap.empty()) {
			state std::shared_ptr<KmsUrlCtx> curUrl = ctx->kmsUrlHeap.top();
			ctx->kmsUrlHeap.pop();
			tempStack.push(curUrl);

			try {
				std::string kmsEncryptionFullUrl = getEncryptionFullUrl(curUrl->url);
				TraceEvent("FetchEncryptionKeyByKeyId_Start", ctx->uid)
				    .detail("KmsEncryptionFullUrl", kmsEncryptionFullUrl);
				Reference<HTTP::Response> _resp = wait(ctx->restClient.doPost(kmsEncryptionFullUrl, requestBody));
				resp = _resp;
				curUrl->nRequests++;

				try {
					parseKmsResponse(ctx, resp, reply.arena, reply.cipherKeyDetails);

					// Push urlCtx back on the ctx->urlHeap
					while (!tempStack.empty()) {
						ctx->kmsUrlHeap.emplace(tempStack.top());
						tempStack.pop();
					}

					TraceEvent("FetchEncryptionKeyByKeyId_Success", ctx->uid).detail("KmsUrl", curUrl->url);
					return reply;
				} catch (Error& e) {
					TraceEvent("FetchEncryptionKeyByKeyId_RespParseFailure").error(e);
					curUrl->nResponseParseFailures++;
					// attempt to fetch encryption details from next KmsUrl
				}
			} catch (Error& e) {
				TraceEvent("FetchEncryptionKeyByKeyId_Failed", ctx->uid).error(e);
				curUrl->nFailedResponses++;
				// attempt to fetch encryption details from next KmsUrl
			}
		}

		if (pass == 1) {
			// Re-discover KMS urls and re-attempt to fetch the encryption key details
			wait(discoverKmsUrls(ctx));
		}
	}

	// Failed to fetch encryption keys from remote Kms
	throw operation_failed();
}

void constructFetchEncryptKeyByDomainIdJsonReqStr(Reference<RESTKmsConnectorCtx> ctx,
                                                  const KmsConnLookupEKsByDomainIdsReq& req,
                                                  const bool refreshKmsUrls,
                                                  std::string& outJsonStr) {
	// Acceptable request payload json format:
	//
	// request_json_payload {
	//   "cipherKeyDetails" = [
	//     {
	//        "encryptDomainId" : <domainId>
	//     },
	//     {
	//         ....
	//	   }
	//   ],
	//   "validationTokens" = [
	//     {
	//        "name" : <name>,
	//        "value": <value>
	//     },
	//     {
	//         ....
	//     }
	//   ]
	//   "refreshKmsUrls" = 1/0
	// }

	rapidjson::Document doc;
	doc.SetObject();

	// Append "cipherKeyDetails" as json array
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (const auto& detail : req.encryptDomainIds) {
		rapidjson::Value keyIdDetail(rapidjson::kObjectType);

		rapidjson::Value key(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(detail);
		keyIdDetail.AddMember(key, domainId, doc.GetAllocator());

		// push above object to the array
		keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());

	// Append "validationTokens" as json array
	addValidationTokensSectionToJsonDoc(ctx, doc);

	// Append "refreshKmsUrls"
	addRefreshKmsUrlsSectionToJsonDoc(ctx, doc, refreshKmsUrls);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);
	outJsonStr.resize(sb.GetSize());
	outJsonStr.assign(sb.GetString(), sb.GetSize());
}

ACTOR Future<KmsConnLookupEKsByDomainIdsRep> fetchEncryptionKeyByDomainId(Reference<RESTKmsConnectorCtx> ctx,
                                                                          KmsConnLookupEKsByDomainIdsReq req) {
	state Reference<HTTP::Response> resp;
	state KmsConnLookupEKsByDomainIdsRep reply;
	state bool refreshKmsUrls = shouldRefreshKmsUrls(ctx);
	state std::string requestBody;

	constructFetchEncryptKeyByDomainIdJsonReqStr(ctx, req, refreshKmsUrls, requestBody);

	state int pass = 1;
	for (; pass <= 2; pass++) {
		state std::stack<std::shared_ptr<KmsUrlCtx>> tempStack;

		// Iterate over Kms URLs
		while (!ctx->kmsUrlHeap.empty()) {
			state std::shared_ptr<KmsUrlCtx> curUrl = ctx->kmsUrlHeap.top();
			ctx->kmsUrlHeap.pop();
			tempStack.push(curUrl);

			try {
				std::string kmsEncryptionFullUrl = getEncryptionFullUrl(curUrl->url);
				TraceEvent("FetchEncryptionKeyByDomainId_Start", ctx->uid)
				    .detail("KmsEncryptionFullUrl", kmsEncryptionFullUrl);
				Reference<HTTP::Response> _resp = wait(ctx->restClient.doPost(kmsEncryptionFullUrl, requestBody));
				resp = _resp;
				curUrl->nRequests++;

				try {
					parseKmsResponse(ctx, resp, reply.arena, reply.cipherKeyDetails);

					// Push urlCtx back on the ctx->urlHeap
					while (!tempStack.empty()) {
						ctx->kmsUrlHeap.emplace(tempStack.top());
						tempStack.pop();
					}

					TraceEvent("FetchEncryptionKeyByDomainId_Success", ctx->uid).detail("KmsUrl", curUrl->url);
					return reply;
				} catch (Error& e) {
					TraceEvent("FetchEncryptionKeyByDomainId_RespParseFailure").error(e);
					curUrl->nResponseParseFailures++;
					// attempt to fetch encryption details from next KmsUrl
				}
			} catch (Error& e) {
				TraceEvent("FetchEncryptionKeyByDomainId_Failed", ctx->uid).error(e);
				curUrl->nFailedResponses++;
				// attempt to fetch encryption details from next KmsUrl
			}
		}

		if (pass == 1) {
			// Re-discover KMS Urls and re-attempt to fetch the encryption key details
			wait(discoverKmsUrls(ctx));
		}
	}

	// Failed to fetch encryption keys from remote KmsUrls.
	throw operation_failed();
}

void procureValidationTokensFromFiles(Reference<RESTKmsConnectorCtx> ctx, StringRef details) {
	if (details.empty()) {
		TraceEvent("ValidationToken_EmptyFileDetails", ctx->uid).log();
		throw operation_failed();
	}

	TraceEvent("ValidationToken", ctx->uid).detail("DetailsStr", details.toString());

	std::unordered_map<std::string, std::string> filePathMap;
	while (!details.empty()) {
		StringRef name = details.eat(":");
		if (name.empty()) {
			break;
		}
		StringRef path = details.eat(",");
		if (path.empty()) {
			TraceEvent("ValidationToken_FileDetailsMalformed", ctx->uid).detail("FileDetails", details.toString());
			throw operation_failed();
		}

		filePathMap.emplace(name.toString(), path.toString());
		TraceEvent("ValidationToken", ctx->uid).detail("FName", name.toString()).detail("Path", path.toString());
	}

	// Clear existing validation tokens if any
	ctx->validationTokens.clear();

	uint64_t tokensPayloadSize = 0;
	for (const auto& item : filePathMap) {
		std::ifstream ifs;
		ifs.open(item.second, std::ios::in | std::ios::binary);
		if (!ifs.good()) {
			TraceEvent("ValidationToken_ReadFileFailure", ctx->uid).detail("FileName", item.second);
			throw io_error();
		}

		const std::size_t& tokenSize = std::filesystem::file_size(item.second);
		if (tokenSize > FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE) {
			TraceEvent("ValidationToken_FileTooLarge", ctx->uid)
			    .detail("FileName", item.first)
			    .detail("Size", tokenSize)
			    .detail("MaxAllowedSize", FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE);
			ifs.close();
			throw file_too_large();
		}

		tokensPayloadSize += tokenSize;
		if (tokensPayloadSize > FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE) {
			TraceEvent("ValidationToken_PayloadTooLarge", ctx->uid)
			    .detail("MaxAllowedSize", FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE);
			ifs.close();
			throw value_too_large();
		}

		// Populate validation token details
		ValidationToken token;
		token.name = item.first;
		token.value.resize(tokenSize);
		ifs.read((char*)token.value.data(), tokenSize);
		token.filePath = item.second;
		token.source = VALIDATION_TOKEN_SOURCE_FILE;
		ctx->validationTokens.emplace(item.first, token);

		// NOTE: avoid logging token-value to prevent token leaks in log files..
		TraceEvent("ValidationToken_ReadFile", ctx->uid)
		    .detail("TokenName", token.name)
		    .detail("TokenSize", token.value.size())
		    .detail("TokenPath", token.filePath.get())
		    .detail("TotalPayloadSize", tokensPayloadSize);

		ifs.close();
	}
}

void procureValidationTokens(Reference<RESTKmsConnectorCtx> ctx) {
	const std::string mode = FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MODE;

	if (mode.compare("file") == 0) {
		procureValidationTokensFromFiles(ctx, StringRef(FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_FILE_DETAILS));
	} else {
		throw not_implemented();
	}
}

ACTOR Future<Void> connectorCore_impl(KmsConnectorInterface interf) {
	state Reference<RESTKmsConnectorCtx> self = makeReference<RESTKmsConnectorCtx>(interf.id());

	TraceEvent("RESTKmsConnector_Init", self->uid).log();

	wait(discoverKmsUrls(self));
	procureValidationTokens(self);

	loop {
		choose {
			when(KmsConnLookupEKsByKeyIdsReq req = waitNext(interf.ekLookupByIds.getFuture())) {
				state KmsConnLookupEKsByKeyIdsReq byKeyIdReq = req;
				state KmsConnLookupEKsByKeyIdsRep byKeyIdResp;
				try {
					KmsConnLookupEKsByKeyIdsRep _rByKeyId = wait(fetchEncryptionKeyByKeyId(self, byKeyIdReq));
					byKeyIdResp = _rByKeyId;
					byKeyIdReq.reply.send(byKeyIdResp);
				} catch (Error& e) {
					TraceEvent("LookupEKsByKeyIds_Failed", self->uid).error(e);
					byKeyIdReq.reply.sendError(e);
				}
			}
			when(KmsConnLookupEKsByDomainIdsReq req = waitNext(interf.ekLookupByDomainIds.getFuture())) {
				state KmsConnLookupEKsByDomainIdsReq byDomainIdReq = req;
				state KmsConnLookupEKsByDomainIdsRep byDomainIdResp;
				try {
					KmsConnLookupEKsByDomainIdsRep _rByDomainId =
					    wait(fetchEncryptionKeyByDomainId(self, byDomainIdReq));
					byDomainIdResp = _rByDomainId;
					byDomainIdReq.reply.send(byDomainIdResp);
				} catch (Error& e) {
					TraceEvent("LookupEKsByDomainIds_Failed", self->uid).error(e);
					byDomainIdReq.reply.sendError(e);
				}
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
const std::string KMS_URL_NAME_TEST = "http://foo/bar";
const std::string BASE_CIPHER_KEY_TEST = "best-cipher-key-ever";

void testFileValidationTokens(Reference<RESTKmsConnectorCtx> ctx) {
	// Case-I: Empty validation token file details
	{
		try {
			procureValidationTokensFromFiles(ctx, StringRef());
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
	// Case-II: Malformed validation token file details
	{
		try {
			std::string malformed("abdc/tmp/foo");
			procureValidationTokensFromFiles(ctx, StringRef(malformed));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
	// Case-III: Validation file size too large
	{
		std::string name("foo");
		const int tokenLen = FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE + 1;
		uint8_t buff[tokenLen];
		generateRandomData(&buff[0], tokenLen);

		const char* fName = std::tmpnam(NULL);
		std::ofstream ofs;
		ofs.open(fName, std::ios::out | std::ios::binary);
		ofs.write((char*)&buff[0], tokenLen);
		ofs.close();

		std::string details;
		details.append(name).append(":").append(fName);

		try {
			procureValidationTokensFromFiles(ctx, StringRef(details));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_file_too_large);
		}

		remove(fName);
	}
	// Case-IV: Validation token payload size (aggregate) too large
	{
		const int tokenLen = FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE;
		const int nTokens = FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKENS_MAX_PAYLOAD_SIZE /
		                        FLOW_KNOBS->REST_KMS_CONNECTOR_VALIDATION_TOKEN_MAX_SIZE +
		                    2;
		uint8_t buff[tokenLen];
		generateRandomData(&buff[0], tokenLen);

		std::string details;
		std::unordered_set<std::string> fNames;
		for (int i = 0; i < nTokens; i++) {
			const char* fName = std::tmpnam(NULL);
			std::ofstream ofs;
			ofs.open(fName, std::ios::out | std::ios::binary);
			ofs.write((char*)&buff[0], tokenLen);
			ofs.close();

			details.append(std::to_string(i)).append(":").append(fName);
			if (i < nTokens)
				details.append(",");
			fNames.emplace(fName);
		}

		try {
			procureValidationTokensFromFiles(ctx, StringRef(details));
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_value_too_large);
		}

		for (const auto& name : fNames) {
			remove(name.c_str());
		}
	}
	// Case-V: Valid multiple validation token files (withing file size and total payload size limits)
	{
		const int numFiles = deterministicRandom()->randomInt(2, 5);
		std::unordered_map<std::string, std::string> tokenNameFilePathMap;
		std::unordered_map<std::string, std::string> tokenNameValueMap;
		std::string tokenDetailsStr;

		const int tokenLen = deterministicRandom()->randomInt(26, 75);
		uint8_t buf[tokenLen];
		for (int i = 1; i <= numFiles; i++) {
			const char* fName = std::tmpnam(NULL);
			generateRandomData(&buf[0], tokenLen);

			std::ofstream ofs;
			ofs.open(fName, std::ios::out | std::ios::binary);
			ofs.write((char*)&buf[0], tokenLen);
			ofs.close();

			std::string token((char*)&buf[0], tokenLen);
			tokenNameFilePathMap.emplace(std::to_string(i), fName);
			tokenNameValueMap.emplace(std::to_string(i), token);
			tokenDetailsStr.append(std::to_string(i)).append(":").append(fName);
			if (i < numFiles)
				tokenDetailsStr.append(",");

			//TraceEvent("ValidationTokenTest").detail("Name", std::to_string(i)).detail("Token", token);
		}

		procureValidationTokensFromFiles(ctx, StringRef(tokenDetailsStr));

		ASSERT_EQ(ctx->validationTokens.size(), tokenNameValueMap.size());
		for (const auto& token : ctx->validationTokens) {
			const auto& itr = tokenNameValueMap.find(token.first);

			ASSERT(itr != tokenNameValueMap.end());
			ASSERT_EQ(token.first.compare(itr->first), 0);
			ASSERT_EQ(token.second.source, VALIDATION_TOKEN_SOURCE_FILE);
			ASSERT_EQ(token.second.value.compare(itr->second), 0);
			ASSERT(token.second.filePath.present());
			ASSERT_EQ(token.second.filePath.get().compare(tokenNameFilePathMap[token.second.name]), 0);
		}

		for (const auto& token : tokenNameFilePathMap) {
			remove(token.first.c_str());
		}
	}
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

void constructFakeKmsResponse(const std::string& jsonReqStr, const bool baseCipherIdPresent, std::string& outJsonStr) {
	rapidjson::Document reqDoc;
	reqDoc.Parse(jsonReqStr.c_str());

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
		baseCipher.SetString(BASE_CIPHER_KEY_TEST.c_str(), resDoc.GetAllocator());
		keyDetail.AddMember(key, baseCipher, resDoc.GetAllocator());

		cipherKeyDetails.PushBack(keyDetail, resDoc.GetAllocator());
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, resDoc.GetAllocator());
	resDoc.AddMember(memberKey, cipherKeyDetails, resDoc.GetAllocator());

	ASSERT(reqDoc.HasMember(REFRESH_KMS_URLS_TAG));
	if (reqDoc[REFRESH_KMS_URLS_TAG].GetBool()) {
		rapidjson::Value kmsUrls(rapidjson::kArrayType);
		for (int i = 0; i < 3; i++) {
			rapidjson::Value url;
			url.SetString(KMS_URL_NAME_TEST.c_str(), resDoc.GetAllocator());
			kmsUrls.PushBack(url, resDoc.GetAllocator());
		}
		memberKey.SetString(KMS_URLS_TAG, resDoc.GetAllocator());
		resDoc.AddMember(memberKey, kmsUrls, resDoc.GetAllocator());
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	resDoc.Accept(writer);
	outJsonStr.resize(sb.GetSize());
	outJsonStr.assign(sb.GetString(), sb.GetSize());
}

void validateKmsUrls(Reference<RESTKmsConnectorCtx> ctx) {
	ASSERT_EQ(ctx->kmsUrlHeap.size(), 3);
	std::shared_ptr<KmsUrlCtx> urlCtx = ctx->kmsUrlHeap.top();
	ASSERT_EQ(urlCtx->url.compare(KMS_URL_NAME_TEST), 0);
}

void testFetchEncryptKeysByKeyIdsJsonReqStrOps(Reference<RESTKmsConnectorCtx> ctx, Arena arena) {
	KmsConnLookupEKsByKeyIdsReq req;
	std::unordered_map<EncryptCipherBaseKeyId, EncryptCipherDomainId> keyMap;
	const int nKeys = deterministicRandom()->randomInt(7, 8);
	for (int i = 1; i < nKeys; i++) {
		EncryptCipherDomainId domainId = getRandomDomainId();
		req.encryptKeyIds.push_back(std::make_pair(i, domainId));
		keyMap[i] = domainId;
	}

	bool refreshKmsUrls = deterministicRandom()->randomInt(0, 100) < 50;

	std::string jsonReqStr;
	constructFetchEncryptKeyByKeyIdJsonReqStr(ctx, req, refreshKmsUrls, jsonReqStr);
	//TraceEvent("FetchKeysByKeyIds", ctx->uid).setMaxFieldLength(10000).detail("JsonReqStr", jsonReqStr);
	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	constructFakeKmsResponse(jsonReqStr, true, httpResp->content);
	//TraceEvent("FetchKeysByKeyIds", ctx->uid).setMaxFieldLength(10000).detail("HttpRespStr", httpResp->content);

	std::vector<EncryptCipherKeyDetails> cipherDetails;
	parseKmsResponse(ctx, httpResp, arena, cipherDetails);
	ASSERT_EQ(cipherDetails.size(), keyMap.size());
	for (const auto& detail : cipherDetails) {
		ASSERT(keyMap.find(detail.encryptKeyId) != keyMap.end());
		ASSERT_EQ(keyMap[detail.encryptKeyId], detail.encryptDomainId);
		ASSERT_EQ(detail.encryptKey.compare(BASE_CIPHER_KEY_TEST), 0);
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testFetchEncryptKeysByDomainIdsJsonReqStrOps(Reference<RESTKmsConnectorCtx> ctx, Arena arena) {
	KmsConnLookupEKsByDomainIdsReq req;
	std::unordered_set<EncryptCipherDomainId> domainIdsSet;
	const int nKeys = deterministicRandom()->randomInt(7, 25);
	for (int i = 1; i < nKeys; i++) {
		domainIdsSet.emplace(getRandomDomainId());
	}
	req.encryptDomainIds.insert(req.encryptDomainIds.begin(), domainIdsSet.begin(), domainIdsSet.end());

	bool refreshKmsUrls = deterministicRandom()->randomInt(0, 100) < 50;

	std::string jsonReqStr;
	constructFetchEncryptKeyByDomainIdJsonReqStr(ctx, req, refreshKmsUrls, jsonReqStr);
	//TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("JsonReqStr", jsonReqStr);
	Reference<HTTP::Response> httpResp = makeReference<HTTP::Response>();
	httpResp->code = HTTP::HTTP_STATUS_CODE_OK;
	constructFakeKmsResponse(jsonReqStr, false, httpResp->content);
	TraceEvent("FetchKeysByDomainIds", ctx->uid).detail("HttpRespStr", httpResp->content);

	std::vector<EncryptCipherKeyDetails> cipherDetails;
	parseKmsResponse(ctx, httpResp, arena, cipherDetails);
	ASSERT_EQ(domainIdsSet.size(), cipherDetails.size());
	for (const auto& detail : cipherDetails) {
		ASSERT(domainIdsSet.find(detail.encryptDomainId) != domainIdsSet.end());
		ASSERT_EQ(detail.encryptKey.compare(BASE_CIPHER_KEY_TEST), 0);
	}
	if (refreshKmsUrls) {
		validateKmsUrls(ctx);
	}
}

void testParseKmsResponseFailure(Reference<RESTKmsConnectorCtx> ctx) {
	Arena arena;
	std::vector<EncryptCipherKeyDetails> cipherDetails;

	// Case-I: Missing CipherDetails tag
	{
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
		httpResp->content.resize(sb.GetSize());
		httpResp->content.assign(sb.GetString(), sb.GetSize());

		try {
			parseKmsResponse(ctx, httpResp, arena, cipherDetails);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
	// Case-II: CipherDetails malformed (not an array)
	{
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
		httpResp->content.resize(sb.GetSize());
		httpResp->content.assign(sb.GetString(), sb.GetSize());

		try {
			parseKmsResponse(ctx, httpResp, arena, cipherDetails);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
	// Case-III: Malformed CipherDetail object - missing encyrptDomainId/baseCipherId
	{
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
		httpResp->content.resize(sb.GetSize());
		httpResp->content.assign(sb.GetString(), sb.GetSize());

		try {
			parseKmsResponse(ctx, httpResp, arena, cipherDetails);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_operation_failed);
		}
	}
}

} // namespace

TEST_CASE("fdbserver/RESTKmsConnector") {
	Reference<RESTKmsConnectorCtx> ctx = makeReference<RESTKmsConnectorCtx>();
	Arena arena;

	testFileValidationTokens(ctx);
	testParseKmsResponseFailure(ctx);

	const int numIterations = deterministicRandom()->randomInt(512, 786);
	for (int i = 0; i < numIterations; i++) {
		testFetchEncryptKeysByKeyIdsJsonReqStrOps(ctx, arena);
		testFetchEncryptKeysByDomainIdsJsonReqStrOps(ctx, arena);
	}
	return Void();
}