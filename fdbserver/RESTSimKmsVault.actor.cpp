/*
 * RESTSimKmsVault.actor.cpp
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

#include "fdbclient/BlobMetadataUtils.h"
#include "fdbserver/RESTSimKmsVault.h"
#include "fdbclient/SimKmsVault.h"
#include "fdbrpc/HTTP.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RESTKmsConnectorUtils.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"

#include <cstring>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using DomIdVec = std::vector<EncryptCipherDomainId>;
using BaseCipherDomIdVec = std::vector<std::pair<EncryptCipherBaseKeyId, Optional<EncryptCipherDomainId>>>;

using namespace RESTKmsConnectorUtils;

namespace {
const std::string missingVersionMsg = "Missing version";
const std::string missingVersionCode = "1234";
const std::string invalidVersionMsg = "Invalid version";
const std::string invalidVersionCode = "5678";
const std::string missingTokensMsg = "Missing validation tokens";
const std::string missingTokenCode = "0123";

const std::string bgUrl = "file://simfdb/fdbblob/";

struct VaultResponse {
	bool failed;
	std::string buff;

	VaultResponse() : failed(false), buff("") {}
};

} // namespace

int64_t getRefreshInterval(const int64_t now, const int64_t defaultTtl) {
	if (BUGGIFY) {
		return now;
	}
	return (now + defaultTtl);
}

int64_t getExpireInterval(const int64_t refTS, const int64_t defaultTtl) {

	if (BUGGIFY) {
		return -1;
	}
	return (refTS + defaultTtl);
}

void validateHeaders(const HTTP::Headers& toCompare) {
	if (toCompare != RESTKmsConnectorUtils::getHTTPHeaders()) {
		throw rest_malformed_response();
	}
}

void addErrorToDoc(rapidjson::Document& doc, const ErrorDetail& details) {
	rapidjson::Value errorDetail(rapidjson::kObjectType);
	if (!details.errorMsg.empty()) {
		// Add "errorMsg"
		rapidjson::Value key(ERROR_MSG_TAG, doc.GetAllocator());
		rapidjson::Value errMsg;
		errMsg.SetString(details.errorMsg.data(), details.errorMsg.size(), doc.GetAllocator());
		errorDetail.AddMember(key, errMsg, doc.GetAllocator());
	}
	if (!details.errorCode.empty()) {
		// Add "value" - token value
		rapidjson::Value key(ERROR_CODE_TAG, doc.GetAllocator());
		rapidjson::Value errCode;
		errCode.SetString(details.errorCode.data(), details.errorCode.size(), doc.GetAllocator());
		errorDetail.AddMember(key, errCode, doc.GetAllocator());
	}

	// Append "error"
	rapidjson::Value key(ERROR_TAG, doc.GetAllocator());
	doc.AddMember(key, errorDetail, doc.GetAllocator());
}

void prepareErrorResponse(VaultResponse* response,
                          const ErrorDetail& errorDetail,
                          Optional<int> version = Optional<int>()) {
	rapidjson::Document doc;
	doc.SetObject();

	if (version.present()) {
		addVersionToDoc(doc, version.get());
	}

	addErrorToDoc(doc, errorDetail);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	ASSERT(!response->failed);
	response->failed = true;
	response->buff = std::string(sb.GetString(), sb.GetSize());
}

// Helper routine to extract 'version' from the input json document. If 'version' is missing or 'invalid', the routine
// is responsible to populate required error details to the 'response'
bool extractVersion(const rapidjson::Document& doc, VaultResponse* response, int* version) {
	// check version tag sanityrest_malformed_response
	if (!doc.HasMember(REQUEST_VERSION_TAG) || !doc[REQUEST_VERSION_TAG].IsInt()) {
		prepareErrorResponse(response, ErrorDetail(missingVersionCode, missingVersionMsg));
		CODE_PROBE(true, "RESTSimKmsVault missing version");
		return false;
	}

	*version = doc[REQUEST_VERSION_TAG].GetInt();
	if (*version < 0 || *version > SERVER_KNOBS->REST_KMS_MAX_CIPHER_REQUEST_VERSION) {
		prepareErrorResponse(response, ErrorDetail(invalidVersionCode, invalidVersionMsg));
		CODE_PROBE(true, "RESTSimKmsVault invalid version");
		return false;
	}

	return true;
}

// Helper routine to validate 'validation-token(s)' from the input json document. If tokens are missing the routine is
// responsible to populate appropriate error to the 'response'
bool checkValidationTokens(const rapidjson::Document& doc, const int version, VaultResponse* response) {
	ASSERT(!response->failed);
	if (!doc.HasMember(VALIDATION_TOKENS_TAG) || !doc[VALIDATION_TOKENS_TAG].IsArray()) {
		prepareErrorResponse(response, ErrorDetail(missingTokenCode, missingTokensMsg), version);
		CODE_PROBE(true, "RESTSimKmsVault missing validation tokens");
		return false;
	}
	ASSERT(!response->failed);
	return true;
}

void addCipherDetailToRespDoc(rapidjson::Document& doc,
                              rapidjson::Value& cipherDetails,
                              const Reference<SimKmsVaultKeyCtx>& keyCtx,
                              const Optional<EncryptCipherDomainId> domId) {
	rapidjson::Value cipherDetail(rapidjson::kObjectType);

	// Add 'base_cipher_id'
	rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
	rapidjson::Value baseKeyId;
	baseKeyId.SetUint64(keyCtx->id);
	cipherDetail.AddMember(key, baseKeyId, doc.GetAllocator());

	// Add 'encrypt_domain_id'
	if (domId.present()) {
		key.SetString(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domainId;
		domainId.SetInt64(domId.get());
		cipherDetail.AddMember(key, domainId, doc.GetAllocator());
	}

	// Add 'cipher'
	key.SetString(BASE_CIPHER_TAG, doc.GetAllocator());
	rapidjson::Value cipher;
	ASSERT_EQ(keyCtx->key.size(), keyCtx->keyLen);
	cipher.SetString(reinterpret_cast<const char*>(keyCtx->key.begin()), keyCtx->keyLen, doc.GetAllocator());
	cipherDetail.AddMember(key, cipher, doc.GetAllocator());

	// Add 'refreshAt'
	key.SetString(REFRESH_AFTER_SEC, doc.GetAllocator());
	const int64_t refreshAt = getRefreshInterval(now(), FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL);
	rapidjson::Value refreshInterval;
	refreshInterval.SetInt64(refreshAt);
	cipherDetail.AddMember(key, refreshInterval, doc.GetAllocator());

	// Add 'expireAt
	key.SetString(EXPIRE_AFTER_SEC, doc.GetAllocator());
	const int64_t expireAt = getExpireInterval(refreshAt, FLOW_KNOBS->ENCRYPT_KEY_REFRESH_INTERVAL);
	rapidjson::Value expireInterval;
	expireInterval.SetInt64(expireAt);
	cipherDetail.AddMember(key, expireInterval, doc.GetAllocator());

	// push above object to the array
	cipherDetails.PushBack(cipherDetail, doc.GetAllocator());
}

void addBlobMetadaToResDoc(rapidjson::Document& doc, rapidjson::Value& blobDetails, const EncryptCipherDomainId domId) {
	Standalone<BlobMetadataDetailsRef> detailsRef = SimKmsVault::getBlobMetadata(domId, bgUrl);
	rapidjson::Value blobDetail(rapidjson::kObjectType);

	rapidjson::Value key(BLOB_METADATA_DOMAIN_ID_TAG, doc.GetAllocator());
	rapidjson::Value domainId;
	domainId.SetInt64(domId);
	blobDetail.AddMember(key, domainId, doc.GetAllocator());

	rapidjson::Value locations(rapidjson::kArrayType);
	for (const auto& loc : detailsRef.locations) {
		rapidjson::Value location(rapidjson::kObjectType);

		// set location-id
		key.SetString(BLOB_METADATA_LOCATION_ID_TAG, doc.GetAllocator());
		rapidjson::Value id;
		id.SetInt64(loc.locationId);
		location.AddMember(key, id, doc.GetAllocator());

		// set location-path
		key.SetString(BLOB_METADATA_LOCATION_PATH_TAG, doc.GetAllocator());
		rapidjson::Value path;
		path.SetString(reinterpret_cast<const char*>(loc.path.begin()), loc.path.size(), doc.GetAllocator());
		location.AddMember(key, path, doc.GetAllocator());

		locations.PushBack(location, doc.GetAllocator());
	}
	key.SetString(BLOB_METADATA_LOCATIONS_TAG, doc.GetAllocator());
	blobDetail.AddMember(key, locations, doc.GetAllocator());

	blobDetails.PushBack(blobDetail, doc.GetAllocator());
}

void addKmsUrlsToDoc(rapidjson::Document& doc) {
	rapidjson::Value kmsUrls(rapidjson::kArrayType);
	// FIXME: fetch latest KMS URLs && append to the doc
	rapidjson::Value memberKey(KMS_URLS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, kmsUrls, doc.GetAllocator());
}

// TODO: inject faults
VaultResponse handleFetchKeysByDomainIds(const std::string& content) {
	VaultResponse response;
	rapidjson::Document doc;

	doc.Parse(content.data());

	int version;
	if (!extractVersion(doc, &response, &version)) {
		// Return HTTP::HTTP_STATUS_CODE_OK with appropriate 'error' details
		ASSERT(response.failed);
		return response;
	}
	ASSERT(!response.failed);

	if (!checkValidationTokens(doc, version, &response)) {
		// Return HTTP::HTTP_STATUS_CODE_OK with appropriate 'error' details
		ASSERT(response.failed);
		return response;
	}
	ASSERT(!response.failed);

	rapidjson::Document result;
	result.SetObject();

	// Append 'request version'
	addVersionToDoc(result, version);

	// Append 'cipher_key_details' as json array
	rapidjson::Value cipherDetails(rapidjson::kArrayType);
	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		EncryptCipherDomainId domainId = cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64();
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(domainId);
		ASSERT(keyCtx.isValid());
		addCipherDetailToRespDoc(result, cipherDetails, keyCtx, domainId);
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, result.GetAllocator());
	result.AddMember(memberKey, cipherDetails, result.GetAllocator());

	if (doc.HasMember(KMS_URLS_TAG) && doc[KMS_URLS_TAG].GetBool()) {
		addKmsUrlsToDoc(result);
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	result.Accept(writer);

	ASSERT(!response.failed);
	response.buff = std::string(sb.GetString(), sb.GetSize());
	//TraceEvent(SevDebug, "FetchByDomainIdsResponseStr").detail("Str", response->buff);
	return response;
}

VaultResponse handleFetchKeysByKeyIds(const std::string& content) {
	VaultResponse response;
	rapidjson::Document doc;

	doc.Parse(content.data());

	int version;

	if (!extractVersion(doc, &response, &version)) {
		// Return HTTP::HTTP_STATUS_CODE_OK with appropriate 'error' details
		ASSERT(response.failed);
		return response;
	}
	ASSERT(!response.failed);

	if (!checkValidationTokens(doc, version, &response)) {
		// Return HTTP::HTTP_STATUS_CODE_OK with appropriate 'error' details
		ASSERT(response.failed);
		return response;
	}
	ASSERT(!response.failed);

	rapidjson::Document result;
	result.SetObject();

	// Append 'request version'
	addVersionToDoc(result, version);

	// Append 'cipher_key_details' as json array
	rapidjson::Value cipherDetails(rapidjson::kArrayType);
	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		Optional<EncryptCipherDomainId> domainId;
		if (cipherDetail.HasMember(ENCRYPT_DOMAIN_ID_TAG) && cipherDetail[ENCRYPT_DOMAIN_ID_TAG].IsInt64()) {
			domainId = cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64();
		}
		EncryptCipherBaseKeyId baseCipherId = cipherDetail[BASE_CIPHER_ID_TAG].GetUint64();
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByBaseCipherId(baseCipherId);
		ASSERT(keyCtx.isValid());
		addCipherDetailToRespDoc(result, cipherDetails, keyCtx, domainId);
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, result.GetAllocator());
	result.AddMember(memberKey, cipherDetails, result.GetAllocator());

	if (doc.HasMember(KMS_URLS_TAG) && doc[KMS_URLS_TAG].GetBool()) {
		addKmsUrlsToDoc(result);
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	result.Accept(writer);

	ASSERT(!response.failed);
	response.buff = std::string(sb.GetString(), sb.GetSize());
	//TraceEvent(SevDebug, "FetchByKeyIdsResponseStr").detail("Str", response.buff);
	return response;
}

VaultResponse handleFetchBlobMetada(const std::string& content) {
	VaultResponse response;
	rapidjson::Document doc;

	doc.Parse(content.data());

	int version;

	if (!extractVersion(doc, &response, &version)) {
		// Return HTTP::HTTP_STATUS_CODE_OK with appropriate 'error' details
		ASSERT(response.failed);
		return response;
	}
	ASSERT(!response.failed);

	if (!checkValidationTokens(doc, version, &response)) {
		// Return HTTP::HTTP_STATUS_CODE_OK with appropriate 'error' details
		ASSERT(response.failed);
		return response;
	}
	ASSERT(!response.failed);

	rapidjson::Document result;
	result.SetObject();

	// Append 'request version'
	addVersionToDoc(result, version);

	// Append 'blob_metadata_details' as json array
	rapidjson::Value blobDetails(rapidjson::kArrayType);
	for (const auto& blobDetail : doc[BLOB_METADATA_DETAILS_TAG].GetArray()) {
		EncryptCipherDomainId domainId = blobDetail[BLOB_METADATA_DOMAIN_ID_TAG].GetInt64();
		addBlobMetadaToResDoc(doc, blobDetails, domainId);
	}
	rapidjson::Value memberKey(BLOB_METADATA_DETAILS_TAG, result.GetAllocator());
	result.AddMember(memberKey, blobDetails, result.GetAllocator());

	if (doc.HasMember(KMS_URLS_TAG) && doc[KMS_URLS_TAG].GetBool()) {
		addKmsUrlsToDoc(result);
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	result.Accept(writer);

	ASSERT(!response.failed);
	response.buff = std::string(sb.GetString(), sb.GetSize());
	//TraceEvent(SevDebug, "FetchBlobMetadataResponeStr").detail("Str", response.buff);
	return response;
}

ACTOR Future<Void> simKmsVaultRequestHandler(Reference<HTTP::IncomingRequest> request,
                                             Reference<HTTP::OutgoingResponse> response) {
	wait(delay(0));
	ASSERT_EQ(request->verb, HTTP::HTTP_VERB_POST);

	validateHeaders(request->data.headers);

	state VaultResponse vaultResponse;
	if (request->resource.compare(REST_SIM_KMS_VAULT_GET_ENCRYPTION_KEYS_BY_KEY_IDS_RESOURCE) == 0) {
		vaultResponse = handleFetchKeysByKeyIds(request->data.content);
	} else if (request->resource.compare(REST_SIM_KMS_VAULT_GET_ENCRYPTION_KEYS_BY_DOMAIN_IDS_RESOURCE) == 0) {
		vaultResponse = handleFetchKeysByDomainIds(request->data.content);
	} else if (request->resource.compare(REST_SIM_KMS_VAULT_GET_BLOB_METADATA_RESOURCE) == 0) {
		vaultResponse = handleFetchBlobMetada(request->data.content);
	} else {
		TraceEvent("UnexpectedResource").detail("Resource", request->resource);
		throw http_bad_response();
	}

	response->code = HTTP::HTTP_STATUS_CODE_OK;
	response->data.headers = request->data.headers;
	PacketWriter pw(response->data.content->getWriteBuffer(vaultResponse.buff.size()), nullptr, Unversioned());
	pw.serializeBytes(vaultResponse.buff.data(), vaultResponse.buff.size());
	response->data.contentLen = vaultResponse.buff.size();

	return Void();
}

Future<Void> RESTSimKmsVaultRequestHandler::handleRequest(Reference<HTTP::IncomingRequest> request,
                                                          Reference<HTTP::OutgoingResponse> response) {
	return simKmsVaultRequestHandler(request, response);
}

// Only used to link unit tests
void forceLinkRESTSimKmsVaultTest() {}

namespace {

enum class FaultType { NONE = 1, MISSING_VERSION = 2, INVALID_VERSION = 3, MISSING_VALIDATION_TOKEN = 4 };

void addFakeValidationTokens(rapidjson::Document& doc) {
	ValidationTokenMap tokenMap;
	tokenMap.emplace("foo", ValidationTokenCtx("bar", ValidationTokenSource::VALIDATION_TOKEN_SOURCE_FILE));

	addValidationTokensSectionToJsonDoc(doc, tokenMap);
}

void constructDomainIds(EncryptCipherDomainIdVec& domIds) {
	domIds.push_back(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID);
	domIds.push_back(FDB_DEFAULT_ENCRYPT_DOMAIN_ID);
	domIds.push_back(ENCRYPT_HEADER_DOMAIN_ID);

	int idx = deterministicRandom()->randomInt(512, 786);
	int count = deterministicRandom()->randomInt(5, 100);
	while (count--) {
		domIds.push_back(idx++);
	}
}

std::string getFakeDomainIdsRequestContent(EncryptCipherDomainIdVec& domIds,
                                           const char* rootTag,
                                           const char* elementTag,
                                           FaultType fault = FaultType::NONE) {
	rapidjson::Document doc;
	doc.SetObject();

	if (fault == FaultType::INVALID_VERSION) {
		addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_MAX_CIPHER_REQUEST_VERSION + 1);
	} else if (fault == FaultType::MISSING_VERSION) {
		// Skip adding the version
	} else {
		addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_MAX_CIPHER_REQUEST_VERSION);
	}

	if (fault != FaultType::MISSING_VALIDATION_TOKEN) {
		addFakeValidationTokens(doc);
	}

	constructDomainIds(domIds);
	addLatestDomainDetailsToDoc(doc, rootTag, elementTag, domIds);

	addRefreshKmsUrlsSectionToJsonDoc(doc, deterministicRandom()->coinflip());

	if (deterministicRandom()->coinflip()) {
		addDebugUidSectionToJsonDoc(doc, deterministicRandom()->randomUniqueID());
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	std::string resp(sb.GetString(), sb.GetSize());
	/*TraceEvent(SevDebug, "FakeDomainIdsRequest")
	    .detail("Str", resp)
	    .detail("RootTag", rootTag)
	    .detail("ElementTag", elementTag);*/
	return resp;
}

std::string getFakeEncryptDomainIdsRequestContent(EncryptCipherDomainIdVec& domIds, FaultType fault = FaultType::NONE) {
	return getFakeDomainIdsRequestContent(domIds, CIPHER_KEY_DETAILS_TAG, ENCRYPT_DOMAIN_ID_TAG, fault);
}

std::string getFakeBlobDomainIdsRequestContent(EncryptCipherDomainIdVec& domIds, FaultType fault = FaultType::NONE) {
	return getFakeDomainIdsRequestContent(domIds, BLOB_METADATA_DETAILS_TAG, BLOB_METADATA_DOMAIN_ID_TAG, fault);
}

std::string getFakeBaseCipherIdsRequestContent(EncryptCipherDomainIdVec& domIds, FaultType fault = FaultType::NONE) {
	rapidjson::Document doc;
	doc.SetObject();

	if (fault != FaultType::MISSING_VERSION) {
		if (fault == FaultType::INVALID_VERSION) {
			addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_MAX_CIPHER_REQUEST_VERSION + 1);
		}
		addVersionToDoc(doc, SERVER_KNOBS->REST_KMS_MAX_CIPHER_REQUEST_VERSION);
	}

	if (fault != FaultType::MISSING_VALIDATION_TOKEN) {
		addFakeValidationTokens(doc);
	}

	constructDomainIds(domIds);
	rapidjson::Value keyIdDetails(rapidjson::kArrayType);
	for (auto domId : domIds) {
		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(domId);
		ASSERT(keyCtx.isValid());
		addBaseCipherIdDomIdToDoc(doc, keyIdDetails, keyCtx->id, domId);
	}
	rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, keyIdDetails, doc.GetAllocator());

	addRefreshKmsUrlsSectionToJsonDoc(doc, deterministicRandom()->coinflip());

	if (deterministicRandom()->coinflip()) {
		addDebugUidSectionToJsonDoc(doc, deterministicRandom()->randomUniqueID());
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	std::string resp(sb.GetString(), sb.GetSize());
	//TraceEvent(SevDebug, "FakeKeyIdsRequest").detail("Str", resp);
	return resp;
}

Optional<ErrorDetail> getErrorDetail(const std::string& buff) {
	rapidjson::Document doc;
	doc.Parse(buff.data());
	return RESTKmsConnectorUtils::getError(doc);
}

void validateEncryptLookup(const VaultResponse& response, const EncryptCipherDomainIdVec& domIds) {
	ASSERT(!response.failed);

	//TraceEvent(SevDebug, "VaultEncryptResponse").detail("Str", response.buff);

	rapidjson::Document doc;
	doc.Parse(response.buff.data());

	ASSERT(doc.HasMember(CIPHER_KEY_DETAILS_TAG) && doc[CIPHER_KEY_DETAILS_TAG].IsArray());

	std::unordered_set<EncryptCipherDomainId> domIdSet(domIds.begin(), domIds.end());
	int count = 0;
	for (const auto& cipherDetail : doc[CIPHER_KEY_DETAILS_TAG].GetArray()) {
		EncryptCipherDomainId domainId = cipherDetail[ENCRYPT_DOMAIN_ID_TAG].GetInt64();
		EncryptCipherBaseKeyId baseCipherId = cipherDetail[BASE_CIPHER_ID_TAG].GetUint64();
		const int cipherKeyLen = cipherDetail[BASE_CIPHER_TAG].GetStringLength();
		Standalone<StringRef> cipherKeyRef = makeString(cipherKeyLen);
		memcpy(mutateString(cipherKeyRef), cipherDetail[BASE_CIPHER_TAG].GetString(), cipherKeyLen);

		ASSERT(domIdSet.find(domainId) != domIdSet.end());

		Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByDomainId(domainId);
		ASSERT_EQ(keyCtx->id, baseCipherId);
		ASSERT_EQ(keyCtx->key.compare(cipherKeyRef), 0);
		const int64_t refreshAfterSec = cipherDetail[REFRESH_AFTER_SEC].GetInt64();
		const int64_t expireAfterSec = cipherDetail[EXPIRE_AFTER_SEC].GetInt64();
		ASSERT(refreshAfterSec <= expireAfterSec || expireAfterSec == -1);
		count++;
	}
	ASSERT_EQ(count, domIds.size());
}

void validateBlobLookup(const VaultResponse& response, const EncryptCipherDomainIdVec& domIds) {
	ASSERT(!response.failed);

	//TraceEvent(SevDebug, "VaultBlobResponse").detail("Str", response.buff);

	rapidjson::Document doc;
	doc.Parse(response.buff.data());

	ASSERT(doc.HasMember(BLOB_METADATA_DETAILS_TAG) && doc[BLOB_METADATA_DETAILS_TAG].IsArray());

	std::unordered_set<EncryptCipherDomainId> domIdSet(domIds.begin(), domIds.end());
	int count = 0;
	for (const auto& blobDetail : doc[BLOB_METADATA_DETAILS_TAG].GetArray()) {
		EncryptCipherDomainId domainId = blobDetail[BLOB_METADATA_DOMAIN_ID_TAG].GetInt64();
		Standalone<BlobMetadataDetailsRef> details = SimKmsVault::getBlobMetadata(domainId, bgUrl);

		std::unordered_map<BlobMetadataLocationId, Standalone<StringRef>> locMap;
		for (const auto& loc : details.locations) {
			locMap[loc.locationId] = loc.path;
		}
		for (const auto& location : blobDetail[BLOB_METADATA_LOCATIONS_TAG].GetArray()) {
			BlobMetadataLocationId locationId = location[BLOB_METADATA_LOCATION_ID_TAG].GetInt64();
			Standalone<StringRef> path = makeString(location[BLOB_METADATA_LOCATION_PATH_TAG].GetStringLength());
			memcpy(mutateString(path),
			       location[BLOB_METADATA_LOCATION_PATH_TAG].GetString(),
			       location[BLOB_METADATA_LOCATION_PATH_TAG].GetStringLength());
			auto it = locMap.find(locationId);
			ASSERT(it != locMap.end());
			ASSERT_EQ(it->second.compare(path), 0);
		}
		const int64_t refreshAfterSec = blobDetail[REFRESH_AFTER_SEC].GetInt64();
		const int64_t expireAfterSec = blobDetail[EXPIRE_AFTER_SEC].GetInt64();
		ASSERT(refreshAfterSec <= expireAfterSec || expireAfterSec == -1);
		count++;
	}
	ASSERT_EQ(count, domIds.size());
}

} // namespace

TEST_CASE("/restSimKmsVault/invalidResource") {
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/whatever";
	request->data.headers = RESTKmsConnectorUtils::getHTTPHeaders();
	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_http_bad_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/invalidHeader") {
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/whatever";
	request->data.headers = RESTKmsConnectorUtils::getHTTPHeaders();
	request->data.headers["Foo"] = "Bar";
	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds/missingVersion") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeEncryptDomainIdsRequestContent(domIds, FaultType::MISSING_VERSION);
	VaultResponse response = handleFetchKeysByDomainIds(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(missingVersionCode, missingVersionMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds/invalidVersion") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeEncryptDomainIdsRequestContent(domIds, FaultType::INVALID_VERSION);
	VaultResponse response = handleFetchKeysByDomainIds(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(invalidVersionCode, invalidVersionMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds/missingValidationTokens") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeEncryptDomainIdsRequestContent(domIds, FaultType::MISSING_VALIDATION_TOKEN);

	VaultResponse response = handleFetchKeysByDomainIds(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(missingTokenCode, missingTokensMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeEncryptDomainIdsRequestContent(domIds);

	VaultResponse response = handleFetchKeysByDomainIds(requestContent);
	validateEncryptLookup(response, domIds);
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/missingVersion") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBaseCipherIdsRequestContent(domIds, FaultType::MISSING_VERSION);

	VaultResponse response = handleFetchKeysByKeyIds(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(missingVersionCode, missingVersionMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/invalidVersion") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBaseCipherIdsRequestContent(domIds, FaultType::INVALID_VERSION);

	VaultResponse response = handleFetchKeysByKeyIds(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(invalidVersionCode, invalidVersionMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/missingValidationTokens") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBaseCipherIdsRequestContent(domIds, FaultType::MISSING_VALIDATION_TOKEN);

	VaultResponse response = handleFetchKeysByKeyIds(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(missingTokenCode, missingTokensMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/missingValidationTokensBlob") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBlobDomainIdsRequestContent(domIds, FaultType::MISSING_VALIDATION_TOKEN);

	VaultResponse response = handleFetchBlobMetada(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(missingTokenCode, missingTokensMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBaseCipherIdsRequestContent(domIds);

	VaultResponse response = handleFetchKeysByKeyIds(requestContent);
	validateEncryptLookup(response, domIds);
	return Void();
}

TEST_CASE("/restSimKmsVault/GetBlobMetadata/missingVersion") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBlobDomainIdsRequestContent(domIds, FaultType::MISSING_VERSION);

	VaultResponse response = handleFetchBlobMetada(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(missingVersionCode, missingVersionMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetBlobMetadata/invalidVersion") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBlobDomainIdsRequestContent(domIds, FaultType::INVALID_VERSION);

	VaultResponse response = handleFetchBlobMetada(requestContent);
	ASSERT(response.failed);
	Optional<ErrorDetail> detail = getErrorDetail(response.buff);
	ASSERT(detail.present());
	ASSERT(detail->isEqual(ErrorDetail(invalidVersionCode, invalidVersionMsg)));

	return Void();
}

TEST_CASE("/restSimKmsVault/GetBlobMetadata/foo") {
	EncryptCipherDomainIdVec domIds;
	std::string requestContent = getFakeBlobDomainIdsRequestContent(domIds);

	VaultResponse response = handleFetchBlobMetada(requestContent);
	validateBlobLookup(response, domIds);
	return Void();
}