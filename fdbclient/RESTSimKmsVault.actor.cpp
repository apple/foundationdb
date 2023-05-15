/*
 * RESTSimKmsVault.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/RESTSimKmsVault.h"
#include "fdbclient/RESTKmsConnectorUtils.h"
#include "fdbclient/SimKmsVault.h"
#include "fdbrpc/HTTP.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"

#include <cstring>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using DomIdVec = std::vector<EncryptCipherDomainId>;
using BaseCipherDomIdVec = std::vector<std::pair<EncryptCipherBaseKeyId, Optional<EncryptCipherDomainId>>>;

using namespace RESTKmsConnectorUtils;

namespace {
struct ErrorDetail {
	std::string errorCode;
	std::string errorMsg;

	ErrorDetail(const std::string& code, const std::string& msg) : errorCode(code), errorMsg(msg) {}
};
} // namespace

Optional<int64_t> getRefreshInterval(const int64_t now, const int64_t defaultTtl) {
	if (BUGGIFY) {
		return Optional<int64_t>(now);
	}
	return Optional<int64_t>(now + defaultTtl);
}

Optional<int64_t> getExpireInterval(Optional<int64_t> refTS, const int64_t defaultTtl) {
	ASSERT(refTS.present());

	if (BUGGIFY) {
		return Optional<int64_t>(-1);
	}
	return (refTS.get() + defaultTtl);
}

void addErrorToDoc(rapidjson::Document& doc, std::vector<ErrorDetail>& details) {
	// Add error details
	rapidjson::Value errorDetails(rapidjson::kArrayType);

	for (const auto& detail : details) {
		rapidjson::Value errorDetail(rapidjson::kObjectType);

		if (!detail.errorMsg.empty()) {
			// Add "errorMsg"
			rapidjson::Value key(ERROR_MSG_TAG, doc.GetAllocator());
			rapidjson::Value errMsg;
			errMsg.SetString(detail.errorMsg.data(), detail.errorMsg.size(), doc.GetAllocator());
			errorDetail.AddMember(key, errMsg, doc.GetAllocator());
		}
		if (!detail.errorCode.empty()) {
			// Add "value" - token value
			rapidjson::Value key(ERROR_CODE_TAG, doc.GetAllocator());
			rapidjson::Value errCode;
			errCode.SetString(detail.errorCode.data(), detail.errorCode.size(), doc.GetAllocator());
			errorDetail.AddMember(key, errCode, doc.GetAllocator());
		}
		errorDetails.PushBack(errorDetail, doc.GetAllocator());
	}

	// Append "error"
	rapidjson::Value key(ERROR_TAG, doc.GetAllocator());
	doc.AddMember(key, errorDetails, doc.GetAllocator());
}

void prepareErrorResponse(Reference<HTTP::IncomingRequest> request,
                          Reference<HTTP::OutgoingResponse> response,
                          const std::string& errMsg,
                          const std::string& errCode,
                          Optional<int> version = Optional<int>()) {
	response->code = HTTP::HTTP_STATUS_CODE_OK;
	response->data.headers = request->data.headers;

	rapidjson::Document doc;
	doc.SetObject();

	if (version.present()) {
		addVersionToDoc(doc, version.get());
	}

	std::vector<ErrorDetail> errors;
	errors.emplace_back(errCode, errMsg);
	addErrorToDoc(doc, errors);

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	std::string str(sb.GetString(), sb.GetSize());
	response->data.contentLen = str.size();
	PacketWriter pw(response->data.content->getWriteBuffer(str.size()), nullptr, Unversioned());
	pw.serializeBytes(str);
}

int extractVersion(Reference<HTTP::IncomingRequest> request,
                   Reference<HTTP::OutgoingResponse> response,
                   const rapidjson::Document& doc) {
	// check version tag sanityrest_malformed_response
	if (!doc.HasMember(REQUEST_VERSION_TAG) || !doc[REQUEST_VERSION_TAG].IsInt()) {
		prepareErrorResponse(request, response, "Missing Version", "1234");
		CODE_PROBE(true, "RESTSimKmsVault missing version");
		throw rest_malformed_response();
	}

	const int version = doc[REQUEST_VERSION_TAG].GetInt();
	if (version < 0 || version > 1) {
		prepareErrorResponse(request, response, "Invalid version", "1234");
		CODE_PROBE(true, "RESTSimKmsVault invalid version");
		throw rest_malformed_response();
	}

	return doc[REQUEST_VERSION_TAG].GetInt();
}

void checkValidationTokens(Reference<HTTP::IncomingRequest> request,
                           Reference<HTTP::OutgoingResponse> response,
                           const int version,
                           const rapidjson::Document& doc) {
	if (!doc.HasMember(VALIDATION_TOKENS_TAG) || !doc[VALIDATION_TOKENS_TAG].IsArray()) {
		prepareErrorResponse(request, response, "Missing validation tokens", "4567", version);
		CODE_PROBE(true, "RESTSimKmsVault missing validation tokens");
		throw rest_malformed_response();
	}
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

	// push above object to the array
	cipherDetails.PushBack(cipherDetail, doc.GetAllocator());
}

void addKmsUrlsToDoc(rapidjson::Document& doc) {
	rapidjson::Value kmsUrls(rapidjson::kArrayType);
	// FIXME: fetch latest KMS URLs && append to the doc
	rapidjson::Value memberKey(KMS_URLS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, kmsUrls, doc.GetAllocator());
}

// TODO: inject faults
void handleFetchKeysByDomainIds(Reference<HTTP::IncomingRequest> request, Reference<HTTP::OutgoingResponse> response) {
	rapidjson::Document doc;
	doc.Parse(request->data.content.data());

	try {
		const int version = extractVersion(request, response, doc);

		checkValidationTokens(request, response, version, doc);

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
			addCipherDetailToRespDoc(doc, cipherDetails, keyCtx, domainId);
		}
		rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
		result.AddMember(memberKey, cipherDetails, doc.GetAllocator());

		if (doc.HasMember(KMS_URLS_TAG) && doc[KMS_URLS_TAG].GetBool()) {
			addKmsUrlsToDoc(result);
		}

		// Serialize json to string
		rapidjson::StringBuffer sb;
		rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
		doc.Accept(writer);

		response->code = HTTP::HTTP_STATUS_CODE_OK;
		response->data.headers = request->data.headers;
		PacketWriter pw(response->data.content->getWriteBuffer(sb.GetSize()), nullptr, Unversioned());
		pw.serializeBytes(sb.GetString(), sb.GetSize());
		response->data.contentLen = sb.GetSize();
	} catch (Error& e) {
		TraceEvent("FetchByDomainIdsFailed").error(e);
		throw;
	}
}

void handleFetchKeysByKeyIds(Reference<HTTP::IncomingRequest> request, Reference<HTTP::OutgoingResponse> response) {
	rapidjson::Document doc;
	doc.Parse(request->data.content.data());

	try {
		const int version = extractVersion(request, response, doc);

		checkValidationTokens(request, response, version, doc);

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
			addCipherDetailToRespDoc(doc, cipherDetails, keyCtx, domainId);
		}
		rapidjson::Value memberKey(CIPHER_KEY_DETAILS_TAG, doc.GetAllocator());
		result.AddMember(memberKey, cipherDetails, doc.GetAllocator());

		if (doc.HasMember(KMS_URLS_TAG) && doc[KMS_URLS_TAG].GetBool()) {
			addKmsUrlsToDoc(result);
		}

		// Serialize json to string
		rapidjson::StringBuffer sb;
		rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
		doc.Accept(writer);

		response->code = HTTP::HTTP_STATUS_CODE_OK;
		response->data.headers = request->data.headers;
		PacketWriter pw(response->data.content->getWriteBuffer(sb.GetSize()), nullptr, Unversioned());
		pw.serializeBytes(sb.GetString(), sb.GetSize());
		response->data.contentLen = sb.GetSize();
	} catch (Error& e) {
		TraceEvent("FetchByKeyIdsFailed").error(e);
		throw;
	}
}

ACTOR Future<Void> simKmsVaultRequestHandler(Reference<HTTP::IncomingRequest> request,
                                             Reference<HTTP::OutgoingResponse> response) {
	wait(delay(0));
	ASSERT_EQ(request->verb, HTTP::HTTP_VERB_POST);

	if (request->resource.compare("/get-encryption-keys-by-key-ids") == 0) {
		handleFetchKeysByKeyIds(request, response);
	} else if (request->resource.compare("/get-encryption-keys-by-domain-ids") == 0) {
		handleFetchKeysByDomainIds(request, response);
	} else {
		throw http_bad_response();
	}

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

EncryptCipherDomainIdVec getDomainIds() {
	EncryptCipherDomainIdVec domIds;
	domIds.push_back(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID);
	domIds.push_back(FDB_DEFAULT_ENCRYPT_DOMAIN_ID);
	domIds.push_back(ENCRYPT_HEADER_DOMAIN_ID);

	int idx = deterministicRandom()->randomInt(512, 786);
	int count = deterministicRandom()->randomInt(5, 100);
	while (count--) {
		domIds.push_back(idx++);
	}
	return domIds;
}

EncryptCipherDomainIdVec getFakeDomainIdsRequest(Reference<HTTP::IncomingRequest> request, FaultType fault) {
	rapidjson::Document doc;
	doc.SetObject();

	if (fault != FaultType::MISSING_VERSION) {
		if (fault == FaultType::INVALID_VERSION) {
			addVersionToDoc(doc, 100);
		}
		addVersionToDoc(doc, 1);
	}
	if (fault != FaultType::MISSING_VALIDATION_TOKEN) {
		addFakeValidationTokens(doc);
	}

	EncryptCipherDomainIdVec domIds = getDomainIds();
	addLatestDomainDetailsToDoc(doc, CIPHER_KEY_DETAILS_TAG, ENCRYPT_DOMAIN_ID_TAG, domIds);

	addRefreshKmsUrlsSectionToJsonDoc(doc, deterministicRandom()->coinflip());

	if (deterministicRandom()->coinflip()) {
		addDebugUidSectionToJsonDoc(doc, deterministicRandom()->randomUniqueID());
	}

	// Serialize json to string
	rapidjson::StringBuffer sb;
	rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
	doc.Accept(writer);

	request->data.content = std::string(sb.GetString(), sb.GetSize());
	request->data.contentLen = sb.GetSize();
	request->data.headers["fdb"] = "awesome";
	return domIds;
}

} // namespace

TEST_CASE("/restSimKmsVault/invalidResource") {
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/whatever";
	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_http_bad_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds/missingVersion") {
	state UnsentPacketQueue content;
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/get-encryption-keys-by-domain-ids";

	EncryptCipherDomainIdVec domIds = getFakeDomainIdsRequest(request, FaultType::MISSING_VERSION);

	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds/invalidVersion") {
	state UnsentPacketQueue content;
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/get-encryption-keys-by-domain-ids";
	EncryptCipherDomainIdVec domIds = getFakeDomainIdsRequest(request, FaultType::INVALID_VERSION);

	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByDomainIds/missingValidationTokens") {
	state UnsentPacketQueue content;
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/get-encryption-keys-by-domain-ids";
	EncryptCipherDomainIdVec domIds = getFakeDomainIdsRequest(request, FaultType::MISSING_VALIDATION_TOKEN);

	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/missingVersion") {
	state UnsentPacketQueue content;
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/get-encryption-keys-by-key-ids";
	EncryptCipherDomainIdVec domIds = getFakeDomainIdsRequest(request, FaultType::MISSING_VERSION);

	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/invalidVersion") {
	state UnsentPacketQueue content;
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/get-encryption-keys-by-key-ids";
	EncryptCipherDomainIdVec domIds = getFakeDomainIdsRequest(request, FaultType::INVALID_VERSION);

	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}

TEST_CASE("/restSimKmsVault/GetByKeyIds/missingValidationTokens") {
	state UnsentPacketQueue content;
	state Reference<HTTP::IncomingRequest> request = makeReference<HTTP::IncomingRequest>();
	state Reference<HTTP::OutgoingResponse> response = makeReference<HTTP::OutgoingResponse>();

	request->verb = HTTP::HTTP_VERB_POST;
	request->resource = "/get-encryption-keys-by-key-ids";
	EncryptCipherDomainIdVec domIds = getFakeDomainIdsRequest(request, FaultType::MISSING_VALIDATION_TOKEN);

	response->data.content = &content;
	response->data.contentLen = 0;

	try {
		wait(simKmsVaultRequestHandler(request, response));
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_rest_malformed_response);
	}
	return Void();
}
