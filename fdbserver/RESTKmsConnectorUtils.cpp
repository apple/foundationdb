/*
 * RESTKmsConnectorUtils.cpp
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

#include "fdbserver/RESTKmsConnectorUtils.h"
#include "fdbrpc/HTTP.h"
#include "flow/EncryptUtils.h"

namespace RESTKmsConnectorUtils {

const char* BASE_CIPHER_ID_TAG = "base_cipher_id";
const char* BASE_CIPHER_TAG = "base_cipher";
const char* CIPHER_KEY_DETAILS_TAG = "cipher_key_details";
const char* ENCRYPT_DOMAIN_ID_TAG = "encrypt_domain_id";
const char* REFRESH_AFTER_SEC = "refresh_after_sec";
const char* EXPIRE_AFTER_SEC = "expire_after_sec";
const char* ERROR_TAG = "error";
const char* ERROR_MSG_TAG = "err_msg";
const char* ERROR_CODE_TAG = "err_code";
const char* KMS_URLS_TAG = "kms_urls";
const char* REFRESH_KMS_URLS_TAG = "refresh_kms_urls";
const char* REQUEST_VERSION_TAG = "version";
const char* VALIDATION_TOKENS_TAG = "validation_tokens";
const char* VALIDATION_TOKEN_NAME_TAG = "token_name";
const char* VALIDATION_TOKEN_VALUE_TAG = "token_value";
const char* DEBUG_UID_TAG = "debug_uid";

const char* TOKEN_NAME_FILE_SEP = "$";
const char* TOKEN_TUPLE_SEP = ",";
const char DISCOVER_URL_FILE_URL_SEP = '\n';

const char* BLOB_METADATA_DETAILS_TAG = "blob_metadata_details";
const char* BLOB_METADATA_DOMAIN_ID_TAG = "domain_id";
const char* BLOB_METADATA_LOCATIONS_TAG = "locations";
const char* BLOB_METADATA_LOCATION_ID_TAG = "id";
const char* BLOB_METADATA_LOCATION_PATH_TAG = "path";

const int INVALID_REQUEST_VERSION = 0;

HTTP::Headers getHTTPHeaders() {
	HTTP::Headers headers;
	headers["Content-type"] = "application/json";
	headers["Accept"] = "application/json";

	return headers;
}

void addVersionToDoc(rapidjson::Document& doc, const int requestVersion) {
	rapidjson::Value version;
	version.SetInt(requestVersion);

	rapidjson::Value versionKey(REQUEST_VERSION_TAG, doc.GetAllocator());
	doc.AddMember(versionKey, version, doc.GetAllocator());
}

void addLatestDomainDetailsToDoc(rapidjson::Document& doc,
                                 const char* rootTagName,
                                 const char* idTagName,
                                 const EncryptCipherDomainIdVec& domainIds) {
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

void addBaseCipherIdDomIdToDoc(rapidjson::Document& doc,
                               rapidjson::Value& keyIdDetails,
                               const EncryptCipherBaseKeyId baseCipherId,
                               const Optional<EncryptCipherDomainId> domainId) {
	rapidjson::Value keyIdDetail(rapidjson::kObjectType);

	// Add 'base_cipher_id'
	rapidjson::Value key(BASE_CIPHER_ID_TAG, doc.GetAllocator());
	rapidjson::Value baseKeyId;
	baseKeyId.SetUint64(baseCipherId);
	keyIdDetail.AddMember(key, baseKeyId, doc.GetAllocator());

	if (domainId.present()) {
		// Add 'encrypt_domain_id'
		key.SetString(ENCRYPT_DOMAIN_ID_TAG, doc.GetAllocator());
		rapidjson::Value domId;
		domId.SetInt64(domainId.get());
		keyIdDetail.AddMember(key, domId, doc.GetAllocator());
	}

	// push above object to the array
	keyIdDetails.PushBack(keyIdDetail, doc.GetAllocator());
}

void addValidationTokensSectionToJsonDoc(rapidjson::Document& doc, const ValidationTokenMap& tokenMap) {
	// Append "validationTokens" as json array
	rapidjson::Value validationTokens(rapidjson::kArrayType);

	for (const auto& token : tokenMap) {
		rapidjson::Value validationToken(rapidjson::kObjectType);

		// Add "name" - token name
		rapidjson::Value key(VALIDATION_TOKEN_NAME_TAG, doc.GetAllocator());
		rapidjson::Value tokenName(token.second.name.data(), doc.GetAllocator());
		validationToken.AddMember(key, tokenName, doc.GetAllocator());

		// Add "value" - token value
		key.SetString(VALIDATION_TOKEN_VALUE_TAG, doc.GetAllocator());
		rapidjson::Value tokenValue;
		tokenValue.SetString(token.second.value.data(), token.second.value.size(), doc.GetAllocator());
		validationToken.AddMember(key, tokenValue, doc.GetAllocator());

		validationTokens.PushBack(validationToken, doc.GetAllocator());
	}

	// Append 'validation_token[]' to the parent document
	rapidjson::Value memberKey(VALIDATION_TOKENS_TAG, doc.GetAllocator());
	doc.AddMember(memberKey, validationTokens, doc.GetAllocator());
}

void addRefreshKmsUrlsSectionToJsonDoc(rapidjson::Document& doc, const bool refreshKmsUrls) {
	rapidjson::Value key(REFRESH_KMS_URLS_TAG, doc.GetAllocator());
	rapidjson::Value refreshUrls;
	refreshUrls.SetBool(refreshKmsUrls);

	// Append 'refresh_kms_urls' object to the parent document
	doc.AddMember(key, refreshUrls, doc.GetAllocator());
}

void addDebugUidSectionToJsonDoc(rapidjson::Document& doc, Optional<UID> dbgId) {
	if (!dbgId.present()) {
		// Debug id not present; do nothing
		return;
	}
	rapidjson::Value key(DEBUG_UID_TAG, doc.GetAllocator());
	rapidjson::Value debugIdVal;
	const std::string dbgIdStr = dbgId.get().toString();
	debugIdVal.SetString(dbgIdStr.data(), dbgIdStr.size(), doc.GetAllocator());

	// Append 'debug_uid' object to the parent document
	doc.AddMember(key, debugIdVal, doc.GetAllocator());
}

Optional<ErrorDetail> getError(const rapidjson::Document& doc) {
	// Check if response has error
	if (doc.HasMember(ERROR_TAG) && !doc[ERROR_TAG].IsNull()) {
		ErrorDetail details;

		if (doc[ERROR_TAG].HasMember(ERROR_MSG_TAG) && doc[ERROR_TAG][ERROR_MSG_TAG].IsString()) {
			details.errorMsg.resize(doc[ERROR_TAG][ERROR_MSG_TAG].GetStringLength());
			memcpy(const_cast<char*>(details.errorMsg.data()),
			       doc[ERROR_TAG][ERROR_MSG_TAG].GetString(),
			       doc[ERROR_TAG][ERROR_MSG_TAG].GetStringLength());
		}
		if (doc[ERROR_TAG].HasMember(ERROR_CODE_TAG) && doc[ERROR_TAG][ERROR_CODE_TAG].IsString()) {
			details.errorCode.resize(doc[ERROR_TAG][ERROR_CODE_TAG].GetStringLength());
			memcpy(const_cast<char*>(details.errorCode.data()),
			       doc[ERROR_TAG][ERROR_CODE_TAG].GetString(),
			       doc[ERROR_TAG][ERROR_CODE_TAG].GetStringLength());
		}
		return details;
	}
	return Optional<ErrorDetail>();
}

} // namespace RESTKmsConnectorUtils
