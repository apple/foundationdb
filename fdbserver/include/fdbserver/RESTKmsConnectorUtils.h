/*
 * RESTKmsConnectorUtils.h
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

#ifndef FDBSERVER_REST_KMS_CONNECTOR_UTILS_H
#define FDBSERVER_REST_KMS_CONNECTOR_UTILS_H
#pragma once

#include "fdbrpc/HTTP.h"
#include "flow/EncryptUtils.h"
#include "flow/flow.h"

#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace RESTKmsConnectorUtils {

extern char const* BASE_CIPHER_ID_TAG;
extern char const* BASE_CIPHER_TAG;
extern char const* CIPHER_KEY_DETAILS_TAG;
extern char const* ENCRYPT_DOMAIN_ID_TAG;
extern char const* REFRESH_AFTER_SEC;
extern char const* EXPIRE_AFTER_SEC;
extern char const* ERROR_TAG;
extern char const* ERROR_MSG_TAG;
extern char const* ERROR_CODE_TAG;
extern char const* KMS_URLS_TAG;
extern char const* REFRESH_KMS_URLS_TAG;
extern char const* REQUEST_VERSION_TAG;
extern char const* VALIDATION_TOKENS_TAG;
extern char const* VALIDATION_TOKEN_NAME_TAG;
extern char const* VALIDATION_TOKEN_VALUE_TAG;
extern char const* DEBUG_UID_TAG;

extern char const* TOKEN_NAME_FILE_SEP;
extern char const* TOKEN_TUPLE_SEP;
extern char const DISCOVER_URL_FILE_URL_SEP;

extern char const* BLOB_METADATA_DETAILS_TAG;
extern char const* BLOB_METADATA_DOMAIN_ID_TAG;
extern char const* BLOB_METADATA_LOCATIONS_TAG;
extern char const* BLOB_METADATA_LOCATION_ID_TAG;
extern char const* BLOB_METADATA_LOCATION_PATH_TAG;

extern int const INVALID_REQUEST_VERSION;

enum class ValidationTokenSource {
	VALIDATION_TOKEN_SOURCE_FILE = 1,
	VALIDATION_TOKEN_SOURCE_LAST // Always the last element
};

struct ErrorDetail {
	std::string errorCode;
	std::string errorMsg;

	ErrorDetail() {}
	ErrorDetail(std::string const& code, std::string const& msg) : errorCode(code), errorMsg(msg) {}

	bool isEqual(ErrorDetail const& toCompare) const {
		return errorCode.compare(toCompare.errorCode) == 0 && errorMsg.compare(toCompare.errorMsg) == 0;
	}
};

struct ValidationTokenCtx {
	std::string name;
	std::string value;
	ValidationTokenSource source;
	Optional<std::string> filePath;

	explicit ValidationTokenCtx(std::string const& n, ValidationTokenSource s)
	  : name(n), value(""), source(s), filePath(Optional<std::string>()), readTS(now()) {}
	double getReadTS() const { return readTS; }

private:
	double readTS; // Approach assists refreshing token based on time of creation
};
using ValidationTokenMap = std::unordered_map<std::string, ValidationTokenCtx>;

HTTP::Headers getHTTPHeaders();

void addVersionToDoc(rapidjson::Document& doc, int const requestVersion);
void addLatestDomainDetailsToDoc(rapidjson::Document& doc,
                                 char const* rootTagName,
                                 char const* idTagName,
                                 EncryptCipherDomainIdVec const& domainIds);
void addBaseCipherIdDomIdToDoc(rapidjson::Document& doc,
                               rapidjson::Value& keyIdDetails,
                               EncryptCipherBaseKeyId const baseCipherId,
                               Optional<EncryptCipherDomainId> const domainId);
void addValidationTokensSectionToJsonDoc(rapidjson::Document& doc, ValidationTokenMap const& tokenMap);
void addRefreshKmsUrlsSectionToJsonDoc(rapidjson::Document& doc, bool const refreshKmsUrls);
void addDebugUidSectionToJsonDoc(rapidjson::Document& doc, Optional<UID> dbgId);

Optional<ErrorDetail> getError(rapidjson::Document const& doc);

} // namespace RESTKmsConnectorUtils

#endif
