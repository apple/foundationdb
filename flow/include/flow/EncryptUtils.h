/*
 * EncryptUtils.h
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

#ifndef ENCRYPT_UTILS_H
#define ENCRYPT_UTILS_H
#pragma once

#include "flow/Arena.h"
#include "flow/xxhash.h"

#include <cstdint>
#include <limits>
#include <openssl/evp.h>
#include <string>
#include <string_view>
#include <unordered_set>

#define DEBUG_ENCRYPT_KEY_CIPHER false

constexpr const int AUTH_TOKEN_HMAC_SHA_SIZE = 32;
constexpr const int AUTH_TOKEN_AES_CMAC_SIZE = 16;
constexpr const int AUTH_TOKEN_MAX_SIZE = AUTH_TOKEN_HMAC_SHA_SIZE;

using EncryptCipherDomainId = int64_t;
using EncryptCipherBaseKeyId = uint64_t;
using EncryptCipherRandomSalt = uint64_t;
using EncryptCipherKeyCheckValue = uint32_t;

constexpr const int MAX_BASE_CIPHER_LEN = EVP_MAX_KEY_LENGTH - sizeof(EncryptCipherRandomSalt);

constexpr const EncryptCipherDomainId INVALID_ENCRYPT_DOMAIN_ID = -1;
constexpr const EncryptCipherDomainId SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID = -2;
constexpr const EncryptCipherDomainId ENCRYPT_HEADER_DOMAIN_ID = -3;
constexpr const EncryptCipherDomainId FDB_DEFAULT_ENCRYPT_DOMAIN_ID = -4;

constexpr const EncryptCipherBaseKeyId INVALID_ENCRYPT_CIPHER_KEY_ID = 0;

constexpr const EncryptCipherRandomSalt INVALID_ENCRYPT_RANDOM_SALT = 0;

static const std::unordered_set<EncryptCipherDomainId> ENCRYPT_CIPHER_SYSTEM_DOMAINS = {
	SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID,
	ENCRYPT_HEADER_DOMAIN_ID
};

static const std::unordered_set<EncryptCipherDomainId> ENCRYPT_CIPHER_DETAULT_DOMAINS = {
	SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID,
	ENCRYPT_HEADER_DOMAIN_ID,
	FDB_DEFAULT_ENCRYPT_DOMAIN_ID,
};

typedef enum {
	ENCRYPT_CIPHER_MODE_NONE = 0,
	ENCRYPT_CIPHER_MODE_AES_256_CTR = 1,
	ENCRYPT_CIPHER_MODE_LAST = 2
} EncryptCipherMode;

static_assert(EncryptCipherMode::ENCRYPT_CIPHER_MODE_LAST <= std::numeric_limits<uint8_t>::max(),
              "EncryptCipherMode value overflow");

EncryptCipherMode encryptModeFromString(const std::string& modeStr);

// EncryptionHeader authentication modes
// 1. NONE - No 'authentication token' generation needed for EncryptionHeader i.e. no protection against header OR
// cipherText 'tampering' and/or bit rot/flip corruptions.
// 2. Single/Multi - Encryption header would generate one or more 'authentication tokens' to protect the header against
// 'tempering' and/or bit rot/flip corruptions. Refer to BlobCipher.h for detailed usage recommendations.
// 3. LAST - Invalid mode, used for static asserts.

typedef enum {
	ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE = 0,
	ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE = 1,
	ENCRYPT_HEADER_AUTH_TOKEN_LAST = 3 // Always the last element
} EncryptAuthTokenMode;

static_assert(EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_LAST <= std::numeric_limits<uint8_t>::max(),
              "EncryptHeaderAuthToken value overflow");

typedef enum {
	ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE = 0,
	ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA = 1,
	ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC = 2,
	ENCRYPT_HEADER_AUTH_TOKEN_ALGO_LAST = 3 // Always the last element
} EncryptAuthTokenAlgo;

static_assert(EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_LAST <= std::numeric_limits<uint8_t>::max(),
              "EncryptHeaerAuthTokenAlgo value overflow");

bool isEncryptHeaderAuthTokenModeValid(const EncryptAuthTokenMode mode);
bool isEncryptHeaderAuthTokenAlgoValid(const EncryptAuthTokenAlgo algo);
bool isEncryptHeaderAuthTokenDetailsValid(const EncryptAuthTokenMode mode, const EncryptAuthTokenAlgo algo);
EncryptAuthTokenAlgo getAuthTokenAlgoFromMode(const EncryptAuthTokenMode mode);
EncryptAuthTokenMode getRandomAuthTokenMode();
EncryptAuthTokenAlgo getRandomAuthTokenAlgo();

constexpr std::string_view ENCRYPT_DBG_TRACE_CACHED_PREFIX = "Chd";
constexpr std::string_view ENCRYPT_DBG_TRACE_QUERY_PREFIX = "Qry";
constexpr std::string_view ENCRYPT_DBG_TRACE_INSERT_PREFIX = "Ins";
constexpr std::string_view ENCRYPT_DBG_TRACE_RESULT_PREFIX = "Res";

// Utility interface to construct TraceEvent key for debugging
std::string getEncryptDbgTraceKey(std::string_view prefix,
                                  EncryptCipherDomainId domainId,
                                  Optional<EncryptCipherBaseKeyId> baseCipherId = Optional<EncryptCipherBaseKeyId>());

std::string getEncryptDbgTraceKeyWithTS(std::string_view prefix,
                                        EncryptCipherDomainId domainId,
                                        EncryptCipherBaseKeyId baseCipherId,
                                        int64_t refAfterTS,
                                        int64_t expAfterTS);

int getEncryptHeaderAuthTokenSize(int algo);

bool isReservedEncryptDomain(EncryptCipherDomainId domainId);
bool isEncryptHeaderDomain(EncryptCipherDomainId domainId);

#endif
