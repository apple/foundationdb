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

#include <cstdint>

#define ENCRYPT_INVALID_DOMAIN_ID 0
#define ENCRYPT_INVALID_CIPHER_KEY_ID 0

#define AUTH_TOKEN_SIZE 16

#define SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID -1
#define ENCRYPT_HEADER_DOMAIN_ID -2

using EncryptCipherDomainId = int64_t;
using EncryptCipherRandomSalt = uint64_t;
using EncryptCipherBaseKeyId = uint64_t;

typedef enum { ENCRYPT_CIPHER_MODE_NONE = 0, ENCRYPT_CIPHER_MODE_AES_256_CTR = 1 } EncryptCipherMode;
typedef enum {
	ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE = 0,
	ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE = 1,
	ENCRYPT_HEADER_AUTH_TOKEN_MODE_MULTI = 2
} EncryptAuthTokenMode;

#endif
