/*
 * GetEncryptCipherKeys.h
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
#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_GETCIPHERKEYS_ACTOR_G_H)
#define FDBSERVER_GETCIPHERKEYS_ACTOR_G_H
#include "fdbserver/GetEncryptCipherKeys.actor.g.h"
#elif !defined(FDBSERVER_GETCIPHERKEYS_ACTOR_H)
#define FDBSERVER_GETCIPHERKEYS_ACTOR_H

#include "fdbserver/ServerDBInfo.h"
#include "flow/BlobCipher.h"

#include <unordered_map>
#include <unordered_set>

#include "flow/actorcompiler.h"

// Get latest cipher keys for given encryption domains. It tries to get the cipher keys from local cache.
// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
// before return.
ACTOR Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> getLatestEncryptCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_map<EncryptCipherDomainId, EncryptCipherDomainName> domains);

// Get cipher keys specified by the list of cipher details. It tries to get the cipher keys from local cache.
// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
// before return.
ACTOR Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> getEncryptCipherKeys(
    Reference<AsyncVar<ServerDBInfo> const> db,
    std::unordered_set<BlobCipherDetails> cipherDetails);

struct TextAndHeaderCipherKeys {
	Reference<BlobCipherKey> cipherTextKey;
	Reference<BlobCipherKey> cipherHeaderKey;
};

// Helper method to get latest cipher text key and cipher header key for system domain,
// used for encrypting system data.
ACTOR Future<TextAndHeaderCipherKeys> getLatestSystemEncryptCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db);

// Helper method to get both text cipher key and header cipher key for the given encryption header,
// used for decrypting given encrypted data with encryption header.
ACTOR Future<TextAndHeaderCipherKeys> getEncryptCipherKeys(Reference<AsyncVar<ServerDBInfo> const> db,
                                                           BlobCipherEncryptHeader header);

#include "flow/unactorcompiler.h"
#endif