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

#ifndef FDBCLIENT_GETCIPHERKEYS_H
#define FDBCLIENT_GETCIPHERKEYS_H
#pragma once

#include "flow/EncryptUtils.h"
#include "flow/genericactors.actor.h"
#include "fdbclient/BlobCipher.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
#include "fdbclient/Knobs.h"
#include "fdbrpc/Stats.h"
#include "fdbrpc/TenantInfo.h"
#include "flow/Knobs.h"
#include "flow/IRandom.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

using BaseCipherIndex = std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>;

struct TextAndHeaderCipherKeys {
	Reference<BlobCipherKey> cipherTextKey;
	Reference<BlobCipherKey> cipherHeaderKey;
};

template <class T>
class GetEncryptCipherKeys {
public:
	// Get latest cipher keys for given encryption domains. It tries to get the cipher keys from local cache.
	// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
	// before return.
	static Future<std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>> getLatestEncryptCipherKeys(
	    Reference<AsyncVar<T> const> db,
	    std::unordered_set<EncryptCipherDomainId> domainIds,
	    BlobCipherMetrics::UsageType usageType);

	// Get latest cipher key for given a encryption domain. It tries to get the cipher key from the local cache.
	// In case of cache miss, it fetches the cipher key from EncryptKeyProxy and put the result in the local cache
	// before return.
	static Future<Reference<BlobCipherKey>> getLatestEncryptCipherKey(Reference<AsyncVar<T> const> db,
	                                                                  EncryptCipherDomainId domainId,
	                                                                  BlobCipherMetrics::UsageType usageType);

	// Get cipher keys specified by the list of cipher details. It tries to get the cipher keys from local cache.
	// In case of cache miss, it fetches the cipher keys from EncryptKeyProxy and put the result in the local cache
	// before return.
	static Future<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> getEncryptCipherKeys(
	    Reference<AsyncVar<T> const> db,
	    std::unordered_set<BlobCipherDetails> cipherDetails,
	    BlobCipherMetrics::UsageType usageType);

	static Future<TextAndHeaderCipherKeys> getLatestEncryptCipherKeysForDomain(Reference<AsyncVar<T> const> db,
	                                                                           EncryptCipherDomainId domainId,
	                                                                           BlobCipherMetrics::UsageType usageType);

	static Future<TextAndHeaderCipherKeys> getLatestSystemEncryptCipherKeys(const Reference<AsyncVar<T> const>& db,
	                                                                        BlobCipherMetrics::UsageType usageType);

	static Future<TextAndHeaderCipherKeys> getEncryptCipherKeys(Reference<AsyncVar<T> const> db,
	                                                            BlobCipherEncryptHeader header,
	                                                            BlobCipherMetrics::UsageType usageType);

	static Future<TextAndHeaderCipherKeys> getEncryptCipherKeys(Reference<AsyncVar<T> const> db,
	                                                            BlobCipherEncryptHeaderRef header,
	                                                            BlobCipherMetrics::UsageType usageType);
};

#endif