/*
 * SimKmsVault.h
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

#ifndef FDBCLIENT_SIM_KMS_VAULT_H
#define FDBCLIENT_SIM_KMS_VAULT_H
#pragma once

#include "fdbclient/BlobMetadataUtils.h"
#include "flow/EncryptUtils.h"

#define DEBUG_SIM_KEY_CIPHER DEBUG_ENCRYPT_KEY_CIPHER

struct SimKmsVaultKeyCtx : NonCopyable, public ReferenceCounted<SimKmsVaultKeyCtx> {
	static const int minCipherLen = 4;
	static const int maxCipherLen = 256;

	EncryptCipherBaseKeyId id;
	int keyLen;
	Standalone<StringRef> key;
	EncryptCipherKeyCheckValue kcv;

	SimKmsVaultKeyCtx() : id(INVALID_ENCRYPT_CIPHER_KEY_ID), keyLen(-1), kcv(0) {}
	explicit SimKmsVaultKeyCtx(EncryptCipherBaseKeyId kId, const uint8_t* data, const int dataLen);

	static int getKeyLen(const EncryptCipherBaseKeyId id);
};

namespace SimKmsVault {
Reference<SimKmsVaultKeyCtx> getByBaseCipherId(const EncryptCipherBaseKeyId baseCipherId);
Reference<SimKmsVaultKeyCtx> getByDomainId(const EncryptCipherDomainId domainId);
uint32_t maxSimKeys();

Standalone<BlobMetadataDetailsRef> getBlobMetadata(const BlobMetadataDomainId domainId, const std::string& bgUrl);
} // namespace SimKmsVault

#endif
