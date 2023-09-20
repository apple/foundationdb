/*
 * IPager.cpp
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
#include "fdbserver/IPager.h"

#include "fdbclient/BlobCipher.h"
#include "flow/EncryptUtils.h"
#include "flow/IRandom.h"
#include "flow/UnitTest.h"
#include <limits>

TEST_CASE("/fdbserver/IPager/ArenaPage/PageContentChecksum") {
	auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
	for (uint8_t et = 0; et < EncodingType::MAX_ENCODING_TYPE; et++) {
		constexpr int _PAGE_SIZE = 8 * 1024;
		EncodingType encodingType = (EncodingType)et;
		Reference<ArenaPage> page = makeReference<ArenaPage>(_PAGE_SIZE, _PAGE_SIZE);
		page->init(encodingType, PageType::BTreeNode, 1);
		deterministicRandom()->randomBytes(page->mutateData(), page->dataSize());
		PhysicalPageID pageID = deterministicRandom()->randomUInt32();
		if (encodingType == AESEncryption || encodingType == AESEncryptionWithAuth) {
			const int cipherBytesLen = deterministicRandom()->randomInt(4, MAX_BASE_CIPHER_LEN + 1);
			uint8_t cipherKeyBytes[cipherBytesLen];
			deterministicRandom()->randomBytes(cipherKeyBytes, cipherBytesLen);
			const EncryptCipherKeyCheckValue cipherKCV = Sha256KCV().computeKCV(cipherKeyBytes, cipherBytesLen);
			Reference<BlobCipherKey> cipherKey =
			    makeReference<BlobCipherKey>(0 /*domainId*/,
			                                 1 /*baseCipherId*/,
			                                 &cipherKeyBytes[0],
			                                 cipherBytesLen,
			                                 cipherKCV,
			                                 std::numeric_limits<int64_t>::max() /*refreshAt*/,
			                                 std::numeric_limits<int64_t>::max() /*expireAt*/
			    );
			page->encryptionKey.aesKey.cipherTextKey = cipherKey;
			page->encryptionKey.aesKey.cipherHeaderKey = cipherKey;
			if (encodingType == AESEncryption) {
				g_knobs.setKnob("encrypt_header_auth_token_enabled", KnobValueRef::create(bool{ false }));
			} else {
				g_knobs.setKnob("encrypt_header_auth_token_enabled", KnobValueRef::create(bool{ true }));
				g_knobs.setKnob("encrypt_header_auth_token_algo", KnobValueRef::create(int{ 1 }));
			}
		} else if (encodingType == XOREncryption_TestOnly) {
			page->encryptionKey.xorKey = deterministicRandom()->randomInt(0, std::numeric_limits<uint8_t>::max());
			page->encryptionKey.xorWith = deterministicRandom()->randomInt(0, std::numeric_limits<uint8_t>::max());
		}
		page->setWriteInfo(pageID, 1 /*version*/);
		page->preWrite(pageID);
		// Randomly corrupt the data.
		uint8_t* byte = page->mutateData() + deterministicRandom()->randomInt(0, page->dataSize());
		*byte = ~(*byte);
		page->postReadHeader(pageID);
		try {
			// Assert checksum failure is thrown.
			page->postReadPayload(pageID);
			UNREACHABLE();
		} catch (Error& e) {
			if (encodingType == AESEncryptionWithAuth) {
				ASSERT_EQ(e.code(), error_code_encrypt_header_authtoken_mismatch);
			} else {
				ASSERT_EQ(e.code(), error_code_page_decoding_failed);
			}
		}
	}
	return Void();
}
