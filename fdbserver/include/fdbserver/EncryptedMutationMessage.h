/*
 * EncryptedMutationMessage.h
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

#ifndef FDBSERVER_ENCRYPTEDMUTATIONMESSAGE_H
#define FDBSERVER_ENCRYPTEDMUTATIONMESSAGE_H

#pragma once

#include "fdbclient/CommitTransaction.h"
#include "fdbserver/Knobs.h"
#include "flow/BlobCipher.h"

struct EncryptedMutationMessage {

	BlobCipherEncryptHeader header;
	StringRef encrypted;

	EncryptedMutationMessage() {}

	std::string toString() const {
		return format("code: %d, encryption info: %s",
		              MutationRef::Reserved_For_EncryptedMutationMessage,
		              header.toString().c_str());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		uint8_t poly = MutationRef::Reserved_For_EncryptedMutationMessage;
		serializer(ar, poly, header, encrypted);
	}

	static bool startsEncryptedMutationMessage(uint8_t byte) {
		return byte == MutationRef::Reserved_For_EncryptedMutationMessage;
	}
	template <class Ar>
	static bool isNextIn(Ar& ar) {
		return startsEncryptedMutationMessage(*(const uint8_t*)ar.peekBytes(1));
	}

	// Encrypt given mutation and return an EncryptedMutationMessage.
	static EncryptedMutationMessage encrypt(
	    Arena& arena,
	    const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>& cipherKeys,
	    const EncryptCipherDomainId& domainId,
	    const MutationRef& mutation) {
		ASSERT_NE(domainId, ENCRYPT_INVALID_DOMAIN_ID);
		auto textCipherItr = cipherKeys.find(domainId);
		auto headerCipherItr = cipherKeys.find(ENCRYPT_HEADER_DOMAIN_ID);
		ASSERT(textCipherItr != cipherKeys.end() && textCipherItr->second.isValid());
		ASSERT(headerCipherItr != cipherKeys.end() && headerCipherItr->second.isValid());
		uint8_t iv[AES_256_IV_LENGTH];
		generateRandomData(iv, AES_256_IV_LENGTH);
		BinaryWriter bw(AssumeVersion(g_network->protocolVersion()));
		bw << mutation;
		EncryptedMutationMessage encrypted_mutation;
		EncryptBlobCipherAes265Ctr cipher(textCipherItr->second,
		                                  headerCipherItr->second,
		                                  iv,
		                                  AES_256_IV_LENGTH,
		                                  ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
		encrypted_mutation.encrypted =
		    cipher
		        .encrypt(static_cast<const uint8_t*>(bw.getData()), bw.getLength(), &encrypted_mutation.header, arena)
		        ->toStringRef();
		return encrypted_mutation;
	}

	// Encrypt system key space mutation and return an EncryptedMutationMessage.
	static EncryptedMutationMessage encryptMetadata(
	    Arena& arena,
	    const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>& cipherKeys,
	    const MutationRef& mutation) {
		return encrypt(arena, cipherKeys, SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID, mutation);
	}

	// Read an EncryptedMutationMessage from given reader, decrypt and return the encrypted mutation.
	// Also return decrypt buffer through buf, if it is specified.
	template <class Ar>
	static MutationRef decrypt(Ar& ar,
	                           Arena& arena,
	                           const std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>& cipherKeys,
	                           StringRef* buf = nullptr) {
		ASSERT(SERVER_KNOBS->ENABLE_ENCRYPTION);
		EncryptedMutationMessage msg;
		ar >> msg;
		auto textCipherItr = cipherKeys.find(msg.header.cipherTextDetails);
		auto headerCipherItr = cipherKeys.find(msg.header.cipherHeaderDetails);
		ASSERT(textCipherItr != cipherKeys.end() && textCipherItr->second.isValid());
		ASSERT(headerCipherItr != cipherKeys.end() && headerCipherItr->second.isValid());
		DecryptBlobCipherAes256Ctr cipher(textCipherItr->second, headerCipherItr->second, msg.header.iv);
		StringRef plaintext =
		    cipher.decrypt(msg.encrypted.begin(), msg.encrypted.size(), msg.header, arena)->toStringRef();
		if (buf != nullptr) {
			*buf = plaintext;
		}
		ArenaReader reader(arena, plaintext, AssumeVersion(g_network->protocolVersion()));
		MutationRef mutation;
		reader >> mutation;
		return mutation;
	}
};
#endif