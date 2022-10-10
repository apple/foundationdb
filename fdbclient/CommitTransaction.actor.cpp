#include "fdbclient/CommitTransaction.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// Future<MutationRef> MutationRef::encrypt(const std::unordered_map<EncryptCipherDomainId, Reference<BlobCipherKey>>&
// cipherKeys,
//                     const EncryptCipherDomainId& domainId,
//                     Arena& arena,
//                     BlobCipherMetrics::UsageType usageType) const {
//     ASSERT_NE(domainId, INVALID_ENCRYPT_DOMAIN_ID);
//     auto textCipherItr = cipherKeys.find(domainId);
//     auto headerCipherItr = cipherKeys.find(ENCRYPT_HEADER_DOMAIN_ID);
//     ASSERT(textCipherItr != cipherKeys.end() && textCipherItr->second.isValid());
//     ASSERT(headerCipherItr != cipherKeys.end() && headerCipherItr->second.isValid());
//     uint8_t iv[AES_256_IV_LENGTH] = { 0 };
//     deterministicRandom()->randomBytes(iv, AES_256_IV_LENGTH);
//     BinaryWriter bw(AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
//     bw << *this;
//     EncryptBlobCipherAes265Ctr cipher(
//         textCipherItr->second,
//         headerCipherItr->second,
//         iv,
//         AES_256_IV_LENGTH,
//         getEncryptAuthTokenMode(EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE),
//         usageType);
//     BlobCipherEncryptHeader* header = new (arena) BlobCipherEncryptHeader;
//     StringRef headerRef(reinterpret_cast<const uint8_t*>(header), sizeof(BlobCipherEncryptHeader));
//     StringRef payload =
//         cipher.encrypt(static_cast<const uint8_t*>(bw.getData()), bw.getLength(), header, arena)->toStringRef();
//     return MutationRef(Encrypted, headerRef, payload);
// }

ACTOR Future<MutationRef> encryptActor(const MutationRef* self,
                                       EncryptCipherDomainId domainId,
                                       Arena* arena,
                                       BlobCipherMetrics::UsageType usageType) {
	/*
	        ASSERT_NE(domainId, INVALID_ENCRYPT_DOMAIN_ID);
	        auto textCipherItr = cipherKeys.find(domainId);
	        auto headerCipherItr = cipherKeys.find(ENCRYPT_HEADER_DOMAIN_ID);
	        ASSERT(textCipherItr != cipherKeys.end() && textCipherItr->second.isValid());
	        ASSERT(headerCipherItr != cipherKeys.end() && headerCipherItr->second.isValid());
	        uint8_t iv[AES_256_IV_LENGTH] = { 0 };
	        deterministicRandom()->randomBytes(iv, AES_256_IV_LENGTH);
	        BinaryWriter bw(AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
	        bw << *this;
	        EncryptBlobCipherAes265Ctr cipher(
	            textCipherItr->second,
	            headerCipherItr->second,
	            iv,
	            AES_256_IV_LENGTH,
	            getEncryptAuthTokenMode(EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE),
	            usageType);
	        BlobCipherEncryptHeader* header = new (arena) BlobCipherEncryptHeader;
	        StringRef headerRef(reinterpret_cast<const uint8_t*>(header), sizeof(BlobCipherEncryptHeader));
	        StringRef payload =
	            cipher.encrypt(static_cast<const uint8_t*>(bw.getData()), bw.getLength(), header, arena)->toStringRef();
	*/
	return MutationRef();
}

Future<MutationRef> MutationRef::encrypt(const EncryptCipherDomainId& domainId,
                                         Arena& arena,
                                         BlobCipherMetrics::UsageType usageType) const {
	return encryptActor(this, domainId, &arena, usageType);
}

ACTOR Future<MutationRef> decryptActor(const MutationRef* self,
                                       Arena* arena,
                                       BlobCipherMetrics::UsageType usageType,
                                       StringRef* buf) {
	/*
	        const BlobCipherEncryptHeader* header = encryptionHeader();
	        auto textCipherItr = cipherKeys.find(header->cipherTextDetails);
	        auto headerCipherItr = cipherKeys.find(header->cipherHeaderDetails);
	        ASSERT(textCipherItr != cipherKeys.end() && textCipherItr->second.isValid());
	        ASSERT(headerCipherItr != cipherKeys.end() && headerCipherItr->second.isValid());
	        TextAndHeaderCipherKeys textAndHeaderKeys;
	        textAndHeaderKeys.cipherHeaderKey = headerCipherItr->second;
	        textAndHeaderKeys.cipherTextKey = textCipherItr->second;
	*/
	return MutationRef();
};

Future<MutationRef> MutationRef::decrypt(Arena& arena, BlobCipherMetrics::UsageType usageType, StringRef* buf) const {
	return decryptActor(this, &arena, usageType, buf);
};
