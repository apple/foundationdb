/*
 * EvolvableApiRequestHandler.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/EvolvableApiRequestHandler.h"
#include "fdbclient/EvolvableApiTypes.h"
#include "foundationdb/fdb_c_evolvable.h"
#include "flow/actorcompiler.h" // has to be last include

namespace {

void setEncryptionKey(FDBBGEncryptionKey* dest, const BlobGranuleCipherKey& source) {
	dest->domain_id = source.encryptDomainId;
	dest->base_key_id = source.baseCipherId;
	dest->base_kcv = source.baseCipherKCV;
	dest->random_salt = source.salt;
	dest->base_key.key = source.baseCipher.begin();
	dest->base_key.key_length = source.baseCipher.size();
}

void setEncryptionKeyCtx(FDBBGEncryptionCtx* dest, const BlobGranuleCipherKeysCtx& source, Arena& ar) {
	dest->present = true;
	dest->textKey = new (ar) FDBBGEncryptionKey();
	setEncryptionKey(dest->textKey, source.textCipherKey);
	dest->textKCV = source.textCipherKey.baseCipherKCV;
	dest->headerKey = new (ar) FDBBGEncryptionKey();
	setEncryptionKey(dest->headerKey, source.headerCipherKey);
	dest->headerKCV = source.headerCipherKey.baseCipherKCV;
	dest->iv.key = source.ivRef.begin();
	dest->iv.key_length = source.ivRef.size();
}

void setBlobFilePointer(FDBBGFilePointer* dest, const BlobFilePointerRef& source, Arena& ar) {
	dest->filename_ptr = source.filename.begin();
	dest->filename_length = source.filename.size();
	dest->file_offset = source.offset;
	dest->file_length = source.length;
	dest->full_file_length = source.fullFileLength;
	dest->file_version = source.fileVersion;

	// handle encryption
	dest->encryption_ctx = new (ar) FDBBGEncryptionCtx();
	if (source.cipherKeysCtx.present()) {
		setEncryptionKeyCtx(dest->encryption_ctx, source.cipherKeysCtx.get(), ar);
	} else {
		dest->encryption_ctx->present = false;
	}
}

void setBGMutation(FDBBGMutation* dest,
                   int64_t version,
                   FDBBGTenantPrefix const* tenantPrefix,
                   const MutationRef& source) {
	dest->version = version;
	dest->type = source.type;
	dest->param1_ptr = source.param1.begin();
	dest->param1_length = source.param1.size();
	dest->param2_ptr = source.param2.begin();
	dest->param2_length = source.param2.size();

	if (tenantPrefix->present) {
		dest->param1_ptr += tenantPrefix->prefix.key_length;
		dest->param1_length -= tenantPrefix->prefix.key_length;
		if (dest->type == FDB_BG_MUTATION_TYPE_CLEAR_RANGE) {
			dest->param2_ptr += tenantPrefix->prefix.key_length;
			dest->param2_length -= tenantPrefix->prefix.key_length;
		}
	}
}

void setBGMutations(FDBBGMutation*** mutationsOut,
                    int* mutationCountOut,
                    FDBBGTenantPrefix const* tenantPrefix,
                    Arena& ar,
                    const GranuleDeltas& deltas) {
	// convert mutations from MutationsAndVersionRef to single mutations
	int mutationCount = 0;
	for (auto& it : deltas) {
		mutationCount += it.mutations.size();
	}
	*mutationCountOut = mutationCount;
	if (mutationCount > 0) {
		*mutationsOut = new (ar) FDBBGMutation*[mutationCount];
		int mutationIdx = 0;
		for (auto& it : deltas) {
			for (auto& m : it.mutations) {
				(*mutationsOut)[mutationIdx] = new (ar) FDBBGMutation();
				setBGMutation((*mutationsOut)[mutationIdx], it.version, tenantPrefix, m);
				mutationIdx++;
			}
		}
		ASSERT(mutationIdx == *mutationCountOut);
	}
}

ACTOR Future<ApiResponse> handleReadBgDescriptionRequest(ISingleThreadTransaction* tr, ApiRequest req) {
	auto input = req.getTypedRequest<FDBReadBGDescriptionRequest>();
	Optional<Version> readVersion;
	state Version readVersionOut;
	if (input->read_version != latestVersion) {
		readVersion = input->read_version;
	}
	Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
	    wait(tr->readBlobGranules((KeyRangeRef&)input->key_range, input->begin_version, readVersion, &readVersionOut));

	ApiResponse res = ApiResponse::create<FDBReadBGDescriptionResponse>(req);
	Arena& arena = res.arena();
	arena.dependsOn(chunks.arena());

	auto resp = res.getTypedResponse<FDBReadBGDescriptionResponse>();
	resp->desc_count = chunks.size();
	resp->read_version = readVersionOut;

	resp->desc_arr = new (arena) FDBBGFileDescription*[chunks.size()];
	for (int chunkIdx = 0; chunkIdx < chunks.size(); chunkIdx++) {
		const BlobGranuleChunkRef& chunk = chunks[chunkIdx];
		resp->desc_arr[chunkIdx] = new (arena) FDBBGFileDescription();
		FDBBGFileDescription& desc = *resp->desc_arr[chunkIdx];

		// set key range
		desc.key_range.begin_key = chunk.keyRange.begin.begin();
		desc.key_range.begin_key_length = chunk.keyRange.begin.size();
		desc.key_range.end_key = chunk.keyRange.end.begin();
		desc.key_range.end_key_length = chunk.keyRange.end.size();

		// set tenant metadata
		if (chunk.tenantPrefix.present()) {
			desc.tenant_prefix.present = true;
			desc.tenant_prefix.prefix.key = chunk.tenantPrefix.get().begin();
			desc.tenant_prefix.prefix.key_length = chunk.tenantPrefix.get().size();

			desc.key_range.begin_key += desc.tenant_prefix.prefix.key_length;
			desc.key_range.begin_key_length -= desc.tenant_prefix.prefix.key_length;
			desc.key_range.end_key += desc.tenant_prefix.prefix.key_length;
			desc.key_range.end_key_length -= desc.tenant_prefix.prefix.key_length;
		} else {
			desc.tenant_prefix.present = false;
		}

		// snapshot file
		desc.snapshot_present = chunk.snapshotFile.present();
		if (desc.snapshot_present) {
			desc.snapshot_file_pointer = new (arena) FDBBGFilePointer();
			setBlobFilePointer(desc.snapshot_file_pointer, chunk.snapshotFile.get(), arena);
		}

		// delta files
		desc.delta_file_count = chunk.deltaFiles.size();
		if (chunk.deltaFiles.size()) {
			desc.delta_files = new (arena) FDBBGFilePointer*[chunk.deltaFiles.size()];
			for (int d = 0; d < chunk.deltaFiles.size(); d++) {
				desc.delta_files[d] = new (arena) FDBBGFilePointer();
				setBlobFilePointer(desc.delta_files[d], chunk.deltaFiles[d], arena);
			}
		}

		setBGMutations(
		    &desc.memory_mutations, &desc.memory_mutation_count, &desc.tenant_prefix, arena, chunk.newDeltas);
	}
	return res;
}

} // namespace

Future<ApiResponse> handleEvolvableApiRequest(ISingleThreadTransaction* tr, ApiRequest req) {
	switch (req.getType()) {
	case FDBApiRequest_ReadBGDescriptionRequest:
		return handleReadBgDescriptionRequest(tr, req);
		break;
	default:
		return unknown_api_request();
	}
}
