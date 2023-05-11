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

void setEncryptionKeyCtx(FDBBGEncryptionCtx* dest, const BlobGranuleCipherKeysCtx& source) {
	dest->present = true;
	setEncryptionKey(&dest->textKey, source.textCipherKey);
	dest->textKCV = source.textCipherKey.baseCipherKCV;
	setEncryptionKey(&dest->headerKey, source.headerCipherKey);
	dest->headerKCV = source.headerCipherKey.baseCipherKCV;
	dest->iv.key = source.ivRef.begin();
	dest->iv.key_length = source.ivRef.size();
}

void setBlobFilePointer(FDBBGFilePointer* dest, const BlobFilePointerRef& source) {
	dest->filename_ptr = source.filename.begin();
	dest->filename_length = source.filename.size();
	dest->file_offset = source.offset;
	dest->file_length = source.length;
	dest->full_file_length = source.fullFileLength;
	dest->file_version = source.fileVersion;

	// handle encryption
	if (source.cipherKeysCtx.present()) {
		setEncryptionKeyCtx(&dest->encryption_ctx, source.cipherKeysCtx.get());
	} else {
		dest->encryption_ctx.present = false;
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

void setBGMutations(FDBBGMutation** mutationsOut,
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
		*mutationsOut = new (ar) FDBBGMutation[mutationCount];
		mutationCount = 0;
		for (auto& it : deltas) {
			for (auto& m : it.mutations) {
				setBGMutation(&((*mutationsOut)[mutationCount]), it.version, tenantPrefix, m);
				mutationCount++;
			}
		}
		ASSERT(mutationCount == *mutationCountOut);
	}
}

template <class ResponseType>
ApiResponse createApiResponse(const ApiRequestRef& req) {
	Arena arena(sizeof(ResponseType) + sizeof(FDBResponseHeader));
	ResponseType* resp = new (arena) ResponseType();
	resp->header = new (arena) FDBResponseHeader();
	resp->header->request_type = req.getType();
	resp->header->allocator.handle = arena.getPtr();
	resp->header->allocator.ifc = req.getAllocatorInterface();
	return ApiResponse((FDBResponse*)resp, arena);
}

ACTOR Future<ApiResponse> handleReadBgDescriptionRequest(ISingleThreadTransaction* tr, ApiRequest req) {
	auto input = req.getRequest<FDBReadBGDescriptionRequest>();
	Optional<Version> readVersion;
	state Version readVersionOut;
	if (input->read_version != latestVersion) {
		readVersion = input->read_version;
	}
	Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
	    wait(tr->readBlobGranules((KeyRangeRef&)input->key_range, input->begin_version, readVersion, &readVersionOut));

	ApiResponse res = createApiResponse<FDBReadBGDescriptionResponse>(req);
	res.arena().dependsOn(chunks.arena());

	auto resp = res.getResponse<FDBReadBGDescriptionResponse>();
	resp->desc_count = chunks.size();
	resp->read_version = readVersionOut;

	resp->desc_arr = new (res.arena()) FDBBGFileDescription*[chunks.size()];
	for (int chunkIdx = 0; chunkIdx < chunks.size(); chunkIdx++) {
		const BlobGranuleChunkRef& chunk = chunks[chunkIdx];
		resp->desc_arr[chunkIdx] = new (res.arena()) FDBBGFileDescription();
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
			setBlobFilePointer(&desc.snapshot_file_pointer, chunk.snapshotFile.get());
		}

		// delta files
		desc.delta_file_count = chunk.deltaFiles.size();
		if (chunk.deltaFiles.size()) {
			desc.delta_files = new (res.arena()) FDBBGFilePointer[chunk.deltaFiles.size()];
			for (int d = 0; d < chunk.deltaFiles.size(); d++) {
				setBlobFilePointer(&desc.delta_files[d], chunk.deltaFiles[d]);
			}
		}

		setBGMutations(
		    &desc.memory_mutations, &desc.memory_mutation_count, &desc.tenant_prefix, res.arena(), chunk.newDeltas);
	}
	return res;
}

} // namespace

Future<ApiResponse> handleEvolvableApiRequest(ISingleThreadTransaction* tr, ApiRequest req) {
	switch (req.getType()) {
	case FDB_API_READ_BG_DESCRIPTION_REQUEST:
		return handleReadBgDescriptionRequest(tr, req);
		break;
	default:
		return unknown_api_request();
	}
}
