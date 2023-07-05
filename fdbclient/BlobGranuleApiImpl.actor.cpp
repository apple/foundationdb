/*
 * BlobGranuleApiImpl.actor.cpp
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#include "fdbclient/BlobGranuleApiImpl.h"
#include "fdbclient/ApiRequestHandler.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleFiles.h"
#include "flow/actorcompiler.h" // has to be last include

namespace {

/* -------------------------------------------------------------------------------------------
 * Converting Native Client structures to public API structures
 */

void nativeToApiBGEncryptionKey(FDBBGEncryptionKey* dest, const BlobGranuleCipherKey& source) {
	dest->domain_id = source.encryptDomainId;
	dest->base_key_id = source.baseCipherId;
	dest->base_kcv = source.baseCipherKCV;
	dest->random_salt = source.salt;
	dest->base_key.key = source.baseCipher.begin();
	dest->base_key.key_length = source.baseCipher.size();
}

void nativeToApiBGEncryptionKeyCtx(FDBBGEncryptionCtxV2* dest, const BlobGranuleCipherKeysCtx& source, Arena& ar) {
	dest->textKey = new (ar) FDBBGEncryptionKey();
	nativeToApiBGEncryptionKey(dest->textKey, source.textCipherKey);
	dest->textKCV = source.textCipherKey.baseCipherKCV;
	dest->headerKey = new (ar) FDBBGEncryptionKey();
	nativeToApiBGEncryptionKey(dest->headerKey, source.headerCipherKey);
	dest->headerKCV = source.headerCipherKey.baseCipherKCV;
	dest->iv.key = source.ivRef.begin();
	dest->iv.key_length = source.ivRef.size();
}

void nativeToApiBGEncryptionCipherKey(FDBKey* dest, Reference<BlobCipherKey> source, Arena& ar) {
	dest->key = new (ar) uint8_t[AES_256_KEY_LENGTH];
	dest->key_length = AES_256_KEY_LENGTH;
	memcpy((void*)dest->key, source->rawBaseCipher(), AES_256_KEY_LENGTH);
}

void nativeToApiBGEncryptionKeys(FDBBGEncryptionKeys* dest, const BlobGranuleCipherKeysCtx& source, Arena& ar) {
	BlobGranuleFileEncryptionKeys derivedKeys = getEncryptBlobCipherKey(source);
	nativeToApiBGEncryptionCipherKey(&dest->headerKey, derivedKeys.headerCipherKey, ar);
	nativeToApiBGEncryptionCipherKey(&dest->textKey, derivedKeys.textCipherKey, ar);
}

void nativeToApiBGFilePointer(FDBBGFilePointerV2* dest, const BlobFilePointerRef& source, Arena& ar) {
	dest->filename_ptr = source.filename.begin();
	dest->filename_length = source.filename.size();
	dest->file_offset = source.offset;
	dest->file_length = source.length;
	dest->full_file_length = source.fullFileLength;
	dest->file_version = source.fileVersion;

	// handle encryption
	dest->encryption_ctx = nullptr;
	dest->encryption_keys = nullptr;
	if (source.cipherKeysCtx.present()) {
		dest->encryption_ctx = new (ar) FDBBGEncryptionCtxV2();
		nativeToApiBGEncryptionKeyCtx(dest->encryption_ctx, source.cipherKeysCtx.get(), ar);
		dest->encryption_keys = new (ar) FDBBGEncryptionKeys();
		nativeToApiBGEncryptionKeys(dest->encryption_keys, source.cipherKeysCtx.get(), ar);
	}
}

void removeTenantPrefix(const uint8_t** outBegin,
                        int* outLen,
                        const KeyRef& key,
                        const Optional<KeyRef>& tenantPrefix) {
	if (tenantPrefix.present()) {
		*outBegin = key.begin() + tenantPrefix.get().size();
		*outLen = key.size() - tenantPrefix.get().size();
	} else {
		*outBegin = key.begin();
		*outLen = key.size();
	}
}

void nativeToApiBGMutation(FDBBGMutation* dest,
                           const MutationRef& source,
                           Version version,
                           const Optional<KeyRef>& tenantPrefix) {
	dest->version = version;
	dest->type = source.type;
	removeTenantPrefix(&dest->param1_ptr, &dest->param1_length, source.param1, tenantPrefix);
	if (source.type == FDB_BG_MUTATION_TYPE_CLEAR_RANGE) {
		removeTenantPrefix(&dest->param2_ptr, &dest->param2_length, source.param2, tenantPrefix);
	} else {
		dest->param2_ptr = source.param2.begin();
		dest->param2_length = source.param2.size();
	}
}

void nativeToApiBGMutations(FDBBGMutation*** mutationsOut,
                            int* mutationCountOut,
                            const GranuleDeltas& deltas,
                            const Optional<KeyRef>& tenantPrefix,
                            Arena& ar) {
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
				nativeToApiBGMutation((*mutationsOut)[mutationIdx], m, it.version, tenantPrefix);
				mutationIdx++;
			}
		}
		ASSERT(mutationIdx == *mutationCountOut);
	}
}

void nativeToApiBGFileDescription(FDBBGFileDescriptionV2* dest, const BlobGranuleChunkRef& source, Arena& ar) {
	// set key range
	removeTenantPrefix(
	    &dest->key_range.begin_key, &dest->key_range.begin_key_length, source.keyRange.begin, source.tenantPrefix);
	removeTenantPrefix(
	    &dest->key_range.end_key, &dest->key_range.end_key_length, source.keyRange.end, source.tenantPrefix);

	// set tenant metadata
	if (source.tenantPrefix.present()) {
		dest->tenant_prefix.present = true;
		dest->tenant_prefix.prefix.key = source.tenantPrefix.get().begin();
		dest->tenant_prefix.prefix.key_length = source.tenantPrefix.get().size();
	} else {
		dest->tenant_prefix.present = false;
	}

	// snapshot file
	dest->snapshot_file_pointer = nullptr;
	if (source.snapshotFile.present()) {
		dest->snapshot_file_pointer = new (ar) FDBBGFilePointerV2();
		nativeToApiBGFilePointer(dest->snapshot_file_pointer, source.snapshotFile.get(), ar);
	}

	// delta files
	dest->delta_file_count = source.deltaFiles.size();
	if (source.deltaFiles.size()) {
		dest->delta_files = new (ar) FDBBGFilePointerV2*[source.deltaFiles.size()];
		for (int d = 0; d < source.deltaFiles.size(); d++) {
			dest->delta_files[d] = new (ar) FDBBGFilePointerV2();
			nativeToApiBGFilePointer(dest->delta_files[d], source.deltaFiles[d], ar);
		}
	}

	nativeToApiBGMutations(
	    &dest->memory_mutations, &dest->memory_mutation_count, source.newDeltas, source.tenantPrefix, ar);
}

/* -------------------------------------------------------------------------------------------
 * Converting public API structures to Native Client structures
 */

void apiToNativeBGTenantPrefix(Optional<KeyRef>& dest, FDBBGTenantPrefix const* source) {
	if (source->present) {
		dest = StringRef(source->prefix.key, source->prefix.key_length);
	}
}

void apiToNativeBGEncryptionKey(BlobGranuleCipherKey& dest, FDBBGEncryptionKey const* source) {
	dest.encryptDomainId = source->domain_id;
	dest.baseCipherId = source->base_key_id;
	dest.baseCipherKCV = source->base_kcv;
	dest.salt = source->random_salt;
	dest.baseCipher = StringRef(source->base_key.key, source->base_key.key_length);
}

void apiToNativeBGEncryptionKeyCtxV1(Optional<BlobGranuleCipherKeysCtx>& dest, FDBBGEncryptionCtxV1 const* source) {
	if (source) {
		dest = BlobGranuleCipherKeysCtx();
		apiToNativeBGEncryptionKey(dest.get().textCipherKey, &source->textKey);
		apiToNativeBGEncryptionKey(dest.get().headerCipherKey, &source->headerKey);
		dest.get().ivRef = StringRef(source->iv.key, source->iv.key_length);
	}
}

void apiToNativeBGEncryptionKeyCtxV2(Optional<BlobGranuleCipherKeysCtx>& dest, FDBBGEncryptionCtxV2 const* source) {
	if (source) {
		dest = BlobGranuleCipherKeysCtx();
		apiToNativeBGEncryptionKey(dest.get().textCipherKey, source->textKey);
		apiToNativeBGEncryptionKey(dest.get().headerCipherKey, source->headerKey);
		dest.get().ivRef = StringRef(source->iv.key, source->iv.key_length);
	}
}

void apiToNativeBGFilePointer(BlobFilePointerRef& dest, const FDBBGFilePointerV2* source) {
	dest.filename = StringRef(source->filename_ptr, source->filename_length);
	dest.offset = source->file_offset;
	dest.length = source->file_length;
	dest.fullFileLength = source->full_file_length;
	dest.fileVersion = source->file_version;

	apiToNativeBGEncryptionKeyCtxV2(dest.cipherKeysCtx, source->encryption_ctx);
}

KeyRef applyTenantPrefix(const KeyRef& key, const Optional<KeyRef>& tenantPrefix, Arena& ar) {
	if (tenantPrefix.present()) {
		return key.withPrefix(tenantPrefix.get(), ar);
	} else {
		return KeyRef(ar, key);
	}
}

void apiToNativeBGMutation(MutationRef& dest,
                           const FDBBGMutation* source,
                           const Optional<KeyRef>& tenantPrefix,
                           Arena& ar) {
	dest.type = source->type;
	dest.param1 = applyTenantPrefix(KeyRef(source->param1_ptr, source->param1_length), tenantPrefix, ar);
	if (dest.type == FDB_BG_MUTATION_TYPE_CLEAR_RANGE) {
		dest.param2 = applyTenantPrefix(KeyRef(source->param2_ptr, source->param2_length), tenantPrefix, ar);
	} else {
		dest.param2 = KeyRef(source->param2_ptr, source->param2_length);
	}
}

void apiToNativeBGMutations(GranuleDeltas& deltas,
                            FDBBGMutation* const* mutations,
                            int mutationCount,
                            const Optional<KeyRef>& tenantPrefix,
                            Arena& ar) {
	if (mutationCount == 0) {
		return;
	}

	// Determine the number of equal version mutation sequences
	int numSeq = 1;
	for (int i = 1; i < mutationCount; i++) {
		if (mutations[i]->version != mutations[i - 1]->version) {
			numSeq++;
		}
	}

	// For every mutation sequence we create one element in the delta array
	deltas.resize(ar, numSeq);
	int beginIdx = 0;
	for (int i = 0; i < numSeq; i++) {
		MutationsAndVersionRef& delta = deltas[i];
		delta.version = mutations[beginIdx]->version;

		// Find the end of the current mutation sequence
		int endIdx = beginIdx + 1;
		while (endIdx < mutationCount && mutations[endIdx]->version == delta.version) {
			endIdx++;
		}

		// Fill the mutations array
		int seqLen = endIdx - beginIdx;
		delta.mutations.resize(ar, seqLen);
		for (int j = 0; j < seqLen; j++) {
			apiToNativeBGMutation(delta.mutations[j], mutations[beginIdx + j], tenantPrefix, ar);
		}
		beginIdx = endIdx;
	}

	ASSERT(beginIdx == mutationCount);
}

void apiToNativeBGFileDescription(BlobGranuleChunkRef& dest,
                                  FDBBGFileDescriptionV2* source,
                                  Version version,
                                  Arena& ar) {
	apiToNativeBGTenantPrefix(dest.tenantPrefix, &source->tenant_prefix);

	dest.keyRange = KeyRangeRef(
	    applyTenantPrefix(
	        KeyRef(source->key_range.begin_key, source->key_range.begin_key_length), dest.tenantPrefix, ar),
	    applyTenantPrefix(KeyRef(source->key_range.end_key, source->key_range.end_key_length), dest.tenantPrefix, ar));

	dest.snapshotVersion = invalidVersion;
	if (source->snapshot_file_pointer) {
		dest.snapshotFile = BlobFilePointerRef();
		apiToNativeBGFilePointer(dest.snapshotFile.get(), source->snapshot_file_pointer);
		dest.snapshotVersion = dest.snapshotFile->fileVersion;
	}

	if (source->delta_file_count > 0) {
		dest.deltaFiles.resize(ar, source->delta_file_count);
		for (int i = 0; i < source->delta_file_count; i++) {
			apiToNativeBGFilePointer(dest.deltaFiles[i], source->delta_files[i]);
		}
	}

	apiToNativeBGMutations(
	    dest.newDeltas, source->memory_mutations, source->memory_mutation_count, dest.tenantPrefix, ar);

	dest.includedVersion = version;
}

Standalone<VectorRef<BlobGranuleChunkRef>> apiToNativeBGFileDescriptions(FDBBGFileDescriptionV2** desc_arr,
                                                                         int desc_count,
                                                                         Version version,
                                                                         const Arena& srcAr) {
	Standalone<VectorRef<BlobGranuleChunkRef>> res;
	res.arena().dependsOn(srcAr);
	res.resize(res.arena(), desc_count);
	for (int i = 0; i < desc_count; i++) {
		apiToNativeBGFileDescription(res[i], desc_arr[i], version, res.arena());
	}
	return res;
}

ReadBGMutationsApiResult createBGMutationsApiResult(Standalone<VectorRef<GranuleMutationRef>> mutationsResult) {
	auto ret = ReadBGMutationsApiResult::create(FDBApiResult_ReadBGMutations);
	ret.arena().dependsOn(mutationsResult.arena());
	auto data = ret.getPtr();
	static_assert(sizeof(FDBBGMutation) == sizeof(GranuleMutationRef));
	data->mutation_arr = (FDBBGMutation*)mutationsResult.begin();
	data->mutation_count = mutationsResult.size();
	return ret;
}

/* -------------------------------------------------------------------------------------------
 * Loading blob granule files
 */

struct GranuleLoadFreeHandle : NonCopyable, ReferenceCounted<GranuleLoadFreeHandle> {
	const FDBReadBlobGranuleContext* granuleContext;
	int64_t loadId;

	GranuleLoadFreeHandle(const FDBReadBlobGranuleContext* granuleContext, int64_t loadId)
	  : granuleContext(granuleContext), loadId(loadId) {}

	~GranuleLoadFreeHandle() { granuleContext->free_load_f(loadId, granuleContext->userContext); }
};

struct GranuleLoadIds {
	Optional<int64_t> snapshotId;
	std::vector<int64_t> deltaIds;
	std::vector<Reference<GranuleLoadFreeHandle>> freeHandles;
};

void startLoadingBlobGranule(const FDBReadBlobGranuleContext* granuleContext,
                             const BlobGranuleChunkRef& chunk,
                             GranuleLoadIds& loadIds) {

	// Start load process for all files in chunk
	if (chunk.snapshotFile.present()) {
		std::string snapshotFname = chunk.snapshotFile.get().filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.snapshotFile.get().offset == 0);
		ASSERT(chunk.snapshotFile.get().length == chunk.snapshotFile.get().fullFileLength);
		loadIds.snapshotId = granuleContext->start_load_f(snapshotFname.c_str(),
		                                                  snapshotFname.size(),
		                                                  chunk.snapshotFile.get().offset,
		                                                  chunk.snapshotFile.get().length,
		                                                  chunk.snapshotFile.get().fullFileLength,
		                                                  granuleContext->userContext);
		loadIds.freeHandles.push_back(makeReference<GranuleLoadFreeHandle>(granuleContext, loadIds.snapshotId.get()));
	}
	loadIds.deltaIds.reserve(chunk.deltaFiles.size());
	for (int deltaFileIdx = 0; deltaFileIdx < chunk.deltaFiles.size(); deltaFileIdx++) {
		std::string deltaFName = chunk.deltaFiles[deltaFileIdx].filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.deltaFiles[deltaFileIdx].offset == 0);
		ASSERT(chunk.deltaFiles[deltaFileIdx].length == chunk.deltaFiles[deltaFileIdx].fullFileLength);
		int64_t deltaLoadId = granuleContext->start_load_f(deltaFName.c_str(),
		                                                   deltaFName.size(),
		                                                   chunk.deltaFiles[deltaFileIdx].offset,
		                                                   chunk.deltaFiles[deltaFileIdx].length,
		                                                   chunk.deltaFiles[deltaFileIdx].fullFileLength,
		                                                   granuleContext->userContext);
		loadIds.deltaIds.push_back(deltaLoadId);
		loadIds.freeHandles.push_back(makeReference<GranuleLoadFreeHandle>(granuleContext, deltaLoadId));
	}
}

RangeResult loadAndMaterializeBlobGranulesImpl(const Standalone<VectorRef<BlobGranuleChunkRef>>& files,
                                               const KeyRangeRef& keyRange,
                                               Version beginVersion,
                                               Version readVersion,
                                               const FDBReadBlobGranuleContext* granuleContext,
                                               GranuleMaterializeStats& stats) {
	int64_t parallelism = granuleContext->granuleParallelism;
	if (parallelism < 1) {
		parallelism = 1;
	}
	if (parallelism >= CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM) {
		parallelism = CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM;
	}

	GranuleLoadIds loadIds[files.size()];

	// Kick off first file reads if parallelism > 1
	for (int i = 0; i < parallelism - 1 && i < files.size(); i++) {
		startLoadingBlobGranule(granuleContext, files[i], loadIds[i]);
	}
	RangeResult results;
	for (int chunkIdx = 0; chunkIdx < files.size(); chunkIdx++) {
		// Kick off files for this granule if parallelism == 1, or future granule if parallelism > 1
		if (chunkIdx + parallelism - 1 < files.size()) {
			startLoadingBlobGranule(
			    granuleContext, files[chunkIdx + parallelism - 1], loadIds[chunkIdx + parallelism - 1]);
		}

		RangeResult chunkRows;

		// once all loads kicked off, load data for chunk
		Optional<StringRef> snapshotData;
		if (files[chunkIdx].snapshotFile.present()) {
			snapshotData =
			    StringRef(granuleContext->get_load_f(loadIds[chunkIdx].snapshotId.get(), granuleContext->userContext),
			              files[chunkIdx].snapshotFile.get().length);
			if (!snapshotData.get().begin()) {
				throw blob_granule_file_load_error();
			}
		}

		std::vector<StringRef> deltaData;
		deltaData.resize(files[chunkIdx].deltaFiles.size());
		for (int i = 0; i < files[chunkIdx].deltaFiles.size(); i++) {
			deltaData[i] =
			    StringRef(granuleContext->get_load_f(loadIds[chunkIdx].deltaIds[i], granuleContext->userContext),
			              files[chunkIdx].deltaFiles[i].length);
			// null data is error
			if (!deltaData[i].begin()) {
				throw blob_granule_file_load_error();
			}
		}

		// materialize rows from chunk
		chunkRows = materializeBlobGranule(
		    files[chunkIdx], keyRange, beginVersion, readVersion, snapshotData, deltaData, stats);

		results.arena().dependsOn(chunkRows.arena());
		results.append(results.arena(), chunkRows.begin(), chunkRows.size());

		// free once done by forcing FreeHandles to trigger
		loadIds[chunkIdx].freeHandles.clear();
	}
	return results;
}

ACTOR Future<ApiResult> readBlobGranuleDescriptionsImpl(ISingleThreadTransaction* tr, ApiRequest req) {
	auto input = req.getTypedRequest<FDBReadBGDescriptionRequest>();
	Optional<Version> readVersion;
	state Version readVersionOut;
	if (input->read_version != latestVersion) {
		readVersion = input->read_version;
	}
	Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
	    wait(tr->readBlobGranules((KeyRangeRef&)input->key_range, input->begin_version, readVersion, &readVersionOut));

	auto res = TypedApiResult<FDBReadBGDescriptionResult>::create(FDBApiResult_ReadBGDescription);
	Arena& arena = res.arena();
	arena.dependsOn(chunks.arena());

	auto resp = res.getPtr();
	resp->desc_count = chunks.size();
	resp->read_version = readVersionOut;

	resp->desc_arr = new (arena) FDBBGFileDescriptionV2*[chunks.size()];
	for (int chunkIdx = 0; chunkIdx < chunks.size(); chunkIdx++) {
		const BlobGranuleChunkRef& chunk = chunks[chunkIdx];
		resp->desc_arr[chunkIdx] = new (arena) FDBBGFileDescriptionV2();
		nativeToApiBGFileDescription(resp->desc_arr[chunkIdx], chunk, arena);
	}
	return res;
}

} // namespace

ReadRangeApiResult parseBlobGranulesSnapshotFileV1(StringRef fileName,
                                                   FDBBGTenantPrefix const* tenant_prefix,
                                                   FDBBGEncryptionCtxV1 const* encryption_ctx) {
	Optional<KeyRef> tenantPrefix;
	Optional<BlobGranuleCipherKeysCtx> encryptionCtx;
	apiToNativeBGTenantPrefix(tenantPrefix, tenant_prefix);
	apiToNativeBGEncryptionKeyCtxV1(encryptionCtx, encryption_ctx);
	auto parsedData = bgReadSnapshotFile(fileName, tenantPrefix, encryptionCtx);
	return createReadRangeApiResult(parsedData);
}

ReadBGMutationsApiResult parseBlobGranulesDeltaFileV1(StringRef fileName,
                                                      FDBBGTenantPrefix const* tenant_prefix,
                                                      FDBBGEncryptionCtxV1 const* encryption_ctx) {
	Optional<KeyRef> tenantPrefix;
	Optional<BlobGranuleCipherKeysCtx> encryptionCtx;
	apiToNativeBGTenantPrefix(tenantPrefix, tenant_prefix);
	apiToNativeBGEncryptionKeyCtxV1(encryptionCtx, encryption_ctx);
	auto parsedData = bgReadDeltaFile(fileName, tenantPrefix, encryptionCtx);
	return createBGMutationsApiResult(parsedData);
}

ReadRangeApiResult parseBlobGranulesSnapshotFileV2(StringRef fileName,
                                                   FDBBGTenantPrefix const* tenant_prefix,
                                                   FDBBGEncryptionCtxV2 const* encryption_ctx) {
	Optional<KeyRef> tenantPrefix;
	Optional<BlobGranuleCipherKeysCtx> encryptionCtx;
	apiToNativeBGTenantPrefix(tenantPrefix, tenant_prefix);
	apiToNativeBGEncryptionKeyCtxV2(encryptionCtx, encryption_ctx);
	auto parsedData = bgReadSnapshotFile(fileName, tenantPrefix, encryptionCtx);
	return createReadRangeApiResult(parsedData);
}

ReadBGMutationsApiResult parseBlobGranulesDeltaFileV2(StringRef fileName,
                                                      FDBBGTenantPrefix const* tenant_prefix,
                                                      FDBBGEncryptionCtxV2 const* encryption_ctx) {
	Optional<KeyRef> tenantPrefix;
	Optional<BlobGranuleCipherKeysCtx> encryptionCtx;
	apiToNativeBGTenantPrefix(tenantPrefix, tenant_prefix);
	apiToNativeBGEncryptionKeyCtxV2(encryptionCtx, encryption_ctx);
	auto parsedData = bgReadDeltaFile(fileName, tenantPrefix, encryptionCtx);
	return createBGMutationsApiResult(parsedData);
}

ReadRangeApiResult loadAndMaterializeBlobGranules(const ReadBGDescriptionsApiResult& blobGranuleDescr,
                                                  const KeyRangeRef& keyRange,
                                                  Version beginVersion,
                                                  const FDBReadBlobGranuleContext* granuleContext) {
	auto descrData = blobGranuleDescr.getData();
	Standalone<VectorRef<BlobGranuleChunkRef>> fileRefs = apiToNativeBGFileDescriptions(
	    descrData->desc_arr, descrData->desc_count, descrData->read_version, blobGranuleDescr.arena());
	GranuleMaterializeStats stats;
	auto rangeResult = loadAndMaterializeBlobGranulesImpl(
	    fileRefs, keyRange, beginVersion, descrData->read_version, granuleContext, stats);
	// TODO: Reimplement blob granule materiation stats tracing
	return createReadRangeApiResult(rangeResult);
}

Future<ApiResult> readBlobGranuleDescriptions(ISingleThreadTransaction* tr, ApiRequest req) {
	return readBlobGranuleDescriptionsImpl(tr, req);
}

/* -------------------------------------------------------------------------------------------
 * Conversion to previous version of Blob Granule API structures
 */

namespace {

void convertBGFilePointerToV1(FDBBGFilePointerV1* dest, const FDBBGFilePointerV2* source, Arena& ar) {
	dest->filename_ptr = source->filename_ptr;
	dest->filename_length = source->filename_length;
	dest->file_offset = source->file_offset;
	dest->file_length = source->file_length;
	dest->full_file_length = source->full_file_length;
	dest->file_version = source->file_version;
	dest->encryption_ctx.present = (source->encryption_ctx != nullptr);
	if (dest->encryption_ctx.present) {
		dest->encryption_ctx.textKey = *source->encryption_ctx->textKey;
		dest->encryption_ctx.textKCV = source->encryption_ctx->textKCV;
		dest->encryption_ctx.headerKey = *source->encryption_ctx->headerKey;
		dest->encryption_ctx.headerKCV = source->encryption_ctx->headerKCV;
		dest->encryption_ctx.iv = source->encryption_ctx->iv;
	}
}

void convertBGFileDescriptionToV1(FDBBGFileDescriptionV1* dest, const FDBBGFileDescriptionV2* source, Arena& ar) {
	dest->key_range = source->key_range;
	dest->tenant_prefix = source->tenant_prefix;
	dest->snapshot_present = (source->snapshot_file_pointer != nullptr);
	if (dest->snapshot_present) {
		convertBGFilePointerToV1(&dest->snapshot_file_pointer, source->snapshot_file_pointer, ar);
	}
	dest->delta_file_count = source->delta_file_count;
	if (source->delta_file_count > 0) {
		dest->delta_files = new (ar) FDBBGFilePointerV1[source->delta_file_count];
		for (int i = 0; i < source->delta_file_count; i++) {
			convertBGFilePointerToV1(&dest->delta_files[i], source->delta_files[i], ar);
		}
	}
	dest->memory_mutation_count = source->memory_mutation_count;
	if (source->memory_mutation_count > 0) {
		dest->memory_mutations = new (ar) FDBBGMutation[source->memory_mutation_count];
		for (int i = 0; i < source->memory_mutation_count; i++) {
			dest->memory_mutations[i] = *source->memory_mutations[i];
		}
	}
}

} // namespace

ReadBGDescriptionsApiResultV1 convertBlobGranulesDescriptionsToV1(ReadBGDescriptionsApiResult input) noexcept {
	ReadBGDescriptionsApiResultV1 res;
	res.input = input;
	auto source = input.getPtr();
	res.desc_count = source->desc_count;
	if (source->desc_count > 0) {
		res.desc_arr = new (res.arena) FDBBGFileDescriptionV1[source->desc_count];
		for (int i = 0; i < source->desc_count; i++) {
			convertBGFileDescriptionToV1(&res.desc_arr[i], source->desc_arr[i], res.arena);
		}
	}
	return res;
}