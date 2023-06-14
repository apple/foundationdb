/*
 * BlobGranuleFilesApi.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef __FDBCLIENT_BLOB_GRANULE_FILES_API_H__
#define __FDBCLIENT_BLOB_GRANULE_FILES_API_H__
#pragma once

#include "fdbclient/IClientApi.h"
#include "fdbclient/ApiRequest.h"
#include "fdbclient/ISingleThreadTransaction.h"

ReadRangeApiResult parseBlobGranulesSnapshotFileV1(StringRef fileName,
                                                   FDBBGTenantPrefix const* tenant_prefix,
                                                   FDBBGEncryptionCtxV1 const* encryption_ctx);

ReadRangeApiResult parseBlobGranulesSnapshotFileV2(StringRef fileName,
                                                   FDBBGTenantPrefix const* tenant_prefix,
                                                   FDBBGEncryptionCtxV2 const* encryption_ctx);

ReadBGMutationsApiResult parseBlobGranulesDeltaFileV1(StringRef fileName,
                                                      FDBBGTenantPrefix const* tenant_prefix,
                                                      FDBBGEncryptionCtxV1 const* encryption_ctx);

ReadBGMutationsApiResult parseBlobGranulesDeltaFileV2(StringRef fileName,
                                                      FDBBGTenantPrefix const* tenant_prefix,
                                                      FDBBGEncryptionCtxV2 const* encryption_ctx);

ReadRangeApiResult loadAndMaterializeBlobGranules(const ReadBGDescriptionsApiResult& blobGranuleDescr,
                                                  const KeyRangeRef& keyRange,
                                                  Version beginVersion,
                                                  const FDBReadBlobGranuleContext* granuleContext);

Future<ApiResult> readBlobGranuleDescriptions(ISingleThreadTransaction* tr, ApiRequest req);

struct ReadBGDescriptionsApiResultV1 {
	Arena arena;
	FDBBGFileDescriptionV1* desc_arr;
	int desc_count;
	ReadBGDescriptionsApiResult input;
};

ReadBGDescriptionsApiResultV1 convertBlobGranulesDescriptionsToV1(ReadBGDescriptionsApiResult input) noexcept;

#endif