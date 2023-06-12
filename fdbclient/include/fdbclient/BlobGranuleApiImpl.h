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

ReadRangeApiResult parseBlobGranulesSnapshotFile(StringRef fileName,
                                                 FDBBGTenantPrefix const* tenant_prefix,
                                                 FDBBGEncryptionCtx const* encryption_ctx);

ReadBGMutationsApiResult parseBlobGranulesDeltaFile(StringRef fileName,
                                                    FDBBGTenantPrefix const* tenant_prefix,
                                                    FDBBGEncryptionCtx const* encryption_ctx);

ReadRangeApiResult loadAndMaterializeBlobGranules(const ReadBGDescriptionsApiResult& blobGranuleDescr,
                                                  const KeyRangeRef& keyRange,
                                                  Version beginVersion,
                                                  const FDBReadBlobGranuleContext* granuleContext);

Future<ApiResult> readBlobGranuleDescriptions(ISingleThreadTransaction* tr, ApiRequest req);

#endif