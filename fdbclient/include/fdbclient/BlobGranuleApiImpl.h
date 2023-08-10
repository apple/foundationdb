/*
 * BlobGranuleFilesApi.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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