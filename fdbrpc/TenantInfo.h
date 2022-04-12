/*
 * TenantInfo.h
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

#pragma once
#ifndef FDBRPC_TENANTINFO_H_
#define FDBRPC_TENANTINFO_H_
#include "flow/Arena.h"

typedef StringRef TenantNameRef;
typedef Standalone<TenantNameRef> TenantName;

struct TenantInfo {
	static const int64_t INVALID_TENANT = -1;

	Optional<TenantName> name;
	int64_t tenantId;
	// this field is not serialized and instead set by FlowTransport during
	// deserialization. This field indicates whether the client is trusted.
	// Untrusted clients are generally expected to set a TenantName
	bool trusted = false;
	bool verified = false;

	TenantInfo() : tenantId(INVALID_TENANT) {}
	TenantInfo(TenantName name, int64_t tenantId) : name(name), tenantId(tenantId) {}
};

#endif // FDBRPC_TENANTINFO_H_
