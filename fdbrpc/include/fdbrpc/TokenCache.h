/*
 * TokenCache.h
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

#ifndef TOKENCACHE_H_
#define TOKENCACHE_H_
#include "fdbrpc/TenantName.h"
#include "fdbrpc/TokenSpec.h"
#include "flow/Arena.h"

class TokenCache : NonCopyable {
	struct TokenCacheImpl* impl;
	TokenCache();

public:
	~TokenCache();
	static void createInstance();
	static TokenCache& instance();
	bool validate(authz::TenantId tenant, StringRef token);
};

#endif // TOKENCACHE_H_
