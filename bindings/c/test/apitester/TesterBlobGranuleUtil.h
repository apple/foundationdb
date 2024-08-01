/*
 * TesterBlobGranuleUtil.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef APITESTER_BLOBGRANULE_UTIL_H
#define APITESTER_BLOBGRANULE_UTIL_H
#include "TesterUtil.h"
#include "test/fdb_api.hpp"
#include <unordered_map>

namespace FdbApiTester {

class TesterGranuleContext {
public:
	std::unordered_map<int64_t, uint8_t*> loadsInProgress;
	std::string basePath;
	int64_t nextId;

	TesterGranuleContext(const std::string& basePath) : basePath(basePath), nextId(0) {}

	~TesterGranuleContext() {
		// this should now never happen with proper memory management
		ASSERT(loadsInProgress.empty());
	}
};

fdb::native::FDBReadBlobGranuleContext createGranuleContext(const TesterGranuleContext* testerContext);

} // namespace FdbApiTester

#endif
