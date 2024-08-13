/*
 * blob_granules.hpp
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

#ifndef MAKO_BLOB_GRANULES_HPP
#define MAKO_BLOB_GRANULES_HPP

#include <cstdint>
#include <memory>
#include <fdb_api.hpp>

namespace mako::blob_granules::local_file {

constexpr const int MAX_BG_IDS = 1000;

// TODO: could always abstract this into something more generically usable by something other than mako.
// But outside of testing there are likely few use cases for local granules
struct UserContext {
	char const* bgFilePath;
	int nextId;
	std::unique_ptr<uint8_t*[]> dataByIdMem;
	uint8_t** dataById;

	UserContext(char const* filePath)
	  : bgFilePath(filePath), nextId(0), dataByIdMem(new uint8_t*[MAX_BG_IDS]()), dataById(dataByIdMem.get()) {}

	void clear() { dataByIdMem.reset(); }
};

fdb::native::FDBReadBlobGranuleContext createApiContext(UserContext& ctx, bool materialize_files);

} // namespace mako::blob_granules::local_file

#endif /*MAKO_BLOB_GRANULES_HPP*/
