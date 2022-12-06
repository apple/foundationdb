/*
 * tenant.hpp
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

#include <cassert>
#include <map>
#include <string>
#include "fdb_api.hpp"
#include "utils.hpp"

namespace mako {

std::map<std::string, std::string> generateAuthorizationTokenMap(int tenants,
                                                                 std::string public_key_id,
                                                                 std::string private_key_pem);

inline std::string getTenantNameByIndex(int index) {
	assert(index >= 0);
	return "tenant" + std::to_string(index);
}

inline void computeTenantPrefix(fdb::ByteString& s, uint64_t id) {
	uint64_t swapped = byteswapHelper(id);
	fdb::BytesRef temp = reinterpret_cast<const uint8_t*>(&swapped);
	memcpy(&s[0], temp.data(), 8);
}

} // namespace mako
