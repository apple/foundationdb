/*
 * tenant.cpp
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

#include <chrono>
#include "tenant.hpp"
#include "time.hpp"
#include "utils.hpp"
#include "fdbrpc/TokenSignStdTypes.h"

namespace mako {

std::map<std::string, std::string> generateAuthorizationTokenMap(int num_tenants,
                                                                 std::string public_key_id,
                                                                 std::string private_key_pem,
                                                                 const std::vector<int64_t>& tenant_ids) {
	std::map<std::string, std::string> m;
	auto t = authz::jwt::stdtypes::TokenSpec{};
	auto const now = toIntegerSeconds(std::chrono::system_clock::now().time_since_epoch());
	t.algorithm = authz::Algorithm::ES256;
	t.keyId = public_key_id;
	t.issuer = "mako";
	t.subject = "benchmark";
	t.audience = std::vector<std::string>{ "fdb_benchmark_server" };
	t.issuedAtUnixTime = now;
	t.expiresAtUnixTime = now + 60 * 60 * 12; // Good for 12 hours
	t.notBeforeUnixTime = now - 60 * 5; // activated 5 mins ago
	const int tokenid_len = 36; // UUID length
	auto tokenid = std::string(tokenid_len, '\0');
	for (auto i = 0; i < num_tenants; i++) {
		std::string tenant_name = getTenantNameByIndex(i);
		// swap out only the token ids and tenant names
		randomAlphanumString(tokenid.data(), tokenid_len);
		t.tokenId = tokenid;
		t.tenants = std::vector<int64_t>{ tenant_ids[i] };
		m[tenant_name] = authz::jwt::stdtypes::signToken(t, private_key_pem);
	}
	return m;
}

} // namespace mako
