/*
 * Tenant.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "flow/UnitTest.h"

TEST_CASE("/fdbclient/TenantMapEntry/Serialization") {
	TenantMapEntry entry1(1, ""_sr);
	ASSERT(entry1.prefix == "\x00\x00\x00\x00\x00\x00\x00\x01"_sr);
	TenantMapEntry entry2 = decodeTenantEntry(encodeTenantEntry(entry1));
	ASSERT(entry1.id == entry2.id && entry1.prefix == entry2.prefix);

	TenantMapEntry entry3(std::numeric_limits<int64_t>::max(), "foo"_sr);
	ASSERT(entry3.prefix == "foo\xfe\xff\xff\xff\xff\xff\xff\xff"_sr);
	TenantMapEntry entry4 = decodeTenantEntry(encodeTenantEntry(entry3));
	ASSERT(entry3.id == entry4.id && entry3.prefix == entry4.prefix);

	for (int i = 0; i < 100; ++i) {
		int bits = deterministicRandom()->randomInt(1, 64);
		int64_t min = bits == 1 ? 0 : (1 << (bits - 1));
		int64_t maxPlusOne = std::min<uint64_t>(1 << bits, std::numeric_limits<int64_t>::max());
		int64_t id = deterministicRandom()->randomInt64(min, maxPlusOne);

		int subspaceLength = deterministicRandom()->randomInt(0, 20);
		Standalone<StringRef> subspace = makeString(subspaceLength);
		generateRandomData(mutateString(subspace), subspaceLength);

		TenantMapEntry entry(id, subspace);
		int64_t bigEndianId = bigEndian64(id);
		ASSERT(entry.id == id && entry.prefix.startsWith(subspace) &&
		       entry.prefix.endsWith(StringRef(reinterpret_cast<uint8_t*>(&bigEndianId), 8)) &&
		       entry.prefix.size() == subspaceLength + 8);

		TenantMapEntry decodedEntry = decodeTenantEntry(encodeTenantEntry(entry));
		ASSERT(decodedEntry.id = entry.id && decodedEntry.prefix == entry.prefix);
	}

	return Void();
}
