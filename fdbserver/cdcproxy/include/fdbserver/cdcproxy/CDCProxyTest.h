/*
 * CDCProxyTest.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include <map>
#include "fdbclient/FDBTypes.h"
#include "flow/flow.h"

// Simulation-only barrier at the point where two real tag readers need to expand their reservations.
class CDCProxyMaterializationTest : public ReferenceCounted<CDCProxyMaterializationTest> {
	UID owner;
	std::map<Tag, int64_t> reservations;
	bool released = false;
	int expiredLeases = 0;
	inline static Reference<CDCProxyMaterializationTest> installed;

public:
	CDCProxyMaterializationTest(UID owner, Tag first, Tag second) : owner(owner) {
		ASSERT_NE(first, second);
		reservations.emplace(first, 0);
		reservations.emplace(second, 0);
	}

	static Reference<CDCProxyMaterializationTest> get() {
		return g_network->isSimulated() ? installed : Reference<CDCProxyMaterializationTest>();
	}
	static void install(Reference<CDCProxyMaterializationTest> test) {
		ASSERT(g_network->isSimulated());
		ASSERT(!installed);
		installed = test;
	}
	static void uninstall() {
		if (installed) {
			installed->released = true;
			installed.clear();
		}
	}
	bool holdExpansion(UID proxy, Tag tag, int64_t reserved) {
		if (proxy != owner || released || !reservations.contains(tag)) {
			return false;
		}
		reservations.at(tag) = reserved;
		return true;
	}
	bool bothReadersHeld() const { return reservations.begin()->second > 0 && reservations.rbegin()->second > 0; }
	int64_t heldBytes() const { return reservations.begin()->second + reservations.rbegin()->second; }
	void release() {
		ASSERT(bothReadersHeld());
		released = true;
	}
	void recordLeaseExpiry(UID proxy) {
		if (proxy == owner) {
			++expiredLeases;
		}
	}
	int leaseExpiries() const { return expiredLeases; }
};
