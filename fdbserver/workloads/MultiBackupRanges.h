/*
 * MultiBackupRanges.h
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

#ifndef FDBSERVER_MULTI_BACKUP_RANGES_H
#define FDBSERVER_MULTI_BACKUP_RANGES_H

#include <vector>

#include "fdbclient/BackupAgent.h"
#include "fdbclient/FDBTypes.h"
#include "flow/Error.h"

// Key-range scenarios for tests that run several concurrent v1 backups (distinct tags) against one database.
//
// Each backup workload instance has an index in [0, MAX_BACKUPS). The test's shared random number picks the
// scenario and how many of the instances are active (MIN_BACKUPS..MAX_BACKUPS); instances at or beyond the active
// count stay idle. Both choices derive from the shared number, so every instance agrees without coordination.
//
// Ranges are expressed in terms of the Cycle workload's key space: keys are 16 hex digits encoding a double in
// [0, 1), so node 0 is "0000000000000000" and every other key sorts within ["3f", "3ff"). The first boundary is "0" so
// that a range starting there covers node 0 too; a restore that left node 0 out would mix versions of the ring.
namespace MultiBackupRanges {

enum class Scenario {
	AllDefault, // every backup covers all keys, including system ranges
	AllSameRange, // every backup covers the same single range
	Nested, // backup i covers a prefix of the Cycle space; later backups strictly contain earlier ones
	AllSameMultiRange, // every backup covers the same two disjoint ranges
	Adjacent, // backups cover consecutive, non-overlapping slices of the Cycle space
	COUNT
};

inline const char* toString(Scenario s) {
	switch (s) {
	case Scenario::AllDefault:
		return "AllDefault";
	case Scenario::AllSameRange:
		return "AllSameRange";
	case Scenario::Nested:
		return "Nested";
	case Scenario::AllSameMultiRange:
		return "AllSameMultiRange";
	case Scenario::Adjacent:
		return "Adjacent";
	default:
		return "<undefined>";
	}
}

constexpr int MIN_BACKUPS = 2;
constexpr int MAX_BACKUPS = 3;

inline Scenario scenarioFor(int64_t sharedRandomNumber) {
	return static_cast<Scenario>(static_cast<uint64_t>(sharedRandomNumber) % static_cast<uint64_t>(Scenario::COUNT));
}

inline int activeBackupCount(int64_t sharedRandomNumber) {
	uint64_t n = static_cast<uint64_t>(sharedRandomNumber) / static_cast<uint64_t>(Scenario::COUNT);
	return MIN_BACKUPS + static_cast<int>(n % (MAX_BACKUPS - MIN_BACKUPS + 1));
}

// Boundaries inside the Cycle key space, in increasing order.
//   [0, 3fd): p < 0.25    [3fd, 3fe): 0.25 <= p < 0.5    [3fe, 3ff): p >= 0.5
inline const std::vector<KeyRef>& boundaries() {
	static const std::vector<KeyRef> b = { "0"_sr, "3fd"_sr, "3fe"_sr, "3ff"_sr };
	return b;
}

// Cut points splitting the Cycle space into `count` consecutive slices; cuts[i], cuts[i+1] bound slice i.
inline std::vector<KeyRef> cutPoints(int count) {
	const auto& b = boundaries();
	ASSERT(count >= 1 && count < static_cast<int>(b.size()));
	std::vector<KeyRef> cuts(count + 1);
	cuts[0] = b.front();
	cuts[count] = b.back();
	for (int k = 1; k < count; ++k) {
		cuts[k] = b[b.size() - 1 - (count - k)];
	}
	return cuts;
}

// Appends the ranges backup `index` of `count` covers in `scenario` to `out`.
inline void addRanges(Scenario scenario, int index, int count, Standalone<VectorRef<KeyRangeRef>>& out) {
	ASSERT(index >= 0 && index < count);
	auto add = [&](KeyRef begin, KeyRef end) { out.push_back_deep(out.arena(), KeyRangeRef(begin, end)); };
	const auto& b = boundaries();
	std::vector<KeyRef> cuts = cutPoints(count);
	switch (scenario) {
	case Scenario::AllDefault:
		addDefaultBackupRanges(out);
		break;
	case Scenario::AllSameRange:
		add(b.front(), b.back());
		break;
	case Scenario::Nested:
		add(b.front(), cuts[index + 1]);
		break;
	case Scenario::AllSameMultiRange:
		add(b[0], b[1]);
		add(b[2], b[3]);
		break;
	case Scenario::Adjacent:
		add(cuts[index], cuts[index + 1]);
		break;
	default:
		UNREACHABLE();
	}
}

// True if `ranges` cover the whole Cycle key space.
inline bool coversCycleData(const Standalone<VectorRef<KeyRangeRef>>& ranges) {
	KeyRef covered = boundaries().front();
	bool progressed = true;
	while (progressed && covered < boundaries().back()) {
		progressed = false;
		for (const auto& r : ranges) {
			if (r.begin <= covered && covered < r.end) {
				covered = r.end;
				progressed = true;
			}
		}
	}
	return covered >= boundaries().back();
}

// Restoring only part of the Cycle data to an older version breaks the cycle invariant, and concurrent restores race
// over the same keys. So only the first active backup whose ranges cover all the Cycle data restores (none if no
// backup does).
inline bool shouldRestore(Scenario scenario, int index, int count) {
	for (int i = 0; i <= index; ++i) {
		Standalone<VectorRef<KeyRangeRef>> ranges;
		addRanges(scenario, i, count, ranges);
		if (coversCycleData(ranges)) {
			return i == index;
		}
	}
	return false;
}

} // namespace MultiBackupRanges

#endif
