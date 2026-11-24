/*
 * RangeDigestProgressRows.cpp
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

// Persists RangeDigest per-range progress rows directly and checks what the audit scheduler reads back.
//
// A row that getRangeDigestSummary refuses to count must not read as Complete from getAuditStateByRange,
// because the scheduler skips Complete ranges: the audit would then be retried forever with the range
// never re-audited. The uncountable rows reproduced here are the two the coverage check names: a wide
// row truncated by a later narrower krmSetRange, and a Complete row persisted without a digest.

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/RangeDigest.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

struct RangeDigestProgressRowsWorkload : TestWorkload {
	static constexpr auto NAME = "RangeDigestProgressRows";

	bool passed = false;

	explicit RangeDigestProgressRowsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return run(cx);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return clientId != 0 || passed; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Value rowValue(UID id, KeyRange range, AuditPhase phase, bool withDigest) {
		AuditStorageState s(id, range, AuditType::RangeDigest);
		s.setPhase(phase);
		if (withDigest) {
			s.digest = RangeDigest().bytes();
		}
		return auditStorageStateValue(s);
	}

	static Future<Void> writeRow(Database cx, UID id, KeyRange range, AuditPhase phase, bool withDigest) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				co_await krmSetRange(&tr,
				                     auditRangeBasedProgressPrefixFor(AuditType::RangeDigest, id),
				                     range,
				                     rowValue(id, range, phase, withDigest));
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> clearRows(Database cx, UID id) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(prefixRange(auditRangeBasedProgressPrefixFor(AuditType::RangeDigest, id)));
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> run(Database cx) {
		const UID id = deterministicRandom()->randomUniqueID();
		const Key a = "rdpr/a"_sr, b = "rdpr/b"_sr, c = "rdpr/c"_sr, d = "rdpr/d"_sr, e = "rdpr/e"_sr, f = "rdpr/f"_sr;

		// [a,c) is written first and [b,c) after it, so the row stored at `a` keeps describing [a,c)
		// while its boundary range shrinks to [a,b).
		co_await writeRow(cx, id, KeyRangeRef(a, c), AuditPhase::Complete, true);
		co_await writeRow(cx, id, KeyRangeRef(b, c), AuditPhase::Complete, true);
		co_await writeRow(cx, id, KeyRangeRef(c, d), AuditPhase::Complete, true);
		co_await writeRow(cx, id, KeyRangeRef(d, e), AuditPhase::Running, false);
		co_await writeRow(cx, id, KeyRangeRef(e, f), AuditPhase::Complete, false);

		const KeyRange whole = KeyRangeRef(a, f);
		RangeDigestSummary summary = co_await getRangeDigestSummary(cx, id, whole);
		// getAuditStateByRange returns one krm page at a time, which the knobs may shrink to a couple of rows.
		std::vector<AuditStorageState> states;
		for (Key begin = a; begin < f;) {
			std::vector<AuditStorageState> page =
			    co_await getAuditStateByRange(cx, AuditType::RangeDigest, id, KeyRangeRef(begin, f));
			ASSERT(!page.empty());
			states.insert(states.end(), page.begin(), page.end());
			begin = page.back().range.end;
		}
		co_await clearRows(cx, id);

		// What the coverage check rejects must be what the scheduler sees as unaudited.
		const std::vector<std::pair<KeyRange, AuditPhase>> expected = {
			{ KeyRangeRef(a, b), AuditPhase::Invalid }, // wide row truncated by a narrower one
			{ KeyRangeRef(b, c), AuditPhase::Complete },
			{ KeyRangeRef(c, d), AuditPhase::Complete },
			{ KeyRangeRef(d, e), AuditPhase::Running },
			{ KeyRangeRef(e, f), AuditPhase::Invalid }, // Complete without a digest
		};

		bool ok = !summary.complete && summary.incompleteRange == KeyRangeRef(a, b) && states.size() == expected.size();
		for (int i = 0; ok && i < expected.size(); ++i) {
			ok = states[i].range == expected[i].first && states[i].getPhase() == expected[i].second;
		}
		if (!ok) {
			TraceEvent(SevError, "RangeDigestProgressRowsMismatch")
			    .detail("SummaryComplete", summary.complete)
			    .detail("IncompleteRange", summary.incompleteRange)
			    .detail("StateCount", states.size());
			for (int i = 0; i < states.size(); ++i) {
				TraceEvent(SevError, "RangeDigestProgressRowsState")
				    .detail("Index", i)
				    .detail("Range", states[i].range)
				    .detail("Phase", states[i].getPhase());
			}
			ASSERT(false);
		}
		passed = true;
	}
};

WorkloadFactory<RangeDigestProgressRowsWorkload> RangeDigestProgressRowsWorkloadFactory;
