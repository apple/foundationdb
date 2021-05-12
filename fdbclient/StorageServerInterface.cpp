/*
 * StorageServerInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/StorageServerInterface.h"
#include "flow/crc32c.h" // for crc32c_append, to checksum values in tss trace events

// Includes template specializations for all tss operations on storage server types.
// New StorageServerInterface reply types must be added here or it won't compile.

// if size + hex of checksum is shorter than value, record that instead of actual value. break-even point is 12
// characters
std::string traceChecksumValue(ValueRef s) {
	return s.size() > 12 ? format("(%d)%08x", s.size(), crc32c_append(0, s.begin(), s.size())) : s.toString();
}

template <>
bool TSS_doCompare(const GetValueRequest& req,
                   const GetValueReply& src,
                   const GetValueReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	if (src.value.present() != tss.value.present() || (src.value.present() && src.value.get() != tss.value.get())) {
		TraceEvent(traceSeverity, "TSSMismatchGetValue")
		    .suppressFor(1.0)
		    .detail("TSSID", tssId)
		    .detail("Key", req.key.printable())
		    .detail("Version", req.version)
		    .detail("SSReply", src.value.present() ? traceChecksumValue(src.value.get()) : "missing")
		    .detail("TSSReply", tss.value.present() ? traceChecksumValue(tss.value.get()) : "missing");

		return false;
	}
	return true;
}

template <>
bool TSS_doCompare(const GetKeyRequest& req,
                   const GetKeyReply& src,
                   const GetKeyReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	// This process is a bit complicated. Since the tss and ss can return different results if neighboring shards to
	// req.sel.key are currently being moved, We validate that the results are the same IF the returned key selectors
	// are final. Otherwise, we only mark the request as a mismatch if the difference between the two returned key
	// selectors could ONLY be because of different results from the storage engines. We can afford to only partially
	// check key selectors that start in a TSS shard and end in a non-TSS shard because the other read queries and the
	// consistency check will eventually catch a misbehaving storage engine.
	bool matches = true;
	if (src.sel.orEqual == tss.sel.orEqual && src.sel.offset == tss.sel.offset) {
		// full matching case
		if (src.sel.offset == 0 && src.sel.orEqual) {
			// found exact key, should be identical
			matches = src.sel.getKey() == tss.sel.getKey();
		}
		// if the query doesn't return the final key, there is an edge case where the ss and tss have different shard
		// boundaries, so they pass different shard boundary keys back for the same offset
	} else if (src.sel.getKey() == tss.sel.getKey()) {
		// There is one case with a positive offset where the shard boundary the incomplete query stopped at is the next
		// key in the shard that the complete query returned. This is not possible with a negative offset because the
		// shard boundary is exclusive backwards
		if (src.sel.offset == 0 && src.sel.orEqual && tss.sel.offset == 1 && !tss.sel.orEqual) {
			// case where ss was complete and tss was incomplete
		} else if (tss.sel.offset == 0 && tss.sel.orEqual && src.sel.offset == 1 && !src.sel.orEqual) {
			// case where tss was complete and ss was incomplete
		} else {
			matches = false;
		}
	} else {
		// ss/tss returned different keys, and different offsets and/or orEqual
		// here we just validate that ordering of the keys matches the ordering of the offsets
		bool tssKeyLarger = src.sel.getKey() < tss.sel.getKey();
		// the only case offsets are equal and orEqual aren't equal is the case with a negative offset,
		// where one response has <=0 with the actual result and the other has <0 with the shard upper boundary.
		// So whichever one has the actual result should have the lower key.
		bool tssOffsetLarger = (src.sel.offset == tss.sel.offset) ? tss.sel.orEqual : src.sel.offset < tss.sel.offset;
		matches = tssKeyLarger != tssOffsetLarger;
	}
	if (!matches) {
		TraceEvent(traceSeverity, "TSSMismatchGetKey")
		    .suppressFor(1.0)
		    .detail("TSSID", tssId)
		    .detail("KeySelector",
		            format("%s%s:%d", req.sel.orEqual ? "=" : "", req.sel.getKey().printable().c_str(), req.sel.offset))
		    .detail("Version", req.version)
		    .detail("SSReply",
		            format("%s%s:%d", src.sel.orEqual ? "=" : "", src.sel.getKey().printable().c_str(), src.sel.offset))
		    .detail(
		        "TSSReply",
		        format("%s%s:%d", tss.sel.orEqual ? "=" : "", tss.sel.getKey().printable().c_str(), tss.sel.offset));
	}
	return matches;
}

template <>
bool TSS_doCompare(const GetKeyValuesRequest& req,
                   const GetKeyValuesReply& src,
                   const GetKeyValuesReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	if (src.more != tss.more || src.data != tss.data) {

		std::string ssResultsString = format("(%d)%s:\n", src.data.size(), src.more ? "+" : "");
		for (auto& it : src.data) {
			ssResultsString += "\n" + it.key.printable() + "=" + traceChecksumValue(it.value);
		}

		std::string tssResultsString = format("(%d)%s:\n", tss.data.size(), tss.more ? "+" : "");
		for (auto& it : tss.data) {
			tssResultsString += "\n" + it.key.printable() + "=" + traceChecksumValue(it.value);
		}

		TraceEvent(traceSeverity, "TSSMismatchGetKeyValues")
		    .suppressFor(1.0)
		    .detail("TSSID", tssId)
		    .detail(
		        "Begin",
		        format(
		            "%s%s:%d", req.begin.orEqual ? "=" : "", req.begin.getKey().printable().c_str(), req.begin.offset))
		    .detail("End",
		            format("%s%s:%d", req.end.orEqual ? "=" : "", req.end.getKey().printable().c_str(), req.end.offset))
		    .detail("Version", req.version)
		    .detail("Limit", req.limit)
		    .detail("LimitBytes", req.limitBytes)
		    .detail("SSReply", ssResultsString)
		    .detail("TSSReply", tssResultsString);

		return false;
	}
	return true;
}

template <>
bool TSS_doCompare(const WatchValueRequest& req,
                   const WatchValueReply& src,
                   const WatchValueReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	// We duplicate watches just for load, no need to validte replies.
	return true;
}

// no-op template specializations for metrics replies
template <>
bool TSS_doCompare(const WaitMetricsRequest& req,
                   const StorageMetrics& src,
                   const StorageMetrics& tss,
                   Severity traceSeverity,
                   UID tssId) {
	return true;
}

template <>
bool TSS_doCompare(const SplitMetricsRequest& req,
                   const SplitMetricsReply& src,
                   const SplitMetricsReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	return true;
}

template <>
bool TSS_doCompare(const ReadHotSubRangeRequest& req,
                   const ReadHotSubRangeReply& src,
                   const ReadHotSubRangeReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	return true;
}

template <>
bool TSS_doCompare(const SplitRangeRequest& req,
                   const SplitRangeReply& src,
                   const SplitRangeReply& tss,
                   Severity traceSeverity,
                   UID tssId) {
	return true;
}

// only record metrics for data reads

template <>
void TSSMetrics::recordLatency(const GetValueRequest& req, double ssLatency, double tssLatency) {
	SSgetValueLatency.addSample(ssLatency);
	TSSgetValueLatency.addSample(tssLatency);
}

template <>
void TSSMetrics::recordLatency(const GetKeyRequest& req, double ssLatency, double tssLatency) {
	SSgetKeyLatency.addSample(ssLatency);
	TSSgetKeyLatency.addSample(tssLatency);
}

template <>
void TSSMetrics::recordLatency(const GetKeyValuesRequest& req, double ssLatency, double tssLatency) {
	SSgetKeyValuesLatency.addSample(ssLatency);
	TSSgetKeyValuesLatency.addSample(tssLatency);
}

template <>
void TSSMetrics::recordLatency(const WatchValueRequest& req, double ssLatency, double tssLatency) {}

template <>
void TSSMetrics::recordLatency(const WaitMetricsRequest& req, double ssLatency, double tssLatency) {}

template <>
void TSSMetrics::recordLatency(const SplitMetricsRequest& req, double ssLatency, double tssLatency) {}

template <>
void TSSMetrics::recordLatency(const ReadHotSubRangeRequest& req, double ssLatency, double tssLatency) {}

template <>
void TSSMetrics::recordLatency(const SplitRangeRequest& req, double ssLatency, double tssLatency) {}

// -------------------

TEST_CASE("/StorageServerInterface/TSSCompare/TestComparison") {
	printf("testing tss comparisons\n");

	// to avoid compiler issues that StringRef(char* is deprecated)
	std::string s_a = "a";
	std::string s_b = "b";
	std::string s_c = "c";
	std::string s_d = "d";
	std::string s_e = "e";

	// test getValue
	GetValueRequest gvReq;
	gvReq.key = StringRef(s_a);
	gvReq.version = 5;

	UID tssId;

	GetValueReply gvReplyMissing;
	GetValueReply gvReplyA(Optional<Value>(StringRef(s_a)), false);
	GetValueReply gvReplyB(Optional<Value>(StringRef(s_b)), false);
	ASSERT(TSS_doCompare(gvReq, gvReplyMissing, gvReplyMissing, SevInfo, tssId));
	ASSERT(TSS_doCompare(gvReq, gvReplyA, gvReplyA, SevInfo, tssId));
	ASSERT(TSS_doCompare(gvReq, gvReplyB, gvReplyB, SevInfo, tssId));

	ASSERT(!TSS_doCompare(gvReq, gvReplyMissing, gvReplyA, SevInfo, tssId));
	ASSERT(!TSS_doCompare(gvReq, gvReplyA, gvReplyB, SevInfo, tssId));

	// test GetKeyValues
	Arena a; // for all of the refs. ASAN complains if this isn't done. Could also make them all standalone i guess
	GetKeyValuesRequest gkvReq;
	gkvReq.begin = firstGreaterOrEqual(StringRef(a, s_a));
	gkvReq.end = firstGreaterOrEqual(StringRef(a, s_b));
	gkvReq.version = 5;

	GetKeyValuesReply gkvReplyEmpty;
	GetKeyValuesReply gkvReplyOne;
	KeyValueRef v;
	v.key = StringRef(a, s_a);
	v.value = StringRef(a, s_b);
	gkvReplyOne.data.push_back_deep(gkvReplyOne.arena, v);
	GetKeyValuesReply gkvReplyOneMore;
	gkvReplyOneMore.data.push_back_deep(gkvReplyOneMore.arena, v);
	gkvReplyOneMore.more = true;

	ASSERT(TSS_doCompare(gkvReq, gkvReplyEmpty, gkvReplyEmpty, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkvReq, gkvReplyOne, gkvReplyOne, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkvReq, gkvReplyOneMore, gkvReplyOneMore, SevInfo, tssId));
	ASSERT(!TSS_doCompare(gkvReq, gkvReplyEmpty, gkvReplyOne, SevInfo, tssId));
	ASSERT(!TSS_doCompare(gkvReq, gkvReplyOne, gkvReplyOneMore, SevInfo, tssId));

	// test GetKey
	GetKeyRequest gkReq;
	gkReq.sel = KeySelectorRef(StringRef(a, s_a), false, 1);
	gkReq.version = 5;

	GetKeyReply gkReplyA(KeySelectorRef(StringRef(a, s_a), false, 20), false);
	GetKeyReply gkReplyB(KeySelectorRef(StringRef(a, s_b), false, 10), false);
	GetKeyReply gkReplyC(KeySelectorRef(StringRef(a, s_c), true, 0), false);
	GetKeyReply gkReplyD(KeySelectorRef(StringRef(a, s_d), false, -10), false);
	GetKeyReply gkReplyE(KeySelectorRef(StringRef(a, s_e), false, -20), false);

	// identical cases
	ASSERT(TSS_doCompare(gkReq, gkReplyA, gkReplyA, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyB, gkReplyB, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyC, gkReplyC, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyD, gkReplyD, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyE, gkReplyE, SevInfo, tssId));

	// relative offset cases
	ASSERT(TSS_doCompare(gkReq, gkReplyA, gkReplyB, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyB, gkReplyA, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyA, gkReplyC, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyC, gkReplyA, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyB, gkReplyC, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyC, gkReplyB, SevInfo, tssId));

	ASSERT(TSS_doCompare(gkReq, gkReplyC, gkReplyD, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyD, gkReplyC, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyC, gkReplyE, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyE, gkReplyC, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyD, gkReplyE, SevInfo, tssId));
	ASSERT(TSS_doCompare(gkReq, gkReplyE, gkReplyD, SevInfo, tssId));

	// test same offset/orEqual wrong key
	ASSERT(!TSS_doCompare(gkReq,
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), true, 0), false),
	                      SevInfo,
	                      tssId));
	// this could be from different shard boundaries, so don't say it's a mismatch
	ASSERT(TSS_doCompare(gkReq,
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 10), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 10), false),
	                     SevInfo,
	                     tssId));

	// test offsets and key difference don't match
	ASSERT(!TSS_doCompare(gkReq,
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 10), false),
	                      SevInfo,
	                      tssId));
	ASSERT(!TSS_doCompare(gkReq,
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, -10), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 0), false),
	                      SevInfo,
	                      tssId));

	// test key is next over in one shard, one found it and other didn't
	// positive
	// one that didn't find is +1
	ASSERT(TSS_doCompare(gkReq,
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 1), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_b), true, 0), false),
	                     SevInfo,
	                     tssId));
	ASSERT(!TSS_doCompare(gkReq,
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 1), false),
	                      SevInfo,
	                      tssId));

	// negative will have zero offset but not equal set
	ASSERT(TSS_doCompare(gkReq,
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 0), false),
	                     SevInfo,
	                     tssId));
	ASSERT(!TSS_doCompare(gkReq,
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), true, 0), false),
	                      SevInfo,
	                      tssId));

	// test shard boundary key returned by incomplete query is the same as the key found by the other (only possible in
	// positive direction)
	ASSERT(TSS_doCompare(gkReq,
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 1), false),
	                     SevInfo,
	                     tssId));

	// explictly test checksum function
	std::string s12 = "ABCDEFGHIJKL";
	std::string s13 = "ABCDEFGHIJKLO";
	std::string checksumStart13 = "(13)";
	ASSERT(s_a == traceChecksumValue(StringRef(s_a)));
	ASSERT(s12 == traceChecksumValue(StringRef(s12)));
	ASSERT(checksumStart13 == traceChecksumValue(StringRef(s13)).substr(0, 4));
	return Void();
}