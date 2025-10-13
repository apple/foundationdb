/*
 * StorageServerInterface.cpp
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

// TODO this should really be renamed "TSSComparison.cpp"
// FIXME: actually it should be renamed "ReplyComparison" because TSS vs SS
// is just one use case.  This code is agnostic to the specific use cases.
// Fundamentally it is just about comparing replies. Where they came from is incidental.
#include "fdbclient/StorageServerInterface.h"

#include "crc32/crc32c.h" // for crc32c_append, to checksum values in tss trace events

// Includes template specializations for all tss operations on storage server types.
// New StorageServerInterface reply types must be added here or it won't compile.

// if size + hex of checksum is shorter than value, record that instead of actual value. break-even point is 12
// characters
std::string traceChecksumValue(const ValueRef& s) {
	return s.size() > 12 ? format("(%d)%08x", s.size(), crc32c_append(0, s.begin(), s.size())) : s.toString();
}

// point reads
template <>
bool TSS_doCompare(const GetValueReply& src, const GetValueReply& tss) {
	return src.value.present() == tss.value.present() && (!src.value.present() || src.value.get() == tss.value.get());
}

template <>
const char* LB_mismatchTraceName(const GetValueRequest& req, const ComparisonType& type) {
	return type == TSS_COMPARISON ? "TSSMismatchGetValue" : "ReplicaMismatchGetValue";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const GetValueRequest& req,
                       const GetValueReply& src,
                       const GetValueReply& tss,
                       const ComparisonType& type) {
	event.detail("Key", req.key)
	    .detail("Tenant", req.tenantInfo.tenantId)
	    .detail("Version", req.version)
	    .detail(type == TSS_COMPARISON ? "SSReply" : "SourceSSReply",
	            src.value.present() ? traceChecksumValue(src.value.get()) : "missing")
	    .detail(type == TSS_COMPARISON ? "TSSReply" : "ReplicaSSReply",
	            tss.value.present() ? traceChecksumValue(tss.value.get()) : "missing");
}

// key selector reads
template <>
bool TSS_doCompare(const GetKeyReply& src, const GetKeyReply& tss) {
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
	return matches;
}

template <>
const char* LB_mismatchTraceName(const GetKeyRequest& req, const ComparisonType& type) {
	return type == TSS_COMPARISON ? "TSSMismatchGetKey" : "ReplicaMismatchGetKey";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const GetKeyRequest& req,
                       const GetKeyReply& src,
                       const GetKeyReply& tss,
                       const ComparisonType& type) {
	event
	    .detail("KeySelector",
	            format("%s%s:%d", req.sel.orEqual ? "=" : "", req.sel.getKey().printable().c_str(), req.sel.offset))
	    .detail("Tenant", req.tenantInfo.tenantId)
	    .detail("Version", req.version)
	    .detail(type == TSS_COMPARISON ? "SSReply" : "SourceSSReply",
	            format("%s%s:%d", src.sel.orEqual ? "=" : "", src.sel.getKey().printable().c_str(), src.sel.offset))
	    .detail(type == TSS_COMPARISON ? "TSSReply" : "ReplicaSSReply",
	            format("%s%s:%d", tss.sel.orEqual ? "=" : "", tss.sel.getKey().printable().c_str(), tss.sel.offset));
}

// range reads
template <>
bool TSS_doCompare(const GetKeyValuesReply& src, const GetKeyValuesReply& tss) {
	return src.more == tss.more && src.data == tss.data;
}

template <>
const char* LB_mismatchTraceName(const GetKeyValuesRequest& req, const ComparisonType& type) {
	return type == TSS_COMPARISON ? "TSSMismatchGetKeyValues" : "ReplicaMismatchGetKeyValues";
}

static void traceKeyValuesSummary(TraceEvent& event,
                                  const KeySelectorRef& begin,
                                  const KeySelectorRef& end,
                                  int64_t tenantId,
                                  Version version,
                                  int limit,
                                  int limitBytes,
                                  size_t ssSize,
                                  bool ssMore,
                                  size_t tssSize,
                                  bool tssMore,
                                  const ComparisonType& type) {
	std::string ssSummaryString = format("(%d)%s", ssSize, ssMore ? "+" : "");
	std::string tssSummaryString = format("(%d)%s", tssSize, tssMore ? "+" : "");
	event.detail("Begin", format("%s%s:%d", begin.orEqual ? "=" : "", begin.getKey().printable().c_str(), begin.offset))
	    .detail("End", format("%s%s:%d", end.orEqual ? "=" : "", end.getKey().printable().c_str(), end.offset))
	    .detail("Tenant", tenantId)
	    .detail("Version", version)
	    .detail("Limit", limit)
	    .detail("LimitBytes", limitBytes)
	    .detail((type == TSS_COMPARISON ? "SSReplySummary" : "SourceSSReplySummary"), ssSummaryString)
	    .detail((type == TSS_COMPARISON ? "TSSReplySummary" : "ReplicaSSReplySummary"), tssSummaryString);
}

// convert a StringRef to Hex string
static std::string hexStringRef(const StringRef& s) {
	std::string result;
	result.reserve(s.size() * 2);
	for (int i = 0; i < s.size(); i++) {
		result.append(format("%02x", s[i]));
	}
	return result;
}

static void traceKeyValuesDiff(TraceEvent& event,
                               const KeySelectorRef& begin,
                               const KeySelectorRef& end,
                               int64_t tenantId,
                               Version version,
                               int limit,
                               int limitBytes,
                               const VectorRef<KeyValueRef>& ssKV,
                               bool ssMore,
                               const VectorRef<KeyValueRef>& tssKV,
                               bool tssMore,
                               const ComparisonType& type) {
	traceKeyValuesSummary(
	    event, begin, end, tenantId, version, limit, limitBytes, ssKV.size(), ssMore, tssKV.size(), tssMore, type);
	bool mismatchFound = false;
	for (int i = 0; i < std::max(ssKV.size(), tssKV.size()); i++) {
		if (i >= ssKV.size() || i >= tssKV.size() || ssKV[i] != tssKV[i]) {
			event.detail("MismatchIndex", i);
			if (i >= ssKV.size() || i >= tssKV.size() || ssKV[i].key != tssKV[i].key) {
				event.detail("MismatchSSKey", i < ssKV.size() ? ssKV[i].key : "missing"_sr);
				event.detail("MismatchSSKeyHex", i < ssKV.size() ? hexStringRef(ssKV[i].key) : "missing"_sr);
				event.detail("MismatchTSSKey", i < tssKV.size() ? tssKV[i].key : "missing"_sr);
				event.detail("MismatchTSSKeyHex", i < tssKV.size() ? hexStringRef(tssKV[i].key) : "missing"_sr)
				    .setMaxFieldLength(-1);
			} else {
				event.detail("MismatchKey", ssKV[i].key);
				event.detail("MismatchSSValue", traceChecksumValue(ssKV[i].value));
				event.detail("MismatchSSValueHex", hexStringRef(traceChecksumValue(ssKV[i].value)));
				event.detail("MismatchTSSValue", traceChecksumValue(tssKV[i].value));
				event.detail("MismatchTSSValueHex", hexStringRef(traceChecksumValue(tssKV[i].value)))
				    .setMaxFieldLength(-1);
			}
			mismatchFound = true;
			break;
		}
	}
	ASSERT(mismatchFound);
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const GetKeyValuesRequest& req,
                       const GetKeyValuesReply& src,
                       const GetKeyValuesReply& tss,
                       const ComparisonType& type) {
	traceKeyValuesDiff(event,
	                   req.begin,
	                   req.end,
	                   req.tenantInfo.tenantId,
	                   req.version,
	                   req.limit,
	                   req.limitBytes,
	                   src.data,
	                   src.more,
	                   tss.data,
	                   tss.more,
	                   type);
}

// range reads and flat map
template <>
bool TSS_doCompare(const GetMappedKeyValuesReply& src, const GetMappedKeyValuesReply& tss) {
	return src.more == tss.more && src.data == tss.data;
}

template <>
const char* LB_mismatchTraceName(const GetMappedKeyValuesRequest& req, const ComparisonType& type) {
	return type == TSS_COMPARISON ? "TSSMismatchGetMappedKeyValues" : "ReplicaMismatchGetMappedKeyValues";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const GetMappedKeyValuesRequest& req,
                       const GetMappedKeyValuesReply& src,
                       const GetMappedKeyValuesReply& tss,
                       const ComparisonType& type) {
	traceKeyValuesSummary(event,
	                      req.begin,
	                      req.end,
	                      req.tenantInfo.tenantId,
	                      req.version,
	                      req.limit,
	                      req.limitBytes,
	                      src.data.size(),
	                      src.more,
	                      tss.data.size(),
	                      tss.more,
	                      type);
	// FIXME: trace details for TSS mismatch of mapped data
}

// streaming range reads
template <>
bool TSS_doCompare(const GetKeyValuesStreamReply& src, const GetKeyValuesStreamReply& tss) {
	return src.more == tss.more && src.data == tss.data;
}

template <>
const char* LB_mismatchTraceName(const GetKeyValuesStreamRequest& req, const ComparisonType& type) {
	return type == TSS_COMPARISON ? "TSSMismatchGetKeyValuesStream" : "ReplicaMismatchGetKeyValuesStream";
}

// TODO this is all duplicated from above, simplify?
template <>
void TSS_traceMismatch(TraceEvent& event,
                       const GetKeyValuesStreamRequest& req,
                       const GetKeyValuesStreamReply& src,
                       const GetKeyValuesStreamReply& tss,
                       const ComparisonType& type) {
	traceKeyValuesDiff(event,
	                   req.begin,
	                   req.end,
	                   req.tenantInfo.tenantId,
	                   req.version,
	                   req.limit,
	                   req.limitBytes,
	                   src.data,
	                   src.more,
	                   tss.data,
	                   tss.more,
	                   type);
}

template <>
bool TSS_doCompare(const WatchValueReply& src, const WatchValueReply& tss) {
	// We duplicate watches just for load, no need to validate replies.
	return true;
}

template <>
const char* LB_mismatchTraceName(const WatchValueRequest& req, const ComparisonType& type) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const WatchValueRequest& req,
                       const WatchValueReply& src,
                       const WatchValueReply& tss,
                       const ComparisonType& type) {
	ASSERT(false);
}

template <>
bool TSS_doCompare(const SplitMetricsReply& src, const SplitMetricsReply& tss) {
	// We duplicate split metrics just for load, no need to validate replies.
	return true;
}

template <>
const char* LB_mismatchTraceName(const SplitMetricsRequest& req, const ComparisonType& type) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const SplitMetricsRequest& req,
                       const SplitMetricsReply& src,
                       const SplitMetricsReply& tss,
                       const ComparisonType& type) {
	ASSERT(false);
}

template <>
bool TSS_doCompare(const ReadHotSubRangeReply& src, const ReadHotSubRangeReply& tss) {
	// We duplicate read hot sub range metrics just for load, no need to validate replies.
	return true;
}

template <>
const char* LB_mismatchTraceName(const ReadHotSubRangeRequest& req, const ComparisonType& type) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const ReadHotSubRangeRequest& req,
                       const ReadHotSubRangeReply& src,
                       const ReadHotSubRangeReply& tss,
                       const ComparisonType& type) {
	ASSERT(false);
}

template <>
bool TSS_doCompare(const SplitRangeReply& src, const SplitRangeReply& tss) {
	// We duplicate read hot sub range metrics just for load, no need to validate replies.
	return true;
}

template <>
const char* LB_mismatchTraceName(const SplitRangeRequest& req, const ComparisonType& type) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const SplitRangeRequest& req,
                       const SplitRangeReply& src,
                       const SplitRangeReply& tss,
                       const ComparisonType& type) {
	ASSERT(false);
}

// change feed
template <>
bool TSS_doCompare(const OverlappingChangeFeedsReply& src, const OverlappingChangeFeedsReply& tss) {
	// We duplicate for load, no need to validate replies
	return true;
}

template <>
const char* LB_mismatchTraceName(const OverlappingChangeFeedsRequest& req, const ComparisonType& type) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const OverlappingChangeFeedsRequest& req,
                       const OverlappingChangeFeedsReply& src,
                       const OverlappingChangeFeedsReply& tss,
                       const ComparisonType& type) {
	ASSERT(false);
}

// template specializations for metrics replies that should never be called because these requests aren't duplicated

// storage metrics
template <>
bool TSS_doCompare(const StorageMetrics& src, const StorageMetrics& tss) {
	ASSERT(false);
	return true;
}

template <>
const char* LB_mismatchTraceName(const WaitMetricsRequest& req, const ComparisonType& type) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent& event,
                       const WaitMetricsRequest& req,
                       const StorageMetrics& src,
                       const StorageMetrics& tss,
                       const ComparisonType& type) {
	ASSERT(false);
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
void TSSMetrics::recordLatency(const GetMappedKeyValuesRequest& req, double ssLatency, double tssLatency) {
	SSgetMappedKeyValuesLatency.addSample(ssLatency);
	TSSgetMappedKeyValuesLatency.addSample(tssLatency);
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

template <>
void TSSMetrics::recordLatency(const GetKeyValuesStreamRequest& req, double ssLatency, double tssLatency) {}

template <>
void TSSMetrics::recordLatency(const OverlappingChangeFeedsRequest& req, double ssLatency, double tssLatency) {}

// -------------------

TEST_CASE("/StorageServerInterface/TSSCompare/TestComparison") {
	printf("testing tss comparisons\n");

	// to avoid compiler issues that StringRef(char* is deprecated)
	std::string s_a = "a";
	std::string s_b = "b";
	std::string s_c = "c";
	std::string s_d = "d";
	std::string s_e = "e";

	UID tssId;

	GetValueReply gvReplyMissing;
	GetValueReply gvReplyA(Optional<Value>(StringRef(s_a)), false);
	GetValueReply gvReplyB(Optional<Value>(StringRef(s_b)), false);
	ASSERT(TSS_doCompare(gvReplyMissing, gvReplyMissing));
	ASSERT(TSS_doCompare(gvReplyA, gvReplyA));
	ASSERT(TSS_doCompare(gvReplyB, gvReplyB));

	ASSERT(!TSS_doCompare(gvReplyMissing, gvReplyA));
	ASSERT(!TSS_doCompare(gvReplyA, gvReplyB));

	// test GetKeyValues
	Arena a;
	GetKeyValuesReply gkvReplyEmpty;
	GetKeyValuesReply gkvReplyOne;
	KeyValueRef v;
	v.key = StringRef(a, s_a);
	v.value = StringRef(a, s_b);
	gkvReplyOne.data.push_back_deep(gkvReplyOne.arena, v);
	GetKeyValuesReply gkvReplyOneMore;
	gkvReplyOneMore.data.push_back_deep(gkvReplyOneMore.arena, v);
	gkvReplyOneMore.more = true;

	ASSERT(TSS_doCompare(gkvReplyEmpty, gkvReplyEmpty));
	ASSERT(TSS_doCompare(gkvReplyOne, gkvReplyOne));
	ASSERT(TSS_doCompare(gkvReplyOneMore, gkvReplyOneMore));
	ASSERT(!TSS_doCompare(gkvReplyEmpty, gkvReplyOne));
	ASSERT(!TSS_doCompare(gkvReplyOne, gkvReplyOneMore));

	GetKeyReply gkReplyA(KeySelectorRef(StringRef(a, s_a), false, 20), false);
	GetKeyReply gkReplyB(KeySelectorRef(StringRef(a, s_b), false, 10), false);
	GetKeyReply gkReplyC(KeySelectorRef(StringRef(a, s_c), true, 0), false);
	GetKeyReply gkReplyD(KeySelectorRef(StringRef(a, s_d), false, -10), false);
	GetKeyReply gkReplyE(KeySelectorRef(StringRef(a, s_e), false, -20), false);

	// identical cases
	ASSERT(TSS_doCompare(gkReplyA, gkReplyA));
	ASSERT(TSS_doCompare(gkReplyB, gkReplyB));
	ASSERT(TSS_doCompare(gkReplyC, gkReplyC));
	ASSERT(TSS_doCompare(gkReplyD, gkReplyD));
	ASSERT(TSS_doCompare(gkReplyE, gkReplyE));

	// relative offset cases
	ASSERT(TSS_doCompare(gkReplyA, gkReplyB));
	ASSERT(TSS_doCompare(gkReplyB, gkReplyA));
	ASSERT(TSS_doCompare(gkReplyA, gkReplyC));
	ASSERT(TSS_doCompare(gkReplyC, gkReplyA));
	ASSERT(TSS_doCompare(gkReplyB, gkReplyC));
	ASSERT(TSS_doCompare(gkReplyC, gkReplyB));

	ASSERT(TSS_doCompare(gkReplyC, gkReplyD));
	ASSERT(TSS_doCompare(gkReplyD, gkReplyC));
	ASSERT(TSS_doCompare(gkReplyC, gkReplyE));
	ASSERT(TSS_doCompare(gkReplyE, gkReplyC));
	ASSERT(TSS_doCompare(gkReplyD, gkReplyE));
	ASSERT(TSS_doCompare(gkReplyE, gkReplyD));

	// test same offset/orEqual wrong key
	ASSERT(!TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), true, 0), false)));
	// this could be from different shard boundaries, so don't say it's a mismatch
	ASSERT(TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 10), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 10), false)));

	// test offsets and key difference don't match
	ASSERT(!TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 10), false)));
	ASSERT(!TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, -10), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 0), false)));

	// test key is next over in one shard, one found it and other didn't
	// positive
	// one that didn't find is +1
	ASSERT(TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 1), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_b), true, 0), false)));
	ASSERT(!TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 1), false)));

	// negative will have zero offset but not equal set
	ASSERT(TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_b), false, 0), false)));
	ASSERT(!TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 0), false),
	                      GetKeyReply(KeySelectorRef(StringRef(a, s_b), true, 0), false)));

	// test shard boundary key returned by incomplete query is the same as the key found by the other (only possible in
	// positive direction)
	ASSERT(TSS_doCompare(GetKeyReply(KeySelectorRef(StringRef(a, s_a), true, 0), false),
	                     GetKeyReply(KeySelectorRef(StringRef(a, s_a), false, 1), false)));

	// explicitly test checksum function
	std::string s12 = "ABCDEFGHIJKL";
	std::string s13 = "ABCDEFGHIJKLO";
	std::string checksumStart13 = "(13)";
	ASSERT(s_a == traceChecksumValue(StringRef(s_a)));
	ASSERT(s12 == traceChecksumValue(StringRef(s12)));
	ASSERT(checksumStart13 == traceChecksumValue(StringRef(s13)).substr(0, 4));
	return Void();
}
