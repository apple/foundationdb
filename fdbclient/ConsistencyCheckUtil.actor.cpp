/*
 * ConsistencyCheckUtil.actor.cpp
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

#include "fdbclient/ConsistencyCheckUtil.actor.h"
#include "fdbclient/ConsistencyCheck.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<std::vector<KeyRange>> loadRangesToCheckFromAssignmentMetadata(Database cx,
                                                                            int clientId,
                                                                            int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state std::vector<KeyRange> res;
	state Transaction tr(cx);
	state Key rangeToReadBegin = allKeys.begin;
	state Key rangeToReadEnd = allKeys.end;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			KeyRange rangeToRead = Standalone(KeyRangeRef(rangeToReadBegin, rangeToReadEnd));
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     consistencyCheckAssignmentPrefixFor(clientId),
			                                     rangeToRead,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			for (int i = 0; i < res_.size() - 1; ++i) {
				KeyRange currentRange = KeyRangeRef(res_[i].key, res_[i + 1].key);
				if (!res_[i].value.empty()) {
					ConsistencyCheckState ccState = decodeConsistencyCheckerStateValue(res_[i].value);
					if (ccState.getAssignment() == ConsistencyCheckAssignment::Assigned) {
						res.push_back(currentRange);
					}
				}
				rangeToReadBegin = res_[i + 1].key;
			}
			if (rangeToReadBegin >= rangeToReadEnd) {
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return res;
}

ACTOR Future<Void> persistConsistencyCheckProgress(Database cx, KeyRange range, int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			wait(krmSetRange(&tr,
			                 consistencyCheckProgressPrefix,
			                 range,
			                 consistencyCheckerStateValue(ConsistencyCheckState(ConsistencyCheckPhase::Complete))));
			wait(tr.commit());
			TraceEvent("ConsistencyCheckUrgent_PersistProgress")
			    .detail("Range", range)
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

ACTOR Future<Void> initConsistencyCheckAssignmentMetadata(Database cx, int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			tr.clear(consistencyCheckAssignmentKeys);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e) && delay(5.0));
		}
	}
	return Void();
}

ACTOR Future<Void> initConsistencyCheckProgressMetadata(Database cx,
                                                        std::vector<KeyRange> rangesToCheck,
                                                        int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			tr.clear(consistencyCheckProgressKeys);
			wait(krmSetRange(&tr,
			                 consistencyCheckProgressPrefix,
			                 allKeys,
			                 consistencyCheckerStateValue(
			                     ConsistencyCheckState(ConsistencyCheckPhase::Complete)))); // Mark all as completes
			for (const auto& rangeToCheck : rangesToCheck) {
				wait(krmSetRange(&tr,
				                 consistencyCheckProgressPrefix,
				                 rangeToCheck,
				                 consistencyCheckerStateValue(ConsistencyCheckState(
				                     ConsistencyCheckPhase::Invalid)))); // Mark rangesToCheck as incomplete
			}
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e) && delay(10.0));
		}
	}
	return Void();
}

ACTOR Future<Void> persistConsistencyCheckerId(Database cx, int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.set(consistencyCheckerIdKey, consistencyCheckerStateValue(ConsistencyCheckState(consistencyCheckerId)));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e) && delay(10.0));
		}
	}
	return Void();
}

ACTOR Future<Void> clearConsistencyCheckMetadata(Database cx, int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			tr.clear(consistencyCheckAssignmentKeys);
			tr.clear(consistencyCheckProgressKeys);
			tr.clear(consistencyCheckerIdKey);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e) && delay(10.0));
		}
	}
	return Void();
}

ACTOR Future<std::vector<KeyRange>> loadRangesToCheckFromProgressMetadata(Database cx, int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state std::vector<KeyRange> res;
	state int64_t shardCompleted = 0;
	state Transaction tr(cx);
	state Key rangeToReadBegin = allKeys.begin;
	state Key rangeToReadEnd = allKeys.end;
	loop {
		try {
			shardCompleted = 0;
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			KeyRange rangeToRead = Standalone(KeyRangeRef(rangeToReadBegin, rangeToReadEnd));
			RangeResult res_ = wait(krmGetRanges(&tr,
			                                     consistencyCheckProgressPrefix,
			                                     rangeToRead,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
			                                     CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES));
			for (int i = 0; i < res_.size() - 1; ++i) {
				KeyRange currentRange = KeyRangeRef(res_[i].key, res_[i + 1].key);
				if (!res_[i].value.empty()) {
					ConsistencyCheckState ccState = decodeConsistencyCheckerStateValue(res_[i].value);
					if (ccState.getPhase() == ConsistencyCheckPhase::Invalid) {
						res.push_back(currentRange);
					} else {
						shardCompleted++;
					}
				}
				rangeToReadBegin = res_[i + 1].key;
			}
			if (rangeToReadBegin >= rangeToReadEnd) {
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	TraceEvent("ConsistencyCheckUrgent_GlobalProgress")
	    .suppressFor(10.0)
	    .detail("ConsistencyCheckerId", consistencyCheckerId)
	    .detail("ShardCompleted", shardCompleted);
	return res;
}

ACTOR Future<Void> persistConsistencyCheckAssignment(Database cx,
                                                     int clientId,
                                                     std::vector<KeyRange> assignedRanges,
                                                     int64_t consistencyCheckerId) {
	ASSERT(SERVER_KNOBS->CONSISTENCY_CHECK_USE_PERSIST_DATA);
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> res_ = wait(tr.get(consistencyCheckerIdKey));
			if (!res_.present()) {
				throw key_not_found();
			} else if (consistencyCheckerId != decodeConsistencyCheckerStateValue(res_.get()).consistencyCheckerId) {
				throw consistency_check_task_outdated();
			}
			KeyRangeMap<bool> assignedRangesMap;
			std::vector<Future<Void>> fs;
			for (const auto& assignedRange : assignedRanges) {
				assignedRangesMap.insert(assignedRange, true);
			}
			assignedRangesMap.coalesce(allKeys);
			for (auto& assignedRange : assignedRangesMap.ranges()) {
				if (assignedRange.value() == false) {
					continue;
				}
				fs.push_back(krmSetRange(
				    &tr,
				    consistencyCheckAssignmentPrefixFor(clientId),
				    assignedRange.range(),
				    consistencyCheckerStateValue(ConsistencyCheckState(ConsistencyCheckAssignment::Assigned))));
			}
			wait(waitForAll(fs));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

const std::unordered_map<char, uint8_t> parseCharMap{
	{ '0', 0 },  { '1', 1 },  { '2', 2 },  { '3', 3 },  { '4', 4 },  { '5', 5 },  { '6', 6 },  { '7', 7 },
	{ '8', 8 },  { '9', 9 },  { 'a', 10 }, { 'b', 11 }, { 'c', 12 }, { 'd', 13 }, { 'e', 14 }, { 'f', 15 },
	{ 'A', 10 }, { 'B', 11 }, { 'C', 12 }, { 'D', 13 }, { 'E', 14 }, { 'F', 15 },
};

Key getKeyFromString(const std::string& str) {
	Key emptyKey;
	if (str.size() == 0) {
		return emptyKey;
	}
	if (str.size() % 4 != 0) {
		TraceEvent(SevWarnAlways, "ConsistencyCheck_GetKeyFromStringError")
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("Reason", "WrongLength")
		    .detail("InputStr", str);
		throw internal_error();
	}
	std::vector<uint8_t> byteList;
	for (int i = 0; i < str.size(); i += 4) {
		if (str.at(i + 0) != '\\' || str.at(i + 1) != 'x') {
			TraceEvent(SevWarnAlways, "ConsistencyCheck_GetKeyFromStringError")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Reason", "WrongBytePrefix")
			    .detail("InputStr", str);
			throw internal_error();
		}
		const char first = str.at(i + 2);
		const char second = str.at(i + 3);
		if (parseCharMap.count(first) == 0 || parseCharMap.count(second) == 0) {
			TraceEvent(SevWarnAlways, "ConsistencyCheck_GetKeyFromStringError")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Reason", "WrongByteContent")
			    .detail("InputStr", str);
			throw internal_error();
		}
		uint8_t parsedValue = parseCharMap.at(first) * 16 + parseCharMap.at(second);
		byteList.push_back(parsedValue);
	}
	return Standalone(StringRef(byteList.data(), byteList.size()));
}

std::vector<KeyRange> loadRangesToCheckFromKnob() {
	// Load string from knob
	std::vector<std::string> beginKeyStrs = {
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_0,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_1,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_2,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_3,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_4,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_5,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_6,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_7,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_8,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_9,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_10, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_11,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_12, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_13,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_14, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_15,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_16, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_17,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_18, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_19,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_20, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_21,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_22, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_23,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_24, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_25,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_BEGIN_26,
	};
	std::vector<std::string> endKeyStrs = {
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_0,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_1,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_2,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_3,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_4,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_5,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_6,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_7,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_8,  CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_9,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_10, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_11,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_12, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_13,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_14, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_15,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_16, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_17,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_18, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_19,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_20, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_21,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_22, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_23,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_24, CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_25,
		CLIENT_KNOBS->CONSISTENCY_CHECK_RANGE_END_26,
	};

	// Get keys from strings
	std::vector<Key> beginKeys;
	for (const auto& beginKeyStr : beginKeyStrs) {
		Key key = getKeyFromString(beginKeyStr);
		beginKeys.push_back(key);
	}
	std::vector<Key> endKeys;
	for (const auto& endKeyStr : endKeyStrs) {
		Key key = getKeyFromString(endKeyStr);
		endKeys.push_back(key);
	}
	ASSERT(beginKeys.size() == endKeys.size());

	// Get ranges
	KeyRangeMap<bool> rangeToCheckMap;
	for (int i = 0; i < beginKeys.size(); i++) {
		Key rangeBegin = beginKeys[i];
		Key rangeEnd = endKeys[i];
		if (rangeBegin.empty() && rangeEnd.empty()) {
			continue;
		}
		if (rangeBegin > allKeys.end) {
			rangeBegin = allKeys.end;
		}
		if (rangeEnd > allKeys.end) {
			TraceEvent("ConsistencyCheck_ReverseInputRange")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Index", i)
			    .detail("RangeBegin", rangeBegin)
			    .detail("RangeEnd", rangeEnd);
			rangeEnd = allKeys.end;
		}

		KeyRange rangeToCheck;
		if (rangeBegin < rangeEnd) {
			rangeToCheck = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		} else if (rangeBegin > rangeEnd) {
			rangeToCheck = Standalone(KeyRangeRef(rangeEnd, rangeBegin));
		} else {
			TraceEvent("ConsistencyCheck_EmptyInputRange")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Index", i)
			    .detail("RangeBegin", rangeBegin)
			    .detail("RangeEnd", rangeEnd);
			continue;
		}
		rangeToCheckMap.insert(rangeToCheck, true);
	}

	rangeToCheckMap.coalesce(allKeys);

	std::vector<KeyRange> res;
	for (auto rangeToCheck : rangeToCheckMap.ranges()) {
		if (rangeToCheck.value() == true) {
			res.push_back(rangeToCheck.range());
		}
	}
	TraceEvent e("ConsistencyCheck_LoadedInputRange");
	e.setMaxEventLength(-1);
	e.setMaxFieldLength(-1);
	for (int i = 0; i < res.size(); i++) {
		e.detail("RangeBegin" + std::to_string(i), res[i].begin);
		e.detail("RangeEnd" + std::to_string(i), res[i].end);
	}
	return res;
}
