/*
 * Unreadable.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct UnreadableWorkload : TestWorkload {
	static constexpr auto NAME = "Unreadable";
	uint64_t nodeCount;
	double testDuration;
	std::vector<Future<Void>> clients;

	UnreadableWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 600.0);
		nodeCount = getOption(options, "nodeCount"_sr, (uint64_t)100000);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx, this);
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	static Optional<KeyRef> containsUnreadable(KeyRangeMap<bool>& unreadableMap,
	                                           KeyRangeRef const& range,
	                                           bool forward) {
		auto unreadableRanges = unreadableMap.intersectingRanges(range);

		if (forward) {
			for (auto it : unreadableRanges) {
				if (it.value()) {
					//TraceEvent("RYWT_CheckingRange1").detail("Range", printable(range)).detail("UnreadableRange", printable(it.range()));
					return it.range().begin;
				}
			}
		} else {
			auto it = unreadableRanges.end();
			auto itEnd = unreadableRanges.begin();
			--it;
			--itEnd;

			for (; it != itEnd; --it) {
				if (it.value()) {
					//TraceEvent("RYWT_CheckingRange2").detail("Range", printable(range)).detail("UnreadableRange", printable(it.range()));
					return it.range().end;
				}
			}
		}
		return Optional<KeyRef>();
	}

	static void resolveKeySelector(std::map<KeyRef, ValueRef> const& setMap,
	                               KeyRangeMap<bool>& unreadableMap,
	                               KeySelector& key) {
		ASSERT(!key.orEqual);
		if (key.offset == 1)
			return;

		auto it = setMap.lower_bound(key.getKey());

		if (key.offset >= 1) {
			auto unreadable = containsUnreadable(unreadableMap, KeyRangeRef(key.getKey(), it->first), true);
			if (unreadable.present()) {
				key.setKey(unreadable.get());
				return;
			}
			key.setKey(it->first);
		}

		while (key.offset < 1 && it->first != normalKeys.begin) {
			--it;

			auto unreadable = containsUnreadable(unreadableMap, KeyRangeRef(it->first, key.getKey()), false);
			if (unreadable.present()) {
				key.setKey(unreadable.get());
				return;
			}

			key.setKey(it->first);
			++key.offset;
		}
		while (key.offset > 1 && it->first != normalKeys.end) {
			++it;

			auto unreadable = containsUnreadable(unreadableMap, KeyRangeRef(key.getKey(), it->first), true);
			if (unreadable.present()) {
				if (unreadable.get() > key.getKey())
					--key.offset;
				key.setKey(unreadable.get());
				return;
			}

			key.setKey(it->first);
			--key.offset;
		}

		key.setKey(it->first);
	}

	static void checkUnreadability(std::map<KeyRef, ValueRef> const& setMap,
	                               KeyRangeMap<bool>& unreadableMap,
	                               KeySelectorRef const& _begin,
	                               KeySelectorRef const& _end,
	                               bool isUnreadable,
	                               int limit,
	                               Reverse reverse) {

		/*
		for (auto it : setMap) {
		    TraceEvent("RYWT_SetMapContents").detail("Key", printable(it.first));
		}

		for (auto it : unreadableMap.ranges()) {
		    TraceEvent("RYWT_UnreadableMapContents").detail("Key", printable(it.range())).detail("Value", it.value());
		}
		*/

		KeySelector begin = _begin;
		KeySelector end = _end;

		begin.removeOrEqual(begin.arena());
		end.removeOrEqual(end.arena());

		if (begin.offset >= end.offset && begin.getKey() >= end.getKey()) {
			ASSERT(isUnreadable == false);
			return;
		}

		//TraceEvent("RYWT_CheckUnreadability").detail("Begin", begin.toString()).detail("End", end.toString());

		KeySelector resolvedBegin = begin;
		KeySelector resolvedEnd = end;

		resolveKeySelector(setMap, unreadableMap, resolvedBegin);
		resolveKeySelector(setMap, unreadableMap, resolvedEnd);

		//TraceEvent("RYWT_CheckUnreadability2").detail("ResolvedBegin", resolvedBegin.toString()).detail("ResolvedEnd", resolvedEnd.toString());

		if ((resolvedBegin.offset >= resolvedEnd.offset && resolvedBegin.getKey() >= resolvedEnd.getKey()) ||
		    (resolvedBegin.offset >= end.offset && resolvedBegin.getKey() >= end.getKey()) ||
		    (begin.offset >= resolvedEnd.offset && begin.getKey() >= resolvedEnd.getKey())) {
			// RYW does not perfectly optimize this scenario, it should be readable but might unnecessarily return as
			// unreadable
			return;
		}

		if (resolvedEnd.getKey() == normalKeys.begin || resolvedBegin.getKey() == normalKeys.end) {
			// RYW does not perfectly optimize this scenario, it should be readable but might unnecessarily return as
			// unreadable
			return;
		}

		bool beginUnreadable = resolvedBegin.offset != 1 && resolvedBegin.getKey() != normalKeys.begin;
		bool endUnreadable = resolvedEnd.offset != 1 && resolvedEnd.getKey() != normalKeys.end;

		if (!reverse && !beginUnreadable) {
			int itemCount = resolvedBegin.getKey() == normalKeys.begin ? 0 : 1;
			itemCount += std::min(0, resolvedEnd.offset - 1);
			auto setItem = setMap.lower_bound(resolvedBegin.getKey());

			while (itemCount < limit && setItem->first != normalKeys.end) {
				++setItem;
				++itemCount;
			}

			//TraceEvent("RYWT_ModifiedEnd").detail("SetItem", printable(setItem->first)).detail("ResolvedEnd", printable(resolvedEnd.getKey())).detail("Limit", limit).detail("ItemCount", resolvedBegin.getKey() == normalKeys.begin ? 0 : 1 + std::min(0, resolvedEnd.offset - 1));

			KeyRef keyAfterSet = keyAfter(setItem->first, resolvedEnd.arena());
			if (keyAfterSet <= resolvedEnd.getKey()) {
				if (std::max(begin.getKey(), resolvedBegin.getKey()) > std::min(end.getKey(), resolvedEnd.getKey())) {
					// RYW might have to resolve the key selector anyways in this scenario, even though the limit should
					// make this unnecessary.
					return;
				}

				if (end.offset < resolvedEnd.offset && end.offset < 1) {
					int itemCount2 = resolvedBegin.getKey() == normalKeys.begin ? 0 : 1;
					itemCount2 += std::min(0, end.offset - 1);
					auto setItem2 = setMap.lower_bound(resolvedBegin.getKey());

					while (itemCount2 < limit && setItem2->first != normalKeys.end) {
						++setItem2;
						++itemCount2;
					}

					Key keyAfterSet2 = keyAfter(setItem2->first);
					if (containsUnreadable(unreadableMap, KeyRangeRef(keyAfterSet, keyAfterSet2), true).present()) {
						// RYW might not have resolved the end selector
						return;
					}
				}

				resolvedEnd.setKey(keyAfterSet);
				resolvedEnd.offset = 1;
				endUnreadable = false;
			}
		} else if (reverse && !endUnreadable) {
			int itemCount = 0;
			itemCount -= std::max(0, resolvedBegin.offset - 1);
			auto setItem = setMap.lower_bound(resolvedEnd.getKey());
			while (itemCount < limit && setItem->first != normalKeys.begin) {
				--setItem;
				++itemCount;
			}

			//TraceEvent("RYWT_ModifiedBegin").detail("SetItem", printable(setItem->first)).detail("ResolvedBegin", printable(resolvedBegin.getKey())).detail("Limit", limit).detail("ItemCount", -1*std::max(0, resolvedBegin.offset - 1));

			if (setItem->first >= resolvedBegin.getKey()) {
				if (std::max(begin.getKey(), resolvedBegin.getKey()) > std::min(end.getKey(), resolvedEnd.getKey())) {
					// RYW might have to resolve the key selector anyways in this scenario, even though the limit should
					// make this unnecessary.
					return;
				}

				if (begin.offset > resolvedBegin.offset && begin.offset > 1) {
					int itemCount2 = 0;
					itemCount2 -= std::max(0, begin.offset - 1);
					auto setItem2 = setMap.lower_bound(resolvedEnd.getKey());

					while (itemCount2 < limit && setItem2->first != normalKeys.begin) {
						--setItem2;
						++itemCount2;
					}

					if (containsUnreadable(unreadableMap, KeyRangeRef(setItem2->first, setItem->first), true)
					        .present()) {
						// RYW might not have resolved the begin selector
						return;
					}
				}

				resolvedBegin.setKey(setItem->first);
				resolvedBegin.offset = 1;
				beginUnreadable = false;
			}
		}

		if (beginUnreadable || endUnreadable) {
			ASSERT(isUnreadable == true);
			return;
		}

		if (!reverse && resolvedEnd.getKey() != normalKeys.end &&
		    std::max(begin.getKey(), resolvedBegin.getKey()) > std::min(end.getKey(), resolvedEnd.getKey()) &&
		    !end.isFirstGreaterOrEqual()) {
			auto setNext = setMap.lower_bound(resolvedEnd.getKey());
			auto unreadableRanges = unreadableMap.intersectingRanges(KeyRangeRef(resolvedEnd.getKey(), normalKeys.end));
			KeyRef unreadableNext = normalKeys.end;
			for (auto it : unreadableRanges) {
				if (it.value()) {
					unreadableNext = it.begin();
					break;
				}
			}

			//TraceEvent("RYWT_CheckUnreadability3").detail("SetNext", printable(setNext->first)).detail("UnreadableNext", printable(unreadableNext));
			if (setNext->first >= unreadableNext) {
				// RYW resolves the end key selector, even though it does not need the exact key to answer the query.
				return;
			}
		}

		ASSERT(isUnreadable ==
		       containsUnreadable(unreadableMap, KeyRangeRef(resolvedBegin.getKey(), resolvedEnd.getKey()), true)
		           .present());
	}

	ACTOR Future<Void> _start(Database cx, UnreadableWorkload* self) {
		state int testCount = 0;
		state Reverse reverse = Reverse::False;
		state Snapshot snapshot = Snapshot::False;
		for (; testCount < 100; testCount++) {
			//TraceEvent("RYWT_Start").detail("TestCount", testCount);
			state ReadYourWritesTransaction tr(cx);
			state Arena arena;

			state std::map<KeyRef, ValueRef> setMap;
			state KeyRangeMap<bool> unreadableMap;

			state int opCount = 0;
			state KeyRangeRef range;
			state KeyRef key;
			state ValueRef value;
			state int limit;
			state KeySelectorRef begin;
			state KeySelectorRef end;
			state bool bypassUnreadable = deterministicRandom()->coinflip();
			if (bypassUnreadable) {
				tr.setOption(FDBTransactionOptions::BYPASS_UNREADABLE);
			}

			setMap[normalKeys.begin] = ValueRef();
			setMap[normalKeys.end] = ValueRef();

			for (; opCount < 500; opCount++) {
				int r = deterministicRandom()->randomInt(0, 19);
				if (r <= 10) {
					key = RandomTestImpl::getRandomKey(arena);
					value = RandomTestImpl::getRandomValue(arena);
					tr.set(key, value);
					setMap[key] = value;
					//TraceEvent("RYWT_Set").detail("Key", printable(key));
				} else if (r == 11) {
					range = RandomTestImpl::getRandomRange(arena);
					tr.addReadConflictRange(range);
					//TraceEvent("RYWT_AddReadConflictRange").detail("Range", printable(range));
				} else if (r == 12) {
					range = RandomTestImpl::getRandomRange(arena);
					tr.addWriteConflictRange(range);
					//TraceEvent("RYWT_AddWriteConflictRange").detail("Range", printable(range));
				} else if (r == 13) {
					range = RandomTestImpl::getRandomRange(arena);
					tr.clear(range);
					unreadableMap.insert(range, false);
					setMap.erase(setMap.lower_bound(range.begin), setMap.lower_bound(range.end));
					//TraceEvent("RYWT_Clear").detail("Range", printable(range));
				} else if (r == 14) {
					key = RandomTestImpl::getRandomKey(arena);
					value = RandomTestImpl::getRandomVersionstampValue(arena);
					tr.atomicOp(key, value, MutationRef::SetVersionstampedValue);
					unreadableMap.insert(key, true);
					//TraceEvent("RYWT_SetVersionstampValue").detail("Key", printable(key));
				} else if (r == 15) {
					key = RandomTestImpl::getRandomVersionstampKey(arena);
					value = RandomTestImpl::getRandomValue(arena);
					tr.atomicOp(key, value, MutationRef::SetVersionstampedKey);
					range = getVersionstampKeyRange(arena, key, tr.getCachedReadVersion().orDefault(0), allKeys.end);
					unreadableMap.insert(range, true);
					//TraceEvent("RYWT_SetVersionstampKey").detail("Range", printable(range));
				} else if (r == 16) {
					range = RandomTestImpl::getRandomRange(arena);
					snapshot.set(deterministicRandom()->random01() < 0.05);
					reverse.set(deterministicRandom()->coinflip());

					if (snapshot)
						tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);

					ErrorOr<RangeResult> value =
					    wait(errorOr(tr.getRange(range, CLIENT_KNOBS->TOO_MANY, snapshot, reverse)));

					if (snapshot)
						tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_ENABLE);
					if (!value.isError() || value.getError().code() == error_code_accessed_unreadable) {
						//TraceEvent("RYWT_GetRange").detail("Range", printable(range)).detail("IsUnreadable", value.isError());
						if (snapshot) {
							ASSERT(!value.isError());
						} else {
							ASSERT(containsUnreadable(unreadableMap, range, true).present() == value.isError());
						}
					} else {
						//TraceEvent("RYWT_Reset1").error(value.getError(), true);
						setMap = std::map<KeyRef, ValueRef>();
						unreadableMap = KeyRangeMap<bool>();
						tr = ReadYourWritesTransaction(cx);
						if (bypassUnreadable) {
							tr.setOption(FDBTransactionOptions::BYPASS_UNREADABLE);
						}
						arena = Arena();

						setMap[normalKeys.begin] = ValueRef();
						setMap[normalKeys.end] = ValueRef();
					}
				} else if (r == 17) {
					begin = RandomTestImpl::getRandomKeySelector(arena);
					end = RandomTestImpl::getRandomKeySelector(arena);
					limit = deterministicRandom()->randomInt(1, 100); // maximum number of results to return from the db
					snapshot.set(deterministicRandom()->random01() < 0.05);
					reverse.set(deterministicRandom()->coinflip());

					if (snapshot)
						tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);

					//TraceEvent("RYWT_GetRangeBefore").detail("Reverse", reverse).detail("Begin", begin.toString()).detail("End", end.toString()).detail("Limit", limit);
					ErrorOr<RangeResult> value = wait(errorOr(tr.getRange(begin, end, limit, snapshot, reverse)));

					if (snapshot)
						tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_ENABLE);
					bool isUnreadable = value.isError() && value.getError().code() == error_code_accessed_unreadable;
					if (!value.isError() || value.getError().code() == error_code_accessed_unreadable) {
						/*
						RangeResult result = value.get();
						TraceEvent("RYWT_GetKeySelRangeOk")
						    .detail("Begin", begin.toString())
						    .detail("End", end.toString())
						    .detail("Size", result.size())
						    .detail("Reverse", reverse);
						for (auto it : result) {
						    TraceEvent("RYWT_GetKeySelRange returned").detail("Key", printable(it.key));
						}
						*/
						//TraceEvent("RYWT_GetKeySelRangeUnreadable").detail("Begin", begin.toString()).detail("End", end.toString()).detail("Reverse", reverse);
						if (snapshot) {
							ASSERT(!isUnreadable);
						} else {
							checkUnreadability(setMap, unreadableMap, begin, end, isUnreadable, limit, reverse);
						}
					} else {
						//TraceEvent("RYWT_GetKeySelRangeReset");
						setMap = std::map<KeyRef, ValueRef>();
						unreadableMap = KeyRangeMap<bool>();
						tr = ReadYourWritesTransaction(cx);
						if (bypassUnreadable) {
							tr.setOption(FDBTransactionOptions::BYPASS_UNREADABLE);
						}
						arena = Arena();

						setMap[normalKeys.begin] = ValueRef();
						setMap[normalKeys.end] = ValueRef();
					}
				} else if (r == 18) {
					key = RandomTestImpl::getRandomKey(arena);
					snapshot.set(deterministicRandom()->random01() < 0.05);

					if (snapshot)
						tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);

					ErrorOr<Optional<Value>> value = wait(errorOr(tr.get(key, snapshot)));

					if (snapshot)
						tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_ENABLE);

					if (!value.isError() || value.getError().code() == error_code_accessed_unreadable) {
						//TraceEvent("RYWT_Get").detail("Key", printable(key)).detail("IsUnreadable", value.isError());
						if (snapshot || bypassUnreadable) {
							ASSERT(!value.isError());
						} else {
							ASSERT(unreadableMap[key] == value.isError());
						}
					} else {
						//TraceEvent("RYWT_Reset3");
						setMap = std::map<KeyRef, ValueRef>();
						unreadableMap = KeyRangeMap<bool>();
						tr = ReadYourWritesTransaction(cx);
						if (bypassUnreadable) {
							tr.setOption(FDBTransactionOptions::BYPASS_UNREADABLE);
						}
						arena = Arena();

						setMap[normalKeys.begin] = ValueRef();
						setMap[normalKeys.end] = ValueRef();
					}
				}
			}
		}

		return Void();
	}
};

WorkloadFactory<UnreadableWorkload> UnreadableWorkloadFactory;
