/*
 * Tester.actor.cpp
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

#include "Tester.actor.h"
#include <cinttypes>
#ifdef __linux__
#include <string.h>
#endif

#include "bindings/flow/Tuple.h"
#include "bindings/flow/FDBLoanerTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/DeterministicRandom.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Otherwise we have to type setupNetwork(), FDB::open(), etc.
using namespace FDB;

std::map<std::string, FDBMutationType> optionInfo;
std::set<std::string> opsThatCreateDirectories;

std::map<Standalone<StringRef>, Reference<Transaction>> trMap;

// NOTE: This was taken from within fdb_c.cpp (where it is defined as a static within the get_range function).
// If that changes, this will also have to be changed.
const int ITERATION_PROGRESSION[] = { 256, 1000, 4096, 6144, 9216, 13824, 20736, 31104, 46656, 69984, 80000 };
const int MAX_ITERATION = sizeof(ITERATION_PROGRESSION) / sizeof(int);

static Future<Void> runTest(Reference<FlowTesterData> const& data,
                            Reference<Database> const& db,
                            StringRef const& prefix);

THREAD_FUNC networkThread(void* api) {
	// This is the fdb_flow network we're running on a thread
	((API*)api)->runNetwork();
	THREAD_RETURN;
}

bool hasEnding(std::string const& fullString, std::string const& ending) {
	if (fullString.length() >= ending.length()) {
		return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
	} else {
		return false;
	}
}

ACTOR Future<std::vector<Tuple>> waitAndPop(FlowTesterStack* self, int count) {
	state std::vector<Tuple> tuples;
	state std::vector<StackItem> items = self->pop(count);

	state int index;
	for (index = 0; index < items.size(); ++index) {
		Standalone<StringRef> itemStr = wait(items[index].value);
		tuples.push_back(Tuple::unpack(itemStr));
	}

	return tuples;
}

Future<std::vector<Tuple>> FlowTesterStack::waitAndPop(int count) {
	return ::waitAndPop(this, count);
}

ACTOR Future<Tuple> waitAndPop(FlowTesterStack* self) {
	std::vector<Tuple> tuples = wait(waitAndPop(self, 1));
	return tuples[0];
}

Future<Tuple> FlowTesterStack::waitAndPop() {
	return ::waitAndPop(this);
}

std::string tupleToString(Tuple const& tuple) {
	std::string str = "(";
	for (int i = 0; i < tuple.size(); ++i) {
		Tuple::ElementType type = tuple.getType(i);
		if (type == Tuple::NULL_TYPE) {
			str += "NULL";
		} else if (type == Tuple::BYTES || type == Tuple::UTF8) {
			if (type == Tuple::UTF8) {
				str += "u";
			}
			str += "\'" + tuple.getString(i).printable() + "\'";
		} else if (type == Tuple::INT) {
			str += format("%ld", tuple.getInt(i));
		} else if (type == Tuple::FLOAT) {
			str += format("%f", tuple.getFloat(i));
		} else if (type == Tuple::DOUBLE) {
			str += format("%f", tuple.getDouble(i));
		} else if (type == Tuple::BOOL) {
			str += tuple.getBool(i) ? "true" : "false";
		} else if (type == Tuple::UUID) {
			Uuid u = tuple.getUuid(i);
			str += format("%016llx%016llx", *(uint64_t*)u.getData().begin(), *(uint64_t*)(u.getData().begin() + 8));
		} else if (type == Tuple::NESTED) {
			str += tupleToString(tuple.getNested(i));
		} else {
			ASSERT(false);
		}

		if (i < tuple.size() - 1) {
			str += ", ";
		}
	}

	str += ")";
	return str;
}

ACTOR Future<Standalone<RangeResultRef>> getRange(Reference<Transaction> tr,
                                                  KeySelectorRef begin,
                                                  KeySelectorRef end,
                                                  int limits = 0,
                                                  bool snapshot = false,
                                                  bool reverse = false,
                                                  FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) {
	state KeySelector ks_begin(begin);
	state KeySelector ks_end(end);
	state Standalone<RangeResultRef> results;
	state int iteration = 1;
	loop {
		// printf("=====DB: begin:%s, end:%s, limits:%d\n", printable(begin.key).c_str(), printable(end.key).c_str(),
		// limits);
		state FDBStandalone<RangeResultRef> r;
		if (streamingMode == FDB_STREAMING_MODE_ITERATOR && iteration > 1) {
			int effective_iteration = std::min(iteration, MAX_ITERATION);
			int bytes_limit = ITERATION_PROGRESSION[effective_iteration - 1];
			FDBStandalone<RangeResultRef> rTemp = wait(tr->getRange(ks_begin,
			                                                        ks_end,
			                                                        GetRangeLimits(limits, bytes_limit),
			                                                        snapshot,
			                                                        reverse,
			                                                        (FDBStreamingMode)FDB_STREAMING_MODE_EXACT));
			r = rTemp;
		} else {
			FDBStandalone<RangeResultRef> rTemp =
			    wait(tr->getRange(ks_begin, ks_end, limits, snapshot, reverse, streamingMode));
			r = rTemp;
		}
		iteration += 1;
		// printf("=====DB: count:%d\n", r.size());
		for (auto& s : r) {
			// printf("=====key:%s, value:%s\n", printable(StringRef(s.key)).c_str(),
			// printable(StringRef(s.value)).c_str());
			results.push_back_deep(results.arena(), s);

			if (reverse)
				ks_end = KeySelector(firstGreaterOrEqual(s.key));
			else
				ks_begin = KeySelector(firstGreaterThan(s.key));
		}

		ASSERT(limits == 0 || limits >= r.size());

		if (!r.more || (limits > 0 && limits == r.size())) {
			return results;
		}

		if (limits > 0) {
			limits -= r.size();
		}
	}
}

ACTOR Future<Standalone<RangeResultRef>> getRange(Reference<Transaction> tr,
                                                  KeyRange keys,
                                                  int limits = 0,
                                                  bool snapshot = false,
                                                  bool reverse = false,
                                                  FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) {
	state Key begin(keys.begin);
	state Key end(keys.end);
	state Standalone<RangeResultRef> results;
	state int iteration = 1;
	loop {
		// printf("=====DB: begin:%s, limits:%d\n", printable(begin).c_str(), limits);
		KeyRange keyRange(KeyRangeRef(begin, end > begin ? end : begin));
		state FDBStandalone<RangeResultRef> r;
		if (streamingMode == FDB_STREAMING_MODE_ITERATOR && iteration > 1) {
			int effective_iteration = std::min(iteration, MAX_ITERATION);
			int bytes_limit = ITERATION_PROGRESSION[effective_iteration - 1];
			FDBStandalone<RangeResultRef> rTemp = wait(tr->getRange(keyRange,
			                                                        GetRangeLimits(limits, bytes_limit),
			                                                        snapshot,
			                                                        reverse,
			                                                        (FDBStreamingMode)FDB_STREAMING_MODE_EXACT));
			r = rTemp;
		} else {
			FDBStandalone<RangeResultRef> rTemp =
			    wait(tr->getRange(keyRange, limits, snapshot, reverse, streamingMode));
			r = rTemp;
		}
		iteration += 1;
		// printf("=====DB: count:%d\n", r.size());
		for (auto& s : r) {
			// printf("=====key:%s, value:%s\n", printable(StringRef(s.key)).c_str(),
			// printable(StringRef(s.value)).c_str());
			results.push_back_deep(results.arena(), s);

			if (reverse)
				end = s.key;
			else
				begin = keyAfter(s.key);
		}

		ASSERT(limits == 0 || limits >= r.size());

		if (!r.more || (limits > 0 && limits == r.size())) {
			return results;
		}

		if (limits > 0) {
			limits -= r.size();
		}
	}
}

// ACTOR static Future<Void> debugPrintRange(Reference<Transaction> tr, std::string subspace, std::string msg) {
//	if (!tr)
//		return Void();
//
//	Standalone<RangeResultRef> results = wait(getRange(tr, KeyRange(KeyRangeRef(subspace + '\x00', subspace +
//'\xff')))); 	printf("==================================================DB:%s:%s, count:%d\n", msg.c_str(),
//	       StringRef(subspace).printable().c_str(), results.size());
//	for (auto & s : results) {
//		printf("=====key:%s, value:%s\n", StringRef(s.key).printable().c_str(), StringRef(s.value).printable().c_str());
//	}
//
//	return Void();
//}

ACTOR Future<Void> stackSub(FlowTesterStack* stack) {
	if (stack->data.size() < 2)
		return Void();

	StackItem a = stack->data.back();
	stack->data.pop_back();
	state Standalone<StringRef> sa = wait(a.value);

	StackItem b = stack->data.back();
	stack->data.pop_back();
	Standalone<StringRef> sb = wait(b.value);

	int64_t c = Tuple::unpack(sa).getInt(0) - Tuple::unpack(sb).getInt(0);
	Tuple f;
	f.append(c);
	stack->push(f.pack());

	return Void();
}

ACTOR Future<Void> stackConcat(FlowTesterStack* stack) {
	if (stack->data.size() < 2)
		return Void();

	StackItem a = stack->data.back();
	stack->data.pop_back();
	state Standalone<StringRef> sa = wait(a.value);
	state Tuple ta = Tuple::unpack(sa);

	StackItem b = stack->data.back();
	stack->data.pop_back();
	Standalone<StringRef> sb = wait(b.value);
	state Tuple tb = Tuple::unpack(sb);

	ASSERT(ta.getType(0) == tb.getType(0));
	stack->pushTuple(tb.getString(0).withPrefix(ta.getString(0)), ta.getType(0) == Tuple::ElementType::UTF8);

	return Void();
}

ACTOR Future<Void> stackSwap(FlowTesterStack* stack) {
	if (stack->data.size() < 3)
		return Void();

	StackItem pop = stack->data.back();
	stack->data.pop_back();

	Standalone<StringRef> sv = wait(pop.value);
	int64_t idx = stack->data.size() - 1;
	int64_t idx1 = idx - Tuple::unpack(sv).getInt(0);
	if (idx1 < idx) {
		// printf("=============SWAP:%d,%d\n", idx, stack->data.size());
		StackItem item = stack->data[idx];
		stack->data[idx] = stack->data[idx1];
		stack->data[idx1] = item;
	}
	return Void();
}

ACTOR Future<Void> printFlowTesterStack(FlowTesterStack* stack) {
	// printf("====================stack item count:%ld\n", stack->data.size());
	state int idx;
	for (idx = stack->data.size() - 1; idx >= 0; --idx) {
		Standalone<StringRef> value = wait(stack->data[idx].value);
		// printf("==========stack item:%d, index:%d, value:%s\n", idx, stack->data[idx].index,
		// value.printable().c_str());
	}
	return Void();
}

//
// Data Operations
//

struct PushFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		Tuple t = Tuple::unpack(instruction->instruction);
		Standalone<StringRef> param = t.subTuple(1).pack();
		data->stack.push(param);
		return Void();
	}
};
const char* PushFunc::name = "PUSH";
REGISTER_INSTRUCTION_FUNC(PushFunc);

struct DupFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		data->stack.dup();
		return Void();
	}
};
const char* DupFunc::name = "DUP";
REGISTER_INSTRUCTION_FUNC(DupFunc);

struct EmptyStackFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		// wait(printFlowTesterStack(&(data->stack)));
		// wait(debugPrintRange(instruction->tr, "\x01test_results", ""));
		data->stack.clear();
		return Void();
	}
};
const char* EmptyStackFunc::name = "EMPTY_STACK";
REGISTER_INSTRUCTION_FUNC(EmptyStackFunc);

struct SwapFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		wait(stackSwap(&(data->stack)));
		return Void();
	}
};
const char* SwapFunc::name = "SWAP";
REGISTER_INSTRUCTION_FUNC(SwapFunc);

struct PopFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		for (StackItem item : items) {
			wait(success(item.value));
		}
		return Void();
	}
};
const char* PopFunc::name = "POP";
REGISTER_INSTRUCTION_FUNC(PopFunc);

struct SubFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		wait(stackSub(&(data->stack)));
		return Void();
	}
};
const char* SubFunc::name = "SUB";
REGISTER_INSTRUCTION_FUNC(SubFunc);

struct ConcatFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		wait(stackConcat(&(data->stack)));
		return Void();
	}
};
const char* ConcatFunc::name = "CONCAT";
REGISTER_INSTRUCTION_FUNC(ConcatFunc);

struct LogStackFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> logStack(Reference<FlowTesterData> data,
	                                   std::map<int, StackItem> entries,
	                                   Standalone<StringRef> prefix) {
		loop {
			state Reference<Transaction> tr = data->db->createTransaction();
			try {
				for (auto it : entries) {
					Tuple tk;
					tk.append(it.first);
					tk.append((int64_t)it.second.index);
					state Standalone<StringRef> pk = tk.pack().withPrefix(prefix);
					Standalone<StringRef> pv = wait(it.second.value);
					tr->set(pk, pv.substr(0, std::min(pv.size(), 40000)));
				}

				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.empty())
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> prefix = Tuple::unpack(s1).getString(0);

		state std::map<int, StackItem> entries;
		while (data->stack.data.size() > 0) {
			state std::vector<StackItem> it = data->stack.pop();
			ASSERT(it.size() == 1);
			entries[data->stack.data.size()] = it.front();
			if (entries.size() == 100) {
				wait(logStack(data, entries, prefix));
				entries.clear();
			}
		}
		wait(logStack(data, entries, prefix));

		return Void();
	}
};
const char* LogStackFunc::name = "LOG_STACK";
REGISTER_INSTRUCTION_FUNC(LogStackFunc);

//
// FoundationDB Operations
//
ACTOR Future<Standalone<StringRef>> waitForVoid(Future<Void> f) {
	try {
		wait(f);
		Tuple t;
		t.append(LiteralStringRef("RESULT_NOT_PRESENT"));
		return t.pack();
	} catch (Error& e) {
		// printf("FDBError1:%d\n", e.code());
		Tuple t;
		t.append(LiteralStringRef("ERROR"));
		t.append(format("%d", e.code()));
		// pack above as error string into another tuple
		Tuple ret;
		ret.append(t.pack());
		return ret.pack();
	}
}

ACTOR Future<Standalone<StringRef>> waitForValue(Future<FDBStandalone<KeyRef>> f) {
	try {
		FDBStandalone<KeyRef> value = wait(f);
		Tuple t;
		t.append(value);
		return t.pack();
	} catch (Error& e) {
		// printf("FDBError2:%d\n", e.code());
		Tuple t;
		t.append(LiteralStringRef("ERROR"));
		t.append(format("%d", e.code()));
		// pack above as error string into another tuple
		Tuple ret;
		ret.append(t.pack());
		return ret.pack();
	}
}

ACTOR Future<Standalone<StringRef>> waitForValue(Future<Optional<FDBStandalone<ValueRef>>> f) {
	try {
		Optional<FDBStandalone<ValueRef>> value = wait(f);
		Standalone<StringRef> str;
		if (value.present())
			str = value.get();
		else
			str = LiteralStringRef("RESULT_NOT_PRESENT");

		Tuple t;
		t.append(str);
		return t.pack();
	} catch (Error& e) {
		// printf("FDBError3:%d\n", e.code());
		Tuple t;
		t.append(LiteralStringRef("ERROR"));
		t.append(format("%d", e.code()));
		// pack above as error string into another tuple
		Tuple ret;
		ret.append(t.pack());
		return ret.pack();
	}
}

ACTOR Future<Standalone<StringRef>> getKey(Future<FDBStandalone<KeyRef>> f, Standalone<StringRef> prefixFilter) {
	try {
		FDBStandalone<KeyRef> key = wait(f);
		Tuple t;

		if (key.startsWith(prefixFilter)) {
			t.append(key);
		} else if (key < prefixFilter) {
			t.append(prefixFilter);
		} else {
			t.append(strinc(prefixFilter));
		}

		return t.pack();
	} catch (Error& e) {
		// printf("FDBError4:%d\n", e.code());
		Tuple t;
		t.append(LiteralStringRef("ERROR"));
		t.append(format("%d", e.code()));
		// pack above as error string into another tuple
		Tuple ret;
		ret.append(t.pack());
		return ret.pack();
	}
}

struct NewTransactionFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		trMap[data->trName] = data->db->createTransaction();
		return Void();
	}
};
const char* NewTransactionFunc::name = "NEW_TRANSACTION";
REGISTER_INSTRUCTION_FUNC(NewTransactionFunc);

struct UseTransactionFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		Standalone<StringRef> name = wait(items[0].value);
		data->trName = name;

		if (trMap.count(data->trName) == 0) {
			trMap[data->trName] = data->db->createTransaction();
		}
		return Void();
	}
};
const char* UseTransactionFunc::name = "USE_TRANSACTION";
REGISTER_INSTRUCTION_FUNC(UseTransactionFunc);

struct OnErrorFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.empty())
			return Void();

		Standalone<StringRef> value = wait(items[0].value);
		int err_code = Tuple::unpack(value).getInt(0);
		// printf("OnError:%d:%d:%s\n", err_code, items[0].index, printable(value).c_str());

		data->stack.push(waitForVoid(instruction->tr->onError(Error(err_code))));
		return Void();
	}
};
const char* OnErrorFunc::name = "ON_ERROR";
REGISTER_INSTRUCTION_FUNC(OnErrorFunc);

struct SetFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(2);
		if (items.size() != 2)
			return Void();

		Standalone<StringRef> sk = wait(items[0].value);
		state Standalone<StringRef> key = Tuple::unpack(sk).getString(0);
		// if (instruction->isDatabase)
		// printf("SetDatabase:%s, isDatabase:%d\n", printable(key).c_str(), instruction->isDatabase);
		Standalone<StringRef> sv = wait(items[1].value);
		Standalone<StringRef> value = Tuple::unpack(sv).getString(0);
		// printf("SetDatabase:%s:%s:%s\n", printable(key).c_str(), printable(sv).c_str(), printable(value).c_str());

		Reference<InstructionData> instructionCopy = instruction;
		Standalone<StringRef> keyCopy = key;

		Future<Void> mutation = executeMutation(instruction, [instructionCopy, keyCopy, value]() -> Future<Void> {
			instructionCopy->tr->set(keyCopy, value);
			return Void();
		});

		if (instruction->isDatabase) {
			data->stack.push(waitForVoid(mutation));
		} else {
			wait(mutation);
		}

		return Void();
	}
};
const char* SetFunc::name = "SET";
REGISTER_INSTRUCTION_FUNC(SetFunc);

struct GetFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> sk = wait(items[0].value);
		state Standalone<StringRef> key = Tuple::unpack(sk).getString(0);

		Future<Optional<FDBStandalone<ValueRef>>> fk = instruction->tr->get(StringRef(key), instruction->isSnapshot);
		data->stack.push(waitForValue(holdWhile(instruction->tr, fk)));

		return Void();
	}
};
const char* GetFunc::name = "GET";
REGISTER_INSTRUCTION_FUNC(GetFunc);

struct GetEstimatedRangeSize : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(2);
		if (items.size() != 2)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> beginKey = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		state Standalone<StringRef> endKey = Tuple::unpack(s2).getString(0);
		Future<int64_t> fsize = instruction->tr->getEstimatedRangeSizeBytes(KeyRangeRef(beginKey, endKey));
		int64_t size = wait(fsize);
		data->stack.pushTuple(LiteralStringRef("GOT_ESTIMATED_RANGE_SIZE"));

		return Void();
	}
};
const char* GetEstimatedRangeSize::name = "GET_ESTIMATED_RANGE_SIZE";
REGISTER_INSTRUCTION_FUNC(GetEstimatedRangeSize);

struct GetRangeSplitPoints : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(3);
		if (items.size() != 3)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> beginKey = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		state Standalone<StringRef> endKey = Tuple::unpack(s2).getString(0);

		Standalone<StringRef> s3 = wait(items[2].value);
		state int64_t chunkSize = Tuple::unpack(s3).getInt(0);

		Future<FDBStandalone<VectorRef<KeyRef>>> fsplitPoints =
		    instruction->tr->getRangeSplitPoints(KeyRangeRef(beginKey, endKey), chunkSize);
		FDBStandalone<VectorRef<KeyRef>> splitPoints = wait(fsplitPoints);
		data->stack.pushTuple(LiteralStringRef("GOT_RANGE_SPLIT_POINTS"));

		return Void();
	}
};
const char* GetRangeSplitPoints::name = "GET_RANGE_SPLIT_POINTS";
REGISTER_INSTRUCTION_FUNC(GetRangeSplitPoints);

struct GetKeyFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(4);
		if (items.size() != 4)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> key = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		state int64_t or_equal = Tuple::unpack(s2).getInt(0);

		Standalone<StringRef> s3 = wait(items[2].value);
		state int64_t offset = Tuple::unpack(s3).getInt(0);

		Standalone<StringRef> s4 = wait(items[3].value);
		Standalone<StringRef> prefix = Tuple::unpack(s4).getString(0);

		// printf("===================GET_KEY:%s, %ld, %ld\n", printable(key).c_str(), or_equal, offset);
		Future<FDBStandalone<KeyRef>> fk =
		    instruction->tr->getKey(KeySelector(KeySelectorRef(key, or_equal, offset)), instruction->isSnapshot);
		data->stack.push(getKey(holdWhile(instruction->tr, fk), prefix));

		return Void();
	}
};
const char* GetKeyFunc::name = "GET_KEY";
REGISTER_INSTRUCTION_FUNC(GetKeyFunc);

struct GetReadVersionFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Version v = wait(instruction->tr->getReadVersion());
		data->lastVersion = v;
		data->stack.pushTuple(LiteralStringRef("GOT_READ_VERSION"));
		return Void();
	}
};
const char* GetReadVersionFunc::name = "GET_READ_VERSION";
REGISTER_INSTRUCTION_FUNC(GetReadVersionFunc);

struct SetReadVersionFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		instruction->tr->setReadVersion(data->lastVersion);
		return Void();
	}
};
const char* SetReadVersionFunc::name = "SET_READ_VERSION";
REGISTER_INSTRUCTION_FUNC(SetReadVersionFunc);

// GET_COMMITTED_VERSION
struct GetCommittedVersionFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		data->lastVersion = instruction->tr->getCommittedVersion();
		data->stack.pushTuple(LiteralStringRef("GOT_COMMITTED_VERSION"));
		return Void();
	}
};
const char* GetCommittedVersionFunc::name = "GET_COMMITTED_VERSION";
REGISTER_INSTRUCTION_FUNC(GetCommittedVersionFunc);

// GET_APPROXIMATE_SIZE
struct GetApproximateSizeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		int64_t _ = wait(instruction->tr->getApproximateSize());
		(void)_; // disable unused variable warning
		data->stack.pushTuple(LiteralStringRef("GOT_APPROXIMATE_SIZE"));
		return Void();
	}
};
const char* GetApproximateSizeFunc::name = "GET_APPROXIMATE_SIZE";
REGISTER_INSTRUCTION_FUNC(GetApproximateSizeFunc);

// GET_VERSIONSTAMP
struct GetVersionstampFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		data->stack.push(waitForValue(instruction->tr->getVersionstamp()));
		return Void();
	}
};
const char* GetVersionstampFunc::name = "GET_VERSIONSTAMP";
REGISTER_INSTRUCTION_FUNC(GetVersionstampFunc);

// COMMIT
struct CommitFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		data->stack.push(waitForVoid(holdWhile(instruction->tr, instruction->tr->commit())));
		return Void();
	}
};
const char* CommitFunc::name = "COMMIT";
REGISTER_INSTRUCTION_FUNC(CommitFunc);

// WAIT_FUTURE
struct WaitFutureFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> sk = wait(items[0].value);
		data->stack.push(StackItem(items[0].index, sk));
		return Void();
	}
};
const char* WaitFutureFunc::name = "WAIT_FUTURE";
REGISTER_INSTRUCTION_FUNC(WaitFutureFunc);

// CLEAR
struct ClearFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> sk = wait(items[0].value);
		Standalone<StringRef> key = Tuple::unpack(sk).getString(0);

		Reference<InstructionData> instructionCopy = instruction;

		Future<Void> mutation = executeMutation(instruction, [instructionCopy, key]() -> Future<Void> {
			instructionCopy->tr->clear(key);
			return Void();
		});

		if (instruction->isDatabase) {
			data->stack.push(waitForVoid(mutation));
		} else {
			wait(mutation);
		}

		return Void();
	}
};
const char* ClearFunc::name = "CLEAR";
REGISTER_INSTRUCTION_FUNC(ClearFunc);

// RESET
struct ResetFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		instruction->tr->reset();
		return Void();
	}
};
const char* ResetFunc::name = "RESET";
REGISTER_INSTRUCTION_FUNC(ResetFunc);

// CANCEL
struct CancelFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		instruction->tr->cancel();
		return Void();
	}
};
const char* CancelFunc::name = "CANCEL";
REGISTER_INSTRUCTION_FUNC(CancelFunc);

struct GetRangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(5);
		if (items.size() != 5)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> begin = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		state Standalone<StringRef> end = Tuple::unpack(s2).getString(0);

		Standalone<StringRef> s3 = wait(items[2].value);
		state int limit = Tuple::unpack(s3).getInt(0);

		Standalone<StringRef> s4 = wait(items[3].value);
		state int reverse = Tuple::unpack(s4).getInt(0);

		Standalone<StringRef> s5 = wait(items[4].value);
		FDBStreamingMode mode = (FDBStreamingMode)Tuple::unpack(s5).getInt(0);

		// printf("================GetRange: %s, %s, %d, %d, %d, %d\n", printable(begin).c_str(),
		// printable(end).c_str(), limit, reverse, mode, instruction->isSnapshot);

		Standalone<RangeResultRef> results = wait(getRange(instruction->tr,
		                                                   KeyRange(KeyRangeRef(begin, end > begin ? end : begin)),
		                                                   limit,
		                                                   instruction->isSnapshot,
		                                                   reverse,
		                                                   mode));
		Tuple t;
		for (auto& s : results) {
			t.append(s.key);
			t.append(s.value);
			// printf("=====key:%s, value:%s\n", printable(StringRef(s.key)).c_str(),
			// printable(StringRef(s.value)).c_str());
		}
		// printf("=====Results Count:%d, size:%d\n", results.size(), str.size());

		data->stack.push(Tuple().append(t.pack()).pack());
		return Void();
	}
};
const char* GetRangeFunc::name = "GET_RANGE";
REGISTER_INSTRUCTION_FUNC(GetRangeFunc);

struct GetRangeStartsWithFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(4);
		if (items.size() != 4)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> prefix = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		state int limit = Tuple::unpack(s2).getInt(0);

		Standalone<StringRef> s3 = wait(items[2].value);
		state int reverse = Tuple::unpack(s3).getInt(0);

		Standalone<StringRef> s4 = wait(items[3].value);
		FDBStreamingMode mode = (FDBStreamingMode)Tuple::unpack(s4).getInt(0);

		// printf("================GetRangeStartsWithFunc: %s, %d, %d, %d, %d\n", printable(prefix).c_str(), limit,
		// reverse, mode, isSnapshot);
		Standalone<RangeResultRef> results = wait(getRange(instruction->tr,
		                                                   KeyRange(KeyRangeRef(prefix, strinc(prefix))),
		                                                   limit,
		                                                   instruction->isSnapshot,
		                                                   reverse,
		                                                   mode));
		Tuple t;
		// printf("=====Results Count:%d\n", results.size());
		for (auto& s : results) {
			t.append(s.key);
			t.append(s.value);
			// printf("=====key:%s, value:%s\n", printable(StringRef(s.key)).c_str(),
			// printable(StringRef(s.value)).c_str());
		}

		data->stack.push(Tuple().append(t.pack()).pack());
		return Void();
	}
};
const char* GetRangeStartsWithFunc::name = "GET_RANGE_STARTS_WITH";
REGISTER_INSTRUCTION_FUNC(GetRangeStartsWithFunc);

struct ClearRangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(2);
		if (items.size() != 2)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> begin = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		Standalone<StringRef> end = Tuple::unpack(s2).getString(0);

		Reference<InstructionData> instructionCopy = instruction;
		Standalone<StringRef> beginCopy = begin;

		Future<Void> mutation = executeMutation(instruction, [instructionCopy, beginCopy, end]() -> Future<Void> {
			instructionCopy->tr->clear(KeyRangeRef(beginCopy, end));
			return Void();
		});

		if (instruction->isDatabase) {
			data->stack.push(waitForVoid(mutation));
		} else {
			wait(mutation);
		}

		return Void();
	}
};
const char* ClearRangeFunc::name = "CLEAR_RANGE";
REGISTER_INSTRUCTION_FUNC(ClearRangeFunc);

struct ClearRangeStartWithFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		Standalone<StringRef> begin = Tuple::unpack(s1).getString(0);

		Reference<InstructionData> instructionCopy = instruction;

		Future<Void> mutation = executeMutation(instruction, [instructionCopy, begin]() -> Future<Void> {
			instructionCopy->tr->clear(KeyRangeRef(begin, strinc(begin)));
			return Void();
		});

		if (instruction->isDatabase) {
			data->stack.push(waitForVoid(mutation));
		} else {
			wait(mutation);
		}

		return Void();
	}
};
const char* ClearRangeStartWithFunc::name = "CLEAR_RANGE_STARTS_WITH";
REGISTER_INSTRUCTION_FUNC(ClearRangeStartWithFunc);

struct GetRangeSelectorFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(10);
		if (items.size() != 10)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> begin = Tuple::unpack(s1).getString(0);

		Standalone<StringRef> s2 = wait(items[1].value);
		state bool begin_or_equal = Tuple::unpack(s2).getInt(0);

		Standalone<StringRef> s3 = wait(items[2].value);
		state int64_t begin_offset = Tuple::unpack(s3).getInt(0);

		Standalone<StringRef> s4 = wait(items[3].value);
		state Standalone<StringRef> end = Tuple::unpack(s4).getString(0);

		Standalone<StringRef> s5 = wait(items[4].value);
		state bool end_or_equal = Tuple::unpack(s5).getInt(0);

		Standalone<StringRef> s6 = wait(items[5].value);
		state int64_t end_offset = Tuple::unpack(s6).getInt(0);

		Standalone<StringRef> s7 = wait(items[6].value);
		state int limit = Tuple::unpack(s7).getInt(0);

		Standalone<StringRef> s8 = wait(items[7].value);
		state int reverse = Tuple::unpack(s8).getInt(0);

		Standalone<StringRef> s9 = wait(items[8].value);
		state FDBStreamingMode mode = (FDBStreamingMode)Tuple::unpack(s9).getInt(0);

		Standalone<StringRef> s10 = wait(items[9].value);
		state Optional<Standalone<StringRef>> prefix;
		Tuple t10 = Tuple::unpack(s10);
		if (t10.getType(0) != Tuple::ElementType::NULL_TYPE) {
			prefix = t10.getString(0);
		}

		// printf("================GetRangeSelectorFunc: %s, %d, %ld, %s, %d, %ld, %d, %d, %d, %d, %s\n",
		// printable(begin).c_str(), begin_or_equal, begin_offset, 	printable(end).c_str(), end_or_equal, end_offset,
		//	limit, reverse, mode, instruction->isSnapshot, printable(prefix).c_str());
		Future<Standalone<RangeResultRef>> f = getRange(instruction->tr,
		                                                KeySelectorRef(begin, begin_or_equal, begin_offset),
		                                                KeySelectorRef(end, end_or_equal, end_offset),
		                                                limit,
		                                                instruction->isSnapshot,
		                                                reverse,
		                                                mode);
		Standalone<RangeResultRef> results = wait(holdWhile(instruction->tr, f));
		Tuple t;
		// printf("=====Results Count:%d\n", results.size());
		for (auto& s : results) {
			if (!prefix.present() || s.key.startsWith(prefix.get())) {
				t.append(s.key);
				t.append(s.value);
				// printf("=====key:%s, value:%s\n", printable(StringRef(s.key)).c_str(),
				// printable(StringRef(s.value)).c_str());
			}
		}

		data->stack.push(Tuple().append(t.pack()).pack());
		return Void();
	}
};
const char* GetRangeSelectorFunc::name = "GET_RANGE_SELECTOR";
REGISTER_INSTRUCTION_FUNC(GetRangeSelectorFunc);

// Tuple Operations
// TUPLE_PACK
struct TuplePackFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state int64_t count = Tuple::unpack(s1).getInt(0);

		state std::vector<StackItem> items1 = data->stack.pop(count);
		if (items1.size() != count)
			return Void();

		state Tuple tuple;
		state int i = 0;
		for (; i < items1.size(); ++i) {
			Standalone<StringRef> str = wait(items1[i].value);
			Tuple itemTuple = Tuple::unpack(str);
			if (deterministicRandom()->coinflip()) {
				Tuple::ElementType type = itemTuple.getType(0);
				if (type == Tuple::NULL_TYPE) {
					tuple.appendNull();
				} else if (type == Tuple::INT) {
					tuple << itemTuple.getInt(0);
				} else if (type == Tuple::BYTES) {
					tuple.append(itemTuple.getString(0), false);
				} else if (type == Tuple::UTF8) {
					tuple.append(itemTuple.getString(0), true);
				} else if (type == Tuple::FLOAT) {
					tuple << itemTuple.getFloat(0);
				} else if (type == Tuple::DOUBLE) {
					tuple << itemTuple.getDouble(0);
				} else if (type == Tuple::BOOL) {
					tuple << itemTuple.getBool(0);
				} else if (type == Tuple::UUID) {
					tuple << itemTuple.getUuid(0);
				} else if (type == Tuple::NESTED) {
					tuple.appendNested(itemTuple.getNested(0));
				} else {
					ASSERT(false);
				}
			} else {
				tuple << itemTuple;
			}
		}

		data->stack.pushTuple(tuple.pack());
		return Void();
	}
};
const char* TuplePackFunc::name = "TUPLE_PACK";
REGISTER_INSTRUCTION_FUNC(TuplePackFunc);

// TUPLE_UNPACK
struct TupleUnpackFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		Tuple t = Tuple::unpack(Tuple::unpack(s1).getString(0));

		for (int i = 0; i < t.size(); ++i) {
			Standalone<StringRef> str = t.subTuple(i, i + 1).pack();
			// printf("=====value:%s\n", printable(str).c_str());
			data->stack.pushTuple(str);
		}
		return Void();
	}
};
const char* TupleUnpackFunc::name = "TUPLE_UNPACK";
REGISTER_INSTRUCTION_FUNC(TupleUnpackFunc);

// TUPLE_RANGE
struct TupleRangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state int64_t count = Tuple::unpack(s1).getInt(0);

		state std::vector<StackItem> items1 = data->stack.pop(count);
		if (items1.size() != count)
			return Void();

		state Tuple tuple;
		state size_t i = 0;
		for (; i < items1.size(); ++i) {
			Standalone<StringRef> str = wait(items1[i].value);
			Tuple itemTuple = Tuple::unpack(str);
			if (deterministicRandom()->coinflip()) {
				Tuple::ElementType type = itemTuple.getType(0);
				if (type == Tuple::NULL_TYPE) {
					tuple.appendNull();
				} else if (type == Tuple::INT) {
					tuple << itemTuple.getInt(0);
				} else if (type == Tuple::BYTES) {
					tuple.append(itemTuple.getString(0), false);
				} else if (type == Tuple::UTF8) {
					tuple.append(itemTuple.getString(0), true);
				} else if (type == Tuple::FLOAT) {
					tuple << itemTuple.getFloat(0);
				} else if (type == Tuple::DOUBLE) {
					tuple << itemTuple.getDouble(0);
				} else if (type == Tuple::BOOL) {
					tuple << itemTuple.getBool(0);
				} else if (type == Tuple::UUID) {
					tuple << itemTuple.getUuid(0);
				} else if (type == Tuple::NESTED) {
					tuple.appendNested(itemTuple.getNested(0));
				} else {
					ASSERT(false);
				}
			} else {
				tuple << itemTuple;
			}
		}

		KeyRange range = tuple.range();

		data->stack.pushTuple(range.begin);
		data->stack.pushTuple(range.end);
		return Void();
	}
};
const char* TupleRangeFunc::name = "TUPLE_RANGE";
REGISTER_INSTRUCTION_FUNC(TupleRangeFunc);

// TUPLE_SORT
struct TupleSortFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state int64_t count = Tuple::unpack(s1).getInt(0);

		state std::vector<StackItem> items1 = data->stack.pop(count);
		if (items1.size() != count)
			return Void();

		state std::vector<Tuple> tuples;
		state size_t i = 0;
		for (; i < items1.size(); i++) {
			Standalone<StringRef> value = wait(items1[i].value);
			tuples.push_back(Tuple::unpack(value));
		}

		std::sort(tuples.begin(), tuples.end());
		for (Tuple const& t : tuples) {
			data->stack.push(t.pack());
		}

		return Void();
	}
};
const char* TupleSortFunc::name = "TUPLE_SORT";
REGISTER_INSTRUCTION_FUNC(TupleSortFunc);

// ENCODE_FLOAT
struct EncodeFloatFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		Standalone<StringRef> fBytes = Tuple::unpack(s1).getString(0);
		ASSERT(fBytes.size() == 4);

		int32_t intVal = *(int32_t*)fBytes.begin();
		intVal = bigEndian32(intVal);
		float fVal = *(float*)&intVal;

		Tuple t;
		t.append(fVal);
		data->stack.push(t.pack());

		return Void();
	}
};
const char* EncodeFloatFunc::name = "ENCODE_FLOAT";
REGISTER_INSTRUCTION_FUNC(EncodeFloatFunc);

// ENCODE_DOUBLE
struct EncodeDoubleFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		Standalone<StringRef> dBytes = Tuple::unpack(s1).getString(0);
		ASSERT(dBytes.size() == 8);

		int64_t intVal = *(int64_t*)dBytes.begin();
		intVal = bigEndian64(intVal);
		double dVal = *(double*)&intVal;

		Tuple t;
		t.append(dVal);
		data->stack.push(t.pack());

		return Void();
	}
};
const char* EncodeDoubleFunc::name = "ENCODE_DOUBLE";
REGISTER_INSTRUCTION_FUNC(EncodeDoubleFunc);

// DECODE_FLOAT
struct DecodeFloatFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		float fVal = Tuple::unpack(s1).getFloat(0);
		int32_t intVal = *(int32_t*)&fVal;
		intVal = bigEndian32(intVal);

		Tuple t;
		t.append(StringRef((uint8_t*)&intVal, 4), false);
		data->stack.push(t.pack());

		return Void();
	}
};
const char* DecodeFloatFunc::name = "DECODE_FLOAT";
REGISTER_INSTRUCTION_FUNC(DecodeFloatFunc);

// DECODE_DOUBLE
struct DecodeDoubleFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		double dVal = Tuple::unpack(s1).getDouble(0);
		int64_t intVal = *(int64_t*)&dVal;
		intVal = bigEndian64(intVal);

		Tuple t;
		t.append(StringRef((uint8_t*)&intVal, 8), false);
		data->stack.push(t.pack());
		return Void();
	}
};
const char* DecodeDoubleFunc::name = "DECODE_DOUBLE";
REGISTER_INSTRUCTION_FUNC(DecodeDoubleFunc);

// Thread Operations
// START_THREAD
struct StartThreadFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> prefix = Tuple::unpack(s1).getString(0);
		// printf("=========START_THREAD:%s\n", printable(prefix).c_str());

		Reference<FlowTesterData> newData = Reference<FlowTesterData>(new FlowTesterData(data->api));
		data->subThreads.push_back(runTest(newData, data->db, prefix));

		return Void();
	}
};
const char* StartThreadFunc::name = "START_THREAD";
REGISTER_INSTRUCTION_FUNC(StartThreadFunc);

ACTOR template <class Function>
Future<decltype(std::declval<Function>()(Reference<ReadTransaction>()).getValue())> read(Reference<Database> db,
                                                                                         Function func) {
	state Reference<ReadTransaction> tr = db->createTransaction();
	loop {
		try {
			state decltype(std::declval<Function>()(Reference<ReadTransaction>()).getValue()) result = wait(func(tr));
			return result;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// WAIT_EMPTY
struct WaitEmptyFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		Standalone<StringRef> prefix = Tuple::unpack(s1).getString(0);
		// printf("=========WAIT_EMPTY:%s\n", printable(prefix).c_str());

		wait(read(data->db,
		          [=](Reference<ReadTransaction> tr) -> Future<Void> { return checkEmptyPrefix(tr, prefix); }));

		return Void();
	}

private:
	ACTOR static Future<Void> checkEmptyPrefix(Reference<ReadTransaction> tr, Standalone<StringRef> prefix) {
		FDBStandalone<RangeResultRef> results = wait(tr->getRange(KeyRangeRef(prefix, strinc(prefix)), 1));
		if (results.size() > 0) {
			throw not_committed();
		}
		return Void();
	}
};
const char* WaitEmptyFunc::name = "WAIT_EMPTY";
REGISTER_INSTRUCTION_FUNC(WaitEmptyFunc);

// DISABLE_WRITE_CONFLICT
struct DisableWriteConflictFunc : InstructionFunc {
	static const char* name;

	static Future<Void> call(Reference<FlowTesterData> const& data, Reference<InstructionData> const& instruction) {
		if (instruction->tr) {
			instruction->tr->setOption(FDBTransactionOption::FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
		}
		return Void();
	}
};
const char* DisableWriteConflictFunc::name = "DISABLE_WRITE_CONFLICT";
REGISTER_INSTRUCTION_FUNC(DisableWriteConflictFunc);

// READ_CONFLICT_KEY
struct ReadConflictKeyFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> key = Tuple::unpack(s1).getString(0);
		// printf("=========READ_CONFLICT_KEY:%s\n", printable(key).c_str());
		instruction->tr->addReadConflictKey(key);

		data->stack.pushTuple(LiteralStringRef("SET_CONFLICT_KEY"));
		return Void();
	}
};
const char* ReadConflictKeyFunc::name = "READ_CONFLICT_KEY";
REGISTER_INSTRUCTION_FUNC(ReadConflictKeyFunc);

// WRITE_CONFLICT_KEY
struct WriteConflictKeyFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop();
		if (items.size() != 1)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> key = Tuple::unpack(s1).getString(0);
		// printf("=========WRITE_CONFLICT_KEY:%s\n", printable(key).c_str());
		instruction->tr->addWriteConflictKey(key);

		data->stack.pushTuple(LiteralStringRef("SET_CONFLICT_KEY"));
		return Void();
	}
};
const char* WriteConflictKeyFunc::name = "WRITE_CONFLICT_KEY";
REGISTER_INSTRUCTION_FUNC(WriteConflictKeyFunc);

// READ_CONFLICT_RANGE
struct ReadConflictRangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(2);
		if (items.size() != 2)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> begin = Tuple::unpack(s1).getString(0);
		Standalone<StringRef> s2 = wait(items[1].value);
		state Standalone<StringRef> end = Tuple::unpack(s2).getString(0);

		// printf("=========READ_CONFLICT_RANGE:%s:%s\n", printable(begin).c_str(), printable(end).c_str());
		instruction->tr->addReadConflictRange(KeyRange(KeyRangeRef(begin, end)));
		data->stack.pushTuple(LiteralStringRef("SET_CONFLICT_RANGE"));
		return Void();
	}
};
const char* ReadConflictRangeFunc::name = "READ_CONFLICT_RANGE";
REGISTER_INSTRUCTION_FUNC(ReadConflictRangeFunc);

// WRITE_CONFLICT_RANGE
struct WriteConflictRangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(2);
		if (items.size() != 2)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> begin = Tuple::unpack(s1).getString(0);
		Standalone<StringRef> s2 = wait(items[1].value);
		state Standalone<StringRef> end = Tuple::unpack(s2).getString(0);

		// printf("=========WRITE_CONFLICT_RANGE:%s:%s\n", printable(begin).c_str(), printable(end).c_str());
		instruction->tr->addWriteConflictRange(KeyRange(KeyRangeRef(begin, end)));

		data->stack.pushTuple(LiteralStringRef("SET_CONFLICT_RANGE"));
		return Void();
	}
};
const char* WriteConflictRangeFunc::name = "WRITE_CONFLICT_RANGE";
REGISTER_INSTRUCTION_FUNC(WriteConflictRangeFunc);

// ATOMIC_OP
struct AtomicOPFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state std::vector<StackItem> items = data->stack.pop(3);
		if (items.size() != 3)
			return Void();

		Standalone<StringRef> s1 = wait(items[0].value);
		state Standalone<StringRef> op = Tuple::unpack(s1).getString(0);
		Standalone<StringRef> s2 = wait(items[1].value);
		state Standalone<StringRef> key = Tuple::unpack(s2).getString(0);
		Standalone<StringRef> s3 = wait(items[2].value);
		state Standalone<StringRef> value = Tuple::unpack(s3).getString(0);

		ASSERT(optionInfo.find(op.toString()) != optionInfo.end());

		FDBMutationType atomicOp = optionInfo[op.toString()];

		Reference<InstructionData> instructionCopy = instruction;
		Standalone<StringRef> keyCopy = key;
		Standalone<StringRef> valueCopy = value;

		// printf("=========ATOMIC_OP:%s:%s:%s\n", printable(op).c_str(), printable(key).c_str(),
		// printable(value).c_str());
		Future<Void> mutation =
		    executeMutation(instruction, [instructionCopy, keyCopy, valueCopy, atomicOp]() -> Future<Void> {
			    instructionCopy->tr->atomicOp(keyCopy, valueCopy, atomicOp);
			    return Void();
		    });

		if (instruction->isDatabase) {
			data->stack.push(waitForVoid(mutation));
		} else {
			wait(mutation);
		}

		return Void();
	}
};
const char* AtomicOPFunc::name = "ATOMIC_OP";
REGISTER_INSTRUCTION_FUNC(AtomicOPFunc);

// UNIT_TESTS
struct UnitTestsFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		ASSERT(data->api->evaluatePredicate(FDBErrorPredicate::FDB_ERROR_PREDICATE_RETRYABLE, Error(1020)));
		ASSERT(!data->api->evaluatePredicate(FDBErrorPredicate::FDB_ERROR_PREDICATE_RETRYABLE, Error(10)));

		ASSERT(API::isAPIVersionSelected());
		state API* fdb = API::getInstance();
		ASSERT(fdb->getAPIVersion() <= FDB_API_VERSION);
		try {
			API::selectAPIVersion(fdb->getAPIVersion() + 1);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_api_version_already_set);
		}
		try {
			API::selectAPIVersion(fdb->getAPIVersion() - 1);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_api_version_already_set);
		}
		API::selectAPIVersion(fdb->getAPIVersion());

		const uint64_t locationCacheSize = 100001;
		const uint64_t maxWatches = 10001;
		const uint64_t timeout = 60 * 1000;
		const uint64_t noTimeout = 0;
		const uint64_t retryLimit = 50;
		const uint64_t noRetryLimit = -1;
		const uint64_t maxRetryDelay = 100;
		const uint64_t sizeLimit = 100000;
		const uint64_t maxFieldLength = 1000;

		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_LOCATION_CACHE_SIZE,
		                            Optional<StringRef>(StringRef((const uint8_t*)&locationCacheSize, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_MAX_WATCHES,
		                            Optional<StringRef>(StringRef((const uint8_t*)&maxWatches, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_DATACENTER_ID,
		                            Optional<StringRef>(LiteralStringRef("dc_id")));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_MACHINE_ID,
		                            Optional<StringRef>(LiteralStringRef("machine_id")));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_SNAPSHOT_RYW_ENABLE);
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE);
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_LOGGING_MAX_FIELD_LENGTH,
		                            Optional<StringRef>(StringRef((const uint8_t*)&maxFieldLength, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_TIMEOUT,
		                            Optional<StringRef>(StringRef((const uint8_t*)&timeout, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_TIMEOUT,
		                            Optional<StringRef>(StringRef((const uint8_t*)&noTimeout, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_MAX_RETRY_DELAY,
		                            Optional<StringRef>(StringRef((const uint8_t*)&maxRetryDelay, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
		                            Optional<StringRef>(StringRef((const uint8_t*)&sizeLimit, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT,
		                            Optional<StringRef>(StringRef((const uint8_t*)&retryLimit, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT,
		                            Optional<StringRef>(StringRef((const uint8_t*)&noRetryLimit, 8)));
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_CAUSAL_READ_RISKY);
		data->db->setDatabaseOption(FDBDatabaseOption::FDB_DB_OPTION_TRANSACTION_INCLUDE_PORT_IN_ADDRESS);

		state Reference<Transaction> tr = data->db->createTransaction();
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_PRIORITY_BATCH);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_CAUSAL_READ_RISKY);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_CAUSAL_WRITE_RISKY);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_TRANSACTION_LOGGING_MAX_FIELD_LENGTH,
		              Optional<StringRef>(StringRef((const uint8_t*)&maxFieldLength, 8)));
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_TIMEOUT,
		              Optional<StringRef>(StringRef((const uint8_t*)&timeout, 8)));
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_RETRY_LIMIT,
		              Optional<StringRef>(StringRef((const uint8_t*)&retryLimit, 8)));
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_MAX_RETRY_DELAY,
		              Optional<StringRef>(StringRef((const uint8_t*)&maxRetryDelay, 8)));
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_USED_DURING_COMMIT_PROTECTION_DISABLE);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_TRANSACTION_LOGGING_ENABLE,
		              Optional<StringRef>(LiteralStringRef("my_transaction")));
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_READ_LOCK_AWARE);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_LOCK_AWARE);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_INCLUDE_PORT_IN_ADDRESS);
		tr->setOption(FDBTransactionOption::FDB_TR_OPTION_REPORT_CONFLICTING_KEYS);

		Optional<FDBStandalone<ValueRef>> _ = wait(tr->get(LiteralStringRef("\xff")));
		tr->cancel();

		return Void();
	}
};
const char* UnitTestsFunc::name = "UNIT_TESTS";
REGISTER_INSTRUCTION_FUNC(UnitTestsFunc);

ACTOR static Future<Void> getInstructions(Reference<FlowTesterData> data, StringRef prefix) {
	state Reference<Transaction> tr = data->db->createTransaction();

	// get test instructions
	state Tuple testSpec;
	testSpec.append(prefix);
	loop {
		try {
			Standalone<RangeResultRef> results = wait(getRange(tr, testSpec.range()));
			data->instructions = results;
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR static Future<Void> doInstructions(Reference<FlowTesterData> data) {
	// printf("Total num instructions:%d\n", data->instructions.size());
	state size_t idx = 0;
	for (; idx < data->instructions.size(); ++idx) {
		Tuple opTuple = Tuple::unpack(data->instructions[idx].value);
		state Standalone<StringRef> op = opTuple.getString(0);

		state bool isDatabase = op.endsWith(LiteralStringRef("_DATABASE"));
		state bool isSnapshot = op.endsWith(LiteralStringRef("_SNAPSHOT"));
		state bool isDirectory = op.startsWith(LiteralStringRef("DIRECTORY_"));

		try {
			if (LOG_INSTRUCTIONS) {
				if (op != LiteralStringRef("SWAP") && op != LiteralStringRef("PUSH")) {
					printf("%zu. %s\n", idx, tupleToString(opTuple).c_str());
					fflush(stdout);
				}
			}

			if (isDatabase)
				op = op.substr(0, op.size() - 9);
			else if (isSnapshot)
				op = op.substr(0, op.size() - 9);

			// printf("[==========]%ld/%ld:%s:%s: isDatabase:%d, isSnapshot:%d, stack count:%ld\n",
			// idx, data->instructions.size(), StringRef(data->instructions[idx].key).printable().c_str(),
			// StringRef(data->instructions[idx].value).printable().c_str(), isDatabase, isSnapshot,
			// data->stack.data.size());

			// wait(printFlowTesterStack(&(data->stack)));
			// wait(debugPrintRange(instruction->tr, "\x01test_results", ""));

			state Reference<InstructionData> instruction = Reference<InstructionData>(
			    new InstructionData(isDatabase, isSnapshot, data->instructions[idx].value, Reference<Transaction>()));
			if (isDatabase) {
				state Reference<Transaction> tr = data->db->createTransaction();
				instruction->tr = tr;
			} else {
				instruction->tr = trMap[data->trName];
			}

			// Flow directory operations don't support snapshot reads
			ASSERT(!isDirectory || !isSnapshot);

			data->stack.index = idx;
			wait(InstructionFunc::call(op.toString(), data, instruction));
		} catch (Error& e) {
			if (LOG_ERRORS) {
				printf("Error: %s (%d)\n", e.name(), e.code());
				fflush(stdout);
			}

			if (isDirectory) {
				if (opsThatCreateDirectories.count(op.toString())) {
					data->directoryData.directoryList.push_back(DirectoryOrSubspace());
				}
				data->stack.pushTuple(LiteralStringRef("DIRECTORY_ERROR"));
			} else {
				data->stack.pushError(e.code());
			}
		}
	}
	// printf("Total num instructions:%d\n", data->instructions.size());
	return Void();
}

ACTOR static Future<Void> runTest(Reference<FlowTesterData> data, Reference<Database> db, StringRef prefix) {
	ASSERT(data);
	try {
		data->db = db;
		wait(getInstructions(data, prefix));
		wait(doInstructions(data));
		wait(waitForAll(data->subThreads));
	} catch (Error& e) {
		TraceEvent(SevError, "FlowTesterDataRunError").error(e);
	}

	return Void();
}

void populateAtomicOpMap() {
	optionInfo["ADD"] = FDBMutationType::FDB_MUTATION_TYPE_ADD;
	optionInfo["AND"] = FDBMutationType::FDB_MUTATION_TYPE_AND;
	optionInfo["BIT_AND"] = FDBMutationType::FDB_MUTATION_TYPE_BIT_AND;
	optionInfo["OR"] = FDBMutationType::FDB_MUTATION_TYPE_OR;
	optionInfo["BIT_OR"] = FDBMutationType::FDB_MUTATION_TYPE_BIT_OR;
	optionInfo["XOR"] = FDBMutationType::FDB_MUTATION_TYPE_XOR;
	optionInfo["BIT_XOR"] = FDBMutationType::FDB_MUTATION_TYPE_BIT_XOR;
	optionInfo["APPEND_IF_FITS"] = FDBMutationType::FDB_MUTATION_TYPE_APPEND_IF_FITS;
	optionInfo["MAX"] = FDBMutationType::FDB_MUTATION_TYPE_MAX;
	optionInfo["MIN"] = FDBMutationType::FDB_MUTATION_TYPE_MIN;
	optionInfo["SET_VERSIONSTAMPED_KEY"] = FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY;
	optionInfo["SET_VERSIONSTAMPED_VALUE"] = FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE;
	optionInfo["BYTE_MIN"] = FDBMutationType::FDB_MUTATION_TYPE_BYTE_MIN;
	optionInfo["BYTE_MAX"] = FDBMutationType::FDB_MUTATION_TYPE_BYTE_MAX;
}

void populateOpsThatCreateDirectories() {
	opsThatCreateDirectories.insert("DIRECTORY_CREATE_SUBSPACE");
	opsThatCreateDirectories.insert("DIRECTORY_CREATE_LAYER");
	opsThatCreateDirectories.insert("DIRECTORY_CREATE_OR_OPEN");
	opsThatCreateDirectories.insert("DIRECTORY_CREATE");
	opsThatCreateDirectories.insert("DIRECTORY_OPEN");
	opsThatCreateDirectories.insert("DIRECTORY_MOVE");
	opsThatCreateDirectories.insert("DIRECTORY_MOVE_TO");
	opsThatCreateDirectories.insert("DIRECTORY_OPEN_SUBSPACE");
}

ACTOR void startTest(std::string clusterFilename, StringRef prefix, int apiVersion) {
	try {
		populateAtomicOpMap(); // FIXME: NOOOOOO!
		populateOpsThatCreateDirectories(); // FIXME

		// This is "our" network
		g_network = newNet2(TLSConfig());

		ASSERT(!API::isAPIVersionSelected());
		try {
			API::getInstance();
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_api_version_unset);
		}

		API* fdb = API::selectAPIVersion(apiVersion);
		ASSERT(API::isAPIVersionSelected());
		ASSERT(fdb->getAPIVersion() == apiVersion);
		// fdb->setNetworkOption(FDBNetworkOption::FDB_NET_OPTION_TRACE_ENABLE);

		// We have to start the fdb_flow network and thread separately!
		fdb->setupNetwork();
		startThread(networkThread, fdb);

		// Connect to the default cluster/database, and create a transaction
		auto db = fdb->createDatabase(clusterFilename);

		Reference<FlowTesterData> data = Reference<FlowTesterData>(new FlowTesterData(fdb));
		wait(runTest(data, db, prefix));

		// Stopping the network returns from g_network->run() and allows
		// the program to terminate
		g_network->stop();
	} catch (Error& e) {
		TraceEvent("ErrorRunningTest").error(e);
		if (LOG_ERRORS) {
			printf("Flow tester encountered error: %s\n", e.name());
			fflush(stdout);
		}
		flushAndExit(1);
	}
}

ACTOR void _test_versionstamp() {
	try {
		g_network = newNet2(TLSConfig());

		API* fdb = FDB::API::selectAPIVersion(710);

		fdb->setupNetwork();
		startThread(networkThread, fdb);

		auto db = fdb->createDatabase();
		state Reference<Transaction> tr = db->createTransaction();

		state Future<FDBStandalone<StringRef>> ftrVersion = tr->getVersionstamp();

		tr->atomicOp(LiteralStringRef("foo"),
		             LiteralStringRef("blahblahbl\x00\x00\x00\x00"),
		             FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);

		wait(tr->commit()); // should use retry loop

		tr->reset();

		Optional<FDBStandalone<StringRef>> optionalDbVersion = wait(tr->get(LiteralStringRef("foo")));
		state FDBStandalone<StringRef> dbVersion = optionalDbVersion.get();
		FDBStandalone<StringRef> trVersion = wait(ftrVersion);

		ASSERT(trVersion.compare(dbVersion) == 0);

		fprintf(stderr, "%s\n", trVersion.printable().c_str());

		g_network->stop();
	} catch (Error& e) {
		TraceEvent("ErrorRunningTest").error(e);
		if (LOG_ERRORS) {
			printf("Flow tester encountered error: %s\n", e.name());
			fflush(stdout);
		}
		flushAndExit(1);
	}
}

int main(int argc, char** argv) {
	try {
		platformInit();
		registerCrashHandler();
		setThreadLocalDeterministicRandomSeed(1);

		// Get arguments
		if (argc < 3) {
			fprintf(stderr, "Missing arguments! Usage: fdb_flow_tester prefix api_version [cluster_filename]\n");
			return 1;

			/*_test_versionstamp();
			g_network->run();

			flushAndExit(FDB_EXIT_SUCCESS);*/
		}
		StringRef prefix((const uint8_t*)argv[1], strlen(argv[1]));
		int apiVersion;
		sscanf(argv[2], "%d", &apiVersion);
		std::string clusterFilename;
		if (argc > 3) {
			clusterFilename = std::string(argv[3]);
		}

		// start test
		startTest(clusterFilename, prefix, apiVersion);

		// Run the network until someone tells us to stop
		g_network->run();

		flushAndExit(FDB_EXIT_SUCCESS);
	} catch (Error& e) {
		fprintf(stderr, "Error: %s\n", e.name());
		TraceEvent(SevError, "MainError").error(e);
		flushAndExit(FDB_EXIT_MAIN_ERROR);
	} catch (std::exception& e) {
		fprintf(stderr, "std::exception: %s\n", e.what());
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		flushAndExit(FDB_EXIT_MAIN_EXCEPTION);
	}
}
