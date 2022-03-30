/*
 * Tester.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDB_FLOW_TESTER_TESTER_ACTOR_G_H)
#define FDB_FLOW_TESTER_TESTER_ACTOR_G_H
#include "Tester.actor.g.h"
#elif !defined(FDB_FLOW_TESTER_TESTER_ACTOR_H)
#define FDB_FLOW_TESTER_TESTER_ACTOR_H

#pragma once

#include <utility>

#include "flow/IDispatched.h"
#include "bindings/flow/fdb_flow.h"
#include "bindings/flow/IDirectory.h"
#include "bindings/flow/Subspace.h"
#include "bindings/flow/DirectoryLayer.h"
#include "flow/actorcompiler.h" // This must be the last #include.

constexpr bool LOG_ALL = false;
constexpr bool LOG_INSTRUCTIONS = LOG_ALL || false;
constexpr bool LOG_OPS = LOG_ALL || false;
constexpr bool LOG_DIRS = LOG_ALL || false;
constexpr bool LOG_ERRORS = LOG_ALL || false;

struct FlowTesterData;

struct StackItem {
	StackItem() : index(-1) {}
	StackItem(uint32_t i, Future<Standalone<StringRef>> v) : index(i), value(v) {}
	StackItem(uint32_t i, Standalone<StringRef> v) : index(i), value(v) {}
	uint32_t index;
	Future<Standalone<StringRef>> value;
};

struct FlowTesterStack {
	uint32_t index;
	std::vector<StackItem> data;

	void push(Future<Standalone<StringRef>> value) { data.push_back(StackItem(index, value)); }

	void push(Standalone<StringRef> value) { push(Future<Standalone<StringRef>>(value)); }

	void push(const StackItem& item) { data.push_back(item); }

	void pushTuple(StringRef value, bool utf8 = false) {
		FDB::Tuple t;
		t.append(value, utf8);
		data.push_back(StackItem(index, t.pack()));
	}

	void pushError(int errorCode) {
		FDB::Tuple t;
		t.append(LiteralStringRef("ERROR"));
		t.append(format("%d", errorCode));
		// pack above as error string into another tuple
		pushTuple(t.pack().toString());
	}

	std::vector<StackItem> pop(uint32_t count = 1) {
		std::vector<StackItem> items;
		while (!data.empty() && count > 0) {
			items.push_back(data.back());
			data.pop_back();
			count--;
		}
		return items;
	}

	Future<std::vector<FDB::Tuple>> waitAndPop(int count);
	Future<FDB::Tuple> waitAndPop();

	void dup() {
		if (data.empty())
			return;
		data.push_back(data.back());
	}

	void clear() { data.clear(); }
};

struct InstructionData : public ReferenceCounted<InstructionData> {
	bool isDatabase;
	bool isSnapshot;
	StringRef instruction;
	Reference<FDB::Transaction> tr;

	InstructionData(bool _isDatabase, bool _isSnapshot, StringRef _instruction, Reference<FDB::Transaction> _tr)
	  : isDatabase(_isDatabase), isSnapshot(_isSnapshot), instruction(_instruction), tr(_tr) {}
};

struct FlowTesterData;

struct InstructionFunc
  : IDispatched<InstructionFunc,
                std::string,
                std::function<Future<Void>(Reference<FlowTesterData> data, Reference<InstructionData> instruction)>> {
	static Future<Void> call(std::string op, Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		ASSERT(data);
		ASSERT(instruction);

		auto it = dispatches().find(op);
		if (it == dispatches().end()) {
			fprintf(stderr, "Unrecognized instruction: %s\n", op.c_str());
			ASSERT(false);
		}

		return dispatch(op)(data, instruction);
	}
};
#define REGISTER_INSTRUCTION_FUNC(Op) REGISTER_COMMAND(InstructionFunc, Op, name, call)

struct DirectoryOrSubspace {
	Optional<Reference<FDB::IDirectory>> directory;
	Optional<FDB::Subspace*> subspace;

	DirectoryOrSubspace() {}
	DirectoryOrSubspace(Reference<FDB::IDirectory> directory) : directory(directory) {}
	DirectoryOrSubspace(FDB::Subspace* subspace) : subspace(subspace) {}
	DirectoryOrSubspace(Reference<FDB::DirectorySubspace> dirSubspace)
	  : directory(dirSubspace), subspace(dirSubspace.getPtr()) {}

	bool valid() { return directory.present() || subspace.present(); }

	std::string typeString() {
		if (directory.present() && subspace.present()) {
			return "DirectorySubspace";
		} else if (directory.present()) {
			return "IDirectory";
		} else if (subspace.present()) {
			return "Subspace";
		} else {
			return "InvalidDirectory";
		}
	}
};

struct DirectoryTesterData {
	std::vector<DirectoryOrSubspace> directoryList;
	int directoryListIndex;
	int directoryErrorIndex;

	Reference<FDB::IDirectory> directory() {
		ASSERT(directoryListIndex < directoryList.size());
		ASSERT(directoryList[directoryListIndex].directory.present());
		return directoryList[directoryListIndex].directory.get();
	}

	FDB::Subspace* subspace() {
		ASSERT(directoryListIndex < directoryList.size());
		ASSERT(directoryList[directoryListIndex].subspace.present());
		return directoryList[directoryListIndex].subspace.get();
	}

	DirectoryTesterData() : directoryListIndex(0), directoryErrorIndex(0) {
		directoryList.push_back(Reference<FDB::IDirectory>(new FDB::DirectoryLayer()));
	}

	template <class T>
	void push(T item) {
		directoryList.push_back(DirectoryOrSubspace(item));
		if (LOG_DIRS) {
			printf("Pushed %s at %lu\n", directoryList.back().typeString().c_str(), directoryList.size() - 1);
			fflush(stdout);
		}
	}

	void push() { push(DirectoryOrSubspace()); }
};

struct FlowTesterData : public ReferenceCounted<FlowTesterData> {
	FDB::API* api;
	Reference<FDB::Database> db;
	Standalone<FDB::RangeResultRef> instructions;
	Standalone<StringRef> trName;
	FlowTesterStack stack;
	FDB::Version lastVersion;
	DirectoryTesterData directoryData;

	std::vector<Future<Void>> subThreads;

	Future<Void> processInstruction(Reference<InstructionData> instruction) {
		return InstructionFunc::call(
		    instruction->instruction.toString(), Reference<FlowTesterData>::addRef(this), instruction);
	}

	FlowTesterData(FDB::API* api) { this->api = api; }
};

std::string tupleToString(FDB::Tuple const& tuple);

ACTOR template <class F>
Future<decltype(std::declval<F>()().getValue())> executeMutation(Reference<InstructionData> instruction, F func) {
	loop {
		try {
			state decltype(std::declval<F>()().getValue()) result = wait(func());
			if (instruction->isDatabase) {
				wait(instruction->tr->commit());
			}
			return result;
		} catch (Error& e) {
			if (instruction->isDatabase) {
				wait(instruction->tr->onError(e));
			} else {
				throw;
			}
		}
	}
}

#include "flow/unactorcompiler.h"
#endif
