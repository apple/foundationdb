/*
 * DirectoryTester.actor.cpp
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
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

ACTOR Future<std::vector<Tuple>> popTuples(Reference<FlowTesterData> data, int count = 1) {
	state std::vector<Tuple> tuples;

	while (tuples.size() < count) {
		Standalone<StringRef> sizeStr = wait(data->stack.pop()[0].value);
		int size = Tuple::unpack(sizeStr).getInt(0);

		state std::vector<StackItem> tupleItems = data->stack.pop(size);
		state Tuple tuple;

		state int index;
		for (index = 0; index < tupleItems.size(); ++index) {
			Standalone<StringRef> itemStr = wait(tupleItems[index].value);
			tuple.append(Tuple::unpack(itemStr));
		}

		tuples.push_back(tuple);
	}

	return tuples;
}

ACTOR Future<Tuple> popTuple(Reference<FlowTesterData> data) {
	std::vector<Tuple> tuples = wait(popTuples(data));
	return tuples[0];
}

ACTOR Future<std::vector<IDirectory::Path>> popPaths(Reference<FlowTesterData> data, int count = 1) {
	std::vector<Tuple> tuples = wait(popTuples(data, count));

	std::vector<IDirectory::Path> paths;
	for (auto& tuple : tuples) {
		IDirectory::Path path;
		for (int i = 0; i < tuple.size(); ++i) {
			path.push_back(tuple.getString(i));
		}

		paths.push_back(path);
	}

	return paths;
}

ACTOR Future<IDirectory::Path> popPath(Reference<FlowTesterData> data) {
	std::vector<IDirectory::Path> paths = wait(popPaths(data));
	return paths[0];
}

std::string pathToString(IDirectory::Path const& path) {
	std::string str;
	str += "[";
	for (int i = 0; i < path.size(); ++i) {
		str += path[i].toString();
		if (i < path.size() - 1) {
			str += ", ";
		}
	}

	return str + "]";
}

IDirectory::Path combinePaths(IDirectory::Path const& path1, IDirectory::Path const& path2) {
	IDirectory::Path outPath(path1.begin(), path1.end());
	for (auto p : path2) {
		outPath.push_back(p);
	}

	return outPath;
}

void logOp(std::string message, bool force = false) {
	if (LOG_OPS || force) {
		printf("%s\n", message.c_str());
		fflush(stdout);
	}
}

// DIRECTORY_CREATE_SUBSPACE
struct DirectoryCreateSubspaceFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state Tuple path = wait(popTuple(data));
		Tuple rawPrefix = wait(data->stack.waitAndPop());

		logOp(format(
		    "Created subspace at %s: %s", tupleToString(path).c_str(), rawPrefix.getString(0).printable().c_str()));
		data->directoryData.push(new Subspace(path, rawPrefix.getString(0)));
		return Void();
	}
};
const char* DirectoryCreateSubspaceFunc::name = "DIRECTORY_CREATE_SUBSPACE";
REGISTER_INSTRUCTION_FUNC(DirectoryCreateSubspaceFunc);

// DIRECTORY_CREATE_LAYER
struct DirectoryCreateLayerFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		std::vector<Tuple> args = wait(data->stack.waitAndPop(3));

		int index1 = args[0].getInt(0);
		int index2 = args[1].getInt(0);
		bool allowManualPrefixes = args[2].getInt(0) != 0;

		if (!data->directoryData.directoryList[index1].valid() || !data->directoryData.directoryList[index2].valid()) {
			logOp("Create directory layer: None");
			data->directoryData.push();
		} else {
			Subspace* nodeSubspace = data->directoryData.directoryList[index1].subspace.get();
			Subspace* contentSubspace = data->directoryData.directoryList[index2].subspace.get();
			logOp(format("Create directory layer: node_subspace (%d) = %s, content_subspace (%d) = %s, "
			             "allow_manual_prefixes = %d",
			             index1,
			             nodeSubspace->key().printable().c_str(),
			             index2,
			             nodeSubspace->key().printable().c_str(),
			             allowManualPrefixes));
			data->directoryData.push(
			    Reference<IDirectory>(new DirectoryLayer(*nodeSubspace, *contentSubspace, allowManualPrefixes)));
		}

		return Void();
	}
};
const char* DirectoryCreateLayerFunc::name = "DIRECTORY_CREATE_LAYER";
REGISTER_INSTRUCTION_FUNC(DirectoryCreateLayerFunc);

// DIRECTORY_CHANGE
struct DirectoryChangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple index = wait(data->stack.waitAndPop());
		data->directoryData.directoryListIndex = index.getInt(0);
		ASSERT(data->directoryData.directoryListIndex < data->directoryData.directoryList.size());

		if (!data->directoryData.directoryList[data->directoryData.directoryListIndex].valid()) {
			data->directoryData.directoryListIndex = data->directoryData.directoryErrorIndex;
		}

		if (LOG_DIRS) {
			DirectoryOrSubspace d = data->directoryData.directoryList[data->directoryData.directoryListIndex];
			printf("Changed directory to %d (%s @\'%s\')\n",
			       data->directoryData.directoryListIndex,
			       d.typeString().c_str(),
			       d.directory.present() ? pathToString(d.directory.get()->getPath()).c_str()
			                             : d.subspace.get()->key().printable().c_str());
			fflush(stdout);
		}

		return Void();
	}
};
const char* DirectoryChangeFunc::name = "DIRECTORY_CHANGE";
REGISTER_INSTRUCTION_FUNC(DirectoryChangeFunc);

// DIRECTORY_SET_ERROR_INDEX
struct DirectorySetErrorIndexFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple index = wait(data->stack.waitAndPop());
		data->directoryData.directoryErrorIndex = index.getInt(0);

		return Void();
	}
};
const char* DirectorySetErrorIndexFunc::name = "DIRECTORY_SET_ERROR_INDEX";
REGISTER_INSTRUCTION_FUNC(DirectorySetErrorIndexFunc);

// DIRECTORY_CREATE_OR_OPEN
struct DirectoryCreateOrOpenFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state IDirectory::Path path = wait(popPath(data));
		Tuple layerTuple = wait(data->stack.waitAndPop());
		Standalone<StringRef> layer = layerTuple.getType(0) == Tuple::NULL_TYPE ? StringRef() : layerTuple.getString(0);

		Reference<IDirectory> directory = data->directoryData.directory();
		logOp(format("create_or_open %s: layer=%s",
		             pathToString(combinePaths(directory->getPath(), path)).c_str(),
		             layer.printable().c_str()));

		Reference<DirectorySubspace> dirSubspace = wait(executeMutation(
		    instruction, [this, directory, layer]() { return directory->createOrOpen(instruction->tr, path, layer); }));

		data->directoryData.push(dirSubspace);

		return Void();
	}
};
const char* DirectoryCreateOrOpenFunc::name = "DIRECTORY_CREATE_OR_OPEN";
REGISTER_INSTRUCTION_FUNC(DirectoryCreateOrOpenFunc);

// DIRECTORY_CREATE
struct DirectoryCreateFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state IDirectory::Path path = wait(popPath(data));
		std::vector<Tuple> args = wait(data->stack.waitAndPop(2));
		Standalone<StringRef> layer = args[0].getType(0) == Tuple::NULL_TYPE ? StringRef() : args[0].getString(0);
		Optional<Standalone<StringRef>> prefix =
		    args[1].getType(0) == Tuple::NULL_TYPE ? Optional<Standalone<StringRef>>() : args[1].getString(0);

		Reference<IDirectory> directory = data->directoryData.directory();
		logOp(format("create %s: layer=%s, prefix=%s",
		             pathToString(combinePaths(directory->getPath(), path)).c_str(),
		             layer.printable().c_str(),
		             prefix.present() ? prefix.get().printable().c_str() : "<not present>"));

		Reference<DirectorySubspace> dirSubspace =
		    wait(executeMutation(instruction, [this, directory, layer, prefix]() {
			    return directory->create(instruction->tr, path, layer, prefix);
		    }));

		data->directoryData.push(dirSubspace);

		return Void();
	}
};
const char* DirectoryCreateFunc::name = "DIRECTORY_CREATE";
REGISTER_INSTRUCTION_FUNC(DirectoryCreateFunc);

// DIRECTORY_OPEN
struct DirectoryOpenFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state IDirectory::Path path = wait(popPath(data));
		Tuple layerTuple = wait(data->stack.waitAndPop());
		Standalone<StringRef> layer = layerTuple.getType(0) == Tuple::NULL_TYPE ? StringRef() : layerTuple.getString(0);

		Reference<IDirectory> directory = data->directoryData.directory();
		logOp(format("open %s: layer=%s",
		             pathToString(combinePaths(directory->getPath(), path)).c_str(),
		             layer.printable().c_str()));
		Reference<DirectorySubspace> dirSubspace = wait(directory->open(instruction->tr, path, layer));
		data->directoryData.push(dirSubspace);

		return Void();
	}
};
const char* DirectoryOpenFunc::name = "DIRECTORY_OPEN";
REGISTER_INSTRUCTION_FUNC(DirectoryOpenFunc);

// DIRECTORY_MOVE
struct DirectoryMoveFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		std::vector<IDirectory::Path> paths = wait(popPaths(data, 2));

		Reference<IDirectory> directory = data->directoryData.directory();
		logOp(format("move %s to %s",
		             pathToString(combinePaths(directory->getPath(), paths[0])).c_str(),
		             pathToString(combinePaths(directory->getPath(), paths[1])).c_str()));

		Reference<DirectorySubspace> dirSubspace = wait(executeMutation(
		    instruction, [this, directory, paths]() { return directory->move(instruction->tr, paths[0], paths[1]); }));

		data->directoryData.push(dirSubspace);

		return Void();
	}
};
const char* DirectoryMoveFunc::name = "DIRECTORY_MOVE";
REGISTER_INSTRUCTION_FUNC(DirectoryMoveFunc);

// DIRECTORY_MOVE_TO
struct DirectoryMoveToFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		IDirectory::Path path = wait(popPath(data));

		Reference<IDirectory> directory = data->directoryData.directory();
		logOp(format("move %s to %s", pathToString(directory->getPath()).c_str(), pathToString(path).c_str()));

		Reference<DirectorySubspace> dirSubspace = wait(executeMutation(
		    instruction, [this, directory, path]() { return directory->moveTo(instruction->tr, path); }));

		data->directoryData.push(dirSubspace);

		return Void();
	}
};
const char* DirectoryMoveToFunc::name = "DIRECTORY_MOVE_TO";
REGISTER_INSTRUCTION_FUNC(DirectoryMoveToFunc);

// DIRECTORY_REMOVE
struct DirectoryRemoveFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple count = wait(data->stack.waitAndPop());
		state Reference<IDirectory> directory = data->directoryData.directory();
		if (count.getInt(0) == 0) {
			logOp(format("remove %s", pathToString(directory->getPath()).c_str()));

			wait(executeMutation(instruction, [this]() { return directory->remove(instruction->tr); }));
		} else {
			IDirectory::Path path = wait(popPath(data));
			logOp(format("remove %s", pathToString(combinePaths(directory->getPath(), path)).c_str()));

			wait(executeMutation(instruction, [this, path]() { return directory->remove(instruction->tr, path); }));
		}

		return Void();
	}
};
const char* DirectoryRemoveFunc::name = "DIRECTORY_REMOVE";
REGISTER_INSTRUCTION_FUNC(DirectoryRemoveFunc);

// DIRECTORY_REMOVE_IF_EXISTS
struct DirectoryRemoveIfExistsFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple count = wait(data->stack.waitAndPop());
		state Reference<IDirectory> directory = data->directoryData.directory();
		if (count.getInt(0) == 0) {
			logOp(format("remove_if_exists %s", pathToString(directory->getPath()).c_str()));

			wait(
			    success(executeMutation(instruction, [this]() { return directory->removeIfExists(instruction->tr); })));
		} else {
			IDirectory::Path path = wait(popPath(data));
			logOp(format("remove_if_exists %s", pathToString(combinePaths(directory->getPath(), path)).c_str()));

			wait(success(executeMutation(instruction,
			                             [this, path]() { return directory->removeIfExists(instruction->tr, path); })));
		}

		return Void();
	}
};
const char* DirectoryRemoveIfExistsFunc::name = "DIRECTORY_REMOVE_IF_EXISTS";
REGISTER_INSTRUCTION_FUNC(DirectoryRemoveIfExistsFunc);

// DIRECTORY_LIST
struct DirectoryListFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple count = wait(data->stack.waitAndPop());
		state Reference<IDirectory> directory = data->directoryData.directory();
		state Standalone<VectorRef<StringRef>> subdirs;
		if (count.getInt(0) == 0) {
			logOp(format("list %s", pathToString(directory->getPath()).c_str()));
			Standalone<VectorRef<StringRef>> _subdirs = wait(directory->list(instruction->tr));
			subdirs = _subdirs;
		} else {
			IDirectory::Path path = wait(popPath(data));
			logOp(format("list %s", pathToString(combinePaths(directory->getPath(), path)).c_str()));
			Standalone<VectorRef<StringRef>> _subdirs = wait(directory->list(instruction->tr, path));
			subdirs = _subdirs;
		}

		Tuple subdirTuple;
		for (auto& sd : subdirs) {
			subdirTuple.append(sd, true);
		}

		data->stack.pushTuple(subdirTuple.pack());
		return Void();
	}
};
const char* DirectoryListFunc::name = "DIRECTORY_LIST";
REGISTER_INSTRUCTION_FUNC(DirectoryListFunc);

// DIRECTORY_EXISTS
struct DirectoryExistsFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple count = wait(data->stack.waitAndPop());
		state Reference<IDirectory> directory = data->directoryData.directory();
		state bool result;
		if (count.getInt(0) == 0) {
			bool _result = wait(directory->exists(instruction->tr));
			result = _result;
			logOp(format("exists %s: %d", pathToString(directory->getPath()).c_str(), result));
		} else {
			state IDirectory::Path path = wait(popPath(data));
			bool _result = wait(directory->exists(instruction->tr, path));
			result = _result;
			logOp(format("exists %s: %d", pathToString(combinePaths(directory->getPath(), path)).c_str(), result));
		}

		data->stack.push(Tuple().append(result ? 1 : 0).pack());
		return Void();
	}
};
const char* DirectoryExistsFunc::name = "DIRECTORY_EXISTS";
REGISTER_INSTRUCTION_FUNC(DirectoryExistsFunc);

// DIRECTORY_PACK_KEY
struct DirectoryPackKeyFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple tuple = wait(popTuple(data));
		data->stack.pushTuple(data->directoryData.subspace()->pack(tuple));

		return Void();
	}
};
const char* DirectoryPackKeyFunc::name = "DIRECTORY_PACK_KEY";
REGISTER_INSTRUCTION_FUNC(DirectoryPackKeyFunc);

// DIRECTORY_UNPACK_KEY
struct DirectoryUnpackKeyFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple key = wait(data->stack.waitAndPop());
		Subspace* subspace = data->directoryData.subspace();
		logOp(format("Unpack %s in subspace with prefix %s",
		             key.getString(0).printable().c_str(),
		             subspace->key().printable().c_str()));
		Tuple tuple = subspace->unpack(key.getString(0));
		for (int i = 0; i < tuple.size(); ++i) {
			data->stack.push(tuple.subTuple(i, i + 1).pack());
		}

		return Void();
	}
};
const char* DirectoryUnpackKeyFunc::name = "DIRECTORY_UNPACK_KEY";
REGISTER_INSTRUCTION_FUNC(DirectoryUnpackKeyFunc);

// DIRECTORY_RANGE
struct DirectoryRangeFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple tuple = wait(popTuple(data));
		KeyRange range = data->directoryData.subspace()->range(tuple);
		data->stack.pushTuple(range.begin);
		data->stack.pushTuple(range.end);

		return Void();
	}
};
const char* DirectoryRangeFunc::name = "DIRECTORY_RANGE";
REGISTER_INSTRUCTION_FUNC(DirectoryRangeFunc);

// DIRECTORY_CONTAINS
struct DirectoryContainsFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple key = wait(data->stack.waitAndPop());
		bool result = data->directoryData.subspace()->contains(key.getString(0));
		data->stack.push(Tuple().append(result ? 1 : 0).pack());

		return Void();
	}
};
const char* DirectoryContainsFunc::name = "DIRECTORY_CONTAINS";
REGISTER_INSTRUCTION_FUNC(DirectoryContainsFunc);

// DIRECTORY_OPEN_SUBSPACE
struct DirectoryOpenSubspaceFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple tuple = wait(popTuple(data));
		Subspace* subspace = data->directoryData.subspace();
		logOp(format("open_subspace %s (at %s)", tupleToString(tuple).c_str(), subspace->key().printable().c_str()));
		Subspace* child = new Subspace(subspace->subspace(tuple));
		data->directoryData.push(child);

		return Void();
	}
};
const char* DirectoryOpenSubspaceFunc::name = "DIRECTORY_OPEN_SUBSPACE";
REGISTER_INSTRUCTION_FUNC(DirectoryOpenSubspaceFunc);

// DIRECTORY_LOG_SUBSPACE
struct DirectoryLogSubspaceFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple prefix = wait(data->stack.waitAndPop());
		Tuple tuple;
		tuple.append(data->directoryData.directoryListIndex);
		instruction->tr->set(Subspace(tuple, prefix.getString(0)).key(), data->directoryData.subspace()->key());

		return Void();
	}
};
const char* DirectoryLogSubspaceFunc::name = "DIRECTORY_LOG_SUBSPACE";
REGISTER_INSTRUCTION_FUNC(DirectoryLogSubspaceFunc);

// DIRECTORY_LOG_DIRECTORY
struct DirectoryLogDirectoryFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		state Reference<IDirectory> directory = data->directoryData.directory();
		state Tuple prefix = wait(data->stack.waitAndPop());
		state bool exists = wait(directory->exists(instruction->tr));

		state Tuple childrenTuple;
		if (exists) {
			Standalone<VectorRef<StringRef>> children = wait(directory->list(instruction->tr));
			for (auto& c : children) {
				childrenTuple.append(c, true);
			}
		}

		Subspace logSubspace(Tuple().append(data->directoryData.directoryListIndex), prefix.getString(0));

		Tuple pathTuple;
		for (auto& p : directory->getPath()) {
			pathTuple.append(p, true);
		}

		instruction->tr->set(logSubspace.pack(LiteralStringRef("path"), true), pathTuple.pack());
		instruction->tr->set(logSubspace.pack(LiteralStringRef("layer"), true),
		                     Tuple().append(directory->getLayer()).pack());
		instruction->tr->set(logSubspace.pack(LiteralStringRef("exists"), true), Tuple().append(exists ? 1 : 0).pack());
		instruction->tr->set(logSubspace.pack(LiteralStringRef("children"), true), childrenTuple.pack());

		return Void();
	}
};
const char* DirectoryLogDirectoryFunc::name = "DIRECTORY_LOG_DIRECTORY";
REGISTER_INSTRUCTION_FUNC(DirectoryLogDirectoryFunc);

// DIRECTORY_STRIP_PREFIX
struct DirectoryStripPrefixFunc : InstructionFunc {
	static const char* name;

	ACTOR static Future<Void> call(Reference<FlowTesterData> data, Reference<InstructionData> instruction) {
		Tuple str = wait(data->stack.waitAndPop());
		Subspace* subspace = data->directoryData.subspace();
		ASSERT(str.getString(0).startsWith(subspace->key()));
		data->stack.pushTuple(str.getString(0).substr(subspace->key().size()));
		return Void();
	}
};
const char* DirectoryStripPrefixFunc::name = "DIRECTORY_STRIP_PREFIX";
REGISTER_INSTRUCTION_FUNC(DirectoryStripPrefixFunc);
