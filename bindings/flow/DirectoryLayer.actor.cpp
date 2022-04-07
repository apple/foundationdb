/*
 * DirectoryLayer.actor.cpp
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

#include "DirectoryLayer.h"
#include "DirectoryPartition.h"

namespace FDB {
const uint8_t DirectoryLayer::LITTLE_ENDIAN_LONG_ONE[8] = { 1, 0, 0, 0, 0, 0, 0, 0 };
const StringRef DirectoryLayer::HIGH_CONTENTION_KEY = LiteralStringRef("hca");
const StringRef DirectoryLayer::LAYER_KEY = LiteralStringRef("layer");
const StringRef DirectoryLayer::VERSION_KEY = LiteralStringRef("version");
const int64_t DirectoryLayer::SUB_DIR_KEY = 0;

const uint32_t DirectoryLayer::VERSION[3] = { 1, 0, 0 };

const StringRef DirectoryLayer::DEFAULT_NODE_SUBSPACE_PREFIX = LiteralStringRef("\xfe");
const Subspace DirectoryLayer::DEFAULT_NODE_SUBSPACE = Subspace(DEFAULT_NODE_SUBSPACE_PREFIX);
const Subspace DirectoryLayer::DEFAULT_CONTENT_SUBSPACE = Subspace();
const StringRef DirectoryLayer::PARTITION_LAYER = LiteralStringRef("partition");

DirectoryLayer::DirectoryLayer(Subspace nodeSubspace, Subspace contentSubspace, bool allowManualPrefixes)
  : rootNode(nodeSubspace.get(nodeSubspace.key())), nodeSubspace(nodeSubspace), contentSubspace(contentSubspace),
    allocator(rootNode.get(HIGH_CONTENTION_KEY)), allowManualPrefixes(allowManualPrefixes) {}

Subspace DirectoryLayer::nodeWithPrefix(StringRef const& prefix) const {
	return nodeSubspace.get(prefix);
}

template <class T>
Optional<Subspace> DirectoryLayer::nodeWithPrefix(Optional<T> const& prefix) const {
	if (!prefix.present()) {
		return Optional<Subspace>();
	}

	return nodeWithPrefix(prefix.get());
}

ACTOR Future<DirectoryLayer::Node> find(Reference<DirectoryLayer> dirLayer,
                                        Reference<Transaction> tr,
                                        IDirectory::Path path) {
	state int pathIndex = 0;
	state DirectoryLayer::Node node = DirectoryLayer::Node(dirLayer, dirLayer->rootNode, IDirectory::Path(), path);

	for (; pathIndex != path.size(); ++pathIndex) {
		ASSERT(node.subspace.present());
		Optional<FDBStandalone<ValueRef>> val =
		    wait(tr->get(node.subspace.get().get(DirectoryLayer::SUB_DIR_KEY).get(path[pathIndex], true).key()));

		node.path.push_back(path[pathIndex]);
		node = DirectoryLayer::Node(dirLayer, dirLayer->nodeWithPrefix(val), node.path, path);

		DirectoryLayer::Node _node = wait(node.loadMetadata(tr));
		node = _node;

		if (!node.exists() || node.layer == DirectoryLayer::PARTITION_LAYER) {
			return node;
		}
	}

	if (!node.loadedMetadata) {
		DirectoryLayer::Node _node = wait(node.loadMetadata(tr));
		node = _node;
	}

	return node;
}

IDirectory::Path DirectoryLayer::toAbsolutePath(IDirectory::Path const& subpath) const {
	Path path;

	path.reserve(this->path.size() + subpath.size());
	path.insert(path.end(), this->path.begin(), this->path.end());
	path.insert(path.end(), subpath.begin(), subpath.end());

	return path;
}

Reference<DirectorySubspace> DirectoryLayer::contentsOfNode(Subspace const& node,
                                                            Path const& path,
                                                            Standalone<StringRef> const& layer) {
	Standalone<StringRef> prefix = nodeSubspace.unpack(node.key()).getString(0);

	if (layer == PARTITION_LAYER) {
		return Reference<DirectorySubspace>(
		    new DirectoryPartition(toAbsolutePath(path), prefix, Reference<DirectoryLayer>::addRef(this)));
	} else {
		return Reference<DirectorySubspace>(
		    new DirectorySubspace(toAbsolutePath(path), prefix, Reference<DirectoryLayer>::addRef(this), layer));
	}
}

Reference<DirectorySubspace> DirectoryLayer::openInternal(Standalone<StringRef> const& layer,
                                                          Node const& existingNode,
                                                          bool allowOpen) {
	if (!allowOpen) {
		throw directory_already_exists();
	}
	if (layer.size() > 0 && layer != existingNode.layer) {
		throw mismatched_layer();
	}

	return existingNode.getContents();
}

Future<Reference<DirectorySubspace>> DirectoryLayer::open(Reference<Transaction> const& tr,
                                                          Path const& path,
                                                          Standalone<StringRef> const& layer) {
	return createOrOpenInternal(tr, path, layer, Optional<Standalone<StringRef>>(), false, true);
}

void DirectoryLayer::initializeDirectory(Reference<Transaction> const& tr) const {
	tr->set(rootNode.pack(VERSION_KEY), StringRef((uint8_t*)VERSION, 12));
}

ACTOR Future<Void> checkVersionInternal(const DirectoryLayer* dirLayer, Reference<Transaction> tr, bool writeAccess) {
	Optional<FDBStandalone<ValueRef>> versionBytes =
	    wait(tr->get(dirLayer->rootNode.pack(DirectoryLayer::VERSION_KEY)));

	if (!versionBytes.present()) {
		if (writeAccess) {
			dirLayer->initializeDirectory(tr);
		}
		return Void();
	} else {
		if (versionBytes.get().size() != 12) {
			throw invalid_directory_layer_metadata();
		}
		if (((uint32_t*)versionBytes.get().begin())[0] > DirectoryLayer::VERSION[0]) {
			throw incompatible_directory_version();
		} else if (((uint32_t*)versionBytes.get().begin())[1] > DirectoryLayer::VERSION[1] && writeAccess) {
			throw incompatible_directory_version();
		}
	}

	return Void();
}

Future<Void> DirectoryLayer::checkVersion(Reference<Transaction> const& tr, bool writeAccess) const {
	return checkVersionInternal(this, tr, writeAccess);
}

ACTOR Future<Standalone<StringRef>> getPrefix(Reference<DirectoryLayer> dirLayer,
                                              Reference<Transaction> tr,
                                              Optional<Standalone<StringRef>> prefix) {
	if (!prefix.present()) {
		Standalone<StringRef> allocated = wait(dirLayer->allocator.allocate(tr));
		state Standalone<StringRef> finalPrefix = allocated.withPrefix(dirLayer->contentSubspace.key());

		FDBStandalone<RangeResultRef> result = wait(tr->getRange(KeyRangeRef(finalPrefix, strinc(finalPrefix)), 1));

		if (result.size() > 0) {
			throw directory_prefix_not_empty();
		}

		return finalPrefix;
	}

	return prefix.get();
}

ACTOR Future<Optional<Subspace>> nodeContainingKey(Reference<DirectoryLayer> dirLayer,
                                                   Reference<Transaction> tr,
                                                   Standalone<StringRef> key,
                                                   bool snapshot) {
	if (key.startsWith(dirLayer->nodeSubspace.key())) {
		return dirLayer->rootNode;
	}

	KeyRange range = KeyRangeRef(dirLayer->nodeSubspace.range().begin, keyAfter(dirLayer->nodeSubspace.pack(key)));
	FDBStandalone<RangeResultRef> result = wait(tr->getRange(range, 1, snapshot, true));

	if (result.size() > 0) {
		Standalone<StringRef> prevPrefix = dirLayer->nodeSubspace.unpack(result[0].key).getString(0);
		if (key.startsWith(prevPrefix)) {
			return dirLayer->nodeWithPrefix(prevPrefix);
		}
	}

	return Optional<Subspace>();
}

ACTOR Future<bool> isPrefixFree(Reference<DirectoryLayer> dirLayer,
                                Reference<Transaction> tr,
                                Standalone<StringRef> prefix,
                                bool snapshot) {
	if (!prefix.size()) {
		return false;
	}

	Optional<Subspace> node = wait(nodeContainingKey(dirLayer, tr, prefix, snapshot));
	if (node.present()) {
		return false;
	}

	FDBStandalone<RangeResultRef> result = wait(tr->getRange(
	    KeyRangeRef(dirLayer->nodeSubspace.pack(prefix), dirLayer->nodeSubspace.pack(strinc(prefix))), 1, snapshot));
	return !result.size();
}

ACTOR Future<Subspace> getParentNode(Reference<DirectoryLayer> dirLayer,
                                     Reference<Transaction> tr,
                                     IDirectory::Path path) {
	if (path.size() > 1) {
		Reference<DirectorySubspace> parent =
		    wait(dirLayer->createOrOpenInternal(tr,
		                                        IDirectory::Path(path.begin(), path.end() - 1),
		                                        StringRef(),
		                                        Optional<Standalone<StringRef>>(),
		                                        true,
		                                        true));
		return dirLayer->nodeWithPrefix(parent->key());
	} else {
		return dirLayer->rootNode;
	}
}

ACTOR Future<Reference<DirectorySubspace>> createInternal(Reference<DirectoryLayer> dirLayer,
                                                          Reference<Transaction> tr,
                                                          IDirectory::Path path,
                                                          Standalone<StringRef> layer,
                                                          Optional<Standalone<StringRef>> prefix,
                                                          bool allowCreate) {
	if (!allowCreate) {
		throw directory_does_not_exist();
	}

	wait(dirLayer->checkVersion(tr, true));

	state Standalone<StringRef> newPrefix = wait(getPrefix(dirLayer, tr, prefix));
	bool isFree = wait(isPrefixFree(dirLayer, tr, newPrefix, !prefix.present()));

	if (!isFree) {
		throw directory_prefix_in_use();
	}

	Subspace parentNode = wait(getParentNode(dirLayer, tr, path));
	Subspace node = dirLayer->nodeWithPrefix(newPrefix);

	tr->set(parentNode.get(DirectoryLayer::SUB_DIR_KEY).get(path.back(), true).key(), newPrefix);
	tr->set(node.get(DirectoryLayer::LAYER_KEY).key(), layer);
	return dirLayer->contentsOfNode(node, path, layer);
}

ACTOR Future<Reference<DirectorySubspace>> _createOrOpenInternal(Reference<DirectoryLayer> dirLayer,
                                                                 Reference<Transaction> tr,
                                                                 IDirectory::Path path,
                                                                 Standalone<StringRef> layer,
                                                                 Optional<Standalone<StringRef>> prefix,
                                                                 bool allowCreate,
                                                                 bool allowOpen) {
	ASSERT(!prefix.present() || allowCreate);
	wait(dirLayer->checkVersion(tr, false));

	if (prefix.present() && !dirLayer->allowManualPrefixes) {
		if (!dirLayer->getPath().size()) {
			throw manual_prefixes_not_enabled();
		} else {
			throw prefix_in_partition();
		}
	}

	if (!path.size()) {
		throw cannot_open_root_directory();
	}

	state DirectoryLayer::Node existingNode = wait(find(dirLayer, tr, path));
	if (existingNode.exists()) {
		if (existingNode.isInPartition()) {
			IDirectory::Path subpath = existingNode.getPartitionSubpath();
			Reference<DirectorySubspace> dirSpace =
			    wait(existingNode.getContents()->getDirectoryLayer()->createOrOpenInternal(
			        tr, subpath, layer, prefix, allowCreate, allowOpen));
			return dirSpace;
		}
		return dirLayer->openInternal(layer, existingNode, allowOpen);
	} else {
		Reference<DirectorySubspace> dirSpace = wait(createInternal(dirLayer, tr, path, layer, prefix, allowCreate));
		return dirSpace;
	}
}

Future<Reference<DirectorySubspace>> DirectoryLayer::createOrOpenInternal(Reference<Transaction> const& tr,
                                                                          Path const& path,
                                                                          Standalone<StringRef> const& layer,
                                                                          Optional<Standalone<StringRef>> const& prefix,
                                                                          bool allowCreate,
                                                                          bool allowOpen) {
	return _createOrOpenInternal(
	    Reference<DirectoryLayer>::addRef(this), tr, path, layer, prefix, allowCreate, allowOpen);
}

Future<Reference<DirectorySubspace>> DirectoryLayer::create(Reference<Transaction> const& tr,
                                                            Path const& path,
                                                            Standalone<StringRef> const& layer,
                                                            Optional<Standalone<StringRef>> const& prefix) {
	return createOrOpenInternal(tr, path, layer, prefix, true, false);
}

Future<Reference<DirectorySubspace>> DirectoryLayer::createOrOpen(Reference<Transaction> const& tr,
                                                                  Path const& path,
                                                                  Standalone<StringRef> const& layer) {
	return createOrOpenInternal(tr, path, layer, Optional<Standalone<StringRef>>(), true, true);
}

ACTOR Future<Standalone<VectorRef<StringRef>>> listInternal(Reference<DirectoryLayer> dirLayer,
                                                            Reference<Transaction> tr,
                                                            IDirectory::Path path) {
	wait(dirLayer->checkVersion(tr, false));

	state DirectoryLayer::Node node = wait(find(dirLayer, tr, path));

	if (!node.exists()) {
		throw directory_does_not_exist();
	}
	if (node.isInPartition(true)) {
		Standalone<VectorRef<StringRef>> partitionList =
		    wait(node.getContents()->getDirectoryLayer()->list(tr, node.getPartitionSubpath()));
		return partitionList;
	}

	state Subspace subdir = node.subspace.get().get(DirectoryLayer::SUB_DIR_KEY);
	state Key begin = subdir.range().begin;
	state Standalone<VectorRef<StringRef>> subdirectories;

	loop {
		FDBStandalone<RangeResultRef> subdirRange = wait(tr->getRange(KeyRangeRef(begin, subdir.range().end)));

		for (int i = 0; i < subdirRange.size(); ++i) {
			subdirectories.push_back_deep(subdirectories.arena(), subdir.unpack(subdirRange[i].key).getString(0));
		}

		if (!subdirRange.more) {
			return subdirectories;
		}

		begin = keyAfter(subdirRange.back().key);
	}
}

Future<Standalone<VectorRef<StringRef>>> DirectoryLayer::list(Reference<Transaction> const& tr, Path const& path) {
	return listInternal(Reference<DirectoryLayer>::addRef(this), tr, path);
}

bool pathsEqual(IDirectory::Path const& path1,
                IDirectory::Path const& path2,
                size_t maxElementsToCheck = std::numeric_limits<size_t>::max()) {
	if (std::min(path1.size(), maxElementsToCheck) != std::min(path2.size(), maxElementsToCheck)) {
		return false;
	}
	for (int i = 0; i < path1.size() && i < maxElementsToCheck; ++i) {
		if (path1[i] != path2[i]) {
			return false;
		}
	}

	return true;
}

ACTOR Future<Void> removeFromParent(Reference<DirectoryLayer> dirLayer,
                                    Reference<Transaction> tr,
                                    IDirectory::Path path) {
	ASSERT(path.size() >= 1);
	DirectoryLayer::Node parentNode = wait(find(dirLayer, tr, IDirectory::Path(path.begin(), path.end() - 1)));
	if (parentNode.subspace.present()) {
		tr->clear(parentNode.subspace.get().get(DirectoryLayer::SUB_DIR_KEY).get(path.back(), true).key());
	}

	return Void();
}

ACTOR Future<Reference<DirectorySubspace>> moveInternal(Reference<DirectoryLayer> dirLayer,
                                                        Reference<Transaction> tr,
                                                        IDirectory::Path oldPath,
                                                        IDirectory::Path newPath) {
	wait(dirLayer->checkVersion(tr, true));

	if (oldPath.size() <= newPath.size()) {
		if (pathsEqual(oldPath, newPath, oldPath.size())) {
			throw invalid_destination_directory();
		}
	}

	std::vector<Future<DirectoryLayer::Node>> futures;
	futures.push_back(find(dirLayer, tr, oldPath));
	futures.push_back(find(dirLayer, tr, newPath));

	std::vector<DirectoryLayer::Node> nodes = wait(getAll(futures));

	state DirectoryLayer::Node oldNode = nodes[0];
	state DirectoryLayer::Node newNode = nodes[1];

	if (!oldNode.exists()) {
		throw directory_does_not_exist();
	}

	if (oldNode.isInPartition() || newNode.isInPartition()) {
		if (!oldNode.isInPartition() || !newNode.isInPartition() || !pathsEqual(oldNode.path, newNode.path)) {
			throw cannot_move_directory_between_partitions();
		}

		Reference<DirectorySubspace> partitionMove =
		    wait(newNode.getContents()->move(tr, oldNode.getPartitionSubpath(), newNode.getPartitionSubpath()));
		return partitionMove;
	}

	if (newNode.exists() || newPath.empty()) {
		throw directory_already_exists();
	}

	DirectoryLayer::Node parentNode = wait(find(dirLayer, tr, IDirectory::Path(newPath.begin(), newPath.end() - 1)));
	if (!parentNode.exists()) {
		throw parent_directory_does_not_exist();
	}

	tr->set(parentNode.subspace.get().get(DirectoryLayer::SUB_DIR_KEY).get(newPath.back(), true).key(),
	        dirLayer->nodeSubspace.unpack(oldNode.subspace.get().key()).getString(0));
	wait(removeFromParent(dirLayer, tr, oldPath));

	return dirLayer->contentsOfNode(oldNode.subspace.get(), newPath, oldNode.layer);
}

Future<Reference<DirectorySubspace>> DirectoryLayer::move(Reference<Transaction> const& tr,
                                                          Path const& oldPath,
                                                          Path const& newPath) {
	return moveInternal(Reference<DirectoryLayer>::addRef(this), tr, oldPath, newPath);
}

Future<Reference<DirectorySubspace>> DirectoryLayer::moveTo(Reference<Transaction> const& tr,
                                                            Path const& newAbsolutePath) {
	throw cannot_modify_root_directory();
}

Future<Void> removeRecursive(Reference<DirectoryLayer> const&, Reference<Transaction> const&, Subspace const&);
ACTOR Future<Void> removeRecursive(Reference<DirectoryLayer> dirLayer, Reference<Transaction> tr, Subspace nodeSub) {
	state Subspace subdir = nodeSub.get(DirectoryLayer::SUB_DIR_KEY);
	state Key begin = subdir.range().begin;
	state std::vector<Future<Void>> futures;

	loop {
		FDBStandalone<RangeResultRef> range = wait(tr->getRange(KeyRangeRef(begin, subdir.range().end)));
		for (int i = 0; i < range.size(); ++i) {
			Subspace subNode = dirLayer->nodeWithPrefix(range[i].value);
			futures.push_back(removeRecursive(dirLayer, tr, subNode));
		}

		if (!range.more) {
			break;
		}

		begin = keyAfter(range.back().key);
	}

	// waits are done concurrently
	wait(waitForAll(futures));

	Standalone<StringRef> nodePrefix = dirLayer->nodeSubspace.unpack(nodeSub.key()).getString(0);

	tr->clear(KeyRangeRef(nodePrefix, strinc(nodePrefix)));
	tr->clear(nodeSub.range());

	return Void();
}

Future<bool> removeInternal(Reference<DirectoryLayer> const&,
                            Reference<Transaction> const&,
                            IDirectory::Path const&,
                            bool const&);
ACTOR Future<bool> removeInternal(Reference<DirectoryLayer> dirLayer,
                                  Reference<Transaction> tr,
                                  IDirectory::Path path,
                                  bool failOnNonexistent) {
	wait(dirLayer->checkVersion(tr, true));

	if (path.empty()) {
		throw cannot_modify_root_directory();
	}

	state DirectoryLayer::Node node = wait(find(dirLayer, tr, path));

	if (!node.exists()) {
		if (failOnNonexistent) {
			throw directory_does_not_exist();
		} else {
			return false;
		}
	}

	if (node.isInPartition()) {
		bool recurse = wait(
		    removeInternal(node.getContents()->getDirectoryLayer(), tr, node.getPartitionSubpath(), failOnNonexistent));
		return recurse;
	}

	state std::vector<Future<Void>> futures;
	futures.push_back(removeRecursive(dirLayer, tr, node.subspace.get()));
	futures.push_back(removeFromParent(dirLayer, tr, path));

	wait(waitForAll(futures));

	return true;
}

Future<Void> DirectoryLayer::remove(Reference<Transaction> const& tr, Path const& path) {
	return success(removeInternal(Reference<DirectoryLayer>::addRef(this), tr, path, true));
}

Future<bool> DirectoryLayer::removeIfExists(Reference<Transaction> const& tr, Path const& path) {
	return removeInternal(Reference<DirectoryLayer>::addRef(this), tr, path, false);
}

ACTOR Future<bool> existsInternal(Reference<DirectoryLayer> dirLayer,
                                  Reference<Transaction> tr,
                                  IDirectory::Path path) {
	wait(dirLayer->checkVersion(tr, false));

	DirectoryLayer::Node node = wait(find(dirLayer, tr, path));

	if (!node.exists()) {
		return false;
	}

	if (node.isInPartition()) {
		bool exists = wait(node.getContents()->getDirectoryLayer()->exists(tr, node.getPartitionSubpath()));
		return exists;
	}

	return true;
}

Future<bool> DirectoryLayer::exists(Reference<Transaction> const& tr, Path const& path) {
	return existsInternal(Reference<DirectoryLayer>::addRef(this), tr, path);
}

Reference<DirectoryLayer> DirectoryLayer::getDirectoryLayer() {
	return Reference<DirectoryLayer>::addRef(this);
}

const Standalone<StringRef> DirectoryLayer::getLayer() const {
	return StringRef();
}

const IDirectory::Path DirectoryLayer::getPath() const {
	return path;
}
} // namespace FDB
