/*
 * Node.actor.cpp
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

namespace FDB {
DirectoryLayer::Node::Node(Reference<DirectoryLayer> const& directoryLayer,
                           Optional<Subspace> const& subspace,
                           IDirectory::Path const& path,
                           IDirectory::Path const& targetPath)
  : directoryLayer(directoryLayer), subspace(subspace), path(path), targetPath(targetPath), loadedMetadata(false) {}

bool DirectoryLayer::Node::exists() const {
	return subspace.present();
}

ACTOR Future<DirectoryLayer::Node> loadMetadata(DirectoryLayer::Node* n, Reference<Transaction> tr) {
	if (!n->exists()) {
		n->loadedMetadata = true;
		return *n;
	}

	Optional<FDBStandalone<ValueRef>> layer = wait(tr->get(n->subspace.get().pack(DirectoryLayer::LAYER_KEY)));

	n->layer = layer.present() ? layer.get() : Standalone<StringRef>();
	n->loadedMetadata = true;

	return *n;
}

// Calls to loadMetadata must keep the Node alive while the future is outstanding
Future<DirectoryLayer::Node> DirectoryLayer::Node::loadMetadata(Reference<Transaction> tr) {
	return FDB::loadMetadata(this, tr);
}

bool DirectoryLayer::Node::isInPartition(bool includeEmptySubpath) const {
	ASSERT(loadedMetadata);
	return exists() && layer == DirectoryLayer::PARTITION_LAYER &&
	       (includeEmptySubpath || targetPath.size() > path.size());
}

IDirectory::Path DirectoryLayer::Node::getPartitionSubpath() const {
	return Path(targetPath.begin() + path.size(), targetPath.end());
}

Reference<DirectorySubspace> DirectoryLayer::Node::getContents() const {
	ASSERT(exists());
	ASSERT(loadedMetadata);

	return directoryLayer->contentsOfNode(subspace.get(), path, layer);
}
} // namespace FDB
