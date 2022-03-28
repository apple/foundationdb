/*
 * DirectoryLayer.h
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

#ifndef FDB_FLOW_DIRECTORY_LAYER_H
#define FDB_FLOW_DIRECTORY_LAYER_H

#pragma once

#include "IDirectory.h"
#include "DirectorySubspace.h"
#include "HighContentionAllocator.h"

namespace FDB {
class DirectoryLayer : public IDirectory {
public:
	DirectoryLayer(Subspace nodeSubspace = DEFAULT_NODE_SUBSPACE,
	               Subspace contentSubspace = DEFAULT_CONTENT_SUBSPACE,
	               bool allowManualPrefixes = false);

	Future<Reference<DirectorySubspace>> create(
	    Reference<Transaction> const& tr,
	    Path const& path,
	    Standalone<StringRef> const& layer = Standalone<StringRef>(),
	    Optional<Standalone<StringRef>> const& prefix = Optional<Standalone<StringRef>>());
	Future<Reference<DirectorySubspace>> open(Reference<Transaction> const& tr,
	                                          Path const& path,
	                                          Standalone<StringRef> const& layer = Standalone<StringRef>());
	Future<Reference<DirectorySubspace>> createOrOpen(Reference<Transaction> const& tr,
	                                                  Path const& path,
	                                                  Standalone<StringRef> const& layer = Standalone<StringRef>());

	Future<bool> exists(Reference<Transaction> const& tr, Path const& path = Path());
	Future<Standalone<VectorRef<StringRef>>> list(Reference<Transaction> const& tr, Path const& path = Path());

	Future<Reference<DirectorySubspace>> move(Reference<Transaction> const& tr,
	                                          Path const& oldPath,
	                                          Path const& newPath);
	Future<Reference<DirectorySubspace>> moveTo(Reference<Transaction> const& tr, Path const& newAbsolutePath);

	Future<Void> remove(Reference<Transaction> const& tr, Path const& path = Path());
	Future<bool> removeIfExists(Reference<Transaction> const& tr, Path const& path = Path());

	Reference<DirectoryLayer> getDirectoryLayer();
	const Standalone<StringRef> getLayer() const;
	const Path getPath() const;

	static const Subspace DEFAULT_NODE_SUBSPACE;
	static const Subspace DEFAULT_CONTENT_SUBSPACE;
	static const StringRef PARTITION_LAYER;

	// private:
	static const uint8_t LITTLE_ENDIAN_LONG_ONE[8];
	static const StringRef HIGH_CONTENTION_KEY;
	static const StringRef LAYER_KEY;
	static const StringRef VERSION_KEY;
	static const int64_t SUB_DIR_KEY;
	static const uint32_t VERSION[3];
	static const StringRef DEFAULT_NODE_SUBSPACE_PREFIX;

	struct Node {
		Node() {}
		Node(Reference<DirectoryLayer> const& directoryLayer,
		     Optional<Subspace> const& subspace,
		     Path const& path,
		     Path const& targetPath);

		bool exists() const;

		Future<Node> loadMetadata(Reference<Transaction> tr);
		void ensureMetadataLoaded() const;

		bool isInPartition(bool includeEmptySubpath = false) const;
		Path getPartitionSubpath() const;
		Reference<DirectorySubspace> getContents() const;

		Reference<DirectoryLayer> directoryLayer;
		Optional<Subspace> subspace;
		Path path;
		Path targetPath;
		Standalone<StringRef> layer;

		bool loadedMetadata;
	};

	Reference<DirectorySubspace> openInternal(Standalone<StringRef> const& layer,
	                                          Node const& existingNode,
	                                          bool allowOpen);
	Future<Reference<DirectorySubspace>> createOrOpenInternal(Reference<Transaction> const& tr,
	                                                          Path const& path,
	                                                          Standalone<StringRef> const& layer,
	                                                          Optional<Standalone<StringRef>> const& prefix,
	                                                          bool allowCreate,
	                                                          bool allowOpen);

	void initializeDirectory(Reference<Transaction> const& tr) const;
	Future<Void> checkVersion(Reference<Transaction> const& tr, bool writeAccess) const;

	template <class T>
	Optional<Subspace> nodeWithPrefix(Optional<T> const& prefix) const;
	Subspace nodeWithPrefix(StringRef const& prefix) const;

	Reference<DirectorySubspace> contentsOfNode(Subspace const& node,
	                                            Path const& path,
	                                            Standalone<StringRef> const& layer);

	Path toAbsolutePath(Path const& subpath) const;

	Subspace rootNode;
	Subspace nodeSubspace;
	Subspace contentSubspace;
	HighContentionAllocator allocator;
	bool allowManualPrefixes;

	Path path;
};
} // namespace FDB

#endif