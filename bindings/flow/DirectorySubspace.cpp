/*
 * DirectorySubspace.cpp
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

#include "DirectorySubspace.h"

namespace FDB {
DirectorySubspace::DirectorySubspace(Path const& path,
                                     StringRef const& prefix,
                                     Reference<DirectoryLayer> directoryLayer,
                                     Standalone<StringRef> const& layer)
  : Subspace(prefix), directoryLayer(directoryLayer), path(path), layer(layer) {}

Future<Reference<DirectorySubspace>> DirectorySubspace::create(Reference<Transaction> const& tr,
                                                               Path const& path,
                                                               Standalone<StringRef> const& layer,
                                                               Optional<Standalone<StringRef>> const& prefix) {
	return directoryLayer->create(tr, getPartitionSubpath(path), layer, prefix);
}

Future<Reference<DirectorySubspace>> DirectorySubspace::open(Reference<Transaction> const& tr,
                                                             Path const& path,
                                                             Standalone<StringRef> const& layer) {
	return directoryLayer->open(tr, getPartitionSubpath(path), layer);
}

Future<Reference<DirectorySubspace>> DirectorySubspace::createOrOpen(Reference<Transaction> const& tr,
                                                                     Path const& path,
                                                                     Standalone<StringRef> const& layer) {
	return directoryLayer->createOrOpen(tr, getPartitionSubpath(path), layer);
}

Future<bool> DirectorySubspace::exists(Reference<Transaction> const& tr, Path const& path) {
	Reference<DirectoryLayer> directoryLayer = getDirectoryLayerForPath(path);
	return directoryLayer->exists(tr, getPartitionSubpath(path, directoryLayer));
}

Future<Standalone<VectorRef<StringRef>>> DirectorySubspace::list(Reference<Transaction> const& tr, Path const& path) {
	return directoryLayer->list(tr, getPartitionSubpath(path));
}

Future<Reference<DirectorySubspace>> DirectorySubspace::move(Reference<Transaction> const& tr,
                                                             Path const& oldPath,
                                                             Path const& newPath) {
	return directoryLayer->move(tr, getPartitionSubpath(oldPath), getPartitionSubpath(newPath));
}

Future<Reference<DirectorySubspace>> DirectorySubspace::moveTo(Reference<Transaction> const& tr,
                                                               Path const& newAbsolutePath) {
	Reference<DirectoryLayer> directoryLayer = getDirectoryLayerForPath(Path());
	Path directoryLayerPath = directoryLayer->getPath();

	if (directoryLayerPath.size() > newAbsolutePath.size()) {
		return cannot_move_directory_between_partitions();
	}

	for (int i = 0; i < directoryLayerPath.size(); ++i) {
		if (directoryLayerPath[i] != newAbsolutePath[i]) {
			return cannot_move_directory_between_partitions();
		}
	}

	Path newRelativePath(newAbsolutePath.begin() + directoryLayerPath.size(), newAbsolutePath.end());
	return directoryLayer->move(tr, getPartitionSubpath(Path(), directoryLayer), newRelativePath);
}

Future<Void> DirectorySubspace::remove(Reference<Transaction> const& tr, Path const& path) {
	Reference<DirectoryLayer> directoryLayer = getDirectoryLayerForPath(path);
	return directoryLayer->remove(tr, getPartitionSubpath(path, directoryLayer));
}

Future<bool> DirectorySubspace::removeIfExists(Reference<Transaction> const& tr, Path const& path) {
	Reference<DirectoryLayer> directoryLayer = getDirectoryLayerForPath(path);
	return directoryLayer->removeIfExists(tr, getPartitionSubpath(path, directoryLayer));
}

Reference<DirectoryLayer> DirectorySubspace::getDirectoryLayer() {
	return directoryLayer;
}

const Standalone<StringRef> DirectorySubspace::getLayer() const {
	return layer;
}

const IDirectory::Path DirectorySubspace::getPath() const {
	return path;
}

IDirectory::Path DirectorySubspace::getPartitionSubpath(Path const& path,
                                                        Reference<DirectoryLayer> directoryLayer) const {
	if (!directoryLayer) {
		directoryLayer = this->directoryLayer;
	}

	Path newPath(this->path.begin() + directoryLayer->getPath().size(), this->path.end());
	newPath.insert(newPath.end(), path.begin(), path.end());

	return newPath;
}

Reference<DirectoryLayer> DirectorySubspace::getDirectoryLayerForPath(Path const& path) const {
	return directoryLayer;
}
} // namespace FDB
