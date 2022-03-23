/*
 * DirectorySubspace.h
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

#ifndef FDB_FLOW_DIRECTORY_SUBSPACE_H
#define FDB_FLOW_DIRECTORY_SUBSPACE_H

#pragma once

#include "IDirectory.h"
#include "DirectoryLayer.h"
#include "Subspace.h"

namespace FDB {
class DirectorySubspace : public IDirectory, public Subspace {

public:
	DirectorySubspace(Path const& path,
	                  StringRef const& prefix,
	                  Reference<DirectoryLayer> directorLayer,
	                  Standalone<StringRef> const& layer = Standalone<StringRef>());
	virtual ~DirectorySubspace() {}

	virtual Future<Reference<DirectorySubspace>> create(
	    Reference<Transaction> const& tr,
	    Path const& path,
	    Standalone<StringRef> const& layer = Standalone<StringRef>(),
	    Optional<Standalone<StringRef>> const& prefix = Optional<Standalone<StringRef>>());

	virtual Future<Reference<DirectorySubspace>> open(Reference<Transaction> const& tr,
	                                                  Path const& path,
	                                                  Standalone<StringRef> const& layer = Standalone<StringRef>());
	virtual Future<Reference<DirectorySubspace>> createOrOpen(
	    Reference<Transaction> const& tr,
	    Path const& path,
	    Standalone<StringRef> const& layer = Standalone<StringRef>());

	virtual Future<bool> exists(Reference<Transaction> const& tr, Path const& path = Path());
	virtual Future<Standalone<VectorRef<StringRef>>> list(Reference<Transaction> const& tr, Path const& path = Path());

	virtual Future<Reference<DirectorySubspace>> move(Reference<Transaction> const& tr,
	                                                  Path const& oldPath,
	                                                  Path const& newPath);
	virtual Future<Reference<DirectorySubspace>> moveTo(Reference<Transaction> const& tr, Path const& newAbsolutePath);

	virtual Future<Void> remove(Reference<Transaction> const& tr, Path const& path = Path());
	virtual Future<bool> removeIfExists(Reference<Transaction> const& tr, Path const& path = Path());

	virtual Reference<DirectoryLayer> getDirectoryLayer();
	virtual const Standalone<StringRef> getLayer() const;
	virtual const Path getPath() const;

protected:
	Reference<DirectoryLayer> directoryLayer;
	Path path;
	Standalone<StringRef> layer;

	virtual Path getPartitionSubpath(Path const& path,
	                                 Reference<DirectoryLayer> directoryLayer = Reference<DirectoryLayer>()) const;
	virtual Reference<DirectoryLayer> getDirectoryLayerForPath(Path const& path) const;
};
} // namespace FDB

#endif