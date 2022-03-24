/*
 * IDirectory.h
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

#ifndef FDB_FLOW_IDIRECTORY_H
#define FDB_FLOW_IDIRECTORY_H

#pragma once

#include "flow/flow.h"
#include "bindings/flow/fdb_flow.h"

namespace FDB {
class DirectoryLayer;
class DirectorySubspace;

class IDirectory : public ReferenceCounted<IDirectory> {
public:
	typedef std::vector<Standalone<StringRef>> Path;

	virtual Future<Reference<DirectorySubspace>> create(
	    Reference<Transaction> const& tr,
	    Path const& path,
	    Standalone<StringRef> const& layer = Standalone<StringRef>(),
	    Optional<Standalone<StringRef>> const& prefix = Optional<Standalone<StringRef>>()) = 0;

	virtual Future<Reference<DirectorySubspace>> open(Reference<Transaction> const& tr,
	                                                  Path const& path,
	                                                  Standalone<StringRef> const& layer = Standalone<StringRef>()) = 0;
	virtual Future<Reference<DirectorySubspace>> createOrOpen(
	    Reference<Transaction> const& tr,
	    Path const& path,
	    Standalone<StringRef> const& layer = Standalone<StringRef>()) = 0;

	virtual Future<bool> exists(Reference<Transaction> const& tr, Path const& path = Path()) = 0;
	virtual Future<Standalone<VectorRef<StringRef>>> list(Reference<Transaction> const& tr,
	                                                      Path const& path = Path()) = 0;

	virtual Future<Reference<DirectorySubspace>> move(Reference<Transaction> const& tr,
	                                                  Path const& oldPath,
	                                                  Path const& newPath) = 0;
	virtual Future<Reference<DirectorySubspace>> moveTo(Reference<Transaction> const& tr,
	                                                    Path const& newAbsolutePath) = 0;

	virtual Future<Void> remove(Reference<Transaction> const& tr, Path const& path = Path()) = 0;
	virtual Future<bool> removeIfExists(Reference<Transaction> const& tr, Path const& path = Path()) = 0;

	virtual Reference<DirectoryLayer> getDirectoryLayer() = 0;
	virtual const Standalone<StringRef> getLayer() const = 0;
	virtual const Path getPath() const = 0;

	virtual ~IDirectory(){};
};
} // namespace FDB

#endif