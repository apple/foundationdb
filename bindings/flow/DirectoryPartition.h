/*
 * DirectoryPartition.h
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

#ifndef FDB_FLOW_DIRECTORY_PARTITION_H
#define FDB_FLOW_DIRECTORY_PARTITION_H

#pragma once

#include "IDirectory.h"
#include "DirectorySubspace.h"
#include "DirectoryLayer.h"

namespace FDB {
class DirectoryPartition : public DirectorySubspace {

public:
	DirectoryPartition(Path const& path, StringRef const& prefix, Reference<DirectoryLayer> parentDirectoryLayer)
	  : DirectorySubspace(path,
	                      prefix,
	                      Reference<DirectoryLayer>(new DirectoryLayer(
	                          Subspace(DirectoryLayer::DEFAULT_NODE_SUBSPACE_PREFIX.withPrefix(prefix)),
	                          Subspace(prefix))),
	                      DirectoryLayer::PARTITION_LAYER),
	    parentDirectoryLayer(parentDirectoryLayer) {
		this->directoryLayer->path = path;
	}
	virtual ~DirectoryPartition() {}

	virtual Key key() const { throw cannot_use_partition_as_subspace(); }
	virtual bool contains(KeyRef const& key) const { throw cannot_use_partition_as_subspace(); }

	virtual Key pack(Tuple const& tuple = Tuple()) const { throw cannot_use_partition_as_subspace(); }
	virtual Tuple unpack(KeyRef const& key) const { throw cannot_use_partition_as_subspace(); }
	virtual KeyRange range(Tuple const& tuple = Tuple()) const { throw cannot_use_partition_as_subspace(); }

	virtual Subspace subspace(Tuple const& tuple) const { throw cannot_use_partition_as_subspace(); }
	virtual Subspace get(Tuple const& tuple) const { throw cannot_use_partition_as_subspace(); }

protected:
	Reference<DirectoryLayer> parentDirectoryLayer;

	virtual Reference<DirectoryLayer> getDirectoryLayerForPath(Path const& path) const {
		return path.empty() ? parentDirectoryLayer : directoryLayer;
	}
};
} // namespace FDB

#endif