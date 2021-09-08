/*
 * BlobGranuleCommon.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_BLOBGRANULECOMMON_H
#define FDBCLIENT_BLOBGRANULECOMMON_H
#pragma once

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"

// TODO better place for this? It's used in change feeds too
struct MutationsAndVersionRef {
	VectorRef<MutationRef> mutations;
	Version version;

	MutationsAndVersionRef() {}
	explicit MutationsAndVersionRef(Version version) : version(version) {}
	MutationsAndVersionRef(VectorRef<MutationRef> mutations, Version version)
	  : mutations(mutations), version(version) {}
	MutationsAndVersionRef(Arena& to, VectorRef<MutationRef> mutations, Version version)
	  : mutations(to, mutations), version(version) {}
	MutationsAndVersionRef(Arena& to, const MutationsAndVersionRef& from)
	  : mutations(to, from.mutations), version(from.version) {}
	int expectedSize() const { return mutations.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutations, version);
	}
};

// TODO should GranuleSnapshot and GranuleDeltas just be typedefs instead of subclasses?
// file format of actual blob files
struct GranuleSnapshot : VectorRef<KeyValueRef> {

	constexpr static FileIdentifier file_identifier = 1300395;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<KeyValueRef>&)*this));
	}
};

struct GranuleDeltas : VectorRef<MutationsAndVersionRef> {
	constexpr static FileIdentifier file_identifier = 8563013;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<MutationsAndVersionRef>&)*this));
	}
};

// TODO better name?
struct BlobFilenameRef {
	constexpr static FileIdentifier file_identifier = 5253554;
	StringRef filename;
	int64_t offset;
	int64_t length;

	BlobFilenameRef() {}
	BlobFilenameRef(Arena& to, std::string filename, int64_t offset, int64_t length)
	  : filename(to, filename), offset(offset), length(length) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, filename, offset, length);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << filename.toString() << ":" << offset << ":" << length;
		return std::move(ss).str();
	}
};

// the assumption of this response is that the client will deserialize the files and apply the mutations themselves
// TODO could filter out delta files that don't intersect the key range being requested?
// TODO since client request passes version, we don't need to include the version of each mutation in the response if we
// pruned it there
struct BlobGranuleChunkRef {
	constexpr static FileIdentifier file_identifier = 991434;
	KeyRangeRef keyRange;
	Version includedVersion;
	Optional<BlobFilenameRef> snapshotFile; // not set if it's an incremental read
	VectorRef<BlobFilenameRef> deltaFiles;
	GranuleDeltas newDeltas;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, includedVersion, snapshotFile, deltaFiles, newDeltas);
	}
};

enum BlobGranuleSplitState { Unknown = 0, Started = 1, Assigned = 2, Done = 3 };
#endif