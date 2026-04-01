/*
 * RestoreCoreUtil.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_RESTORECOREUTIL_H
#define FDBSERVER_RESTORECOREUTIL_H

#pragma once

#include "fdbclient/CommitTransaction.h"
#include "flow/flow.h"

struct VersionedMutationSerialized {
	MutationRef mutation;
	LogMessageVersion version;

	VersionedMutationSerialized() = default;
	explicit VersionedMutationSerialized(MutationRef mutation, LogMessageVersion version)
	  : mutation(mutation), version(version) {}
	explicit VersionedMutationSerialized(Arena& arena, const VersionedMutationSerialized& vm)
	  : mutation(arena, vm.mutation), version(vm.version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutation, version);
	}
};

struct SampledMutation {
	KeyRef key;
	long size;

	explicit SampledMutation(KeyRef key, long size) : key(key), size(size) {}
	explicit SampledMutation(Arena& arena, const SampledMutation& sm) : key(arena, sm.key), size(sm.size) {}
	SampledMutation() = default;

	int totalSize() { return key.size() + sizeof(size); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, size);
	}
};

using MutationsVec = Standalone<VectorRef<MutationRef>>;
using LogMessageVersionVec = Standalone<VectorRef<LogMessageVersion>>;
using VersionedMutationsVec = Standalone<VectorRef<VersionedMutationSerialized>>;
using SampledMutationsVec = Standalone<VectorRef<SampledMutation>>;

bool debugFRMutation(const char* context, Version version, MutationRef const& mutation);
bool isRangeMutation(MutationRef m);

#endif
