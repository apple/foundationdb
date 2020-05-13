/*
 * MutationList.h
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

#ifndef FLOW_FDBCLIENT_MUTATIONLIST_H
#define FLOW_FDBCLIENT_MUTATIONLIST_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"

struct MutationListRef : public VectorRef<StringRef> {
	size_t totalBytes;

	MutationListRef() : totalBytes(0) {}

	void push_back_deep(Arena& arena, const MutationRef& mutation) {
		BinaryWriter wr(Unversioned());
		wr << mutation.type << mutation.param1.size() << mutation.param2.size() << mutation.param1 << mutation.param2;
		const auto& val = wr.toValue();
		totalBytes += val.size();
		VectorRef<StringRef>::push_back_deep(arena, val);
	}

	size_t totalSize() const { return totalBytes; }
};

#endif
