/*
 * Subspace.h
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

#ifndef FDB_FLOW_SUBSPACE_H
#define FDB_FLOW_SUBSPACE_H

#pragma once

#include "flow/flow.h"
#include "bindings/flow/fdb_flow.h"
#include "Tuple.h"

namespace FDB {
class Subspace {
public:
	Subspace(Tuple const& tuple = Tuple(), StringRef const& rawPrefix = StringRef());
	Subspace(StringRef const& rawPrefix);

	virtual ~Subspace();

	virtual Key key() const;
	virtual bool contains(KeyRef const& key) const;

	virtual Key pack(Tuple const& tuple = Tuple()) const;
	virtual Tuple unpack(KeyRef const& key) const;
	virtual KeyRange range(Tuple const& tuple = Tuple()) const;

	template <class T>
	Key pack(T const& item) const {
		Tuple t;
		t.append(item);
		return pack(t);
	}

	Key packNested(Tuple const& item) const {
		Tuple t;
		t.appendNested(item);
		return pack(t);
	}

	Key pack(StringRef const& item, bool utf8 = false) const {
		Tuple t;
		t.append(item, utf8);
		return pack(t);
	}

	virtual Subspace subspace(Tuple const& tuple) const;
	virtual Subspace get(Tuple const& tuple) const;

	template <class T>
	Subspace get(T const& item) const {
		Tuple t;
		t.append(item);
		return get(t);
	}

	Subspace getNested(Tuple const& item) const {
		Tuple t;
		t.appendNested(item);
		return get(t);
	}

	Subspace get(StringRef const& item, bool utf8 = false) const {
		Tuple t;
		t.append(item, utf8);
		return get(t);
	}

private:
	Subspace(Tuple const& tuple, Standalone<VectorRef<uint8_t>> const& rawPrefix);
	Standalone<VectorRef<uint8_t>> rawPrefix;
};
} // namespace FDB

#endif