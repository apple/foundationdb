/*
 * Subspace.cpp
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

#include "Subspace.h"

namespace FDB {
Subspace::Subspace(Tuple const& tuple, StringRef const& rawPrefix) {
	StringRef packed = tuple.pack();

	this->rawPrefix.reserve(this->rawPrefix.arena(), rawPrefix.size() + packed.size());
	this->rawPrefix.append(this->rawPrefix.arena(), rawPrefix.begin(), rawPrefix.size());
	this->rawPrefix.append(this->rawPrefix.arena(), packed.begin(), packed.size());
}

Subspace::Subspace(Tuple const& tuple, Standalone<VectorRef<uint8_t>> const& rawPrefix) {
	this->rawPrefix.reserve(this->rawPrefix.arena(), rawPrefix.size() + tuple.pack().size());
	this->rawPrefix.append(this->rawPrefix.arena(), rawPrefix.begin(), rawPrefix.size());
	this->rawPrefix.append(this->rawPrefix.arena(), tuple.pack().begin(), tuple.pack().size());
}

Subspace::Subspace(StringRef const& rawPrefix) {
	this->rawPrefix.append(this->rawPrefix.arena(), rawPrefix.begin(), rawPrefix.size());
}

Subspace::~Subspace() {}

Key Subspace::key() const {
	return StringRef(rawPrefix.begin(), rawPrefix.size());
}

Key Subspace::pack(const Tuple& tuple) const {
	return tuple.pack().withPrefix(StringRef(rawPrefix.begin(), rawPrefix.size()));
}

Tuple Subspace::unpack(StringRef const& key) const {
	if (!contains(key)) {
		throw key_not_in_subspace();
	}
	return Tuple::unpack(key.substr(rawPrefix.size()));
}

KeyRange Subspace::range(Tuple const& tuple) const {
	VectorRef<uint8_t> begin;
	VectorRef<uint8_t> end;

	KeyRange keyRange;

	begin.reserve(keyRange.arena(), rawPrefix.size() + tuple.pack().size() + 1);
	begin.append(keyRange.arena(), rawPrefix.begin(), rawPrefix.size());
	begin.append(keyRange.arena(), tuple.pack().begin(), tuple.pack().size());
	begin.push_back(keyRange.arena(), uint8_t('\x00'));

	end.reserve(keyRange.arena(), rawPrefix.size() + tuple.pack().size() + 1);
	end.append(keyRange.arena(), rawPrefix.begin(), rawPrefix.size());
	end.append(keyRange.arena(), tuple.pack().begin(), tuple.pack().size());
	end.push_back(keyRange.arena(), uint8_t('\xff'));

	// FIXME: test that this uses the keyRange arena and doesn't create another one
	keyRange.KeyRangeRef::operator=(
	    KeyRangeRef(StringRef(begin.begin(), begin.size()), StringRef(end.begin(), end.size())));
	return keyRange;
}

bool Subspace::contains(KeyRef const& key) const {
	return key.startsWith(StringRef(rawPrefix.begin(), rawPrefix.size()));
}

Subspace Subspace::subspace(Tuple const& tuple) const {
	return Subspace(tuple, rawPrefix);
}

Subspace Subspace::get(Tuple const& tuple) const {
	return subspace(tuple);
}
} // namespace FDB