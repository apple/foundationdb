/*
 * serialize.h
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

#include "flow/serialize.h"

/**
 * This file provides the implementation for the serialization code (or rather
 * for the template instantiation). DO NOT include this file anywhere. This file is
 * included into generated files.
 *
 * If you get linker errors due to new types that don't get generated, add those types
 * into the proper section in cmake/templates.json
 */

namespace {
struct PacketWriterAllocator {
	PacketWriter& w;

	uint8_t* operator()(size_t size) { return w.writeBytes(size); }
};
} // namespace

template <class Ar, class T>
void SerializedMsg<Ar, T>::serialize(Ar& ar, T const& value) {
	if constexpr (Ar::isSerializing) {
		ar << value;
	} else {
		UNSTOPPABLE_ASSERT(false);
	}
}

template <class Ar, class T>
void SerializedMsg<Ar, T>::deserialize(Ar& ar, T value) {
	if constexpr (!Ar::isSerializing) {
		ar >> value;
	} else {
		UNSTOPPABLE_ASSERT(false);
	}
}

template <class T>
void ObjectSerializedMsg<T>::serialize(PacketWriter& w, T const& value) {
	ObjectWriter writer(PacketWriterAllocator{ w }, AssumeVersion(w.protocolVersion()));
	writer.serialize(value);
}

template <class T>
void ObjectSerializedMsg<T>::serialize(ObjectWriter& w, T const& value) {
	w.serialize(value);
}

template <class T>
void ObjectSerializedMsg<T>::deserialize(ArenaObjectReader& reader, T& value) {
	reader.deserialize(value);
}
