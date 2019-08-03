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
#include "flow/ObjectSerializer.h"

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

template <class T, class V>
void MakeSerializeSource<T, V>::serializePacketWriter(PacketWriter& w, bool useObjectSerializer) const {
	if (useObjectSerializer) {
		PacketWriterAllocator alloc{ w };
		ObjectWriter w(alloc, AssumeVersion(w.protocolVersion()));
		w.serialize(get());
	} else {
		static_cast<T const*>(this)->serialize(w);
	}
}

template <class T, class V>
void MakeSerializeSource<T, V>::serializeBinaryWriter(BinaryWriter& w) const {
	static_cast<T const*>(this)->serialize(w);
}

template <class T>
void SerializeSource<T>::serializeObjectWriter(ObjectWriter& w) const {
	w.serialize(value);
}

template <class T>
template <class Ar>
void SerializeSource<T>::serialize(Ar& ar) const {
	ar << value;
}

template <class T>
template <class Ar>
void SerializeBoolAnd<T>::serialize(Ar& ar) const {
	ar << b << value;
}

template <class T, class VersionOptions>
T StringSerializer<T, VersionOptions>::deserialize(StringRef data, VersionOptions vo, bool useFlatBuffers) {
	T res;
	if (useFlatBuffers) {
		ObjectReader reader(data.begin(), vo);
		reader.deserialize(res);
		return res;
	} else {
		return BinaryReader::fromStringRef<T>(data, vo);
	}
}

template <class T, class VersionOptions>
Standalone<StringRef> StringSerializer<T, VersionOptions>::serialize(const T& value, VersionOptions vo,
                                                                     bool useFlatBuffers) {
	if (useFlatBuffers) {
		return ObjectWriter::toValue(value, vo);
	} else {
		return BinaryWriter::toValue(value, vo);
	}
}
