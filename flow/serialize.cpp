/*
 * serialize.cpp
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

#include "flow/network.h"
#include "flow/serialize.h"
#include "flow/UnitTest.h"

_AssumeVersion::_AssumeVersion(ProtocolVersion version) : v(version) {
	if (!version.isValid()) {
		ASSERT(!g_network->isSimulated());
		throw serialization_failed();
	}
}

const void* BinaryReader::readBytes(int bytes) {
	const char* b = begin;
	const char* e = b + bytes;
	if (e > end) {
		ASSERT(!g_network->isSimulated());
		throw serialization_failed();
	}
	begin = e;
	return b;
}

namespace {

struct _Struct {
	static constexpr FileIdentifier file_identifier = 2340487;
	int oldField{ 0 };
};

struct OldStruct : public _Struct {
	void setFields() { oldField = 1; }
	bool isSet() const { return oldField == 1; }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, oldField);
	}
};

struct NewStruct : public _Struct {
	int newField{ 0 };

	bool isSet() const { return oldField == 1 && newField == 2; }
	void setFields() {
		oldField = 1;
		newField = 2;
	}

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, oldField, newField);
	}
};

void verifyData(StringRef value, int numObjects) {
	{
		// use BinaryReader
		BinaryReader reader(value, IncludeVersion());
		std::vector<OldStruct> data;
		reader >> data;
		ASSERT_EQ(data.size(), numObjects);
		for (const auto& object : data) {
			ASSERT(object.isSet());
		}
	}
	{
		// use ArenaReader
		ArenaReader reader(Arena(), value, IncludeVersion());
		std::vector<OldStruct> data;
		reader >> data;
		ASSERT_EQ(data.size(), numObjects);
		for (const auto& oldObject : data) {
			ASSERT(oldObject.isSet());
		}
	}
}

} // namespace

TEST_CASE("flow/serialize/Downgrade/WriteOld") {
	BinaryWriter writer(IncludeVersion(g_network->protocolVersion()));
	auto const numObjects = deterministicRandom()->randomInt(1, 101);
	std::vector<OldStruct> data(numObjects);
	for (auto& oldObject : data) {
		oldObject.setFields();
	}
	writer << data;
	verifyData(writer.toValue(), numObjects);
	return Void();
}

// Verify that old code will still be able to read the values of the struct it knows about, even if we add a new field
// and write a message with new code.
TEST_CASE("flow/serialize/Downgrade/WriteNew") {
	auto protocolVersion = g_network->protocolVersion();
	protocolVersion.addObjectSerializerFlag();
	ObjectWriter writer(IncludeVersion(protocolVersion));
	auto const numObjects = deterministicRandom()->randomInt(1, 101);
	std::vector<NewStruct> data(numObjects);
	for (auto& newObject : data) {
		newObject.setFields();
	}
	writer.serialize(data);
	verifyData(writer.toStringRef(), numObjects);
	return Void();
}
