/*
 * Downgrade.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/serialize.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DowngradeWorkload : TestWorkload {

	static constexpr const char* NAME = "Downgrade";
	Key oldKey, newKey;
	int numObjects;

	DowngradeWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		oldKey = getOption(options, LiteralStringRef("oldKey"), LiteralStringRef("oldKey"));
		newKey = getOption(options, LiteralStringRef("newKey"), LiteralStringRef("newKey"));
		numObjects = getOption(options, LiteralStringRef("numOptions"), deterministicRandom()->randomInt(0,100));
	}

	struct _Struct {
		static constexpr FileIdentifier file_identifier = 2340487;
		int oldField = 0;
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
		int newField = 0;

		bool isSet() const {
			return oldField == 1 && newField == 2;
		}
		void setFields() {
			oldField = 1;
			newField = 2;
		}

		template <class Archive>
		void serialize(Archive& ar) {
			serializer(ar, oldField, newField);
		}
	};

	ACTOR static Future<Void> writeOld(Database cx, int numObjects, Key key) {
		BinaryWriter writer(IncludeVersion(currentProtocolVersion));
		std::vector<OldStruct> data(numObjects);
		for (auto& oldObject : data) {
			oldObject.setFields();
		}
		writer << data;
		state Value value = writer.toValue();

		state Transaction tr(cx);
		loop {
			try {
				tr.set(key, value);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> writeNew(Database cx, int numObjects, Key key) {
		ProtocolVersion protocolVersion = currentProtocolVersion;
		protocolVersion.addObjectSerializerFlag();
		ObjectWriter writer(IncludeVersion(protocolVersion));
		std::vector<NewStruct> data(numObjects);
		for (auto& newObject : data) {
			newObject.setFields();
		}
		writer.serialize(data);
		state Value value = writer.toStringRef();

		state Transaction tr(cx);
		loop {
			try {
				tr.set(key, value);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> readData(Database cx, int numObjects, Key key) {
		state Transaction tr(cx);
		state Value value;

		loop {
			try {
				Optional<Value> _value = wait(tr.get(key));
				ASSERT(_value.present());
				value = _value.get();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		{
			// use BinaryReader
			BinaryReader reader(value, IncludeVersion());
			std::vector<OldStruct> data;
			reader >> data;
			ASSERT(data.size() == numObjects);
			for (const auto& oldObject : data) {
				ASSERT(oldObject.isSet());
			}
		}
		{
			// use ArenaReader
			ArenaReader reader(Arena(), value, IncludeVersion());
			std::vector<OldStruct> data;
			reader >> data;
			ASSERT(data.size() == numObjects);
			for (const auto& oldObject : data) {
				ASSERT(oldObject.isSet());
			}
		}
		return Void();
	}

	std::string description() override { return NAME; }

	Future<Void> setup(Database const& cx) override {
		return clientId ? Void() : (writeOld(cx, numObjects, oldKey) && writeNew(cx, numObjects, newKey));
	}

	Future<Void> start(Database const& cx) override {
		return clientId ? Void() : (readData(cx, numObjects, oldKey) && readData(cx, numObjects, newKey));
	}

	Future<bool> check(Database const& cx) override {
		// Failures are checked with assertions
		return true;
	}
	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<DowngradeWorkload> DowngradeWorkloadFactory(DowngradeWorkload::NAME);
