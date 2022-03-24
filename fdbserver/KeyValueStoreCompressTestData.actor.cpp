/*
 * KeyValueStoreCompressTestData.actor.cpp
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

#include "fdbserver/IKeyValueStore.h"
#include "flow/actorcompiler.h" // has to be last include

// KeyValueStoreCompressTestData wraps an existing IKeyValueStore and
// implements the following rudimentary compression scheme:
//   An arbitrarily long value which consists entirely of a single repeated nonzero byte is mapped to
//     a 5-byte value consisting of that byte followed by a little-endian integer giving the number
//     of repetitions.
//   All other values are mapped to a zero byte followed by the value.
// This store is used in testing to let us simulate having much bigger disks than we actually
//   have, in order to test really big databases.

struct KeyValueStoreCompressTestData final : IKeyValueStore {
	IKeyValueStore* store;

	KeyValueStoreCompressTestData(IKeyValueStore* store) : store(store) {}

	Future<Void> getError() const override { return store->getError(); }
	Future<Void> onClosed() const override { return store->onClosed(); }
	void dispose() override {

		store->dispose();
		delete this;
	}
	void close() override {
		store->close();
		delete this;
	}

	KeyValueStoreType getType() const override { return store->getType(); }
	StorageBytes getStorageBytes() const override { return store->getStorageBytes(); }

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		store->set(KeyValueRef(keyValue.key, pack(keyValue.value)), arena);
	}
	void clear(KeyRangeRef range, const Arena* arena = nullptr) override { store->clear(range, arena); }
	Future<Void> commit(bool sequential = false) override { return store->commit(sequential); }

	Future<Optional<Value>> readValue(KeyRef key, IKeyValueStore::ReadType, Optional<UID> debugID) override {
		return doReadValue(store, key, debugID);
	}

	// Note that readValuePrefix doesn't do anything in this implementation of IKeyValueStore, so the "atomic bomb"
	// problem is still present if you are using this storage interface, but this storage interface is not used by
	// customers ever. However, if you want to try to test malicious atomic op workloads with compressed values for some
	// reason, you will need to fix this.
	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        IKeyValueStore::ReadType,
	                                        Optional<UID> debugID) override {
		return doReadValuePrefix(store, key, maxLength, debugID);
	}

	// If rowLimit>=0, reads first rows sorted ascending, otherwise reads last rows sorted descending
	// The total size of the returned value (less the last entry) will be less than byteLimit
	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit, int byteLimit, IKeyValueStore::ReadType) override {
		return doReadRange(store, keys, rowLimit, byteLimit);
	}

private:
	ACTOR static Future<Optional<Value>> doReadValue(IKeyValueStore* store, Key key, Optional<UID> debugID) {
		Optional<Value> v = wait(store->readValue(key, IKeyValueStore::ReadType::NORMAL, debugID));
		if (!v.present())
			return v;
		return unpack(v.get());
	}

	ACTOR static Future<Optional<Value>> doReadValuePrefix(IKeyValueStore* store,
	                                                       Key key,
	                                                       int maxLength,
	                                                       Optional<UID> debugID) {
		Optional<Value> v = wait(doReadValue(store, key, debugID));
		if (!v.present())
			return v;
		if (maxLength < v.get().size()) {
			return v.get().substr(0, maxLength);
		} else {
			return v;
		}
	}
	ACTOR Future<RangeResult> doReadRange(IKeyValueStore* store, KeyRangeRef keys, int rowLimit, int byteLimit) {
		RangeResult _vs = wait(store->readRange(keys, rowLimit, byteLimit));
		RangeResult vs = _vs; // Get rid of implicit const& from wait statement
		Arena& a = vs.arena();
		for (int i = 0; i < vs.size(); i++)
			vs[i].value = ValueRef(a, (ValueRef const&)unpack(vs[i].value));
		return vs;
	}

	// These implement the actual "compression" scheme
	static Value pack(Value val) {
		if (!val.size())
			return val;
		uint8_t c = val[0];

		// If the value starts with a 0-byte, then we don't compress it
		if (c == 0)
			return val.withPrefix(LiteralStringRef("\x00"));

		for (int i = 1; i < val.size(); i++) {
			if (val[i] != c) {
				// The value is something other than a single repeated character, so not compressible :-)
				return val.withPrefix(LiteralStringRef("\x00"));
			}
		}

		int n = val.size();
		val = makeString(5);
		uint8_t* p = mutateString(val);
		p[0] = c;
		*(int*)(p + 1) = n;
		return val;
	}
	static Value unpack(Value val) {
		if (!val.size())
			return val;
		if (val[0] == 0)
			return val.substr(1); // Uncompressed value
		ASSERT(val.size() == 5);
		uint8_t c = val[0];
		int n = *(int*)(val.begin() + 1);
		val = makeString(n);
		uint8_t* p = mutateString(val);
		memset(p, c, n);
		return val;
	}
};

IKeyValueStore* keyValueStoreCompressTestData(IKeyValueStore* store) {
	return new KeyValueStoreCompressTestData(store);
}
