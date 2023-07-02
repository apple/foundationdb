/*
 * StorageServerUtils.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/StorageServerUtils.h"

#define PERSIST_PREFIX "\xff\xff"

namespace {
const KeyRangeRef persistMoveInShardKeys =
    KeyRangeRef(PERSIST_PREFIX "MoveInShards/"_sr, PERSIST_PREFIX "MoveInShards0"_sr);
const KeyRef persistMoveInUpdatesPrefix = PERSIST_PREFIX "MoveInShardUpdates/"_sr;
} // namespace

KeyRange persistMoveInShardsKeyRange() {
	return persistMoveInShardKeys;
}

KeyRange persistUpdatesKeyRange(const UID& id) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(persistMoveInUpdatesPrefix);
	wr << id;
	wr.serializeBytes("/"_sr);
	return prefixRange(wr.toValue());
}

Key persistMoveInShardKey(const UID& id) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(persistMoveInShardKeys.begin);
	wr << id;
	return wr.toValue();
}

UID decodeMoveInShardKey(const KeyRef& key) {
	UID id;
	BinaryReader rd(key.removePrefix(persistMoveInShardKeys.begin), Unversioned());
	rd >> id;
	return id;
}

Value moveInShardValue(const MoveInShardMetaData& meta) {
	return ObjectWriter::toValue(meta, IncludeVersion());
}

MoveInShardMetaData decodeMoveInShardValue(const ValueRef& value) {
	MoveInShardMetaData shard;
	ObjectReader reader(value.begin(), IncludeVersion());
	reader.deserialize(shard);
	return shard;
}