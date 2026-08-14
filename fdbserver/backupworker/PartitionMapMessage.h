/*
 * PartitionMapMessage.h
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

#ifndef FDBSERVER_PARTITIONMAPMESSAGE_H
#define FDBSERVER_PARTITIONMAPMESSAGE_H
#pragma once

#include "fdbclient/CommitTransaction.h"
#include "fdbserver/core/BackupPartitionMap.h"

struct PartitionMapMessage {
	// Pushed into the transaction logs by the commit proxy to inform backup
	// workers of the per-tag key-range partitioning.
	// Identified by a reserved leading byte so it can be discriminated from
	// mutations on the same stream.

	PartitionMap partitionMap;

	PartitionMapMessage() = default;
	explicit PartitionMapMessage(PartitionMap pm) : partitionMap(std::move(pm)) {}

	std::string toString() const {
		return format("code: %d, num tags: %zu", MutationRef::Reserved_For_PartitionMapMessage, partitionMap.size());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		uint8_t poly = MutationRef::Reserved_For_PartitionMapMessage;
		serializer(ar, poly, partitionMap);
	}

	static bool startsPartitionMapMessage(uint8_t byte) {
		return byte == MutationRef::Reserved_For_PartitionMapMessage;
	}
	template <class Ar>
	static bool isNextIn(Ar& ar) {
		return startsPartitionMapMessage(*(const uint8_t*)ar.peekBytes(1));
	}
};

#endif
