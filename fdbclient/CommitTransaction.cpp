/*
 * CommitTransaction.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Tracing.h"
#include "flow/Knobs.h"
#include "flow/UnitTest.h"

TEST_CASE("noSim/CommitTransaction/MutationRef") {
	printf("testing MutationRef encoding/decoding\n");

	StringRef param1 = "Param1"_sr, param2 = "Param2"_sr;
	Standalone<VectorRef<MutationRef>> mutations;
	mutations.push_back_deep(mutations.arena(), MutationRef(MutationRef::SetValue, param1, param2));
	mutations.push_back_deep(mutations.arena(), MutationRef(MutationRef::ClearRange, param1, param2));
	mutations.push_back_deep(mutations.arena(), MutationRef(MutationRef::AddValue, param1, param2));
	// MutationRef m(MutationRef::SetValue, "TestKey"_sr, "TestValue"_sr);
	// MutationRef mc(MutationRef::ClearRange, "TestKey"_sr, "TestKeyEnd"_sr);
	// BinaryWriter wr(IncludeVersion(ProtocolVersion::withGcTxnGenerations()));
	for (const MutationRef& m : mutations) {
		BinaryWriter wr(AssumeVersion(ProtocolVersion::withMutationChecksum()));
		wr << m;
		Standalone<StringRef> value = wr.toValue();

		TraceEvent("RawMutationBytes").detail("Mutation", m).detail("Bytes", value);

		BinaryReader rd(value, AssumeVersion(ProtocolVersion::withMutationChecksum()));
		Standalone<MutationRef> de;

		rd >> de;

		TraceEvent("DecodeMutation").detail("Mutation", m).detail("DecodedMutation", de);
		// ASSERT()
	}

	BinaryWriter wr(AssumeVersion(ProtocolVersion::withMutationChecksum()));
	wr << mutations;
	Standalone<StringRef> value = wr.toValue();

	TraceEvent("RawMutationBytes").detail("Mutations", mutations).detail("Bytes", value);

	BinaryReader rd(value, AssumeVersion(ProtocolVersion::withMutationChecksum()));
	Standalone<VectorRef<MutationRef>> de;

	rd >> de;

	TraceEvent("DecodeMutation").detail("Mutations", mutations).detail("DecodedMutation", de);

	printf("testing data move ID encoding/decoding complete\n");

	return Void();
}