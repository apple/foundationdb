/*
 * GetKeyValuesReply.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "benchmark/benchmark.h"

#include "fdbclient/StorageServerInterface.h"
#include "flow/IRandom.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/ObjectSerializer.h"
#include "flow/serialize.h"

static void bench_kv_reply_deser(benchmark::State& benchState) {
	GetKeyValuesReply rep;
	const size_t keySize = benchState.range(0);
	const size_t valSize = benchState.range(1);
	const size_t keys = 80000 / (keySize + valSize);
	for (size_t i = 0; i < keys; ++i) {
		BinaryWriter keyWriter(Unversioned());
		keyWriter << i;
		keyWriter.serializeBytes(deterministicRandom()->randomAlphaNumeric(keySize - sizeof(i)).c_str(),
		                         keySize - sizeof(i));
		rep.data.push_back_deep(
		    rep.arena, KeyValueRef(keyWriter.toValue(), StringRef(deterministicRandom()->randomAlphaNumeric(valSize))));
	}
	auto serialized = ObjectWriter::toValue(rep, Unversioned());

	while (benchState.KeepRunning()) {
		ArenaObjectReader reader(serialized.arena(), serialized, Unversioned());
		GetKeyValuesReply rep2;
		reader.deserialize(rep2);
		benchmark::DoNotOptimize(rep2);
	}
	benchState.SetBytesProcessed(benchState.iterations() * keys * (keySize + valSize));
}

BENCHMARK(bench_kv_reply_deser)->Ranges({ { 8, 128 }, { 0, 1024 } })->ReportAggregatesOnly(true);