/*
 * BenchSelectReplicas.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "flow/Arena.h"
#include "benchmark/benchmark.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include <cstdint>

static void bench_select_replicas(int repCount, benchmark::State& state) {

	Reference<IReplicationPolicy> policy = Reference<IReplicationPolicy>(
	    new PolicyAcross(repCount, "rack", Reference<IReplicationPolicy>(new PolicyOne())));

	// Pre-warm the depth cache to avoid measuring lazy initialization overhead
	policy->depth();
	policy->maxdepth();

	std::vector<std::string> indexes;
	int dcTotal = 1;
	int szTotal = 1;
	int rackTotal = 1; // createTestLocalityMap will create two additional racks.
	int slotTotal = 3;
	int independentItems = 0;
	int independentTotal = 0;
	std::vector<LocalityEntry> results;

	Reference<LocalitySet> fromServersSet =
	    createTestLocalityMap(indexes, dcTotal, szTotal, rackTotal, slotTotal, independentItems, independentTotal);
	LocalityGroup* fromServersGroup = (LocalityGroup*)fromServersSet.getPtr();

	const Reference<LocalitySet> alreadyServersSet = Reference<LocalitySet>(new LocalityGroup());

	alreadyServersSet->deep_copy(*fromServersGroup);
	std::vector<LocalityEntry> localityGroupEntries;
	int serverCount = state.range(0);

	// fromServersSet->DisplayEntries();

	for (int i = 0; i < serverCount; i++) {
		localityGroupEntries.push_back(fromServersGroup->getEntry(i));
	}

	for (auto _ : state) {
		state.PauseTiming();
		results.clear();
		state.ResumeTiming();
		policy->selectReplicas(fromServersSet, localityGroupEntries, results);
		state.SetItemsProcessed(static_cast<long>(state.iterations()));
	}
	state.counters.insert({ { "dcTotal", dcTotal }, { "szTotal", szTotal } });
}

static void bench_select_replicas_tripple(benchmark::State& state) {
	return bench_select_replicas(3, state);
}

static void bench_select_replicas_double(benchmark::State& state) {
	return bench_select_replicas(2, state);
}

BENCHMARK(bench_select_replicas_tripple)->Args({ 4 })->Args({ 8 })->ReportAggregatesOnly(true);
BENCHMARK(bench_select_replicas_double)->Args({ 2 })->Args({ 8 })->ReportAggregatesOnly(true);