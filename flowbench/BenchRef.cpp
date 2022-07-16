/*
 * BenchRef.cpp
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

#include "benchmark/benchmark.h"

#include "flow/FastAlloc.h"
#include "flow/FastRef.h"

#include <memory>

struct Empty : public ReferenceCounted<Empty>, public FastAllocated<Empty> {};
struct EmptyTSRC : public ThreadSafeReferenceCounted<EmptyTSRC>, public FastAllocated<EmptyTSRC> {};

enum class RefType {
	RawPointer,
	UniquePointer,
	SharedPointer,
	FlowReference,
	FlowReferenceThreadSafe,
};

template <RefType refType>
class Factory {};

template <>
struct Factory<RefType::RawPointer> {
	static Empty* create() { return new Empty{}; }
	static void cleanup(Empty* empty) { delete empty; }
};

template <>
struct Factory<RefType::UniquePointer> {
	static std::unique_ptr<Empty> create() { return std::make_unique<Empty>(); }
	static void cleanup(const std::unique_ptr<Empty>&) {}
};

template <>
struct Factory<RefType::SharedPointer> {
	static std::shared_ptr<Empty> create() { return std::make_shared<Empty>(); }
	static void cleanup(const std::shared_ptr<Empty>&) {}
};

template <>
struct Factory<RefType::FlowReference> {
	static Reference<Empty> create() { return makeReference<Empty>(); }
	static void cleanup(const Reference<Empty>&) {}
};

template <>
struct Factory<RefType::FlowReferenceThreadSafe> {
	static Reference<EmptyTSRC> create() { return makeReference<EmptyTSRC>(); }
	static void cleanup(const Reference<EmptyTSRC>&) {}
};

template <RefType refType>
static void bench_ref_create_and_destroy(benchmark::State& state) {
	while (state.KeepRunning()) {
		auto ptr = Factory<refType>::create();
		benchmark::DoNotOptimize(ptr);
		Factory<refType>::cleanup(ptr);
	}
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

template <RefType refType>
static void bench_ref_copy(benchmark::State& state) {
	auto ptr = Factory<refType>::create();
	while (state.KeepRunning()) {
		auto ptr2 = ptr;
		benchmark::DoNotOptimize(ptr2);
	}
	Factory<refType>::cleanup(ptr);
	state.SetItemsProcessed(static_cast<long>(state.iterations()));
}

BENCHMARK_TEMPLATE(bench_ref_create_and_destroy, RefType::RawPointer)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_create_and_destroy, RefType::UniquePointer)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_create_and_destroy, RefType::SharedPointer)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_create_and_destroy, RefType::FlowReference)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_create_and_destroy, RefType::FlowReferenceThreadSafe)->ReportAggregatesOnly(true);

BENCHMARK_TEMPLATE(bench_ref_copy, RefType::RawPointer)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_copy, RefType::SharedPointer)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_copy, RefType::FlowReference)->ReportAggregatesOnly(true);
BENCHMARK_TEMPLATE(bench_ref_copy, RefType::FlowReferenceThreadSafe)->ReportAggregatesOnly(true);
