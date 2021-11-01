/*
 * Utils.h
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

#ifndef FDBSERVER_PTXN_TEST_UTILS_H
#define FDBSERVER_PTXN_TEST_UTILS_H

#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "fdbserver/ptxn/test/CommitUtils.h"
#include "fdbserver/ptxn/test/TestTLogPeek.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/WorkerInterface.actor.h"

namespace ptxn::test {

// Shortcut for deterministicRanodm()->randomUniqueID();
UID randomUID();

// Constructs a random StorageTeamID
StorageTeamID getNewStorageTeamID();

// Construct numStorageTeams TeamIDs
std::vector<StorageTeamID> generateRandomStorageTeamIDs(const int numStorageTeams);

// Pick one element from a container, randomly
// The container should support the concept of
//  template <typename T>
//  concept Container = requires(T t) {
//      { container[0] } -> T;
//      { container.size() } -> size_t;
//      { Container::const_reference } -> const T&;
//  };
template <typename Container>
typename Container::const_reference randomlyPick(const Container& container) {
	if (container.size() == 0) {
		throw std::range_error("empty container");
	}
	return container[deterministicRandom()->randomInt(0, container.size())];
};

// Pick multiple elements from a container, randmly
// The container should support the concept that randomlyPick supports
// The returning container should support the concept of
//  tempalte <typename T>
//  concept ResultContainer = requires(T t) {
//	    { container.resize(size_t) } -> void;
// .    { container[0] } -> T&;
//  };
template <typename ResultContainer, typename Container>
ResultContainer randomlyPick(const Container& container, const int numSamples) {
	const int containerSize = container.size();
	ASSERT(containerSize >= numSamples);

	ResultContainer result;
	result.resize(numSamples);

	// Reservoir sampling
	for (int i = 0; i < numSamples; ++i) {
		result[i] = container[i];
	}

	for (int i = numSamples; i < containerSize; ++i) {
		int j = deterministicRandom()->randomInt(1, i + 1);
		if (j < numSamples) {
			result[j] = container[i];
		}
	}

	return result;
}

// Get a random alphabetical/numeric string with length between lower and upper
std::string getRandomAlnum(int lower, int upper);

namespace print {

void print(const TLogCommitRequest&);
void print(const TLogCommitReply&);
void print(const TLogGroup&);
void print(const TLogPeekRequest&);
void print(const TLogPeekReply&);
void print(const TestDriverOptions&);
void print(const CommitRecord&);
void print(const ptxn::test::TestTLogPeekOptions&);

void printCommitRecords(const CommitRecord&);

// Prints timing per step
class PrintTiming {

	using time_point_t = std::chrono::time_point<std::chrono::high_resolution_clock>;
	using clock_t = std::chrono::high_resolution_clock;
	using duration_t = std::chrono::duration<double>;

	std::string functionName;
	time_point_t startTime;
	time_point_t lastTagTime;

	class DummyOStream {};

public:
	// Helper class print out time and duration
	PrintTiming(const std::string&);
	~PrintTiming();

	template <typename T>
	friend DummyOStream operator<<(PrintTiming&, const T& object);

	// Support iomanips like std::endl
	// Here we *MUST* use pointers instead of std::function, see:
	//   * http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0551r3.pdf
	friend DummyOStream operator<<(PrintTiming&, std::ostream& (*)(std::ostream&));

	friend DummyOStream operator<<(PrintTiming&, std::ios_base& (*)(std::ios_base&));

	template <typename T>
	friend DummyOStream&& operator<<(DummyOStream&&, const T& object);

	friend DummyOStream&& operator<<(DummyOStream&&, std::ostream& (*)(std::ostream&));

	friend DummyOStream&& operator<<(DummyOStream&&, std::ios_base& (*)(std::ios_base&));
};

template <typename T>
PrintTiming::DummyOStream operator<<(PrintTiming& printTiming, const T& object) {
	auto now = PrintTiming::clock_t::now();
	std::cout << std::setw(40) << printTiming.functionName << ">> "
	          << "[" << std::setw(12) << std::fixed << std::setprecision(6)
	          << PrintTiming::duration_t(now - printTiming.startTime).count() << "] ";
	std::cout << object;
	printTiming.lastTagTime = now;

	return PrintTiming::DummyOStream();
}

template <typename T>
PrintTiming::DummyOStream&& operator<<(PrintTiming::DummyOStream&& stream, const T& object) {
	std::cout << object;
	return std::move(stream);
}

} // namespace print

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_UTILS_H