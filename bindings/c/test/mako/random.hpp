/*
 * random.hpp
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

#ifndef MAKO_RANDOM_HPP
#define MAKO_RANDOM_HPP

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <unistd.h>

namespace mako {
namespace detail {

class RandomStringGenerator {
	std::mt19937 engine;
	pid_t processId;

	void reseed(std::uint32_t seed, pid_t newProcessId) {
		std::seed_seq seeds{ seed, static_cast<std::uint32_t>(newProcessId) };
		engine.seed(seeds);
		processId = newProcessId;
	}

public:
	RandomStringGenerator(std::uint32_t seed, pid_t processId) : processId(processId) { reseed(seed, processId); }

	void reseedAfterFork(pid_t currentProcessId) {
		if (currentProcessId != processId) {
			// The child inherits both TLS and libc's random state. Mix in its PID to separate the streams.
			reseed(static_cast<std::uint32_t>(std::rand()), currentProcessId);
		}
	}

	template <typename Char>
	void fill(Char* str, int len) {
		assert(len >= 0);
		std::uniform_int_distribution<int> printable('!', 'z');
		for (int i = 0; i < len; ++i) {
			str[i] = static_cast<Char>(printable(engine));
		}
	}
};

inline RandomStringGenerator& randomStringGenerator(pid_t currentProcessId) {
	// Retain Mako's post-fork srand seed policy, but avoid shared random state for every payload byte.
	thread_local RandomStringGenerator generator(static_cast<std::uint32_t>(std::rand()), currentProcessId);
	generator.reseedAfterFork(currentProcessId);
	return generator;
}

} // namespace detail

template <typename Char>
inline void randomString(Char* str, int len) {
	assert(len >= 0);
	if (len == 0) {
		return;
	}
	detail::randomStringGenerator(getpid()).fill(str, len);
}

} // namespace mako

#endif
