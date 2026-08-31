/*
 * mako_random_tests.cpp
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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include "../mako/random.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <mutex>
#include <poll.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <vector>

namespace {

using mako::detail::RandomStringGenerator;

template <typename Char>
void checkBufferBounds() {
	for (int len : { 0, 1, 6000 }) {
		std::vector<Char> buffer(len + 2, static_cast<Char>(0x7f));
		mako::randomString(buffer.data() + 1, len);
		CHECK(buffer.front() == static_cast<Char>(0x7f));
		CHECK(buffer.back() == static_cast<Char>(0x7f));
		CHECK(std::all_of(buffer.begin() + 1, buffer.end() - 1, [](Char value) {
			return value >= static_cast<Char>('!') && value <= static_cast<Char>('z');
		}));
	}
}

class ChildProcess {
	pid_t pid;

public:
	explicit ChildProcess(pid_t pid) : pid(pid) {}
	ChildProcess(const ChildProcess&) = delete;
	ChildProcess& operator=(const ChildProcess&) = delete;

	~ChildProcess() {
		if (pid > 0) {
			kill(pid, SIGKILL);
			while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {
			}
		}
	}

	bool waitForExit(int& status) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
		do {
			const auto result = waitpid(pid, &status, WNOHANG);
			if (result == pid) {
				pid = -1;
				return true;
			}
			if (result < 0 && errno != EINTR) {
				if (errno == ECHILD) {
					pid = -1;
				}
				return false;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		} while (std::chrono::steady_clock::now() < deadline);
		return false;
	}
};

} // namespace

TEST_CASE("random strings preserve char and byte buffer boundaries") {
	checkBufferBounds<char>();
	checkBufferBounds<std::uint8_t>();
	mako::randomString(static_cast<char*>(nullptr), 0);
}

TEST_CASE("random string generators are repeatable and advance across calls") {
	RandomStringGenerator oneCall(12345, 6789);
	RandomStringGenerator splitCalls(12345, 6789);
	std::array<char, 6000> whole;
	std::array<char, 6000> split;
	oneCall.fill(whole.data(), whole.size());
	splitCalls.fill(split.data(), 3000);
	splitCalls.fill(split.data() + 3000, 3000);
	CHECK(whole == split);

	std::array<char, 6000> next;
	oneCall.fill(next.data(), next.size());
	CHECK(next != whole);
	splitCalls.fill(split.data(), split.size());
	CHECK(next == split);
}

TEST_CASE("random string seeds include process identity") {
	RandomStringGenerator parent(12345, 6789);
	RandomStringGenerator child(12345, 6790);
	std::array<char, 1000> parentOutput;
	std::array<char, 1000> childOutput;
	parent.fill(parentOutput.data(), parentOutput.size());
	child.fill(childOutput.data(), childOutput.size());
	CHECK(parentOutput != childOutput);
}

TEST_CASE("random strings use the complete printable alphabet") {
	RandomStringGenerator generator(2468, 1357);
	std::array<char, 9000> output;
	std::array<int, 'z' - '!' + 1> counts{};
	generator.fill(output.data(), output.size());
	for (char value : output) {
		REQUIRE(value >= '!');
		REQUIRE(value <= 'z');
		++counts[value - '!'];
	}
	// A broad deterministic sanity check, not a statistical quality test or a timing assertion.
	for (int count : counts) {
		CHECK(count > 0);
		CHECK(count < static_cast<int>(output.size() / 4));
	}
}

TEST_CASE("random string worker state is independent") {
	constexpr int workers = 8;
	std::array<std::string, workers> output;
	std::array<std::uintptr_t, workers> instances{};
	std::vector<std::thread> threads;
	std::mutex mutex;
	std::condition_variable condition;
	int ready = 0;
	bool released = false;
	for (int i = 0; i < workers; ++i) {
		threads.emplace_back([&, i] {
			RandomStringGenerator generator(1000 + i, 2000);
			output[i].resize(6000);
			generator.fill(output[i].data(), output[i].size());
			instances[i] = reinterpret_cast<std::uintptr_t>(&mako::detail::randomStringGenerator(getpid()));
			std::unique_lock lock(mutex);
			++ready;
			condition.notify_all();
			condition.wait(lock, [&] { return released; });
		});
	}
	bool allReady;
	{
		std::unique_lock lock(mutex);
		allReady = condition.wait_for(lock, std::chrono::seconds(5), [&] { return ready == workers; });
		released = true;
	}
	condition.notify_all();
	for (auto& thread : threads) {
		thread.join();
	}
	REQUIRE(allReady);
	for (int i = 0; i < workers; ++i) {
		RandomStringGenerator reference(1000 + i, 2000);
		std::string expected(6000, '\0');
		reference.fill(expected.data(), expected.size());
		CHECK(output[i] == expected);
		for (int j = 0; j < i; ++j) {
			CHECK(instances[i] != instances[j]);
		}
	}
}

TEST_CASE("random string TLS is reseeded in a forked child") {
	std::array<char, 1000> parentOutput;
	mako::randomString(parentOutput.data(), parentOutput.size());
	int descriptors[2];
	REQUIRE(pipe(descriptors) == 0);
	const pid_t pid = fork();
	if (pid < 0) {
		close(descriptors[0]);
		close(descriptors[1]);
		FAIL("fork failed");
		return;
	}
	if (pid == 0) {
		close(descriptors[0]);
		alarm(3);
		std::srand(314159);
		RandomStringGenerator expected(static_cast<std::uint32_t>(std::rand()), getpid());
		std::srand(314159);
		std::array<char, 1000> expectedOutput;
		std::array<char, 1000> actualOutput;
		expected.fill(expectedOutput.data(), expectedOutput.size());
		mako::randomString(actualOutput.data(), actualOutput.size());
		const char result = actualOutput == expectedOutput ? 'Y' : 'N';
		const auto written = write(descriptors[1], &result, 1);
		close(descriptors[1]);
		_exit(result == 'Y' && written == 1 ? 0 : 1);
	}
	ChildProcess child(pid);
	close(descriptors[1]);
	pollfd descriptor{ descriptors[0], POLLIN, 0 };
	const int pollResult = poll(&descriptor, 1, 5000);
	char result = 'N';
	const auto bytesRead = pollResult > 0 ? read(descriptors[0], &result, 1) : 0;
	close(descriptors[0]);
	int status = 0;
	const bool exited = child.waitForExit(status);
	CHECK(pollResult > 0);
	CHECK(bytesRead == 1);
	CHECK(result == 'Y');
	REQUIRE(exited);
	REQUIRE(WIFEXITED(status));
	CHECK(WEXITSTATUS(status) == 0);
}
