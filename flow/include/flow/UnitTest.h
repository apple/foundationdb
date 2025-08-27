/*
 * UnitTest.h
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

#ifndef FLOW_UNITTEST_H
#define FLOW_UNITTEST_H
#pragma once

/*
 * Flow unit testing framework
 *
 * This is a simple framework for writing optionally asynchronous,
 * optionally randomized unit tests.
 *
 * This framework is not trivial to use correctly. For example, your
 * unit tests will affect the global execution environment of a
 * fdbserver process. If things done in your unit test are not in
 * accordance with global expectations that are only enabled in
 * simulation, then you may break simulation even though your unit
 * test itself runs fine via fdbserver -r unittests.  As a result, to test
 * that your unit tests themselves do not break simulation, you should
 * also run a 100k simulation run.  If you think this sounds
 * backwards, you may be right.
 *
 * Usage:
 *
 * TEST_CASE("/product/module/testcase") {
 *   double random_test_parameter = deterministicRandom()->random01();
 *   ASSERT( something );
 *   return Void();
 * }
 *
 * In an `.actor.cpp` file, the body of a TEST_CASE is an actor (may contain `wait`, `state`, etc)
 * In a `.cpp` file, the body of a TEST_CASE is an ordinary function returning a Future<Void>
 *
 * Our tools for actually executing tests are external to flow (and use g_unittests to find test cases).
 * See the `UnitTestWorkload` class.
 */

#include "flow/flow.h"

#include <cinttypes>

class UnitTestParameters {
	Optional<std::string> dataDir;

public:
	// Map of named case-sensitive parameters
	std::map<std::string, std::string> params;

	// Set a named parameter to a string value, replacing any existing value
	void set(const std::string& name, const std::string& value);

	// Set a named parameter to an integer converted to a string value, replacing any existing value
	void set(const std::string& name, int64_t value);

	// Set a named parameter to a double converted to a string value, replacing any existing value
	void set(const std::string& name, double value);

	// Get a parameter's value, will return !present() if parameter was not set
	Optional<std::string> get(const std::string& name) const;

	// Get a parameter's value as an integer, will return !present() if parameter was not set
	Optional<int64_t> getInt(const std::string& name) const;

	// Get a parameter's value parsed as a double, will return !present() if parameter was not set
	Optional<double> getDouble(const std::string& name) const;

	// This is separate because it assumes data directory has already been set, and doesn't return an optional
	std::string getDataDir() const;

	// Set the data directory used to persist data for this test
	void setDataDir(std::string const&);
};

// Unit test definition structured as a linked list item
struct UnitTest {
	typedef Future<Void> (*TestFunction)(const UnitTestParameters& params);

	const char* name;
	const char* file;
	int line;
	TestFunction func;
	UnitTest* next;

	UnitTest(const char* name, const char* file, int line, TestFunction func);
};

// Collection of unit tests in the form of a linked list
struct UnitTestCollection {
	UnitTest* tests;
};

extern UnitTestCollection g_unittests;

// Set this to `true` to disable RNG state checking after simulation runs.
extern bool noUnseed;

#define APPEND(a, b) a##b

// FILE_UNIQUE_NAME(basename) expands to a name like basename456 if on line 456
#define FILE_UNIQUE_NAME1(name, line) APPEND(name, line)
#define FILE_UNIQUE_NAME(name) FILE_UNIQUE_NAME1(name, __LINE__)

#ifdef FLOW_DISABLE_UNIT_TESTS

#define TEST_CASE(name) static Future<Void> FILE_UNIQUE_NAME(disabled_testcase_func)(const UnitTestParameters& params)
#define ACTOR_TEST_CASE(actorname, name)

#else

#define TEST_CASE(name)                                                                                                \
	static Future<Void> FILE_UNIQUE_NAME(testcase_func)(const UnitTestParameters& params);                             \
	namespace {                                                                                                        \
	static UnitTest FILE_UNIQUE_NAME(testcase)(name, __FILE__, __LINE__, &FILE_UNIQUE_NAME(testcase_func));            \
	}                                                                                                                  \
	static Future<Void> FILE_UNIQUE_NAME(testcase_func)(const UnitTestParameters& params)

// ACTOR_TEST_CASE generated by actorcompiler; don't use directly
#define ACTOR_TEST_CASE(actorname, name)                                                                               \
	namespace {                                                                                                        \
	UnitTest APPEND(testcase_, actorname)(name, __FILE__, __LINE__, &actorname);                                       \
	}

#endif

#endif
