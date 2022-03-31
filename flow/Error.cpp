/*
 * Error.cpp
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

#include <iostream>

#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

bool g_crashOnError = false;

#define DEBUG_ERROR 0

#if DEBUG_ERROR
std::set<int> debugErrorSet = std::set<int>{ error_code_platform_error };
#define SHOULD_LOG_ERROR(x) (debugErrorSet.count(x) > 0)
#endif

#include <iostream>

Error Error::fromUnvalidatedCode(int code) {
	if (code < 0 || code > 30000) {
		Error e = Error::fromCode(error_code_unknown_error);
		TraceEvent(SevWarn, "ConvertedUnvalidatedErrorCode").error(e).detail("OriginalCode", code);
		return e;
	} else
		return Error::fromCode(code);
}

bool Error::isDiskError() const {
	return (error_code == error_code_io_error || error_code == error_code_io_timeout);
}

Error internal_error_impl(const char* file, int line) {
	fprintf(stderr, "Internal Error @ %s %d:\n  %s\n", file, line, platform::get_backtrace().c_str());

	TraceEvent(SevError, "InternalError")
	    .error(Error::fromCode(error_code_internal_error))
	    .detail("File", file)
	    .detail("Line", line)
	    .setErrorKind(ErrorKind::BugDetected)
	    .backtrace();
	flushTraceFileVoid();
	return Error(error_code_internal_error);
}

Error internal_error_impl(const char* msg, const char* file, int line) {
	fprintf(stderr, "Assertion %s failed @ %s %d:\n  %s\n", msg, file, line, platform::get_backtrace().c_str());

	TraceEvent(SevError, "InternalError")
	    .error(Error::fromCode(error_code_internal_error))
	    .detail("FailedAssertion", msg)
	    .detail("File", file)
	    .detail("Line", line)
	    .setErrorKind(ErrorKind::BugDetected)
	    .backtrace();
	flushTraceFileVoid();
	return Error(error_code_internal_error);
}

Error internal_error_impl(const char* a_nm,
                          long long a,
                          const char* op_nm,
                          const char* b_nm,
                          long long b,
                          const char* file,
                          int line) {
	fprintf(stderr, "Assertion failed @ %s %d:\n", file, line);
	fprintf(stderr, "  expression:\n");
	fprintf(stderr, "              %s %s %s\n", a_nm, op_nm, b_nm);
	fprintf(stderr, "  expands to:\n");
	fprintf(stderr, "              %lld %s %lld\n\n", a, op_nm, b);
	fprintf(stderr, "  %s\n", platform::get_backtrace().c_str());

	TraceEvent(SevError, "InternalError")
	    .error(Error::fromCode(error_code_internal_error))
	    .detailf("FailedAssertion", "%s %s %s", a_nm, op_nm, b_nm)
	    .detail("LeftValue", a)
	    .detail("RightValue", b)
	    .detail("File", file)
	    .detail("Line", line)
	    .setErrorKind(ErrorKind::BugDetected)
	    .backtrace();
	flushTraceFileVoid();
	return Error(error_code_internal_error);
}

Error::Error(int error_code) : error_code(error_code), flags(0) {
	if (TRACE_SAMPLE())
		TraceEvent(SevSample, "ErrorCreated").detail("ErrorCode", error_code);
	// std::cout << "Error: " << error_code << std::endl;
	if (error_code >= 3000 && error_code < 6000) {
		{
			TraceEvent te(SevError, "SystemError");
			te.error(*this).backtrace();
			if (error_code == error_code_unknown_error) {
				auto exception = std::current_exception();
				if (exception) {
					try {
						std::rethrow_exception(exception);
					} catch (std::exception& e) {
						te.detail("StdException", e.what());
					} catch (...) {
					}
				}
			}
		}
		if (g_crashOnError) {
			flushOutputStreams();
			flushTraceFileVoid();
			crashAndDie();
		}
	}

#if DEBUG_ERROR
	if (SHOULD_LOG_ERROR(error_code)) {
		TraceEvent te(SevWarn, "DebugError");
		te.error(*this).backtrace();
		if (error_code == error_code_unknown_error) {
			auto exception = std::current_exception();
			if (exception) {
				try {
					std::rethrow_exception(exception);
				} catch (std::exception& e) {
					te.detail("StdException", e.what());
				} catch (...) {
				}
			}
		}
	}
#endif
}

ErrorCodeTable& Error::errorCodeTable() {
	static ErrorCodeTable table;
	return table;
}

const char* Error::name() const {
	const auto& table = errorCodeTable();
	auto it = table.find(error_code);
	if (it == table.end())
		return "UNKNOWN_ERROR";
	return it->second.first;
}

const char* Error::what() const {
	const auto& table = errorCodeTable();
	auto it = table.find(error_code);
	if (it == table.end())
		return "UNKNOWN_ERROR";
	return it->second.second;
}

void Error::init() {
	errorCodeTable();
}

Error Error::asInjectedFault() const {
	Error e = *this;
	e.flags |= FLAG_INJECTED_FAULT;
	return e;
}

ErrorCodeTable::ErrorCodeTable() {
#define ERROR(name, number, description)                                                                               \
	addCode(number, #name, description);                                                                               \
	enum { Duplicate_Error_Code_##number = 0 };
#include "error_definitions.h"
}

void ErrorCodeTable::addCode(int code, const char* name, const char* description) {
	(*this)[code] = std::make_pair(name, description);
}

bool isAssertDisabled(int line) {
	return FLOW_KNOBS && (FLOW_KNOBS->DISABLE_ASSERTS == -1 || FLOW_KNOBS->DISABLE_ASSERTS == line);
}

void breakpoint_me() {
	return;
}

TEST_CASE("/flow/AssertTest") {
	// this is mostly checking bug for bug compatibility with the C integer / sign promotion rules.

	ASSERT_LT(-1, 0);
	ASSERT_EQ(0, 0u);
	ASSERT_GT(1, -1);
	ASSERT_EQ((int32_t)0xFFFFFFFF, (int32_t)-1);
	// ASSERT_LT(-1, 0u);  // fails: -1 is promoted to unsigned value in comparison.
	// ASSERT(-1 < 0u); // also fails
	int sz = 42;
	size_t ln = 43;
	ASSERT(sz < ln);
	ASSERT_EQ(0xFFFFFFFF, (int32_t)-1);

	return Void();
}
