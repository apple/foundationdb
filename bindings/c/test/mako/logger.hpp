/*
 * logger.hpp
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

#ifndef MAKO_LOGGER_HPP
#define MAKO_LOGGER_HPP
#include <fmt/format.h>
#include <cassert>
#include <cstdio>
#include <iterator>
#include <string_view>
#include "process.hpp"

namespace mako {

constexpr const int VERBOSE_NONE = 0; // will still print errors
constexpr const int VERBOSE_DEFAULT = 1; // will print info and work stats
constexpr const int VERBOSE_WARN = 2; // will print expected errors
constexpr const int VERBOSE_DEBUG = 3; // will print everything

template <ProcKind P>
using ProcKindConstant = std::integral_constant<ProcKind, P>;

using MainProcess = ProcKindConstant<ProcKind::MAIN>;
using StatsProcess = ProcKindConstant<ProcKind::STATS>;
using WorkerProcess = ProcKindConstant<ProcKind::WORKER>;
using AdminProcess = ProcKindConstant<ProcKind::ADMIN>;

class Logger {
	ProcKind proc;
	int verbosity{ VERBOSE_DEFAULT };
	int process_id{ -1 };
	int thread_id{ -1 };

	void putHeader(fmt::memory_buffer& buf, std::string_view category) {
		if (proc == ProcKind::MAIN) {
			fmt::format_to(std::back_inserter(buf), "[ MAIN] {}: ", category);
		} else if (proc == ProcKind::STATS) {
			fmt::format_to(std::back_inserter(buf), "[STATS] {}: ", category);
		} else if (proc == ProcKind::ADMIN) {
			fmt::format_to(std::back_inserter(buf), "[ADMIN] {}: ", category);
		} else {
			if (thread_id == -1) {
				fmt::format_to(std::back_inserter(buf), "[WORKER{:3d}] {}: ", process_id + 1, category);
			} else {
				fmt::format_to(
				    std::back_inserter(buf), "[WORKER{:3d}:{:3d}] {}: ", process_id + 1, thread_id + 1, category);
			}
		}
	}

public:
	Logger(MainProcess, int verbosity) noexcept : proc(MainProcess::value), verbosity(verbosity) {}

	Logger(StatsProcess, int verbosity) noexcept : proc(StatsProcess::value), verbosity(verbosity) {}

	Logger(AdminProcess, int verbosity) noexcept : proc(AdminProcess::value), verbosity(verbosity) {}

	Logger(WorkerProcess, int verbosity, int process_id, int thread_id = -1) noexcept
	  : proc(WorkerProcess::value), verbosity(verbosity), process_id(process_id), thread_id(thread_id) {}

	Logger(const Logger&) noexcept = default;
	Logger& operator=(const Logger&) noexcept = default;

	void setVerbosity(int value) noexcept {
		assert(value >= VERBOSE_NONE && value <= VERBOSE_DEBUG);
		verbosity = value;
	}

	template <typename... Args>
	void printWithLogLevel(int log_level,
	                       std::string_view header,
	                       const fmt::format_string<Args...>& fmt_str,
	                       Args&&... args) {
		assert(log_level >= VERBOSE_NONE && log_level <= VERBOSE_DEBUG);
		if (log_level <= verbosity) {
			const auto fp = log_level == VERBOSE_NONE ? stderr : stdout;
			// 500B inline buffer
			auto buf = fmt::memory_buffer{};
			putHeader(buf, header);
			fmt::format_to(std::back_inserter(buf), fmt_str, std::forward<Args>(args)...);
			fmt::print(fp, "{}\n", std::string_view(buf.data(), buf.size()));
		}
	}

	template <typename... Args>
	void error(const fmt::format_string<Args...>& fmt_str, Args&&... args) {
		printWithLogLevel(VERBOSE_NONE, "ERROR", fmt_str, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void info(const fmt::format_string<Args...>& fmt_str, Args&&... args) {
		printWithLogLevel(VERBOSE_DEFAULT, "INFO", fmt_str, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void warn(const fmt::format_string<Args...>& fmt_str, Args&&... args) {
		printWithLogLevel(VERBOSE_WARN, "WARNING", fmt_str, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void debug(const fmt::format_string<Args...>& fmt_str, Args&&... args) {
		printWithLogLevel(VERBOSE_DEBUG, "DEBUG", fmt_str, std::forward<Args>(args)...);
	}

	template <typename... Args>
	void imm(Args&&... args) {
		printWithLogLevel(VERBOSE_NONE, "IMMEDIATE", std::forward<Args>(args)...);
	}

	bool isFor(ProcKind procKind) const noexcept { return proc == procKind; }
};

} // namespace mako

#endif /*MAKO_LOGGER_HPP*/
