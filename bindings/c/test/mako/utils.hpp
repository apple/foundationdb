/*
 * utils.hpp
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

#ifndef UTILS_HPP
#define UTILS_HPP
#pragma once

#include "macro.hpp"
#include "mako.hpp"
#include "fdbclient/zipf.h"
#include <cassert>
#include <chrono>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include <fmt/format.h>

namespace mako {

inline uint64_t byteswapHelper(uint64_t input) {
	uint64_t output = 0;
	for (int i = 0; i < 8; ++i) {
		output <<= 8;
		output += input & 0xFF;
		input >>= 8;
	}
	return output;
}

/* uniform-distribution random */
/* return a uniform random number between low and high, both inclusive */
force_inline int urand(int low, int high) {
	double r = rand() / (1.0 + RAND_MAX);
	int range = high - low + 1;
	return (int)((r * range) + low);
}

force_inline int nextKey(Arguments const& args) {
	if (args.zipf)
		return zipfian_next();
	return urand(0, args.rows - 1);
}

force_inline int intSize(std::string_view sv) {
	return static_cast<int>(sv.size());
}

template <typename Char>
inline void randomAlphanumString(Char* str, int len) {
	constexpr auto chars_per_alpha = 26;
	constexpr auto range = chars_per_alpha * 2 + 10; // uppercase, lowercase, digits
	for (auto i = 0; i < len; i++) {
		auto value = urand(0, range - 1);
		if (value < chars_per_alpha)
			str[i] = 'a' + value;
		else if (value < 2 * chars_per_alpha)
			str[i] = 'A' + value - chars_per_alpha;
		else
			str[i] = '0' + value - 2 * chars_per_alpha;
	}
}

/* random string */
template <typename Char>
force_inline void randomString(Char* str, int len) {
	assert(len >= 0);
	for (auto i = 0; i < len; i++) {
		str[i] = ('!' + urand(0, 'z' - '!')); /* generate a char from '!' to 'z' */
	}
}

/* given the total number of rows to be inserted,
 * the worker process index p_idx and the thread index t_idx (both 0-based),
 * and the total number of processes, total_p, and threads, total_t,
 * returns the first row number assigned to this partition.
 */
force_inline int insertBegin(int rows, int p_idx, int t_idx, int total_p, int total_t) {
	double interval = (double)rows / total_p / total_t;
	return (int)(round(interval * ((p_idx * total_t) + t_idx)));
}

/* similar to insertBegin, insertEnd returns the last row numer */
force_inline int insertEnd(int rows, int p_idx, int t_idx, int total_p, int total_t) {
	double interval = (double)rows / total_p / total_t;
	return (int)(round(interval * ((p_idx * total_t) + t_idx + 1) - 1));
}

/* devide a value equally among threads */
int computeThreadPortion(int val, int p_idx, int t_idx, int total_p, int total_t);

/* similar to insertBegin/end, computeThreadTps computes
 * the per-thread target TPS for given configuration.
 */
#define computeThreadTps(val, p_idx, t_idx, total_p, total_t) computeThreadPortion(val, p_idx, t_idx, total_p, total_t)

/* similar to computeThreadTps,
 * computeThreadIters computs the number of iterations.
 */
#define computeThreadIters(val, p_idx, t_idx, total_p, total_t)                                                        \
	computeThreadPortion(val, p_idx, t_idx, total_p, total_t)

/* get the number of digits */
int digits(int num);

/* fill memory slice [str, str + len) as stringified, zero-padded num */
template <typename Char>
force_inline void numericWithFill(Char* str, int len, int num) {
	static_assert(sizeof(Char) == 1);
	assert(num >= 0);
	memset(str, '0', len);
	for (auto i = len - 1; num > 0 && i >= 0; i--, num /= 10) {
		str[i] = (num % 10) + '0';
	}
}

/* generate a key for a given key number */
/* prefix is "mako" by default, prefixpadding = 1 means 'x' will be in front rather than trailing the keyname */
template <typename Char>
void genKey(Char* str, std::string_view prefix, Arguments const& args, int num) {
	static_assert(sizeof(Char) == 1);
	memset(str, 'x', args.key_length);
	const auto prefix_len = static_cast<int>(prefix.size());
	auto pos = args.prefixpadding ? (args.key_length - prefix_len - args.row_digits) : 0;
	memcpy(&str[pos], prefix.data(), prefix_len);
	pos += prefix_len;
	numericWithFill(&str[pos], args.row_digits, num);
}

template <typename Char>
force_inline void prepareKeys(int op,
                              std::basic_string<Char>& key1,
                              std::basic_string<Char>& key2,
                              Arguments const& args) {
	const auto key1_num = nextKey(args);
	genKey(key1.data(), KEY_PREFIX, args, key1_num);
	if (args.txnspec.ops[op][OP_RANGE] > 0) {
		const auto key2_num = std::min(key1_num + args.txnspec.ops[op][OP_RANGE] - 1, args.rows - 1);
		genKey(key2.data(), KEY_PREFIX, args, key2_num);
	}
}

// invoke user-provided callable when object goes out of scope.
template <typename Func>
class ExitGuard {
	std::decay_t<Func> fn;

public:
	ExitGuard(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~ExitGuard() { fn(); }
};

// invoke user-provided callable when stack unwinds by exception.
template <typename Func>
class FailGuard {
	std::decay_t<Func> fn;

public:
	FailGuard(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~FailGuard() {
		if (std::uncaught_exceptions()) {
			fn();
		}
	}
};

// trace helpers
constexpr const int STATS_TITLE_WIDTH = 12;
constexpr const int STATS_FIELD_WIDTH = 12;

template <typename Value>
void putTitle(Value&& value) {
	fmt::print("{0: <{1}} ", std::forward<Value>(value), STATS_TITLE_WIDTH);
}

template <typename Value>
void putTitleRight(Value&& value) {
	fmt::print("{0: >{1}} ", std::forward<Value>(value), STATS_TITLE_WIDTH);
}

inline void putTitleBar() {
	fmt::print("{0:=<{1}} ", "", STATS_TITLE_WIDTH);
}

template <typename Value>
void putField(Value&& value) {
	fmt::print("{0: >{1}} ", std::forward<Value>(value), STATS_FIELD_WIDTH);
}

inline void putFieldBar() {
	fmt::print("{0:=>{1}} ", "", STATS_FIELD_WIDTH);
}

template <typename Value>
void putFieldFloat(Value&& value, int precision) {
	fmt::print("{0: >{1}.{2}f} ", std::forward<Value>(value), STATS_FIELD_WIDTH, precision);
}

} // namespace mako

#endif /* UTILS_HPP */
