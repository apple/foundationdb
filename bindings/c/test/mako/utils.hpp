#ifndef UTILS_HPP
#define UTILS_HPP
#pragma once

#include "mako.hpp"
#include <cassert>
#include <chrono>
#include <fmt/format.h>
#include <stdint.h>

/* uniform-distribution random */
/* return a uniform random number between low and high, both inclusive */
int urand(int low, int high);

/* random string */
template <bool Clear = true, typename Char>
void randstr(std::basic_string<Char>& str, int len) {
	if constexpr (Clear)
		str.clear();
	assert(len >= 0);
	str.reserve(str.size() + static_cast<size_t>(len));
	for (auto i = 0; i < len; i++) {
		str.push_back('!' + urand(0, 'z' - '!')); /* generage a char from '!' to 'z' */
	}
}

/* random numeric string */
template <bool Clear = true, typename Char>
void randnumstr(std::basic_string<Char>& str, int len) {
	if constexpr (Clear)
		str.clear();
	assert(len >= 0);
	str.reserve(str.size() + static_cast<size_t>(len));
	for (auto i = 0; i < len; i++) {
		str.push_back('0' + urand(0, 9)); /* generage a char from '0' to '9' */
	}
}

/* given the total number of rows to be inserted,
 * the worker process index p_idx and the thread index t_idx (both 0-based),
 * and the total number of processes, total_p, and threads, total_t,
 * returns the first row number assigned to this partition.
 */
int insert_begin(int rows, int p_idx, int t_idx, int total_p, int total_t);

/* similar to insert_begin, insert_end returns the last row numer */
int insert_end(int rows, int p_idx, int t_idx, int total_p, int total_t);

/* devide a value equally among threads */
int compute_thread_portion(int val, int p_idx, int t_idx, int total_p, int total_t);

/* similar to insert_begin/end, compute_thread_tps computes
 * the per-thread target TPS for given configuration.
 */
#define compute_thread_tps(val, p_idx, t_idx, total_p, total_t)                                                        \
	compute_thread_portion(val, p_idx, t_idx, total_p, total_t)

/* similar to compute_thread_tps,
 * compute_thread_iters computs the number of iterations.
 */
#define compute_thread_iters(val, p_idx, t_idx, total_p, total_t)                                                      \
	compute_thread_portion(val, p_idx, t_idx, total_p, total_t)

/* get the number of digits */
int digits(int num);

/* fill (str) with configured key prefix: i.e. non-numeric part
 * (str) is appended with concat([padding], PREFIX)
 */
template <bool Clear = true, typename Char>
void genkeyprefix(std::basic_string<Char>& str, std::string_view prefix, mako_args_t const& args) {
	// concat('x' * padding_len, key_prefix)
	if constexpr (Clear)
		str.clear();
	const auto padding_len =
	    args.prefixpadding ? (args.key_length - args.row_digits - static_cast<int>(prefix.size())) : 0;
	assert(padding_len >= 0);
	str.reserve(str.size() + padding_len + prefix.size());
	fmt::format_to(std::back_inserter(str), "{0:x>{1}}{2}", "", padding_len, prefix);
}

/* generate a key for a given key number */
/* prefix is "mako" by default, prefixpadding = 1 means 'x' will be in front rather than trailing the keyname */
template <bool Clear = true, typename Char>
void genkey(std::basic_string<Char>& str, std::string_view prefix, mako_args_t const& args, int num) {
	static_assert(sizeof(Char) == 1);
	const auto pad_len = args.prefixpadding ? args.key_length - (static_cast<int>(prefix.size()) + args.row_digits) : 0;
	assert(pad_len >= 0);
	if constexpr (Clear)
		str.clear();
	str.reserve(str.size() + static_cast<size_t>(args.key_length));
	fmt::format_to(std::back_inserter(str),
	               "{0:x>{1}}{2}{3:0{4}d}{5:x>{6}}",
	               "",
	               pad_len,
	               prefix,
	               num,
	               args.row_digits,
	               "",
	               args.key_length - pad_len - static_cast<int>(prefix.size()) - args.row_digits);
}

// invoke user-provided callable when object goes out of scope.
template <typename Func>
class exit_guard {
	std::decay_t<Func> fn;

public:
	exit_guard(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~exit_guard() { fn(); }
};

// invoke user-provided callable when stack unwinds by exception.
template <typename Func>
class fail_guard {
	std::decay_t<Func> fn;

public:
	fail_guard(Func&& fn) : fn(std::forward<Func>(fn)) {}

	~fail_guard() {
		if (std::uncaught_exceptions()) {
			fn();
		}
	}
};

// timing helpers
using std::chrono::steady_clock;
using timepoint_t = decltype(steady_clock::now());

template <typename Duration>
double to_double_seconds(Duration duration) {
	return std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
}

template <typename Duration>
uint64_t to_integer_seconds(Duration duration) {
	return std::chrono::duration_cast<std::chrono::duration<uint64_t>>(duration).count();
}

template <typename Duration>
uint64_t to_integer_microseconds(Duration duration) {
	return std::chrono::duration_cast<std::chrono::duration<uint64_t, std::micro>>(duration).count();
}

// trace helpers
constexpr const int STATS_TITLE_WIDTH = 12;
constexpr const int STATS_FIELD_WIDTH = 12;

template <typename Value>
void put_title(Value&& value) {
	fmt::print("{0: <{1}} ", std::forward<Value>(value), STATS_TITLE_WIDTH);
}

template <typename Value>
void put_title_r(Value&& value) {
	fmt::print("{0: >{1}} ", std::forward<Value>(value), STATS_TITLE_WIDTH);
}

inline void put_title_bar() {
	fmt::print("{0:=<{1}} ", "", STATS_TITLE_WIDTH);
}

template <typename Value>
void put_field(Value&& value) {
	fmt::print("{0: >{1}} ", std::forward<Value>(value), STATS_FIELD_WIDTH);
}

inline void put_field_bar() {
	fmt::print("{0:=>{1}} ", "", STATS_FIELD_WIDTH);
}

template <typename Value>
void put_field_f(Value&& value, int precision) {
	fmt::print("{0: >{1}.{2}f} ", std::forward<Value>(value), STATS_FIELD_WIDTH, precision);
}

#endif /* UTILS_HPP */
