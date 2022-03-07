#ifndef MAKO_TIME_HPP
#define MAKO_TIME_HPP

#include <chrono>

namespace mako {

/* time measurement helpers */
using std::chrono::steady_clock;
using timepoint_t = decltype(steady_clock::now());
using timediff_t = decltype(std::declval<timepoint_t>() - std::declval<timepoint_t>());

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

// timing helpers
struct start_at_ctor {};

class Stopwatch {
	timepoint_t p1, p2;

public:
	Stopwatch() noexcept = default;
	Stopwatch(start_at_ctor) noexcept { start(); }
	Stopwatch(timepoint_t start_time) noexcept : p1(start_time), p2() {}
	Stopwatch(const Stopwatch&) noexcept = default;
	Stopwatch& operator=(const Stopwatch&) noexcept = default;
	timepoint_t get_start() const noexcept { return p1; }
	timepoint_t get_stop() const noexcept { return p2; }
	void start() noexcept { p1 = steady_clock::now(); }
	Stopwatch& stop() noexcept {
		p2 = steady_clock::now();
		return *this;
	}
	Stopwatch& set_stop(timepoint_t p_stop) noexcept {
		p2 = p_stop;
		return *this;
	}
	void start_from_stop() noexcept { p1 = p2; }
	auto diff() const noexcept { return p2 - p1; }
};

} // namespace mako

#endif /* MAKO_TIME_HPP */
