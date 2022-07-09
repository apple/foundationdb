#ifndef TESTPROBE_H_
#define TESTPROBE_H_

#include <string_view>
#include <iostream>
#include "flow/Knobs.h"
#include "flow/Trace.h"

#include <fmt/format.h>

#define CODE_PROBE(condition, comment)                                                                                 \
	struct probe_file_name##__LINE__ {                                                                                 \
		constexpr static const char* value() { return __FILE__; }                                                      \
	};                                                                                                                 \
	struct condition_string##__LINE__ {                                                                                \
		constexpr static const char* value() { return #condition; }                                                    \
	};                                                                                                                 \
	struct comment_string##__LINE__ {                                                                                  \
		constexpr static const char* value() { return #comment; }                                                      \
	};                                                                                                                 \
	static_assert(                                                                                                     \
	    TestProbeImpl<probe_file_name##__LINE__, condition_string##__LINE__, comment_string##__LINE__, __LINE__>::     \
	        instancePtr() != nullptr);

template <class FileName, class Condition, class Comment, unsigned Line>
struct TestProbeImpl {
	static constexpr TestProbeImpl* instancePtr() { return &instance; }
	void hit() {
		if (!didHit) {
			TraceEvent(intToSeverity(FLOW_KNOBS->CODE_COV_TRACE_EVENT_SEVERITY), "CodeCoverage").detail("Line", Line);
		}
	}

private:
	constexpr TestProbeImpl() {
		fmt::print("Probe at {}:{} with condition {} and comment {}\n",
		           FileName::value(),
		           Line,
		           Condition::value(),
		           Comment::value());
	}
	inline static TestProbeImpl instance;
	bool didHit = false;
};

#endif // TESTPROBE_H_
