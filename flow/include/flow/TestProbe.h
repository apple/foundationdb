#ifndef TESTPROBE_H_
#define TESTPROBE_H_

#include "flow/Knobs.h"
#include "flow/Trace.h"

namespace probe {

struct Net2 {};
struct Sim {};

template <class... Args>
struct CodeProbeAnnotations;

struct ICodeProbe {
	virtual ~ICodeProbe();
	ICodeProbe(const char* file, unsigned line);

	virtual const char* filename() const = 0;
	virtual unsigned line() const = 0;
	virtual const char* comment() const = 0;
	virtual const char* condition() const = 0;

	static void printProbesXML();
	static void printProbesJSON();
};

template <class FileName, class Condition, class Comment, unsigned Line>
struct CodeProbeImpl : ICodeProbe {
	static constexpr CodeProbeImpl* instancePtr() { return &instance; }
	void hit() {
		if (!didHit) {
			TraceEvent(intToSeverity(FLOW_KNOBS->CODE_COV_TRACE_EVENT_SEVERITY), "CodeCoverage").detail("Line", Line);
			didHit = true;
		}
	}

	const char* filename() const override { return FileName::value(); }
	unsigned line() const override { return Line; }
	const char* comment() const override { return Comment::value(); }
	const char* condition() const override { return Condition::value(); }

private:
	CodeProbeImpl() : ICodeProbe(FileName::value(), Line) {}
	inline static CodeProbeImpl instance;
	bool didHit = false;
};

} // namespace probe

#define _CODE_PROBE_IMPL(file, line, condition, comment, fileType, condType, commentType)                              \
	struct fileType {                                                                                                  \
		constexpr static const char* value() { return file; }                                                          \
	};                                                                                                                 \
	struct condType {                                                                                                  \
		constexpr static const char* value() { return #condition; }                                                    \
	};                                                                                                                 \
	struct commentType {                                                                                               \
		constexpr static const char* value() { return comment; }                                                       \
	};                                                                                                                 \
	static_assert(probe::CodeProbeImpl<fileType, condType, commentType, line>::instancePtr() != nullptr);              \
	if (condition) {                                                                                                   \
		probe::CodeProbeImpl<fileType, condType, commentType, line>::instancePtr()->hit();                             \
	}

#define _CODE_PROBE_T2(type, counter) type##counter
#define _CODE_PROBE_T(type, counter) _CODE_PROBE_T2(type, counter)

#define CODE_PROBE(condition, comment)                                                                                 \
	_CODE_PROBE_IMPL(__FILE__,                                                                                         \
	                 __LINE__,                                                                                         \
	                 condition,                                                                                        \
	                 comment,                                                                                          \
	                 _CODE_PROBE_T(fileType, __COUNTER__),                                                             \
	                 _CODE_PROBE_T(condType, __COUNTER__),                                                             \
	                 _CODE_PROBE_T(commentType, __COUNTER__))

#endif // TESTPROBE_H_
