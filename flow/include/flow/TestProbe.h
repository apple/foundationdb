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

	virtual const char* filename() const = 0;
	virtual unsigned line() const = 0;
	virtual const char* comment() const = 0;
	virtual const char* condition() const = 0;
	virtual const char* compilationUnit() const = 0;

	static void printProbesXML();
	static void printProbesJSON();
};

void registerProbe(ICodeProbe const& probe);

template <class FileName, class Condition, class Comment, class CompUnit, unsigned Line>
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
	const char* compilationUnit() const override { return CompUnit::value(); }

private:
	CodeProbeImpl() { registerProbe(*this); }
	inline static CodeProbeImpl instance;
	bool didHit = false;
};

} // namespace probe

#ifdef COMPILATION_UNIT
#define CODE_PROBE_QUOTE(x) #x
#define CODE_PROBE_EXPAND_AND_QUOTE(x) CODE_PROBE_QUOTE(x)
#define CODE_PROBE_COMPILATION_UNIT CODE_PROBE_EXPAND_AND_QUOTE(COMPILATION_UNIT)
#else
#define CODE_PROBE_COMPILATION_UNIT "COMPILATION_UNIT not set"
#endif

#define _CODE_PROBE_IMPL(file, line, condition, comment, compUnit, fileType, condType, commentType, compUnitType)      \
	struct fileType {                                                                                                  \
		constexpr static const char* value() { return file; }                                                          \
	};                                                                                                                 \
	struct condType {                                                                                                  \
		constexpr static const char* value() { return #condition; }                                                    \
	};                                                                                                                 \
	struct commentType {                                                                                               \
		constexpr static const char* value() { return comment; }                                                       \
	};                                                                                                                 \
	struct compUnitType {                                                                                              \
		constexpr static const char* value() { return compUnit; }                                                      \
	};                                                                                                                 \
	if (condition) {                                                                                                   \
		probe::CodeProbeImpl<fileType, condType, commentType, compUnitType, line>::instancePtr()->hit();               \
	}

#define _CODE_PROBE_T2(type, counter) type##counter
#define _CODE_PROBE_T(type, counter) _CODE_PROBE_T2(type, counter)

#define CODE_PROBE(condition, comment)                                                                                 \
	do {                                                                                                               \
		_CODE_PROBE_IMPL(__FILE__,                                                                                     \
		                 __LINE__,                                                                                     \
		                 condition,                                                                                    \
		                 comment,                                                                                      \
		                 CODE_PROBE_COMPILATION_UNIT,                                                                  \
		                 _CODE_PROBE_T(FileType, __COUNTER__),                                                         \
		                 _CODE_PROBE_T(CondType, __COUNTER__),                                                         \
		                 _CODE_PROBE_T(CommentType, __COUNTER__),                                                      \
		                 _CODE_PROBE_T(CompilationUnitType, __COUNTER__))                                              \
	} while (false);

#endif // TESTPROBE_H_
