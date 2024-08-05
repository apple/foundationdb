/*
 * CodeProbe.h
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

#ifndef FLOW_TESTPROBE_H_
#define FLOW_TESTPROBE_H_

#include "flow/Knobs.h"
#include "flow/Trace.h"

namespace probe {

struct ICodeProbe;

enum class AnnotationType { Decoration, Assertion, Context, Functional };
enum class ExecutionContext { Simulation, Net2 };

namespace context {
struct Net2 {
	constexpr static AnnotationType type = AnnotationType::Context;
	constexpr static ExecutionContext value = ExecutionContext::Net2;
};
struct Sim2 {
	constexpr static AnnotationType type = AnnotationType::Context;
	constexpr static ExecutionContext value = ExecutionContext::Simulation;
};

constexpr Net2 net2;
constexpr Sim2 sim2;

template <class Left, class Right>
struct OrContext {
	typename std::remove_const<Left>::type left;
	typename std::remove_const<Right>::type right;
	constexpr OrContext(Left left, Right right) : left(left), right(right) {}
	constexpr bool operator()(ExecutionContext context) const { return left(context) || right(context); }
};

template <class Left, class Right>
constexpr std::enable_if_t<Left::type == AnnotationType::Context && Right::type == AnnotationType::Context,
                           OrContext<Left, Right>>
operator|(Left const& lhs, Right const& rhs) {
	return OrContext<Left, Right>(lhs, rhs);
}

} // namespace context

namespace assert {
struct NoSim {
	constexpr static AnnotationType type = AnnotationType::Assertion;
	bool operator()(ICodeProbe const* self) const;
};
struct SimOnly {
	constexpr static AnnotationType type = AnnotationType::Assertion;
	bool operator()(ICodeProbe const* self) const;
};

template <class Left, class Right>
struct AssertOr {
	typename std::remove_const<Left>::type left;
	typename std::remove_const<Right>::type right;
	constexpr AssertOr() {}
	constexpr bool operator()(ICodeProbe* self) const { return left(self) || right(self); }
};
template <class Left, class Right>
struct AssertAnd {
	typename std::remove_const<Left>::type left;
	typename std::remove_const<Right>::type right;
	constexpr AssertAnd() {}
	constexpr bool operator()(ICodeProbe* self) const { return left(self) && right(self); }
};
template <class T>
struct AssertNot {
	typename std::remove_const<T>::type other;
	constexpr bool operator()(ICodeProbe* self) const { return !other(self); }
};

template <class Left, class Right>
constexpr std::enable_if_t<Left::type == AnnotationType::Assertion && Right::type == AnnotationType::Assertion,
                           AssertOr<Left, Right>>
operator||(Left const& lhs, Right const& rhs) {
	return AssertOr<Left, Right>();
}
template <class Left, class Right>
constexpr std::enable_if_t<Left::type == AnnotationType::Assertion && Right::type == AnnotationType::Assertion,
                           AssertAnd<Left, Right>>
operator&&(Left const& lhs, Right const& rhs) {
	return AssertAnd<Left, Right>();
}

template <class T>
constexpr std::enable_if_t<T::type == AnnotationType::Assertion, AssertNot<T>> operator!(T const&) {
	return AssertNot<T>();
}

constexpr SimOnly simOnly;
constexpr NoSim noSim;

} // namespace assert

namespace decoration {

// Code probes that currently (as of 9/25/2022) are not expected to show up in a 250k-test Joshua run
// are marked as "rare." This indicates a testing bug, and these probes should either be removed or testing
// coverage should be improved to hit them. Ideally, then, we should remove uses of this annotation in the
// long-term. However, this annotation has been added to prevent further regressions in code coverage, so that
// we can detect changes that fail to hit non-rare code probes.
//
// This should also hopefully help with debugging, because if a code probe is marked as rare, it means that this
// is a case not likely hit in simulation, and it may be a case that is more prone to buggy behaviour.
struct Rare {
	constexpr static AnnotationType type = AnnotationType::Decoration;
	void trace(struct ICodeProbe const*, BaseTraceEvent& evt, bool) const { evt.detail("Rare", true); }
};

constexpr Rare rare;

} // namespace decoration

namespace func {

struct Deduplicate {
	constexpr static AnnotationType type = AnnotationType::Functional;
};

constexpr Deduplicate deduplicate;

} // namespace func

template <class... Args>
struct CodeProbeAnnotations;

template <>
struct CodeProbeAnnotations<> {
	static constexpr bool providesContext = false;
	void hit(ICodeProbe* self) {}
	void trace(const ICodeProbe*, BaseTraceEvent&, bool) const {}
	constexpr bool expectContext(ExecutionContext context, bool prevHadSomeContext = false) const {
		return !prevHadSomeContext;
	}
	constexpr bool shouldTrace(ICodeProbe const*) const { return true; }
	constexpr bool deduplicate() const { return false; }
};

template <class Head, class... Tail>
struct CodeProbeAnnotations<Head, Tail...> {
	using HeadType = typename std::remove_const<Head>::type;
	using ChildType = CodeProbeAnnotations<Tail...>;

	static constexpr bool providesContext = HeadType::type == AnnotationType::Context || ChildType::providesContext;
	static_assert(HeadType::type != AnnotationType::Context || !ChildType::providesContext,
	              "Only one context annotation can be used");

	HeadType head;
	ChildType tail;

	void hit(ICodeProbe* self) {
		if constexpr (Head::type == AnnotationType::Assertion) {
			ASSERT(head(self));
		}
		tail.hit(self);
	}

	bool shouldTrace(ICodeProbe const* self) const {
		if constexpr (Head::type == AnnotationType::Assertion) {
			if (!head(self)) {
				return false;
			}
		}
		return tail.shouldTrace(self);
	}

	void trace(const ICodeProbe* self, BaseTraceEvent& evt, bool condition) const {
		if constexpr (Head::type == AnnotationType::Decoration) {
			head.trace(self, evt, condition);
		}
		tail.trace(self, evt, condition);
	}
	// This should behave like the following:
	// 1. If no context is passed in the code probe, we expect to see this in every context
	// 2. Otherwise we will return true iff the execution context we're looking for has been passed to the probe
	constexpr bool expectContext(ExecutionContext context, bool prevHadSomeContext = false) const {
		if constexpr (HeadType::type == AnnotationType::Context) {
			if (HeadType::value == context) {
				return true;
			} else {
				return tail.expectContext(context, true);
			}
		} else {
			return tail.expectContext(context, prevHadSomeContext);
		}
	}

	constexpr bool deduplicate() const {
		if constexpr (std::is_same_v<HeadType, func::Deduplicate>) {
			return true;
		} else {
			return tail.deduplicate();
		}
	}
};

struct ICodeProbe {
	virtual ~ICodeProbe();

	bool operator==(ICodeProbe const& other) const;
	bool operator!=(ICodeProbe const& other) const;

	std::string_view filename() const;
	virtual const char* filePath() const = 0;
	virtual unsigned line() const = 0;
	virtual const char* comment() const = 0;
	virtual const char* condition() const = 0;
	virtual const char* compilationUnit() const = 0;
	virtual void trace(bool) const = 0;
	virtual bool shouldTrace() const = 0;
	virtual bool wasHit() const = 0;
	virtual unsigned hitCount() const = 0;
	virtual bool expectInContext(ExecutionContext context) const = 0;
	virtual std::string function() const = 0;
	virtual bool deduplicate() const = 0;

	static void printProbesXML();
	static void printProbesJSON(std::vector<std::string> const& ctxs = std::vector<std::string>());
};

void registerProbe(ICodeProbe const& probe);
std::string functionNameFromInnerType(const char* name);

template <class FileName, class Condition, class Comment, class CompUnit, unsigned Line, class Annotations>
struct CodeProbeImpl : ICodeProbe {
	static CodeProbeImpl* instancePtr() { return &_instance; }
	static CodeProbeImpl& instance() { return _instance; }
	void hit() {
		if (_hitCount++ == 0) {
			trace(true);
		}
		annotations.hit(this);
	}

	void trace(bool condition) const override {
		TraceEvent evt(intToSeverity(FLOW_KNOBS->CODE_COV_TRACE_EVENT_SEVERITY), "CodeCoverage");
		evt.detail("File", filename())
		    .detail("Line", Line)
		    .detail("Condition", Condition::value())
		    .detail("Covered", condition)
		    .detail("Comment", Comment::value());
		annotations.trace(this, evt, condition);
	}

	bool shouldTrace() const override { return annotations.shouldTrace(this); }

	bool wasHit() const override { return _hitCount > 0; }
	unsigned hitCount() const override { return _hitCount; }

	const char* filePath() const override { return FileName::value(); }
	unsigned line() const override { return Line; }
	const char* comment() const override { return Comment::value(); }
	const char* condition() const override { return Condition::value(); }
	const char* compilationUnit() const override { return CompUnit::value(); }
	bool expectInContext(ExecutionContext context) const override { return annotations.expectContext(context); }
	std::string function() const override { return functionNameFromInnerType(typeid(FileName).name()); }
	bool deduplicate() const override { return annotations.deduplicate(); }

private:
	CodeProbeImpl() { registerProbe(*this); }
	inline static CodeProbeImpl _instance;
	std::atomic<unsigned> _hitCount = 0;
	Annotations annotations;
};

template <class FileName, class Condition, class Comment, class CompUnit, unsigned Line, class... Annotations>
CodeProbeImpl<FileName, Condition, Comment, CompUnit, Line, CodeProbeAnnotations<Annotations...>>& probeInstance(
    Annotations&... annotations) {
	return CodeProbeImpl<FileName, Condition, Comment, CompUnit, Line, CodeProbeAnnotations<Annotations...>>::
	    instance();
}

} // namespace probe

#ifdef COMPILATION_UNIT
#define CODE_PROBE_QUOTE(x) #x
#define CODE_PROBE_EXPAND_AND_QUOTE(x) CODE_PROBE_QUOTE(x)
#define CODE_PROBE_COMPILATION_UNIT CODE_PROBE_EXPAND_AND_QUOTE(COMPILATION_UNIT)
#else
#define CODE_PROBE_COMPILATION_UNIT "COMPILATION_UNIT not set"
#endif

#define _CODE_PROBE_IMPL(file, line, condition, comment, compUnit, fileType, condType, commentType, compUnitType, ...) \
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
		probe::probeInstance<fileType, condType, commentType, compUnitType, line>(__VA_ARGS__).hit();                  \
	}

#define _CODE_PROBE_T2(type, counter) type##counter
#define _CODE_PROBE_T(type, counter) _CODE_PROBE_T2(type, counter)

#define CODE_PROBE(condition, comment, ...)                                                                            \
	do {                                                                                                               \
		_CODE_PROBE_IMPL(__FILE__,                                                                                     \
		                 __LINE__,                                                                                     \
		                 condition,                                                                                    \
		                 comment,                                                                                      \
		                 CODE_PROBE_COMPILATION_UNIT,                                                                  \
		                 _CODE_PROBE_T(FileType, __LINE__),                                                            \
		                 _CODE_PROBE_T(CondType, __LINE__),                                                            \
		                 _CODE_PROBE_T(CommentType, __LINE__),                                                         \
		                 _CODE_PROBE_T(CompilationUnitType, __LINE__),                                                 \
		                 __VA_ARGS__)                                                                                  \
	} while (false)

#endif // FLOW_TESTPROBE_H_
