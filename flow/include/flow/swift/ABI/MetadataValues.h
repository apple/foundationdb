//===--- MetadataValues.h - Compiler/runtime ABI Metadata -------*- C++ -*-===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2014 - 2017 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//
//
// This header is shared between the runtime and the compiler and
// includes target-independent information which can be usefully shared
// between them.
//
//===----------------------------------------------------------------------===//

#ifndef SWIFT_ABI_METADATAVALUES_H
#define SWIFT_ABI_METADATAVALUES_H

#include "../../swift/Basic/FlagSet.h"

#include <stdlib.h>
#include <stdint.h>

namespace swift {

enum {
	/// The number of words (pointers) in a value buffer.
	NumWords_ValueBuffer = 3,

	/// The number of words in a metadata completion context.
	NumWords_MetadataCompletionContext = 4,

	/// The number of words in a yield-once coroutine buffer.
	NumWords_YieldOnceBuffer = 4,

	/// The number of words in a yield-many coroutine buffer.
	NumWords_YieldManyBuffer = 8,

	/// The number of words (in addition to the heap-object header)
	/// in a default actor.
	NumWords_DefaultActor = 12,

	/// The number of words in a task.
	NumWords_AsyncTask = 24,

	/// The number of words in a task group.
	NumWords_TaskGroup = 32,

	/// The number of words in an AsyncLet (flags + child task context & allocation)
	NumWords_AsyncLet = 80, // 640 bytes ought to be enough for anyone

	/// The size of a unique hash.
	NumBytes_UniqueHash = 16,

	/// The maximum number of generic parameters that can be
	/// implicitly declared, for generic signatures that support that.
	MaxNumImplicitGenericParamDescriptors = 64,
};

struct InProcess;
template <typename Runtime>
struct TargetMetadata;
using Metadata = TargetMetadata<InProcess>;

/// Kinds of schedulable job.s
enum class JobKind : size_t {
	// There are 256 possible job kinds.

	/// An AsyncTask.
	Task = 0,

	/// Job kinds >= 192 are private to the implementation.
	First_Reserved = 192,

	DefaultActorInline = First_Reserved,
	DefaultActorSeparate,
	DefaultActorOverride,
	NullaryContinuation
};

/// The priority of a job.  Higher priorities are larger values.
enum class JobPriority : size_t {
	// This is modelled off of Dispatch.QoS, and the values are directly
	// stolen from there.
	UserInteractive = 0x21, /* UI */
	UserInitiated = 0x19, /* IN */
	Default = 0x15, /* DEF */
	Utility = 0x11, /* UT */
	Background = 0x09, /* BG */
	Unspecified = 0x00, /* UN */
};

/// A tri-valued comparator which orders higher priorities first.
inline int descendingPriorityOrder(JobPriority lhs, JobPriority rhs) {
	return (lhs == rhs ? 0 : lhs > rhs ? -1 : 1);
}

/// Flags for schedulable jobs.
class JobFlags : public FlagSet<uint32_t> {
public:
	enum {
		Kind = 0,
		Kind_width = 8,

		Priority = 8,
		Priority_width = 8,

		// 8 bits reserved for more generic job flags.

		// Kind-specific flags.

		Task_IsChildTask = 24,
		Task_IsFuture = 25,
		Task_IsGroupChildTask = 26,
		// 27 is currently unused
		Task_IsAsyncLetTask = 28,
	};

	explicit JobFlags(uint32_t bits) : FlagSet(bits) {}
	JobFlags(JobKind kind) { setKind(kind); }
	JobFlags(JobKind kind, JobPriority priority) {
		setKind(kind);
		setPriority(priority);
	}
	constexpr JobFlags() {}

	FLAGSET_DEFINE_FIELD_ACCESSORS(Kind, Kind_width, JobKind, getKind, setKind)

	FLAGSET_DEFINE_FIELD_ACCESSORS(Priority, Priority_width, JobPriority, getPriority, setPriority)

	bool isAsyncTask() const { return getKind() == JobKind::Task; }

	FLAGSET_DEFINE_FLAG_ACCESSORS(Task_IsChildTask, task_isChildTask, task_setIsChildTask)
	FLAGSET_DEFINE_FLAG_ACCESSORS(Task_IsFuture, task_isFuture, task_setIsFuture)
	FLAGSET_DEFINE_FLAG_ACCESSORS(Task_IsGroupChildTask, task_isGroupChildTask, task_setIsGroupChildTask)
	FLAGSET_DEFINE_FLAG_ACCESSORS(Task_IsAsyncLetTask, task_isAsyncLetTask, task_setIsAsyncLetTask)
};

/// Kinds of option records that can be passed to creating asynchronous tasks.
enum class TaskOptionRecordKind : uint8_t {
	/// Request a task to be kicked off, or resumed, on a specific executor.
	Executor = 0,
	/// Request a child task to be part of a specific task group.
	TaskGroup = 1,
	/// DEPRECATED. AsyncLetWithBuffer is used instead.
	/// Request a child task for an 'async let'.
	AsyncLet = 2,
	/// Request a child task for an 'async let'.
	AsyncLetWithBuffer = 3,
	/// Request a child task for swift_task_run_inline.
	RunInline = UINT8_MAX,
};

/// Status values for a continuation.  Note that the "not yet"s in
/// the description below aren't quite right because the system
/// does not actually promise to update the status before scheduling
/// the task.  This is because the continuation context is immediately
/// invalidated once the task starts running again, so the window in
/// which we can usefully protect against (say) double-resumption may
/// be very small.
enum class ContinuationStatus : size_t {
	/// The continuation has not yet been awaited or resumed.
	Pending = 0,

	/// The continuation has already been awaited, but not yet resumed.
	Awaited = 1,

	/// The continuation has already been resumed, but not yet awaited.
	Resumed = 2
};

/// Flags that go in a TargetAccessibleFunction structure.
class AccessibleFunctionFlags : public FlagSet<uint32_t> {
public:
	enum {
		/// Whether this is a "distributed" actor function.
		Distributed = 0,
	};

	explicit AccessibleFunctionFlags(uint32_t bits) : FlagSet(bits) {}
	constexpr AccessibleFunctionFlags() {}

	/// Whether the this is a "distributed" actor function.
	FLAGSET_DEFINE_FLAG_ACCESSORS(Distributed, isDistributed, setDistributed)
};

} // end namespace swift

#endif // SWIFT_ABI_METADATAVALUES_H
