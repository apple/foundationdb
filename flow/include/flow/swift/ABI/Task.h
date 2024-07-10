//===--- Task.h - ABI structures for asynchronous tasks ---------*- C++ -*-===//
//
// This source file is part of the Swift.org open source project
//
// Copyright (c) 2014 - 2020 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
// See https://swift.org/CONTRIBUTORS.txt for the list of Swift project authors
//
//===----------------------------------------------------------------------===//
//
// Swift ABI describing tasks.
//
//===----------------------------------------------------------------------===//

#ifndef SWIFT_ABI_TASK_H
#define SWIFT_ABI_TASK_H

#include "MetadataValues.h"

namespace swift {

enum {
	NumWords_HeapObject = 2,
};

/// A schedulable job.
class alignas(2 * alignof(void*)) Job
//    // We don't need the detailed definition of HeapObject, and instead just use __HeapObjectReserved to get the right
//    // layout. Pulling in a full definition of HeapObject sadly takes with it a lot of other runtime,
//    // so until we have to, let's avoid doing so
//    : public HeapObject
{
public:
	// Fake fields, pretending the storage of a HeapObject
	void* __HeapObjectPrivate[NumWords_HeapObject];

	// Indices into SchedulerPrivate, for use by the runtime.
	enum {
		/// The next waiting task link, an AsyncTask that is waiting on a future.
		NextWaitingTaskIndex = 0,

		// The Dispatch object header is one pointer and two ints, which is
		// equivalent to three pointers on 32-bit and two pointers 64-bit. Set the
		// indexes accordingly so that DispatchLinkageIndex points to where Dispatch
		// expects.
		DispatchHasLongObjectHeader = sizeof(void*) == sizeof(int),

		/// An opaque field used by Dispatch when enqueueing Jobs directly.
		DispatchLinkageIndex = DispatchHasLongObjectHeader ? 1 : 0,

		/// The dispatch queue being used when enqueueing a Job directly with
		/// Dispatch.
		DispatchQueueIndex = DispatchHasLongObjectHeader ? 0 : 1,
	};

	// Reserved for the use of the scheduler.
	void* SchedulerPrivate[2];

	JobFlags Flags;

	// Derived classes can use this to store a Job Id.
	uint32_t Id = 0;

	/// The voucher associated with the job. Note: this is currently unused on
	/// non-Darwin platforms, with stub implementations of the functions for
	/// consistency.
	void* Voucher = nullptr; //  voucher_t Voucher = nullptr;

	/// Reserved for future use.
	void* Reserved = nullptr;

	bool isAsyncTask() const { return Flags.isAsyncTask(); }

	JobPriority getPriority() const { return Flags.getPriority(); }
};

} // end namespace swift

#endif
