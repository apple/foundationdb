/*
 * swift_support.h
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

#ifndef FLOW_SWIFT_SUPPORT_H
#define FLOW_SWIFT_SUPPORT_H

#pragma once

#ifdef WITH_SWIFT

#include "flow/swift/ABI/Task.h"
#include "flow/TaskPriority.h"

// ==== ----------------------------------------------------------------------------------------------------------------

/// This annotation bridges immortal C++ singleton types
/// that are always accessed via a pointer or a reference in C++ as immortal class types in Swift.
#define SWIFT_CXX_IMMORTAL_SINGLETON_TYPE                                                                              \
	__attribute__((swift_attr("import_reference"))) __attribute__((swift_attr("retain:immortal")))                     \
	__attribute__((swift_attr("release:immortal")))

#define SWIFT_CXX_REF                                                                                                  \
	__attribute__((swift_attr("import_reference"))) __attribute__((swift_attr("retain:addref")))                       \
	__attribute__((swift_attr("release:delref")))

/// Ignore that a type seems to be an unsafe projection, and import it regardless.
#define SWIFT_CXX_IMPORT_UNSAFE __attribute__((swift_attr("import_unsafe")))

/// Import a C++ type as "owned", meaning that it "owns" all of the data it contains.
///
/// This is in contrast to a type which may have pointers to the "outside" somewhere,
/// where we might end up keeping pointers to memory which has been deallocated,
/// while we still have this projection in hand (and thus, an unsafe pointer access).
#define SWIFT_CXX_IMPORT_OWNED __attribute__((swift_attr("import_owned")))

/// Declare a type as `Sendable` which means that it is safe to be used concurrency,
/// and passed across actor and task boundaries.
///
/// Since in Flow we are single-threaded, basically everything is Sendable,
/// at least in the concurrent access safety sense of this annotation.
#define SWIFT_SENDABLE __attribute__((swift_attr("@Sendable")))

#define SWIFT_STRINGIFY(x) #x

#define CONCAT2(id1, id2) id1##id2
#define CONCAT3(id1, id2, id3) id1##id2##id3

// ==== ----------------------------------------------------------------------------------------------------------------

/// Convert a Swift JobPriority value to a numeric value of Flow/TaskPriority.
TaskPriority swift_priority_to_net2(swift::JobPriority p);

// ==== ----------------------------------------------------------------------------------------------------------------

#if __has_feature(nullability)
// Provide macros to temporarily suppress warning about the use of
// _Nullable and _Nonnull.
#define SWIFT_BEGIN_NULLABILITY_ANNOTATIONS                                                                            \
	_Pragma("clang diagnostic push") _Pragma("clang diagnostic ignored \"-Wnullability-extension\"")                   \
	    _Pragma("clang assume_nonnull begin")

#define SWIFT_END_NULLABILITY_ANNOTATIONS _Pragma("clang diagnostic pop") _Pragma("clang assume_nonnull end")

#else
// #define _Nullable and _Nonnull to nothing if we're not being built
// with a compiler that supports them.
#define _Nullable
#define _Nonnull
#define _Null_unspecified
#define SWIFT_BEGIN_NULLABILITY_ANNOTATIONS
#define SWIFT_END_NULLABILITY_ANNOTATIONS
#endif

#else

// No-op macros for Swift support

// ==== ----------------------------------------------------------------------------------------------------------------

/// This annotation bridges immortal C++ singleton types
/// that are always accessed via a pointer or a reference in C++ as immortal class types in Swift.
#define SWIFT_CXX_IMMORTAL_SINGLETON_TYPE
#define SWIFT_CXX_REF
#define SWIFT_CXX_IMPORT_UNSAFE
#define SWIFT_CXX_IMPORT_OWNED
#define SWIFT_SENDABLE
#define SWIFT_NAME(x)
#define CONCAT2(id1, id2) id1##id2
#define CONCAT3(id1, id2, id3) id1##id2##id3

#define _Nullable
#define _Nonnull
#define _Null_unspecified
#define SWIFT_BEGIN_NULLABILITY_ANNOTATIONS
#define SWIFT_END_NULLABILITY_ANNOTATIONS

#endif /* WITH_SWIFT */

#endif /* FLOW_SWIFT_SUPPORT_H */
