/*
 * network.h
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

#ifndef FLOW_SWIFT_COMPAT_H
#define FLOW_SWIFT_COMPAT_H

#pragma once
#include "flow/swift/ABI/Task.h"

// ==== ----------------------------------------------------------------------------------------------------------------

#define SWIFT_CXX_REF_IMMORTAL                                                                                         \
	__attribute__((swift_attr("import_as_ref")))                                                                       \
    __attribute__((swift_attr("retain:immortal")))                                                                     \
	__attribute__((swift_attr("release:immortal")))

/// This annotation bridges immortal C++ singleton types
/// that are always accessed via a pointer or a reference in C++ as immortal class types in Swift.
#define SWIFT_CXX_IMMORTAL_SINGLETON_TYPE                                                                                        \
    __attribute__((swift_attr("import_as_ref")))                                                                       \
    __attribute__((swift_attr("retain:immortal")))                                                                     \
    __attribute__((swift_attr("release:immortal")))

#define SWIFT_SENDABLE __attribute__((swift_attr("@Sendable")))

// ==== ----------------------------------------------------------------------------------------------------------------

/// Convert a Swift JobPriority value to a numeric value of Flow/TaskPriority.
int64_t swift_priority_to_net2(swift::JobPriority p);

// ==== ----------------------------------------------------------------------------------------------------------------

#if __has_feature(nullability)
// Provide macros to temporarily suppress warning about the use of
// _Nullable and _Nonnull.
# define SWIFT_BEGIN_NULLABILITY_ANNOTATIONS                                   \
  _Pragma("clang diagnostic push")                                             \
  _Pragma("clang diagnostic ignored \"-Wnullability-extension\"")              \
  _Pragma("clang assume_nonnull begin")
# define SWIFT_END_NULLABILITY_ANNOTATIONS                                     \
  _Pragma("clang diagnostic pop")                                              \
  _Pragma("clang assume_nonnull end")

#else
// #define _Nullable and _Nonnull to nothing if we're not being built
// with a compiler that supports them.
# define _Nullable
# define _Nonnull
# define _Null_unspecified
# define SWIFT_BEGIN_NULLABILITY_ANNOTATIONS
# define SWIFT_END_NULLABILITY_ANNOTATIONS
#endif

#endif