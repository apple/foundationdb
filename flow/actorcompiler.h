/*
 * actorcompiler.h
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

#ifdef POST_ACTOR_COMPILER
#ifndef FLOW_DEFINED_WAIT_AND_WAIT_NEXT
#define FLOW_DEFINED_WAIT_AND_WAIT_NEXT

// These should all be re-written by the actor compiler. We don't want to
// accidentally call them from something that's not an actor. `wait` is such a
// common identifier that `wait` calls outside ACTORs might accidentally
// compile.
template <class T>
T wait(const Future<T>&) = delete;
void wait(const Never&) = delete;
template <class T>
T waitNext(const FutureStream<T>&) = delete;

#endif
#endif

#ifndef POST_ACTOR_COMPILER

template <typename T>
struct Future;
struct Never;
template <typename T>
struct FutureStream;

// These are for intellisense to do proper type inferring, etc. They are no included at build time.
#ifndef NO_INTELLISENSE
#define ACTOR
#define DESCR
#define state
#define UNCANCELLABLE
#define choose if (1)
#define when(...) for (__VA_ARGS__;;)
template <class T>
T wait(const Future<T>&);
void wait(const Never&);
template <class T>
T waitNext(const FutureStream<T>&);
#endif

#endif

#define loop while (true)

#ifdef NO_INTELLISENSE
#define THIS this
#define THIS_ADDR uintptr_t(this)
#else
#define THIS nullptr
#define THIS_ADDR uintptr_t(nullptr)
#endif

#ifdef _MSC_VER
#pragma warning(disable : 4355) // 'this' : used in base member initializer list
#endif
