/*
 * actorcompiler.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#pragma once

#include <boost/preprocessor.hpp>
#define FLOW_ACOMPILER_STATE 0
#define FLOW_EMPTY_STR

template<typename T> struct Future;
struct Never;
template<typename T> struct FutureStream;

// These are for intellisense to do proper type inferring, etc. They are no included at build time.
#ifndef NO_INTELLISENSE
#define ACTOR
#define DESCR
#define state
#define UNCANCELLABLE
#define choose if(1)
#define when(x) for(x;;)
template <class T> T wait( const Future<T>& );
void wait(const Never&);
template <class T> T waitNext( const FutureStream<T>& );
#endif

// These are for intellisense to do proper type inferring, etc. They are no
// included at build time.
#ifndef NO_INTELLISENSE
#define ACTOR BOOST_PP_IF(FLOW_ACOMPILER_STATE, FLOW_EMPTY_STR, ACTOR)
#define state BOOST_PP_IF(FLOW_ACOMPILER_STATE, FLOW_EMPTY_STR, state)
#define UNCANCELLABLE BOOST_PP_IF(FLOW_ACOMPILER_STATE, FLOW_EMPTY_STR, UNCANCELLABLE)
#define FLOW_CHOOSE if (1)
#define FLOW_WHEN(...) for (__VA_ARGS__;;)
#define choose BOOST_PP_IF(FLOW_ACOMPILER_STATE, FLOW_EMPTY_STR, FLOW_CHOSE)
#define when(...) BOOST_PP_IF(FLOW_ACOMPILER_STATE, FLOW_WHEN(__VA_ARGS__), when(__VA_ARGS__))
Void wait(const Never&);
template <class T>
typename T::type wait(const T&);
template <class T>
typename T::type waitNext(const T&);
#define COMBINE1(x, y) x##y
#define COMBINE(x, y) COMBINE1(x, y)
#define _uvar COMBINE(uniqueVar, __COUNTER__)
#else
#define _uvar _
#endif

#define loop while(true)

#pragma warning(disable : 4355) // 'this' : used in base member initializer list
