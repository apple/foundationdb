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

#ifndef POST_ACTOR_COMPILER

#include "flow.h"

// These are for intellisense to do proper type inferring, etc. They are no included at build time.
#ifndef NO_INTELLISENSE
#define ACTOR
#define DESCR
#define state
#define UNCANCELLABLE
#define choose if(1)
#define when(x) for(x;;)
template <class T> T wait( const Future<T>& );
template <class T> T waitNext( const FutureStream<T>& );
#endif

#endif

#include "flow.h"
#define loop while(true)

#pragma warning( disable: 4355 )	// 'this' : used in base member initializer list