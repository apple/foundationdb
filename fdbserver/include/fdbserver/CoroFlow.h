/*
 * CoroFlow.h
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

// CoroFlow.h - Tools for interoperating with flow (futures etc) from coroutines instead of actors

#ifndef FDBSERVER_COROFLOW_H
#define FDBSERVER_COROFLOW_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "flow/IThreadPool.h"

class CoroThreadPool {
public:
	static void init();
	static void waitFor(Future<Void> what);

	static Reference<IThreadPool> createThreadPool(bool immediate = false);

protected:
	CoroThreadPool() {}
	~CoroThreadPool() {}
};

template <class T>
inline T waitForAndGet(Future<T> f) {
	if (!f.isReady())
		CoroThreadPool::waitFor(success(f));
	return f.get();
}

inline void waitFor(Future<Void> f) {
	CoroThreadPool::waitFor(f);
	if (f.isError())
		throw f.getError();
}

#endif