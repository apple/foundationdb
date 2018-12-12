/*
 * ThreadPrimitives.h
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

#ifndef FLOW_THREADPRIMITIVES_H
#define FLOW_THREADPRIMITIVES_H
#pragma once

#include "Error.h"
#include "Trace.h"

#ifdef __linux__
#include <semaphore.h>
#endif

#ifdef __APPLE__
#include <mach/mach_init.h>
#include <mach/task.h>
#include <mach/semaphore.h>
#include <mach/sync_policy.h>
#include <mach/mach_error.h>
#include <mach/clock_types.h>
#endif

#if VALGRIND
#include <drd.h>
#endif

class ThreadSpinLock {
public:
// #ifdef _WIN32
	ThreadSpinLock(bool initiallyLocked=false) : isLocked(initiallyLocked) {
#if VALGRIND
		ANNOTATE_RWLOCK_CREATE(this);
#endif
	}
	~ThreadSpinLock() {
#if VALGRIND
		ANNOTATE_RWLOCK_DESTROY(this);
#endif
	}
	void enter() {
		while (interlockedCompareExchange(&isLocked, 1, 0) == 1)
			_mm_pause();
#if VALGRIND
		ANNOTATE_RWLOCK_ACQUIRED(this, true);
#endif
	}
	void leave() {
#if defined(__linux__)
	__sync_synchronize();
#endif
		isLocked = 0;
#if defined(__linux__)
	__sync_synchronize();
#endif
#if VALGRIND
		ANNOTATE_RWLOCK_RELEASED(this, true);
#endif
	}
	void assertNotEntered() {
		ASSERT( !isLocked );
	}
private:
	ThreadSpinLock(const ThreadSpinLock&);
	void operator=(const ThreadSpinLock&);
	volatile int32_t isLocked;
};

class ThreadSpinLockHolder {
	ThreadSpinLock& lock;
public:
	ThreadSpinLockHolder( ThreadSpinLock& lock ) : lock(lock) { lock.enter(); }
	~ThreadSpinLockHolder() { lock.leave(); }
};

class ThreadUnsafeSpinLock { public: void enter(){}; void leave(){}; void assertNotEntered(){}; };
class ThreadUnsafeSpinLockHolder { public: ThreadUnsafeSpinLockHolder(ThreadUnsafeSpinLock&){}; };

#if FLOW_THREAD_SAFE

typedef ThreadSpinLock SpinLock;
typedef ThreadSpinLockHolder SpinLockHolder;

#else

typedef ThreadUnsafeSpinLock SpinLock;
typedef ThreadUnsafeSpinLockHolder SpinLockHolder;

#endif

class Event {
public:
	Event();
	~Event();
	void set();
	void block();

private:
#ifdef _WIN32
	void* ev;
#elif defined(__linux__)
	sem_t sem;
#elif defined(__APPLE__)
	mach_port_t self;
	semaphore_t sem;
#else
#error Port me!
#endif	
};

class Mutex
{
	// A re-entrant process-local blocking lock (e.g. CRITICAL_SECTION on Windows)
	// Thread safe even if !FLOW_THREAD_SAFE
public:
	Mutex();
	~Mutex();
	void enter();
	void leave();
private:
	void* impl;
};

class MutexHolder {
	Mutex& lock;
public:
	MutexHolder( Mutex& lock ) : lock(lock) { lock.enter(); }
	~MutexHolder() { lock.leave(); }
};

#endif
