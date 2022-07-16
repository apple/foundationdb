/*
 * ThreadPrimitives.h
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

#ifndef FLOW_THREADPRIMITIVES_H
#define FLOW_THREADPRIMITIVES_H
#pragma once

#include <atomic>
#include <array>

#include "flow/Error.h"
#include "flow/Trace.h"

#if defined(__linux__) || defined(__FreeBSD__)
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

// TODO: We should make this dependent on the CPU. Maybe cmake
// can set this variable properly?
constexpr size_t MAX_CACHE_LINE_SIZE = 64;

class alignas(MAX_CACHE_LINE_SIZE) ThreadSpinLock {
public:
	// #ifdef _WIN32
	ThreadSpinLock() {
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
		while (isLocked.test_and_set(std::memory_order_acquire))
#if defined(__aarch64__)
			__asm__ volatile("isb");
#elif defined(__powerpc64__)
			__asm__ volatile("or 27,27,27" ::: "memory");
#else
			_mm_pause();
#endif
#if VALGRIND
		ANNOTATE_RWLOCK_ACQUIRED(this, true);
#endif
	}
	void leave() {
		isLocked.clear(std::memory_order_release);
#if VALGRIND
		ANNOTATE_RWLOCK_RELEASED(this, true);
#endif
	}
	void assertNotEntered() {
		ASSERT(!isLocked.test_and_set(std::memory_order_acquire));
		isLocked.clear(std::memory_order_release);
	}

private:
	ThreadSpinLock(const ThreadSpinLock&);
	void operator=(const ThreadSpinLock&);
	std::atomic_flag isLocked = ATOMIC_FLAG_INIT;
	// We want a spin lock to occupy a cache line in order to
	// prevent false sharing.
	std::array<uint8_t, MAX_CACHE_LINE_SIZE - sizeof(isLocked)> padding;
};

class ThreadSpinLockHolder {
	ThreadSpinLock& lock;

public:
	ThreadSpinLockHolder(ThreadSpinLock& lock) : lock(lock) { lock.enter(); }
	~ThreadSpinLockHolder() { lock.leave(); }
};

class ThreadUnsafeSpinLock {
public:
	void enter(){};
	void leave(){};
	void assertNotEntered(){};
};
class ThreadUnsafeSpinLockHolder {
public:
	ThreadUnsafeSpinLockHolder(ThreadUnsafeSpinLock&){};
};

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
#elif defined(__linux__) || defined(__FreeBSD__)
	sem_t sem;
#elif defined(__APPLE__)
	mach_port_t self;
	semaphore_t sem;
#else
#error Port me!
#endif
};

class Mutex {
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
	MutexHolder(Mutex& lock) : lock(lock) { lock.enter(); }
	~MutexHolder() { lock.leave(); }
};

#endif
