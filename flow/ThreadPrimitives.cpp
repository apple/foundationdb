/*
 * ThreadPrimitives.cpp
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

#include "flow/ThreadPrimitives.h"
#include "flow/Trace.h"
#include <stdint.h>
#include <iostream>
#include <errno.h>
#include <string.h>
#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#undef min
#undef max
#endif

extern std::string format(const char* form, ...);

Event::Event() {
#ifdef _WIN32
	ev = CreateEvent(nullptr, FALSE, FALSE, nullptr);
#elif defined(__linux__) || defined(__FreeBSD__)
	int result = sem_init(&sem, 0, 0);
	if (result)
		criticalError(FDB_EXIT_INIT_SEMAPHORE,
		              "UnableToInitializeSemaphore",
		              format("Could not initialize semaphore - %s", strerror(errno)).c_str());
#elif defined(__APPLE__)
	self = mach_task_self();
	kern_return_t ret = semaphore_create(self, &sem, SYNC_POLICY_FIFO, 0);
	if (ret != KERN_SUCCESS)
		criticalError(FDB_EXIT_INIT_SEMAPHORE,
		              "UnableToInitializeSemaphore",
		              format("Could not initialize semaphore - %s", strerror(errno)).c_str());
#else
#error Port me!
#endif
}

Event::~Event() {
#ifdef _WIN32
	CloseHandle(ev);
#elif defined(__linux__) || defined(__FreeBSD__)
	sem_destroy(&sem);
#elif defined(__APPLE__)
	semaphore_destroy(self, sem);
#else
#error Port me!
#endif
}

void Event::set() {
#ifdef _WIN32
	SetEvent(ev);
#elif defined(__linux__) || defined(__FreeBSD__)
	sem_post(&sem);
#elif defined(__APPLE__)
	semaphore_signal(sem);
#else
#error Port me!
#endif
}

void Event::block() {
#ifdef _WIN32
	WaitForSingleObject(ev, INFINITE);
#elif defined(__linux__) || defined(__FreeBSD__)
	int ret;
	do {
		ret = sem_wait(&sem);
	} while (ret != 0 && errno == EINTR);
#elif defined(__APPLE__)
	kern_return_t ret;
	do {
		ret = semaphore_wait(sem);
	} while ((ret != KERN_SUCCESS) && (ret != KERN_TERMINATED));
#else
#error Port me!
#endif
}

// Mutex::impl is allocated from the heap so that its size doesn't need to be in the header.  Is there a better way?
Mutex::Mutex() {
	impl = new CRITICAL_SECTION;
	InitializeCriticalSection((CRITICAL_SECTION*)impl);
}
Mutex::~Mutex() {
	DeleteCriticalSection((CRITICAL_SECTION*)impl);
	delete (CRITICAL_SECTION*)impl;
}
void Mutex::enter() {
	EnterCriticalSection((CRITICAL_SECTION*)impl);
}
void Mutex::leave() {
	LeaveCriticalSection((CRITICAL_SECTION*)impl);
}
