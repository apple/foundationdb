/*
 * ThreadCleanup.cpp
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

#include "flow/Platform.h"
#include "flow/FastAlloc.h"

#if defined(WIN32)

#include <Windows.h>

BOOL WINAPI DllMain( HINSTANCE dll, DWORD reason, LPVOID reserved ) {

	if (reason == DLL_THREAD_DETACH)
		releaseAllThreadMagazines();
	return TRUE;
}

#elif defined( __unixish__ )

#ifdef __INTEL_COMPILER
#pragma warning ( disable:2415 )
#endif

static pthread_key_t threadDestructorKey;

static void threadDestructor(void*) {
	releaseAllThreadMagazines();
}

void registerThread() {
	pthread_setspecific( threadDestructorKey, (const void*)1 );
}

static int initThreadDestructorKey() {
	if (!pthread_key_create(&threadDestructorKey, &threadDestructor)) {
		registerThread();
		setFastAllocatorThreadInitFunction( &registerThread );
	}

	return 0;
}

static int threadDestructorKeyInit = initThreadDestructorKey();

#else
#error Port me!
#endif
