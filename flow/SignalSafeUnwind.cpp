/*
 * SignalSafeUnwind.cpp
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

#include "flow/SignalSafeUnwind.h"

int64_t dl_iterate_phdr_calls = 0;

#if defined(__linux__) && !defined(USE_SANITIZER)

#include <link.h>
#include <mutex>

static int (*chain_dl_iterate_phdr)(int (*callback)(struct dl_phdr_info* info, size_t size, void* data),
                                    void* data) = nullptr;

static void initChain() {
	static std::once_flag flag;

	// Ensure that chain_dl_iterate_phdr points to the "real" function that we are overriding
	std::call_once(flag, []() { *(void**)&chain_dl_iterate_phdr = dlsym(RTLD_NEXT, "dl_iterate_phdr"); });

	if (!chain_dl_iterate_phdr) {
		criticalError(FDB_EXIT_ERROR, "SignalSafeUnwindError", "Unable to find dl_iterate_phdr symbol");
	}
}

// This overrides the function in libc!
extern "C" int dl_iterate_phdr(int (*callback)(struct dl_phdr_info* info, size_t size, void* data), void* data) {
	interlockedIncrement64(&dl_iterate_phdr_calls);

	initChain();

	setProfilingEnabled(0);
	int result = chain_dl_iterate_phdr(callback, data);
	setProfilingEnabled(1);
	return result;
}
#endif