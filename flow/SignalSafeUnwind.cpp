/*
 * SignalSafeUnwind.cpp
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

#include "SignalSafeUnwind.h"

int64_t dl_iterate_phdr_calls = 0;

#ifdef __linux__

#include <link.h>

static bool phdr_cache_initialized = false;
static std::vector< std::vector<uint8_t> > phdr_cache;

static int (*chain_dl_iterate_phdr)(
          int (*callback) (struct dl_phdr_info *info,
                           size_t size, void *data),
          void *data) = 0;

static int phdr_cache_add( struct dl_phdr_info *info, size_t size, void *data ) {
    phdr_cache.push_back( std::vector<uint8_t>((uint8_t*)info, (uint8_t*)info + size) );
    return 0;
}

static void initChain() {
    // Ensure that chain_dl_iterate_phdr points to the "real" function that we are overriding
    *(void**)&chain_dl_iterate_phdr = dlsym(RTLD_NEXT, "dl_iterate_phdr");
    if (!chain_dl_iterate_phdr)
        criticalError(FDB_EXIT_ERROR, "SignalSafeUnwindError", "Unable to find dl_iterate_phdr symbol");
}

void initSignalSafeUnwind() {
    initChain();

    phdr_cache.clear();
    if (chain_dl_iterate_phdr(&phdr_cache_add, 0))
        criticalError(FDB_EXIT_ERROR, "DLIterateError", "dl_iterate_phdr error");
    phdr_cache_initialized = true;
}

// This overrides the function in libc!
extern "C" int dl_iterate_phdr(
          int (*callback) (struct dl_phdr_info *info,
                           size_t size, void *data),
          void *data)
{
    interlockedIncrement64(&dl_iterate_phdr_calls);

    if (phdr_cache_initialized)
    {
        // This path should be async signal safe
        for(int i=0; i<phdr_cache.size(); i++)
        {
            int r = callback( (struct dl_phdr_info*)&phdr_cache[i][0], phdr_cache[i].size(), data );
            if (r!=0)
                return r;
        }
        return 0;
    } else {
        // This path is NOT async signal safe, and serves until and unless initSignalSafeUnwind() is called
        initChain();

		setProfilingEnabled(0);
        int result = chain_dl_iterate_phdr(callback, data);
		setProfilingEnabled(1);
		return result;
    }
}

#else  // __linux__

void initSignalSafeUnwind() {}

#endif