/*
 * Platform.cpp
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

#include "fdbrpc/Platform.h"
#include <algorithm>
#include "flow/ActorCollection.h"
#include "flow/FaultInjection.h"


#ifdef _WIN32
#include <windows.h>
#undef max
#undef min
#include <io.h>
#include <psapi.h>
#include <stdio.h>
#include <conio.h>
#include <pdh.h>
#include <pdhmsg.h>
#pragma comment(lib, "pdh.lib")

// for SHGetFolderPath
#include <ShlObj.h>
#pragma comment(lib, "Shell32.lib")

#define CANONICAL_PATH_SEPARATOR '\\'
#endif

#ifdef __unixish__
#define CANONICAL_PATH_SEPARATOR '/'

#include <dirent.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <unistd.h>
#include <ftw.h>
#include <pwd.h>
#include <sched.h>
#include <cpuid.h>

#ifdef __APPLE__
#include <sys/uio.h>
#include <sys/syslimits.h>
#include <mach/mach.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <sys/sysctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/if_dl.h>
#include <net/route.h>

#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/storage/IOBlockStorageDriver.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/IOBSD.h>
#endif

#endif

extern bool onlyBeforeSimulatorInit();

namespace platform {

// Because the lambda used with nftw below cannot capture
int __eraseDirectoryRecurseiveCount;

int eraseDirectoryRecursive(std::string const& dir) {
	__eraseDirectoryRecurseiveCount = 0;
#ifdef _WIN32
	system( ("rd /s /q \"" + dir + "\"").c_str() );
#elif defined(__linux__) || defined(__APPLE__)
	int error =
		nftw(dir.c_str(),
			[](const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) -> int {
				int r = remove(fpath);
				if(r == 0)
					++__eraseDirectoryRecurseiveCount;
				return r;
			},
			64, FTW_DEPTH | FTW_PHYS);
	/* Looks like calling code expects this to continue silently if
	   the directory we're deleting doesn't exist in the first
	   place */
	if (error && errno != ENOENT) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "EraseDirectoryRecursiveError").detail("Directory", dir).GetLastError().error(e);
		throw e;
	}
#else
#error Port me!
#endif
	//INJECT_FAULT( platform_error, "eraseDirectoryRecursive" );
	return __eraseDirectoryRecurseiveCount;
}

std::string getDefaultConfigPath() {
#ifdef _WIN32
	TCHAR szPath[MAX_PATH];
	if( SHGetFolderPath(NULL, CSIDL_COMMON_APPDATA, NULL, 0, szPath)  != S_OK ) {
		TraceEvent(SevError, "WindowsAppDataError").GetLastError();
		throw platform_error();
	}
	std::string _filepath(szPath);
	return _filepath + "\\foundationdb";
#elif defined(__linux__)
	return "/etc/foundationdb";
#elif defined(__APPLE__)
	return "/usr/local/etc/foundationdb";
#else
	#error Port me!
#endif
}

bool isSse42Supported()
{
#if defined(_WIN32)
	int info[4];
	__cpuid(info, 1);
	return (info[2] & (1 << 20)) != 0;
#elif defined(__unixish__)
	uint32_t eax, ebx, ecx, edx, level = 1, count = 0;
	__cpuid_count(level, count, eax, ebx, ecx, edx);
	return ((ecx >> 20) & 1) != 0;
#else
	#error Port me!
#endif
}

std::string getDefaultClusterFilePath() {
	return joinPath(platform::getDefaultConfigPath(), "fdb.cluster");
}
}; // namespace platform
