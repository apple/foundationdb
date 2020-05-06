/*
 * Platform.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifdef _WIN32
// This has to come as the first include on Win32 for rand_s() to be found
#define _CRT_RAND_S
#include <stdlib.h>
#include <math.h> // For _set_FMA3_enable workaround in platformInit
#endif

#include "flow/Platform.h"
#include "flow/Platform.actor.h"
#include "flow/Arena.h"
#include "flow/Trace.h"
#include "flow/Error.h"
#include "flow/Knobs.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <algorithm>

#include "flow/FaultInjection.h"

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#include <winioctl.h>
#include <io.h>
#include <psapi.h>
#include <stdio.h>
#include <conio.h>
#include <direct.h>
#include <pdh.h>
#include <pdhmsg.h>
#pragma comment(lib, "pdh.lib")

// for SHGetFolderPath
#include <ShlObj.h>
#pragma comment(lib, "Shell32.lib")

//#define CANONICAL_PATH_SEPARATOR '\\'
//#define PATH_MAX MAX_PATH
#endif

#ifdef __unixish__
#include <dirent.h>
#include "flow/stacktrace.h"
#endif

#include "flow/actorcompiler.h"  // This must be the last #include.


using std::vector;

Future<vector<std::string>> listFilesAsync( std::string const& directory, std::string const& extension ) {
	return findFiles( directory, extension, false /* directoryOnly */, true );
}

Future<vector<std::string>> listDirectoriesAsync( std::string const& directory ) {
	return findFiles( directory, "", true /* directoryOnly */, true );
}

ACTOR Future<Void> findFilesRecursivelyAsync(std::string path, vector<std::string> *out) {
	// Add files to output, prefixing path
	state vector<std::string> files = wait(listFilesAsync(path));
	for(auto const &f : files)
		out->push_back(joinPath(path, f));

	// Recurse for directories
	state vector<std::string> directories = wait(listDirectoriesAsync(path));
	for(auto const &dir : directories) {
		if(dir != "." && dir != "..")
			wait(findFilesRecursivelyAsync(joinPath(path, dir), out));
	}
    return Void();
}


#ifdef _WIN32


bool acceptFile( FILE_ATTRIBUTE_DATA fileAttributes, std::string const& name, std::string const& extension ) {
	return !(fileAttributes & FILE_ATTRIBUTE_DIRECTORY) && StringRef(name).endsWith(extension);
}

bool acceptDirectory( FILE_ATTRIBUTE_DATA fileAttributes, std::string const& name, std::string const& extension ) {
	return (fileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
}

ACTOR Future<vector<std::string>> findFiles( std::string directory, std::string extension,
                                                  bool directoryOnly, bool async) {
	INJECT_FAULT( platform_error, "findFiles" );
	state vector<std::string> result;
    state int64_t tsc_begin = __rdtsc();


	state WIN32_FIND_DATA fd;
	state HANDLE h = FindFirstFile( (directory + "/*" + extension).c_str(), &fd );
	if (h == INVALID_HANDLE_VALUE) {
		if (GetLastError() != ERROR_FILE_NOT_FOUND && GetLastError() != ERROR_PATH_NOT_FOUND) {
			TraceEvent(SevError, "FindFirstFile").detail("Directory", directory).detail("Extension", extension).GetLastError();
			throw platform_error();
		}
	} else {
		loop {
			std::string name = fd.cFileName;
            if ((directoryOnly && acceptDirectory(fd.dwFileAttributes, name, extension)) ||
                (!directoryOnly && acceptFile(fd.dwFileAttributes, name, extension))) {
				result.push_back( name );
			}
			if (!FindNextFile( h, &fd ))
				break;
            if (async && __rdtsc() - tsc_begin > FLOW_KNOBS->TSC_YIELD_TIME) {
                wait( yield() );
                tsc_begin = __rdtsc();
            }

		}
		if (GetLastError() != ERROR_NO_MORE_FILES) {
			TraceEvent(SevError, "FindNextFile").detail("Directory", directory).detail("Extension", extension).GetLastError();
			FindClose(h);
			throw platform_error();
		}
		FindClose(h);
	}
	std::sort(result.begin(), result.end());
	return result;
}

#elif (defined(__linux__) || defined(__APPLE__))

bool acceptFile( FILE_ATTRIBUTE_DATA fileAttributes, std::string const& name, std::string const& extension ) {
	return S_ISREG(fileAttributes) && StringRef(name).endsWith(extension);
}

bool acceptDirectory( FILE_ATTRIBUTE_DATA fileAttributes, std::string const& name, std::string const& extension ) {
	return S_ISDIR(fileAttributes);
}

ACTOR Future<vector<std::string>> findFiles( std::string directory, std::string extension,
                                                  bool directoryOnly, bool async) {
	INJECT_FAULT( platform_error, "findFiles" );
	state vector<std::string> result;
    state int64_t tsc_begin = __rdtsc();

	state DIR *dip = NULL;

	if ((dip = opendir(directory.c_str())) != NULL) {
        loop {
            struct dirent *dit;
            dit = readdir(dip);
            if (dit == NULL) {
                break;
            }
			std::string name(dit->d_name);
			struct stat buf;
			if (stat(joinPath(directory, name).c_str(), &buf)) {
				bool isError = errno != ENOENT;
				TraceEvent(isError ? SevError : SevWarn, "StatFailed")
					.detail("Directory", directory)
					.detail("Extension", extension)
					.detail("Name", name)
					.GetLastError();
				if( isError )
					throw platform_error();
				else
					continue;
			}

            if ((directoryOnly && acceptDirectory(buf.st_mode, name, extension)) ||
                (!directoryOnly && acceptFile(buf.st_mode, name, extension))) {
				result.push_back( name );
            }

            if (async && __rdtsc() - tsc_begin > FLOW_KNOBS->TSC_YIELD_TIME) {
                wait( yield() );
                tsc_begin = __rdtsc();
            }
		}

		closedir(dip);
	}
	std::sort(result.begin(), result.end());
	return result;
}


#else
	#error Port me!
#endif
