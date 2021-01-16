/*
 * VFSAsync.h
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

#include "sqlite/sqlite3.h"
#include <string>
#include <map>
#include "fdbrpc/IAsyncFile.h"
#include "fdbserver/CoroFlow.h"

//#include <assert.h>
//#include <string.h>

//#ifdef WIN32
//#include <Windows.h>
//#endif

#ifdef __unixish__
//#include <sys/types.h>
//#include <sys/stat.h>
//#include <sys/file.h>
//#include <sys/param.h>
//#include <sys/time.h>
//#include <unistd.h>
//#include <errno.h>
//#include <fcntl.h>
#endif

/*
** When using this VFS, the sqlite3_file* handles that SQLite uses are
** actually pointers to instances of type VFSAsyncFile.
*/
typedef struct VFSAsyncFile VFSAsyncFile;
struct VFSAsyncFile {
	sqlite3_file base;              /* Base class. Must be first. */
	int flags;
	std::string filename;
	Reference<IAsyncFile> file;
	bool errorInjected;

    bool consumeInjectedError() {
        bool e = errorInjected;
        errorInjected = false;
        return e;
    }

	uint32_t * const pLockCount;  // +1 for each SHARED_LOCK, or 1+X_COUNT for lock level X
	int lockLevel;    // NO_LOCK, SHARED_LOCK, RESERVED_LOCK, PENDING_LOCK, or EXCLUSIVE_LOCK

	struct SharedMemoryInfo *sharedMemory;
	int sharedMemorySharedLocks;
	int sharedMemoryExclusiveLocks;

	int debug_zcrefs, debug_zcreads, debug_reads;

	int chunkSize;

	VFSAsyncFile(std::string const& filename, int flags);
	~VFSAsyncFile();

	static std::map<std::string, std::pair<uint32_t,int>> filename_lockCount_openCount;
};
