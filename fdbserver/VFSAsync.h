/*
 * VFSAsync.h
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

#include "sqlite/sqlite3.h"
#include <string>
#include <map>
#include "fdbrpc/IAsyncFile.h"
#include "fdbrpc/simulator.h"

/*
** When using this VFS, the sqlite3_file* handles that SQLite uses are
** actually pointers to instances of type VFSAsyncFile.
*/
typedef struct VFSAsyncFile VFSAsyncFile;
struct VFSAsyncFile {
	sqlite3_file base; /* Base class. Must be first. */
	int flags;
	std::string filename;
	Reference<IAsyncFile> file;

	// The functions setInjectedError() and checkInjectedError() use an INetwork global to store the last
	// return code from VFSAsyncFile method resulting from catching an injected Error exception.  This allows
	// callers of the SQLite API to determine if an error code returned appears to be due to an injected
	// error in simulation.
	//
	// This scheme is not perfect, as it is possible for non-injected errors to occur after injected errors
	// and be incorrectly recognized as injected.  This problem already existed, however, as the previous scheme
	// assumed that any SQLite error that occurred after any injected error in the simulated process was
	// itself injected.
	//
	// Unfortunately, there is no easy or reliable way to plumb the injectedness of an error though the return
	// path of VFSAsyncFile -> SQLite -> SQLite API calls made by KeyValueStoreSQLite.
	//
	// An attempt was made to store injected errors in VFSAsyncFile instances and expose a SQLiteDB's file
	// instances via the SQLite API.  This failed, however, because sometimes files are opened, encounter an
	// error, and are closed within the lifetime of one SQLite API call so the caller never has an opportunity
	// to access the VFSAsyncFile object or its injected error state.
	//
	// An attempt was also made to match SQLite API return codes to VFSAsyncFile injected error return codes on
	// a 1:1 basis, only flagging a code as rejected if it matches the last injected error code and only once.
	// This would have been more accurate (though coincidences could occur).  This scheme also failed, however,
	// because sometimes errors from SQLite APIs are temporarily ignored, relying on a subsequent API call to error,
	// however this error could be different and would not have been produced directly by VFSAsyncFile.
	//
	static void setInjectedError(int64_t rc) {
		g_network->setGlobal(INetwork::enSQLiteInjectedError, (flowGlobalType)rc);
		TraceEvent("VFSSetInjectedError").detail("ErrorCode", rc).detail("NetworkPtr", (void*)g_network).backtrace();
	}

	static bool checkInjectedError() {
		// Error code is only checked for non-zero because the SQLite API error code after an injected error
		// may not match the error code returned by VFSAsyncFile when the inject error occurred.
		bool e = g_network->global(INetwork::enSQLiteInjectedError) != (flowGlobalType)0;
		bool f = g_simulator.checkInjectedCorruption();
		TraceEvent("VFSCheckInjectedError")
		    .detail("InjectedIOError", e)
		    .detail("InjectedCorruption", f)
		    .detail("ErrorCode", (int64_t)g_network->global(INetwork::enSQLiteInjectedError))
		    .backtrace();
		return e || f;
	}

	uint32_t* const pLockCount; // +1 for each SHARED_LOCK, or 1+X_COUNT for lock level X
	int lockLevel; // NO_LOCK, SHARED_LOCK, RESERVED_LOCK, PENDING_LOCK, or EXCLUSIVE_LOCK

	struct SharedMemoryInfo* sharedMemory;
	int sharedMemorySharedLocks;
	int sharedMemoryExclusiveLocks;

	int debug_zcrefs, debug_zcreads, debug_reads;

	int chunkSize;

	VFSAsyncFile(std::string const& filename, int flags);
	~VFSAsyncFile();

	static std::map<std::string, std::pair<uint32_t, int>> filename_lockCount_openCount;
};
