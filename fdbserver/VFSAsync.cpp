/*
 * VFSAsync.cpp
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
#include <stdio.h>
#include <string>
#include <vector>
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbserver/CoroFlow.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"

#include <assert.h>
#include <string.h>

#ifdef WIN32
#include <Windows.h>
#endif

#ifdef __unixish__
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#endif

#include "fdbserver/VFSAsync.h"

/*
** The maximum pathname length supported by this VFS.
*/
#define MAXPATHNAME 512

#define NO_LOCK 0
#define SHARED_LOCK 1
#define RESERVED_LOCK 2
#define PENDING_LOCK 3
#define EXCLUSIVE_LOCK 4
const uint32_t RESERVED_COUNT = 1U << 29;

VFSAsyncFile::VFSAsyncFile(std::string const& filename, int flags)
  : flags(flags), filename(filename), pLockCount(&filename_lockCount_openCount[filename].first), debug_zcrefs(0),
    debug_zcreads(0), debug_reads(0), chunkSize(0) {
	filename_lockCount_openCount[filename].second++;

	TraceEvent(SevDebug, "VFSAsyncFileConstruct")
	    .detail("Filename", filename)
	    .detail("OpenCount", filename_lockCount_openCount[filename].second)
	    .detail("LockCount", filename_lockCount_openCount[filename].first)
	    .backtrace();
}

std::map<std::string, std::pair<uint32_t, int>> VFSAsyncFile::filename_lockCount_openCount;

static int asyncClose(sqlite3_file* pFile) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;

	TraceEvent(SevDebug, "VFSAsyncFileDestroy").detail("Filename", p->filename).backtrace();

	// printf("Closing %s: %d zcrefs, %d/%d reads zc\n", filename.c_str(), debug_zcrefs, debug_zcreads,
	// debug_zcreads+debug_reads);
	ASSERT(!p->debug_zcrefs);

	p->~VFSAsyncFile();
	return SQLITE_OK;
}

static int asyncRead(sqlite3_file* pFile, void* zBuf, int iAmt, sqlite_int64 iOfst) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		++p->debug_reads;
		int readBytes = waitForAndGet(p->file->read(zBuf, iAmt, iOfst));
		if (readBytes < iAmt) {
			memset((uint8_t*)zBuf + readBytes, 0, iAmt - readBytes); // When reading past the EOF, sqlite expects the
			                                                         // extra portion of the buffer to be zeroed
			return SQLITE_IOERR_SHORT_READ;
		}
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_READ);
		}
		return SQLITE_IOERR_READ;
	}
}

#if 1
static int asyncReleaseZeroCopy(sqlite3_file* pFile, void* data, int iAmt, sqlite_int64 iOfst) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		--p->debug_zcrefs;
		p->file->releaseZeroCopy(data, iAmt, iOfst);
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR);
		}
		return SQLITE_IOERR;
	}
	return SQLITE_OK;
}

static int asyncReadZeroCopy(sqlite3_file* pFile, void** data, int iAmt, sqlite_int64 iOfst, int* pDataWasCached) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		int readBytes = iAmt;
		Future<Void> readFuture = p->file->readZeroCopy(data, &readBytes, iOfst);
		if (pDataWasCached)
			*pDataWasCached = readFuture.isReady() ? 1 : 0;
		waitFor(readFuture);
		++p->debug_zcrefs;
		if (readBytes < iAmt) {
			// When reading past the EOF, sqlite expects the extra portion of the buffer to be zeroed.  We can't do
			// that, so return and sqlite will use the slow path.
			asyncReleaseZeroCopy(pFile, *data, readBytes, iOfst);
			return SQLITE_IOERR_SHORT_READ;
		}
		++p->debug_zcreads;
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_READ);
		}
		return SQLITE_IOERR_READ;
	}
}

#else
static int asyncReadZeroCopy(sqlite3_file* pFile, void** data, int iAmt, sqlite_int64 iOfst) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		*data = new char[iAmt];
		int readBytes = waitForAndGet(p->file->read(*data, iAmt, iOfst));
		// printf("+asyncReadRef %p +%lld %d/%d = %p\n", pFile, iOfst, readBytes, iAmt, *data);
		if (readBytes < iAmt) {
			memset((uint8_t*)*data + readBytes, 0, iAmt - readBytes); // When reading past the EOF, sqlite expects the
			                                                          // extra portion of the buffer to be zeroed
			return SQLITE_IOERR_SHORT_READ;
		}
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_READ);
		}
		return SQLITE_IOERR_READ;
	}
}
static int asyncReleaseZeroCopy(sqlite3_file* pFile, void* data, int iAmt, sqlite_int64 iOfst) {
	// printf("-asyncReleaseRef %p +%lld %d <= %p\n", pFile, iOfst, iAmt, data);
	delete[](char*) data;
	return SQLITE_OK;
}
#endif

static int asyncWrite(sqlite3_file* pFile, const void* zBuf, int iAmt, sqlite_int64 iOfst) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		waitFor(p->file->write(zBuf, iAmt, iOfst));
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_WRITE);
		}
		return SQLITE_IOERR_WRITE;
	}
}

static int asyncTruncate(sqlite3_file* pFile, sqlite_int64 size) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;

	// Adjust size to a multiple of chunkSize if set
	if (p->chunkSize != 0) {
		size = ((size + p->chunkSize - 1) / p->chunkSize) * p->chunkSize;
	}

	try {
		waitFor(p->file->truncate(size));
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_TRUNCATE);
		}
		return SQLITE_IOERR_TRUNCATE;
	}
}

static int asyncSync(sqlite3_file* pFile, int flags) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		waitFor(p->file->sync());
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_FSYNC);
		}

		TraceEvent("VFSAsyncFileSyncError")
		    .error(e)
		    .detail("Filename", p->filename)
		    .detail("Sqlite3File", (int64_t)pFile)
		    .detail("IAsyncFile", (int64_t)p->file.getPtr());

		return SQLITE_IOERR_FSYNC;
	}
}

/*
** Write the size of the file in bytes to *pSize.
*/
static int VFSAsyncFileSize(sqlite3_file* pFile, sqlite_int64* pSize) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	try {
		*pSize = waitForAndGet(p->file->size());
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR_FSTAT);
		}
		return SQLITE_IOERR_FSTAT;
	}
}

static int asyncLock(sqlite3_file* pFile, int eLock) {
	// VFSAsyncFile *p = (VFSAsyncFile*)pFile;

	//TraceEvent("FileLock").detail("File", p->filename).detail("Fd", p->file->debugFD()).detail("PrevLockLevel", p->lockLevel).detail("Op", eLock).detail("LockCount", *p->pLockCount);

	return eLock == EXCLUSIVE_LOCK ? SQLITE_BUSY : SQLITE_OK;
}
static int asyncUnlock(sqlite3_file* pFile, int eLock) {
	assert(eLock <= SHARED_LOCK);

	return SQLITE_OK;
}
static int asyncCheckReservedLock(sqlite3_file* pFile, int* pResOut) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	*pResOut = *p->pLockCount >= RESERVED_COUNT;
	return SQLITE_OK;
}

/*
** No xFileControl() verbs are implemented by this VFS.
*/
static int VFSAsyncFileControl(sqlite3_file* pFile, int op, void* pArg) {
	VFSAsyncFile* p = (VFSAsyncFile*)pFile;
	switch (op) {
	case SQLITE_FCNTL_CHUNK_SIZE:
		p->chunkSize = *(int*)pArg;
		return SQLITE_OK;

	case SQLITE_FCNTL_SIZE_HINT:
		return asyncTruncate(pFile, *(int64_t*)pArg);

	default:
		return SQLITE_NOTFOUND;
	};
}

static int asyncSectorSize(sqlite3_file* pFile) {
	return 512;
} // SOMEDAY: Would 4K be better?
static int asyncDeviceCharacteristics(sqlite3_file* pFile) {
	return 0;
}

#if 1
struct SharedMemoryInfo { // for a file
	std::string filename;
	std::vector<void*> regions;
	int regionSize;
	int refcount; // Number of connections with this open
	int sharedLocks[SQLITE_SHM_NLOCK];
	int exclusiveLocks[SQLITE_SHM_NLOCK];

	SharedMemoryInfo() : regionSize(0), refcount(0) {
		memset(sharedLocks, 0, sizeof(sharedLocks));
		memset(exclusiveLocks, 0, sizeof(exclusiveLocks));
	}
	void cleanup() {
		for (int i = 0; i < regions.size(); i++)
			delete[](uint8_t*) regions[i];
		table.erase(filename);
	}

	static Mutex mutex;
	static std::map<std::string, SharedMemoryInfo> table;
};
Mutex SharedMemoryInfo::mutex;
std::map<std::string, SharedMemoryInfo> SharedMemoryInfo::table;

/*
** This function is called to obtain a pointer to region iRegion of the
** shared-memory associated with the database file fd. Shared-memory regions
** are numbered starting from zero. Each shared-memory region is szRegion
** bytes in size.
**
** If an error occurs, an error code is returned and *pp is set to nullptr.
**
** Otherwise, if the bExtend parameter is 0 and the requested shared-memory
** region has not been allocated (by any client, including one running in a
** separate process), then *pp is set to nullptr and SQLITE_OK returned. If
** bExtend is non-zero and the requested shared-memory region has not yet
** been allocated, it is allocated by this function.
**
** If the shared-memory region has already been allocated or is allocated by
** this call as described above, then it is mapped into this processes
** address space (if it is not already), *pp is set to point to the mapped
** memory and SQLITE_OK returned.
*/
static int asyncShmMap(sqlite3_file* fd, /* Handle open on database file */
                       int iRegion, /* Region to retrieve */
                       int szRegion, /* Size of regions */
                       int bExtend, /* True to extend file if necessary */
                       void volatile** pp /* OUT: Mapped memory */
) {
	MutexHolder hold(SharedMemoryInfo::mutex);

	VFSAsyncFile* pDbFd = (VFSAsyncFile*)fd;
	SharedMemoryInfo* memInfo = pDbFd->sharedMemory;
	if (!memInfo) {
		std::string filename = pDbFd->filename;
		memInfo = pDbFd->sharedMemory = &SharedMemoryInfo::table[filename];
		memInfo->filename = filename;
		memInfo->regionSize = szRegion;
		++memInfo->refcount;
		// printf("Shared memory for: '%s' (%d refs)\n", filename.c_str(), memInfo->refcount);
	} else {
		assert(memInfo->regionSize == szRegion);
	}

	if (iRegion >= memInfo->regions.size()) {
		if (!bExtend) {
			*pp = nullptr;
			return SQLITE_OK;
		}
		while (memInfo->regions.size() <= iRegion) {
			void* mem = new uint8_t[szRegion];
			memset(mem, 0, szRegion);
			memInfo->regions.push_back(mem);
		}
	}
	*pp = memInfo->regions[iRegion];
	return SQLITE_OK;
}

/*
** Change the lock state for a shared-memory segment.
**
** Note that the relationship between SHAREd and EXCLUSIVE locks is a little
** different here than in posix.  In xShmLock(), one can go from unlocked
** to shared and back or from unlocked to exclusive and back.  But one may
** not go from shared to exclusive or from exclusive to shared.
*/
// sqlite doesn't seem to match these up correctly - it happily calls unlock on locks it doesn't hold.
// So we have to keep track of which locks are held by a given sqlite3_file
static int asyncShmLock(sqlite3_file* fd, /* Database file holding the shared memory */
                        int ofst, /* First lock to acquire or release */
                        int n, /* Number of locks to acquire or release */
                        int flags /* What to do with the lock */
) {
	assert(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);
	assert(n >= 1);
	assert(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) || flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) || flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));
	assert(n == 1 || (flags & SQLITE_SHM_EXCLUSIVE) != 0);

	MutexHolder hold(SharedMemoryInfo::mutex);

	VFSAsyncFile* pDbFd = (VFSAsyncFile*)fd;
	SharedMemoryInfo* memInfo = pDbFd->sharedMemory;

	if (flags & SQLITE_SHM_UNLOCK) {
		for (int i = ofst; i < ofst + n; i++) {
			if (pDbFd->sharedMemorySharedLocks & (1 << i)) {
				pDbFd->sharedMemorySharedLocks &= ~(1 << i);
				--memInfo->sharedLocks[i];
			}
			if (pDbFd->sharedMemoryExclusiveLocks & (1 << i)) {
				pDbFd->sharedMemoryExclusiveLocks &= ~(1 << i);
				--memInfo->exclusiveLocks[i];
			}
		}
	} else if (flags & SQLITE_SHM_SHARED) {
		for (int i = ofst; i < ofst + n; i++)
			if (memInfo->exclusiveLocks[i] != ((pDbFd->sharedMemoryExclusiveLocks >> i) & 1)) {
				//TraceEvent("ShmLocked").detail("File", DEBUG_DETERMINISM ? 0 : (int64_t)pDbFd).detail("Acquiring", "Shared").detail("I", i).detail("Exclusive", memInfo->exclusiveLocks[i]).detail("MyExclusive", pDbFd->sharedMemoryExclusiveLocks);
				return SQLITE_BUSY;
			}
		for (int i = ofst; i < ofst + n; i++)
			if (!(pDbFd->sharedMemorySharedLocks & (1 << i))) {
				pDbFd->sharedMemorySharedLocks |= 1 << i;
				memInfo->sharedLocks[i]++;
			}
	} else {
		for (int i = ofst; i < ofst + n; i++)
			if (memInfo->exclusiveLocks[i] != ((pDbFd->sharedMemoryExclusiveLocks >> i) & 1) ||
			    memInfo->sharedLocks[i] != ((pDbFd->sharedMemorySharedLocks >> i) & 1)) {
				//TraceEvent("ShmLocked").detail("File", DEBUG_DETERMINISM ? 0 : (int64_t)pDbFd).detail("Acquiring", "Exclusive").detail("I", i).detail("Exclusive", memInfo->exclusiveLocks[i]).detail("MyExclusive", pDbFd->sharedMemoryExclusiveLocks).detail("Shared", memInfo->sharedLocks[i]).detail("MyShared", pDbFd->sharedMemorySharedLocks);
				return SQLITE_BUSY;
			}
		for (int i = ofst; i < ofst + n; i++)
			if (!(pDbFd->sharedMemoryExclusiveLocks & (1 << i))) {
				pDbFd->sharedMemoryExclusiveLocks |= 1 << i;
				memInfo->exclusiveLocks[i]++;
			}
	}
	return SQLITE_OK;
}

/*
** Implement a memory barrier or memory fence on shared memory.
**
** All loads and stores begun before the barrier must complete before
** any load or store begun after the barrier.
*/
static void asyncShmBarrier(sqlite3_file*) {
#if WIN32
	_ReadWriteBarrier();
#else
	__sync_synchronize();
#endif
}

/*
** Close a connection to shared-memory.  Delete the underlying
** storage if deleteFlag is true.
**
** If there is no shared memory associated with the connection then this
** routine is a harmless no-op.
*/
static int asyncShmUnmap(sqlite3_file* fd, /* The underlying database file */
                         int deleteFlag /* Delete shared-memory if true */
) {
	MutexHolder hold(SharedMemoryInfo::mutex);

	VFSAsyncFile* pDbFd = (VFSAsyncFile*)fd;
	SharedMemoryInfo* memInfo = pDbFd->sharedMemory;
	if (!memInfo)
		return SQLITE_OK;
	pDbFd->sharedMemory = 0;

	// printf("Connection %p closed shared memory\n", fd);

	if (!--memInfo->refcount) {
		// printf("Cleanup shared memory for: '%s' (%d refs; deleteFlag=%d)\n", memInfo->filename.c_str(),
		// memInfo->refcount, deleteFlag); printf("  Shared locks: "); for(int i=0; i<8; i++) printf("%d ",
		// memInfo->sharedLocks[i]); printf("\n"); printf("  Exclusive locks: "); for(int i=0; i<8; i++) printf("%d ",
		// memInfo->exclusiveLocks[i]); printf("\n");

		//TraceEvent("CleanupSharedMemory").detail("Filename", memInfo->filename.c_str()).detail("RefCount", memInfo->refcount).detail("DeleteFlag", deleteFlag);
		// for(int i = 0; i < 8; i++)
		//TraceEvent("CleanupSharedMemory_Locks").detail("Filename", memInfo->filename.c_str()).detail("Num", i).detail("Shared", memInfo->sharedLocks[i]).detail("Exclusive", memInfo->exclusiveLocks[i]);

		// We don't think deleteFlag will ever be set
		ASSERT(!deleteFlag);
	}
	return SQLITE_OK;
}

VFSAsyncFile::~VFSAsyncFile() {

	TraceEvent(SevDebug, "VFSAsyncFileDestroyStart")
	    .detail("Filename", filename)
	    .detail("OpenCount", filename_lockCount_openCount[filename].second)
	    .detail("LockCount", filename_lockCount_openCount[filename].first)
	    .backtrace();

	if (!--filename_lockCount_openCount[filename].second) {
		filename_lockCount_openCount.erase(filename);

		TraceEvent(SevDebug, "VFSAsyncFileDestroy").detail("Filename", filename).backtrace();

		// Always delete the shared memory when the last copy of the file is deleted.  In simulation, this is helpful
		// because "killing" a file without properly closing it can result in a shared memory state that causes
		// corruption when reopening the killed file.  The only expected penalty from doing this is a potentially slower
		// open operation on a database, but that should happen infrequently.
		//
		// We can't do this in ShmUnmap when refcount is 0 because it seems that SQLite sometimes subsequently tries to
		// reopen the WAL from multiple locations simultaneously, resulting in a locking error
		auto itr = SharedMemoryInfo::table.find(filename);
		if (itr != SharedMemoryInfo::table.end()) {
			ASSERT_ABORT(itr->second.refcount == 0);
			itr->second.cleanup();
		}
	}
}

#endif

/*
** Open a file handle.
*/
static int asyncOpen(sqlite3_vfs* pVfs, /* VFS */
                     const char* zName, /* File to open, or 0 for a temp file */
                     sqlite3_file* pFile, /* Pointer to VFSAsyncFile struct to populate */
                     int flags, /* Input SQLITE_OPEN_XXX flags */
                     int* pOutFlags /* Output SQLITE_OPEN_XXX flags (or nullptr) */
) {
	static const sqlite3_io_methods asyncio = { 3, /* iVersion */
		                                        asyncClose, /* xClose */
		                                        asyncRead, /* xRead */
		                                        asyncWrite, /* xWrite */
		                                        asyncTruncate, /* xTruncate */
		                                        asyncSync, /* xSync */
		                                        VFSAsyncFileSize, /* xFileSize */
		                                        asyncLock, /* xLock */
		                                        asyncUnlock, /* xUnlock */
		                                        asyncCheckReservedLock, /* xCheckReservedLock */
		                                        VFSAsyncFileControl, /* xFileControl */
		                                        asyncSectorSize, /* xSectorSize */
		                                        asyncDeviceCharacteristics, /* xDeviceCharacteristics */
		                                        asyncShmMap,
		                                        asyncShmLock,
		                                        asyncShmBarrier,
		                                        asyncShmUnmap,
		                                        asyncReadZeroCopy,
		                                        asyncReleaseZeroCopy };

	VFSAsyncFile* p = (VFSAsyncFile*)pFile; /* Populate this structure */

	if (zName == 0)
		return SQLITE_IOERR;

	static_assert(
	    SQLITE_OPEN_EXCLUSIVE == IAsyncFile::OPEN_EXCLUSIVE && SQLITE_OPEN_CREATE == IAsyncFile::OPEN_CREATE &&
	        SQLITE_OPEN_READONLY == IAsyncFile::OPEN_READONLY && SQLITE_OPEN_READWRITE == IAsyncFile::OPEN_READWRITE,
	    "SQLite flag values don't match IAsyncFile flag values");

	// File creation here is disabled because we always create the files first in KeyValueStoreSQLite, using atomic
	// creation
	int oflags =
	    flags & (/*SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_CREATE |*/ SQLITE_OPEN_READONLY | SQLITE_OPEN_READWRITE);
	if (flags & SQLITE_OPEN_WAL)
		oflags |= IAsyncFile::OPEN_LARGE_PAGES;
	oflags |= IAsyncFile::OPEN_LOCK;

	memset(static_cast<void*>(p), 0, sizeof(VFSAsyncFile));
	new (p) VFSAsyncFile(zName, flags);
	try {
		// Note that SQLiteDB::open also opens the db file, so its flags and modes are important, too
		p->file = waitForAndGet(IAsyncFileSystem::filesystem()->open(p->filename, oflags, 0600));

		TraceEvent(SevDebug, "VFSAsyncFileOpened").detail("Filename", p->filename).backtrace();

	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_CANTOPEN);
		}
		TraceEvent("VFSAsyncFileOpenError").error(e).detail("Filename", p->filename);
		p->~VFSAsyncFile();
		return SQLITE_CANTOPEN;
	}

	if (pOutFlags) {
		*pOutFlags = flags;
	}
	p->base.pMethods = &asyncio;
	return SQLITE_OK;
}

// The next few functions, which perform filesystem operations by path rather than by file, have
// OS-specific implementations.

/*
** Delete the file identified by argument zPath. If the dirSync parameter
** is non-zero, then ensure the file-system modification to delete the
** file has been synced to disk before returning.
*/
static int asyncDelete(sqlite3_vfs* pVfs, const char* zPath, int dirSync) {
	ASSERT(false); // At the moment this isn't used; hence isn't under test.  Could easily use
	               // IAsyncFileSystem::filesystem()->deleteFile().
	return SQLITE_IOERR_DELETE;
}

/*
** Query the file-system to see if the named file exists, is readable or
** is both readable and writable.  For an exists query, treat a zero-length file
** as if it does not exist.
*/
static int asyncAccess(sqlite3_vfs* pVfs, const char* zPath, int flags, int* pResOut) {
#ifdef __unixish__
#ifndef F_OK
#define F_OK 0
#endif
#ifndef R_OK
#define R_OK 4
#endif
#ifndef W_OK
#define W_OK 2
#endif
	int rc; /* access() return code */
	int eAccess = F_OK; /* Second argument to access() */

	assert(flags == SQLITE_ACCESS_EXISTS /* access(zPath, F_OK) */
	       || flags == SQLITE_ACCESS_READ /* access(zPath, R_OK) */
	       || flags == SQLITE_ACCESS_READWRITE /* access(zPath, R_OK|W_OK) */
	);

	if (flags == SQLITE_ACCESS_READWRITE)
		eAccess = R_OK | W_OK;
	if (flags == SQLITE_ACCESS_READ)
		eAccess = R_OK;

	rc = access(zPath, eAccess);
	*pResOut = (rc == 0);

	if (flags == SQLITE_ACCESS_EXISTS && *pResOut) {
		struct stat buf;
		if (0 == stat(zPath, &buf) && buf.st_size == 0) {
			*pResOut = 0;
		}
	}
	return SQLITE_OK;
#else
	WIN32_FILE_ATTRIBUTE_DATA data;
	DWORD attr = INVALID_FILE_ATTRIBUTES;
	memset(&data, 0, sizeof(data));
	if (GetFileAttributesEx(zPath, GetFileExInfoStandard, &data)) {
		if (!(flags == SQLITE_ACCESS_EXISTS && data.nFileSizeHigh == 0 && data.nFileSizeLow == 0))
			attr = data.dwFileAttributes;
	} else if (GetLastError() != ERROR_FILE_NOT_FOUND)
		return SQLITE_IOERR_ACCESS;

	if (flags == SQLITE_ACCESS_READWRITE)
		*pResOut = (attr & FILE_ATTRIBUTE_READONLY) == 0;
	else
		*pResOut = attr != INVALID_FILE_ATTRIBUTES;
	return SQLITE_OK;
#endif
}

/*
** Argument zPath points to a nul-terminated string containing a file path.
** If zPath is an absolute path, then it is copied as is into the output
** buffer. Otherwise, if it is a relative path, then the equivalent full
** path is written to the output buffer.
*/
static int asyncFullPathname(sqlite3_vfs* pVfs, /* VFS */
                             const char* zPath, /* Input path (possibly a relative path) */
                             int nPathOut, /* Size of output buffer in bytes */
                             char* zPathOut /* Pointer to output buffer */
) {
	try {
		auto s = abspath(zPath);
		if (s.size() >= nPathOut)
			return SQLITE_IOERR;
		memcpy(zPathOut, s.c_str(), s.size() + 1);
		return SQLITE_OK;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_IOERR);
		}
		TraceEvent(SevError, "VFSAsyncFullPathnameError").error(e).detail("PathIn", (std::string)zPath);
		return SQLITE_IOERR;
	} catch (...) {
		TraceEvent(SevError, "VFSAsyncFullPathnameError").error(unknown_error()).detail("PathIn", (std::string)zPath);
		return SQLITE_IOERR;
	}
}

/*
** Returns true if there is a shared memory entry for the specified filename,
** and false otherwise.
*/
bool vfsAsyncIsOpen(std::string filename) {
	return SharedMemoryInfo::table.count(abspath(filename)) > 0;
}

/*
** The following four VFS methods:
**
**   xDlOpen
**   xDlError
**   xDlSym
**   xDlClose
**
** are supposed to implement the functionality needed by SQLite to load
** extensions compiled as shared objects. This simple VFS does not support
** this functionality, so the following functions are no-ops.
*/
static void* asyncDlOpen(sqlite3_vfs* pVfs, const char* zPath) {
	return 0;
}
static void asyncDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg) {
	sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
	zErrMsg[nByte - 1] = '\0';
}
static void (*asyncDlSym(sqlite3_vfs* pVfs, void* pH, const char* z))(void) {
	return 0;
}
static void asyncDlClose(sqlite3_vfs* pVfs, void* pHandle) {
	return;
}

/*
** Parameter zByte points to a buffer nByte bytes in size. Populate this
** buffer with pseudo-random data.
*/
static int asyncRandomness(sqlite3_vfs* pVfs, int nByte, char* zByte) {
	for (int i = 0; i < nByte; i++)
		zByte[i] = deterministicRandom()->randomInt(0, 256);
	return SQLITE_OK;
}

/*
** Sleep for at least nMicro microseconds. Return the (approximate) number
** of microseconds slept for.
*/
static int asyncSleep(sqlite3_vfs* pVfs, int microseconds) {
	try {
		Future<Void> simCancel = Never();
		if (g_network->isSimulated())
			simCancel = success(g_simulator.getCurrentProcess()->shutdownSignal.getFuture());
		if (simCancel.isReady()) {
			waitFor(delay(FLOW_KNOBS->MAX_BUGGIFIED_DELAY));
			return 0;
		}
		waitFor(g_network->delay(microseconds * 1e-6, TaskPriority::DefaultDelay) || simCancel);
		return microseconds;
	} catch (Error& e) {
		if (e.isInjectedFault()) {
			VFSAsyncFile::setInjectedError(SQLITE_ERROR);
		}
		TraceEvent(SevError, "VFSAsyncSleepError").errorUnsuppressed(e);
		return 0;
	}
}

/*
** Find the current time (in Universal Coordinated Time).  Write into *piNow
** the current time and date as a Julian Day number times 86_400_000.  In
** other words, write into *piNow the number of milliseconds since the Julian
** epoch of noon in Greenwich on November 24, 4714 B.C according to the
** proleptic Gregorian calendar.
**
** On success, return 0.  Return 1 if the time and date cannot be found.
*/
static int asyncCurrentTimeInt64(sqlite3_vfs* NotUsed, sqlite3_int64* piNow) {
#if __unixish__
	static const sqlite3_int64 unixEpoch = 24405875 * (sqlite3_int64)8640000;
	struct timeval sNow;
	gettimeofday(&sNow, nullptr);
	*piNow = unixEpoch + 1000 * (sqlite3_int64)sNow.tv_sec + sNow.tv_usec / 1000;
#elif defined(_WIN32)
	static const sqlite3_int64 winFiletimeEpoch = 23058135 * (sqlite3_int64)8640000;
	int64_t ft = 0;
	GetSystemTimeAsFileTime((FILETIME*)&ft);
	*piNow = winFiletimeEpoch + ft / 10000;
#else
#error Port me!
#endif
	return 0;
}

/*
** Set *pTime to the current UTC time expressed as a Julian day. Return
** SQLITE_OK if successful, or an error code otherwise.
**
**   http://en.wikipedia.org/wiki/Julian_day
*/
static int asyncCurrentTime(sqlite3_vfs* pVfs, double* pTime) {
	sqlite3_int64 t = 0;
	int rc = asyncCurrentTimeInt64(pVfs, &t);
	if (rc)
		return rc;
	*pTime = t / 86400000.0;
	return SQLITE_OK;
}

static int asyncGetLastError(sqlite3_vfs* NotUsed, int NotUsed2, char* NotUsed3) {
	return 0;
}

/*
** This function returns a pointer to the VFS implemented in this file.
** To make the VFS available to SQLite:
**
**   sqlite3_vfs_register(sqlite3_asyncvfs(), 0);
*/
sqlite3_vfs* vfsAsync() {
	static sqlite3_vfs asyncvfs = {
		3, /* iVersion */
		sizeof(VFSAsyncFile), /* szOsFile */
		MAXPATHNAME, /* mxPathname */
		0, /* pNext */
		"fdb_async", /* zName */
		0, /* pAppData */
		asyncOpen, /* xOpen */
		asyncDelete, /* xDelete */
		asyncAccess, /* xAccess */
		asyncFullPathname, /* xFullPathname */
		asyncDlOpen, /* xDlOpen */
		asyncDlError, /* xDlError */
		asyncDlSym, /* xDlSym */
		asyncDlClose, /* xDlClose */
		asyncRandomness, /* xRandomness */
		asyncSleep, /* xSleep */
		asyncCurrentTime, /* xCurrentTime */
		asyncGetLastError, /* xGetLastError */
		asyncCurrentTimeInt64, /* xCurrentTimeInt64 */
		0, /* xSetSystemCall */
		0, /* xGetSystemCall */
		0, /* xNextSystemCall */

	};
	return &asyncvfs;
}
