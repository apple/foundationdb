/*
 * Platform.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_PLATFORM_H
#define FLOW_PLATFORM_H
#pragma once

#include "flow/config.h"

#if (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__))
#define __unixish__ 1
#endif

#define FLOW_THREAD_SAFE 0

#include <stdlib.h>
#include <filesystem>
#if defined(__cplusplus)
#include <ctime>
#else
#include <time.h>
#endif

#define FDB_EXIT_SUCCESS 0
#define FDB_EXIT_ERROR 1
#define FDB_EXIT_ABORT 3
#define FDB_EXIT_MAIN_ERROR 10
#define FDB_EXIT_MAIN_EXCEPTION 11
#define FDB_EXIT_NO_MEM 20
#define FDB_EXIT_INIT_SEMAPHORE 21

#ifdef __cplusplus
#define EXTERNC extern "C"

#include <cstdlib>
#include <cstdint>
#include <stdio.h>

#ifdef __unixish__
#include <unistd.h>
#endif

#if !(defined(_WIN32) || defined(__unixish__))
#error Compiling on unknown platform
#endif

#if defined(__linux__)
#if defined(__clang__)
#if ((__clang_major__ * 100 + __clang_minor__) < 303)
#error Clang 3.3 or later is required on this platform
#endif
#elif ((__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) < 40500)
#error GCC 4.5.0 or later required on this platform
#endif
#endif

#if defined(_WIN32) && (_MSC_VER < 1600)
#error Visual Studio 2010 required on this platform
#endif

#if defined(__APPLE__) &&                                                                                              \
    (!((__clang__ == 1) || ((__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) > 40800)))
#error Either Clang or GCC 4.8.0 or later required on this platform
#endif

#if (__clang__ == 1)
#define DISABLE_ZERO_DIVISION_FLAG _Pragma("GCC diagnostic ignored \"-Wdivision-by-zero\"")
#elif defined(_MSC_VER)
#define DISABLE_ZERO_DIVISION_FLAG __pragma("GCC diagnostic ignored \"-Wdiv-by-zero\"")
#else
#define DISABLE_ZERO_DIVISION_FLAG _Pragma("GCC diagnostic ignored \"-Wdiv-by-zero\"")
#endif

#if defined(__GNUG__)
#define force_inline inline __attribute__((__always_inline__))
#elif defined(_MSC_VER)
#define force_inline __forceinline
#else
#error Missing force inline
#endif

/*
 * Visual Studio (.NET 2003 and beyond) has an __assume compiler
 * intrinsic to hint to the compiler that a given condition is true
 * and will remain true until the expression is altered. This can be
 * emulated on GCC and ignored elsewhere.
 *
 * http://en.chys.info/2010/07/counterpart-of-assume-in-gcc/
 */
#ifndef _MSC_VER
#if defined(__GNUG__)
#define __assume(cond)                                                                                                 \
	do {                                                                                                               \
		if (!(cond))                                                                                                   \
			__builtin_unreachable();                                                                                   \
	} while (0)
#else
#define __assume(cond)
#endif
#endif

#ifdef __unixish__
#include <pthread.h>
#define CRITICAL_SECTION pthread_mutex_t
#define InitializeCriticalSection(m)                                                                                   \
	do {                                                                                                               \
		pthread_mutexattr_t mta;                                                                                       \
		pthread_mutexattr_init(&mta);                                                                                  \
		pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_RECURSIVE);                                                      \
		pthread_mutex_init(m, &mta);                                                                                   \
		pthread_mutexattr_destroy(&mta);                                                                               \
	} while (0)
#define DeleteCriticalSection(m) pthread_mutex_destroy(m)
#define EnterCriticalSection(m) pthread_mutex_lock(m)
#define LeaveCriticalSection(m) pthread_mutex_unlock(m)
#endif

#if (defined(__GNUG__))
#include <memory>
#include <functional>
#endif

// g++ requires that non-dependent names have to be looked up at
// template definition, which makes circular dependencies a royal
// pain. (For whatever it's worth, g++ appears to be adhering to spec
// here.) Fixing this properly requires quite a bit of reordering
// and/or splitting things into multiple files, but Scherer pointed
// out that it's simple to force a name to be dependent, which is what
// we'll do for now.
template <class Ignore, class T>
inline static T& makeDependent(T& value) {
	return value;
}

#include <map>
#include <string>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#define THREAD_FUNC static void __cdecl
#define THREAD_FUNC_RETURN void
#define THREAD_HANDLE void*
THREAD_HANDLE startThread(void(func)(void*), void* arg, int stackSize = 0, const char* name = nullptr);
#define THREAD_RETURN return
#elif defined(__unixish__)
#define THREAD_FUNC static void*
#define THREAD_FUNC_RETURN void*
#define THREAD_HANDLE pthread_t
// The last parameter is an optional name for the thread. It is only supported on Linux and has a
// limit of 16 characters.
THREAD_HANDLE startThread(void*(func)(void*), void* arg, int stackSize = 0, const char* name = nullptr);
#define THREAD_RETURN return NULL
#else
#error How do I start a new thread on this platform?
#endif

#if defined(_WIN32)
#define DYNAMIC_LIB_EXT ".dll"
#elif defined(__linux)
#define DYNAMIC_LIB_EXT ".so"
#elif defined(__FreeBSD__)
#define DYNAMIC_LIB_EXT ".so"
#elif defined(__APPLE__)
#define DYNAMIC_LIB_EXT ".dylib"
#else
#error Port me
#endif

#if defined(_WIN32)
#define ENV_VAR_PATH_SEPARATOR ';'
#elif defined(__unixish__)
#define ENV_VAR_PATH_SEPARATOR ':'
#else
#error Port me
#endif

void waitThread(THREAD_HANDLE thread);

// Linux-only for now.  Set thread priority.
void setThreadPriority(int pri);

#define DEBUG_DETERMINISM 0

std::string removeWhitespace(const std::string& t);

struct SystemStatistics {
	bool initialized;
	double elapsed;
	double processCPUSeconds, mainThreadCPUSeconds;
	uint64_t processMemory;
	uint64_t processResidentMemory;
	uint64_t processDiskTotalBytes;
	uint64_t processDiskFreeBytes;
	double processDiskQueueDepth;
	double processDiskIdleSeconds;
	double processDiskReadSeconds;
	double processDiskWriteSeconds;
	double processDiskRead;
	double processDiskWrite;
	uint64_t processDiskReadCount;
	uint64_t processDiskWriteCount;
	double processDiskWriteSectors;
	double processDiskReadSectors;
	double machineMegabitsSent;
	double machineMegabitsReceived;
	uint64_t machineOutSegs;
	uint64_t machineRetransSegs;
	double machineCPUSeconds;
	int64_t machineTotalRAM;
	int64_t machineCommittedRAM;
	int64_t machineAvailableRAM;

	SystemStatistics()
	  : initialized(false), elapsed(0), processCPUSeconds(0), mainThreadCPUSeconds(0), processMemory(0),
	    processResidentMemory(0), processDiskTotalBytes(0), processDiskFreeBytes(0), processDiskQueueDepth(0),
	    processDiskIdleSeconds(0), processDiskReadSeconds(0), processDiskWriteSeconds(0), processDiskRead(0),
	    processDiskWrite(0), processDiskReadCount(0), processDiskWriteCount(0), processDiskWriteSectors(0),
	    processDiskReadSectors(0), machineMegabitsSent(0), machineMegabitsReceived(0), machineOutSegs(0),
	    machineRetransSegs(0), machineCPUSeconds(0), machineTotalRAM(0), machineCommittedRAM(0),
	    machineAvailableRAM(0) {}
};

struct SystemStatisticsState;

struct IPAddress;

SystemStatistics getSystemStatistics(std::string const& dataFolder,
                                     const IPAddress* ip,
                                     SystemStatisticsState** statState,
                                     bool logDetails);

double getProcessorTimeThread();

double getProcessorTimeProcess();

#ifdef __linux__
namespace linux_os {

// Collects the /sys/fs/cgroup/cpu,cpuacct/cpu.stat information and returns the content
// For more information about cpu,cpuacct, check manpages for cgroup
std::map<std::string, int64_t> reportCGroupCpuStat();

} // namespace linux_os
#endif // __linux__

uint64_t getMemoryUsage();

uint64_t getResidentMemoryUsage();

struct MachineRAMInfo {
	int64_t total;
	int64_t committed;
	int64_t available;
};

void getMachineRAMInfo(MachineRAMInfo& memInfo);

void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total);

void getNetworkTraffic(uint64_t& bytesSent, uint64_t& bytesReceived, uint64_t& outSegs, uint64_t& retransSegs);

void getDiskStatistics(std::string const& directory,
                       uint64_t& currentIOs,
                       uint64_t& readMilliSecs,
                       uint64_t& writeMilliSecs,
                       uint64_t& IOMilliSecs,
                       uint64_t& reads,
                       uint64_t& writes,
                       uint64_t& writeSectors);

void getMachineLoad(uint64_t& idleTime, uint64_t& totalTime, bool logDetails);

double
timer(); // Returns the system real time clock with high precision.  May jump around when system time is adjusted!
double timer_monotonic(); // Returns a high precision monotonic clock which is adjusted to be kind of similar to timer()
                          // at startup, but might not be a globally accurate time.
uint64_t timer_int(); // Return timer as uint64_t representing epoch nanoseconds

void getLocalTime(const
#if defined(__cplusplus)
                  std::time_t
#else
                  time_t
#endif
                      * timep,
                  struct tm* result);

// get GMT time string from an epoch seconds double
std::string epochsToGMTString(double epochs);

#define ENVIRONMENT_KNOB_OPTION_PREFIX "FDB_KNOB_"
// returns list of environment variables with prefix ENVIRONMENT_KNOB_OPTION_PREFIX
std::vector<std::string> getEnvironmentKnobOptions();

void setMemoryQuota(size_t limit);

void* allocate(size_t length, bool allowLargePages, bool includeGuardPages);

void setAffinity(int proc);

void threadSleep(double seconds);

void threadYield(); // Attempt to yield to other processes or threads

// Returns true iff the file exists
bool fileExists(std::filesystem::path const& filename);

// Returns true iff the directory exists
bool directoryExists(std::filesystem::path const& path);

// Returns size of file in bytes
int64_t fileSize(std::filesystem::path const& filename);

// Returns last modified time of the file.
time_t fileModifiedTime(std::filesystem::path const& filename); // Maybe this should be const for extra protection?

// Returns true if file is deleted, false if it was not found, throws platform_error() otherwise
// Consider using IAsyncFileSystem::filesystem()->deleteFile() instead, especially if you need durability!
bool deleteFile(std::filesystem::path const& path);

// Renames the given file.  Does not fsync the directory.
void renameFile(std::filesystem::path const& fromPath, std::filesystem::path const& toPath);

// Atomically replaces the contents of the specified file.
void atomicReplace(std::filesystem::path const& path,
                   std::string const& content,
                   bool textmode = true); // Maybe the content should also be filesystem? Not sure if there's an
                                          // alternative but there might be some security issues if we leave it as
                                          // std::string. (Could be a victim of injection attacks)

// Read a file into memory
// This requires the file to be seekable
std::string readFileBytes(std::filesystem::path const& filename, size_t maxSize);

// Read a file into memory supplied by the caller
// If 'len' is greater than file size, then read the filesize bytes.
size_t readFileBytes(std::filesystem::path const& filename, uint8_t* buff, size_t len);

// Write data buffer into file
void writeFileBytes(std::filesystem::path const& filename, const uint8_t* data, size_t count);

// Write text into file
void writeFile(std::filesystem::path const& filename, std::string const& content);

// cleanPath() does a 'logical' resolution of the given path string to a canonical form *without*
// following symbolic links or verifying the existence of any path components.  It removes redundant
// "." references and duplicate separators, and resolves any ".." references that can be resolved
// using the preceding path components.
// Relative paths remain relative and are NOT rebased on the current working directory.
std::filesystem::path cleanPath(std::filesystem::path const& path);

// Removes the last component from a path string (if possible) and returns the result with one trailing separator.
// If there is only one path component, the result will be "" for relative paths and "/" for absolute paths.
// Note that this is NOT the same as getting the parent of path, as the final component could be ".."
// or "." and it would still be simply removed.
// ALL of the following inputs will yield the result "/a/"
//   /a/b
//   /a/b/
//   /a/..
//   /a/../
//   /a/.
//   /a/./
//   /a//..//
std::filesystem::path popPath(std::filesystem::path const& path);

// abspath() resolves the given path to a canonical form.
// If path is relative, the result will be based on the current working directory.
// If resolveLinks is true then symbolic links will be expanded BEFORE resolving '..' references.
// An empty path or a non-existent path when mustExist is true will result in a platform_error() exception.
// Upon success, all '..' references will be resolved with the assumption that non-existent components
// are NOT symbolic links.
// User directory references such as '~' or '~user' are effectively treated as symbolic links which
// are impossible to resolve, so resolveLinks=true results in failure and resolveLinks=false results
// in the reference being left in-tact prior to resolving '..' references.
std::filesystem::path abspath(std::filesystem::path const& path, bool resolveLinks = true, bool mustExist = false);

// parentDirectory() returns the parent directory of the given file or directory in a canonical form,
// with a single trailing path separator.
// It uses absPath() with the same bool options to initially obtain a canonical form, and upon success
// removes the final path component, if present.
// std::filesystem::path parentDirectory(std::filesystem::path const& path, bool resolveLinks = true, bool mustExist =
// false); // This doesn't need to exist. Will commit this out to track references.

// Returns the portion of the path following the last path separator (e.g. the filename or directory name)
// std::string basename(std::string const& filename); // This doesn't need to exist. Will commit this out to track
// references.

// Returns the home directory of the current user
std::string getUserHomeDirectory(); // Not sure if this needs to change.

namespace platform {

// Returns true if directory was created, false if it existed, throws platform_error() otherwise
bool createDirectory(std::filesystem::path const& directory);

// e.g. extension==".fdb", returns filenames relative to directory
std::vector<std::filesystem::path> listFiles(std::filesystem::path const& directory, std::string const& extension = "");

// returns directory names relative to directory
std::vector<std::filesystem::path> listDirectories(std::filesystem::path const& directory);

void findFilesRecursively(std::filesystem::path const& path, std::vector<std::filesystem::path>& out);

// Tag the given file as "temporary", i.e. not really needing commits to disk
void makeTemporary(const char* filename);

void setCloseOnExec(int fd);

// Logs an out of memory error and exits the program
void outOfMemory();

int getRandomSeed();

bool getEnvironmentVar(const char* name, std::string& value);
int setEnvironmentVar(const char* name, const char* value, int overwrite);

std::filesystem::path getWorkingDirectory();

// Returns the absolute platform-dependant path for server-based files
std::filesystem::path getDefaultConfigPath();

// Returns the absolute platform-dependant path for the default fdb.cluster file
std::filesystem::path getDefaultClusterFilePath();

struct ImageInfo {
	void* offset = nullptr;
	std::string fileName = "unknown";
	std::string symbolFileName = "unknown";
};

ImageInfo getImageInfo();

// Places the frame pointers in a string formatted as parameters for addr2line.
size_t raw_backtrace(void** addresses, int maxStackDepth);
std::string get_backtrace();
std::string format_backtrace(void** addresses, int numAddresses);

// Avoid in production code: not atomic, not fast, not reliable in all environments
int eraseDirectoryRecursive(std::string const& directory);

// Creates a temporary file; file gets destroyed/deleted along with object destruction.
// If 'tmpDir' is empty, code defaults to 'boost::filesystem::temp_directory_path()'
// If 'pattern' is empty, code defaults to 'fdbtmp'
struct TmpFile {
public:
	TmpFile();
	TmpFile(const std::string& tempDir);
	TmpFile(const std::string& tempDir, std::string const& prefix);
	~TmpFile();
	size_t read(uint8_t* buff, size_t len);
	void write(const uint8_t* buff, size_t len);
	bool destroyFile();
	std::filesystem::path getFileName() const { return filename; }

private:
	std::filesystem::path filename;
	constexpr static std::string_view defaultPrefix = "fdbtmp";

	void createTmpFile(const std::string_view dir, const std::string_view prefix);
};

} // namespace platform

#ifdef __linux__
typedef struct {
	double timestamp;
	size_t length;
	void* frames[];
} ProfilingSample;

dev_t getDeviceId(std::string path);
#endif

#if defined(__aarch64__)
#include "flow/sse2neon.h"
// aarch64 does not have rdtsc counter
// Use cntvct_el0 virtual counter instead
inline static uint64_t timestampCounter() {
	uint64_t timer;
	asm volatile("mrs %0, cntvct_el0" : "=r"(timer));
	return timer;
}
#elif defined(_powerpc64_)
#include <emmintrin.h>
#elif defined(__linux__)
#include <x86intrin.h>
#define timestampCounter() __rdtsc()
#elif defined(__APPLE__) // macOS on Intel
// Version of CLang bundled with XCode doesn't yet include ia32intrin.h.
#if !(__has_builtin(__rdtsc))
inline static uint64_t timestampCounter() {
	uint64_t lo, hi;
	asm("rdtsc" : "=a"(lo), "=d"(hi));
	return (lo | (hi << 32));
}
#else
#define timestampCounter() __rdtsc()
#endif
#else
#define timestampCounter() __rdtsc()
#endif

#ifdef __FreeBSD__
#if !(__has_builtin(__rdtsc))
inline static uint64_t __rdtsc() {
	uint64_t lo, hi;
	asm("rdtsc" : "=a"(lo), "=d"(hi));
	return (lo | (hi << 32));
}
#endif
#elif defined(__powerpc64__) || defined(__ppc64__)
inline static uint64_t __rdtsc() {
	uint64_t tb;
	__asm__ volatile("mfspr %0, 268" : "=r"(tb));
	return tb;
}
#endif

#if defined(__linux__)
#include <features.h>
#endif
#include <sys/stat.h>

#ifdef _WIN32
#include <intrin.h>
inline static int32_t interlockedIncrement(volatile int32_t* a) {
	return _InterlockedIncrement((long*)a);
}
inline static int64_t interlockedIncrement64(volatile int64_t* a) {
	return _InterlockedIncrement64(a);
}
inline static int32_t interlockedDecrement(volatile int32_t* a) {
	return _InterlockedDecrement((long*)a);
}
inline static int64_t interlockedDecrement64(volatile int64_t* a) {
	return _InterlockedDecrement64(a);
}
inline static int32_t interlockedCompareExchange(volatile int32_t* a, int32_t b, int32_t c) {
	return _InterlockedCompareExchange((long*)a, (long)b, (long)c);
}
inline static int64_t interlockedExchangeAdd64(volatile int64_t* a, int64_t b) {
	return _InterlockedExchangeAdd64(a, b);
}
inline static int64_t interlockedExchange64(volatile int64_t* a, int64_t b) {
	return _InterlockedExchange64(a, b);
}
inline static int64_t interlockedOr64(volatile int64_t* a, int64_t b) {
	return _InterlockedOr64(a, b);
}
#elif defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8)
#ifndef __aarch64__
#include <xmmintrin.h>
#endif
inline static int32_t interlockedIncrement(volatile int32_t* a) {
	return __sync_add_and_fetch(a, 1);
}
inline static int64_t interlockedIncrement64(volatile int64_t* a) {
	return __sync_add_and_fetch(a, 1);
}
inline static int32_t interlockedDecrement(volatile int32_t* a) {
	return __sync_add_and_fetch(a, -1);
}
inline static int64_t interlockedDecrement64(volatile int64_t* a) {
	return __sync_add_and_fetch(a, -1);
}
inline static int32_t interlockedCompareExchange(volatile int32_t* a, int32_t b, int32_t c) {
	return __sync_val_compare_and_swap(a, c, b);
}
inline static int64_t interlockedExchangeAdd64(volatile int64_t* a, int64_t b) {
	return __sync_fetch_and_add(a, b);
}
inline static int64_t interlockedExchange64(volatile int64_t* a, int64_t b) {
	__sync_synchronize();
	return __sync_lock_test_and_set(a, b);
}
inline static int64_t interlockedOr64(volatile int64_t* a, int64_t b) {
	return __sync_fetch_and_or(a, b);
}
#else
#error No implementation of atomic instructions
#endif

template <class T>
inline static T* interlockedExchangePtr(T* volatile* a, T* b) {
	static_assert(sizeof(T*) == sizeof(int64_t), "Port me!");
	return (T*)interlockedExchange64((volatile int64_t*)a, (int64_t)b);
}

#if FLOW_THREAD_SAFE
#define thread_volatile volatile
inline static int64_t flowInterlockedExchangeAdd64(volatile int64_t* p, int64_t a) {
	return interlockedExchangeAdd64(p, a);
}
inline static int64_t flowInterlockedIncrement64(volatile int64_t* p) {
	return interlockedIncrement64(p);
}
inline static int64_t flowInterlockedDecrement64(volatile int64_t* p) {
	return interlockedDecrement64(p);
}
inline static int64_t flowInterlockedExchange64(volatile int64_t* p, int64_t a) {
	return interlockedExchange64(p, a);
}
inline static int64_t flowInterlockedOr64(volatile int64_t* p, int64_t a) {
	return interlockedOr64(p, a);
}
inline static int64_t flowInterlockedAnd64(volatile int64_t* p, int64_t a) {
	return interlockedAnd64(p, a);
}
#else
#define thread_volatile
inline static int64_t flowInterlockedExchangeAdd64(int64_t* p, int64_t a) {
	auto old = *p;
	*p += a;
	return old;
}
inline static int64_t flowInterlockedIncrement64(int64_t* p) {
	return ++*p;
}
inline static int64_t flowInterlockedDecrement64(int64_t* p) {
	return --*p;
}
inline static int64_t flowInterlockedExchange64(int64_t* p, int64_t a) {
	auto old = *p;
	*p = a;
	return old;
}
inline static int64_t flowInterlockedOr64(int64_t* p, int64_t a) {
	auto old = *p;
	*p |= a;
	return old;
}
inline static int64_t flowInterlockedAnd64(int64_t* p, int64_t a) {
	auto old = *p;
	*p &= a;
	return old;
}
#endif

// We only run on little-endian system, so conversion to/from bigEndian64 is always a byte swap
#ifdef _MSC_VER
#define bigEndian16(value) uint16_t(_byteswap_ushort(value))
#define bigEndian32(value) uint32_t(_byteswap_ulong(value))
#define bigEndian64(value) uint64_t(_byteswap_uint64(value))
#define fromBigEndian16(value) uint16_t(_byteswap_ushort(value))
#define fromBigEndian32(value) uint32_t(_byteswap_ulong(value))
#define fromBigEndian64(value) uint64_t(_byteswap_uint64(value))
#elif __GNUG__
#define bigEndian16(value) uint16_t((value >> 8) | (value << 8))
#define bigEndian32(value) uint32_t(__builtin_bswap32(value))
#define bigEndian64(value) uint64_t(__builtin_bswap64(value))
#define fromBigEndian16(value) uint16_t((value >> 8) | (value << 8))
#define fromBigEndian32(value) uint32_t(__builtin_bswap32(value))
#define fromBigEndian64(value) uint64_t(__builtin_bswap64(value))
#else
#error Missing byte swap methods
#endif

#define littleEndian16(value) uint16_t(value)
#define littleEndian32(value) uint32_t(value)
#define littleEndian64(value) uint64_t(value)

#if defined(_WIN32)
inline static void flushOutputStreams() {
	_flushall();
}
#elif defined(__unixish__)
inline static void flushOutputStreams() {
	fflush(nullptr);
}
#else
#error Missing flush output stream
#endif

#if defined(_MSC_VER)
#define DLLEXPORT __declspec(dllexport)
#elif defined(__GNUG__)
#undef DLLEXPORT
#define DLLEXPORT __attribute__((visibility("default")))
#else
#error Missing symbol export
#endif

#define crashAndDie() std::abort()

#ifdef _WIN32
#define strcasecmp stricmp
#endif

#if defined(__GNUG__)
#define DEFAULT_CONSTRUCTORS(X)                                                                                        \
	X(X const& rhs) = default;                                                                                         \
	X& operator=(X const& rhs) = default;
#else
#define DEFAULT_CONSTRUCTORS(X)
#endif

#if defined(_WIN32)
#define strtoull(nptr, endptr, base) _strtoui64(nptr, endptr, base)
#endif

#if defined(_MSC_VER)
inline static void* aligned_alloc(size_t alignment, size_t size) {
	return _aligned_malloc(size, alignment);
}
inline static void aligned_free(void* ptr) {
	_aligned_free(ptr);
}
#elif defined(__linux__)
#include <malloc.h>
inline static void aligned_free(void* ptr) {
	free(ptr);
}
#if (!defined(_ISOC11_SOURCE)) // old libc versions
inline static void* aligned_alloc(size_t alignment, size_t size) {
	return memalign(alignment, size);
}
#endif
#elif defined(__FreeBSD__)
inline static void aligned_free(void* ptr) {
	free(ptr);
}
#elif defined(__APPLE__)
#if !defined(HAS_ALIGNED_ALLOC)
#include <cstdlib>
inline static void* aligned_alloc(size_t alignment, size_t size) {
	void* ptr = nullptr;
	posix_memalign(&ptr, alignment, size);
	return ptr;
}
#endif
inline static void aligned_free(void* ptr) {
	free(ptr);
}
#endif

// lib_path may be a relative or absolute path or a name to be
// resolved by whatever linker is hanging around on this system
bool isLibraryLoaded(const char* lib_path);
void* loadLibrary(const char* lib_path);
void closeLibrary(void* handle);
void* loadFunction(void* lib, const char* func_name);

std::filesystem::path exePath();

// get the absolute path
std::filesystem::path getExecPath();

#ifdef _WIN32
inline static int ctzll(uint64_t value) {
	unsigned long count = 0;
	if (_BitScanForward64(&count, value)) {
		return count;
	}
	return 64;
}
inline static int clzll(uint64_t value) {
	unsigned long count = 0;
	if (_BitScanReverse64(&count, value)) {
		return 63 - count;
	}
	return 64;
}
inline static int ctz(uint32_t value) {
	unsigned long count = 0;
	if (_BitScanForward(&count, value)) {
		return count;
	}
	return 64;
}
inline static int clz(uint32_t value) {
	unsigned long count = 0;
	if (_BitScanReverse(&count, value)) {
		return 63 - count;
	}
	return 64;
}
#else
#define ctzll __builtin_ctzll
#define clzll __builtin_clzll
#define ctz __builtin_ctz
#define clz __builtin_clz
#endif

// These return 0 unless run on the network thread
int64_t getNumProfilesDisabled();
int64_t getNumProfilesOverflowed();
int64_t getNumProfilesCaptured();

#else // __cplusplus
#define EXTERNC
#endif // __cplusplus

/*
 * Multiply Defined Symbol (support for weak function declaration).
 */
#ifndef MULTIPLY_DEFINED_SYMBOL
#if defined(_MSC_VER)
#define MULTIPLY_DEFINED_SYMBOL
#else
#define MULTIPLY_DEFINED_SYMBOL __attribute__((weak))
#endif
#endif

// Logs a critical error message and exits the program
EXTERNC void criticalError(int exitCode, const char* type, const char* message);
EXTERNC void flushAndExit(int exitCode);

// Initialization code that's run at the beginning of every entry point (except fdbmonitor)
void platformInit();

// Register a callback which will run as part of the crash handler. Use in conjunction with registerCrashHandler.
// The callback being added should be simple and unlikely to fail, otherwise it will fail the crash handler,
// preventing necessary logging being printed. Also, the crash handler may not be comprehensive in handling all
// failure cases.
void registerCrashHandlerCallback(void (*f)());

void registerCrashHandler();

void setupRunLoopProfiler();
void stopRunLoopProfiler();
EXTERNC void setProfilingEnabled(int enabled);

// Use _exit() or criticalError(), not exit()
#define exit static_assert(false, "Calls to exit() are forbidden by policy");

#if defined(FDB_CLEAN_BUILD) && !(defined(NDEBUG) && !defined(_DEBUG) && !defined(SQLITE_DEBUG))
#error Clean builds must define NDEBUG, and not define various debug macros
#endif

// DTrace probing
#if defined(DTRACE_PROBES)
#include <sys/sdt.h>
#define FDB_TRACE_PROBE_STRING_EXPAND(x) x
#define FDB_TRACE_PROBE_STRING_CONCAT2(h, t) h##t
#define FDB_TRACE_PROBE_STRING_CONCAT(h, t) FDB_TRACE_PROBE_STRING_CONCAT2(h, t)
#define FDB_TRACE_PROBE_EXPAND_MACRO(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, NAME, ...) NAME
#define FDB_TRACE_PROBE(...)                                                                                           \
	FDB_TRACE_PROBE_EXPAND_MACRO(__VA_ARGS__,                                                                          \
	                             DTRACE_PROBE12,                                                                       \
	                             DTRACE_PROBE11,                                                                       \
	                             DTRACE_PROBE10,                                                                       \
	                             DTRACE_PROBE9,                                                                        \
	                             DTRACE_PROBE8,                                                                        \
	                             DTRACE_PROBE7,                                                                        \
	                             DTRACE_PROBE6,                                                                        \
	                             DTRACE_PROBE5,                                                                        \
	                             DTRACE_PROBE4,                                                                        \
	                             DTRACE_PROBE3,                                                                        \
	                             DTRACE_PROBE2,                                                                        \
	                             DTRACE_PROBE1,                                                                        \
	                             DTRACE_PROBE)                                                                         \
	(foundationdb, __VA_ARGS__)

extern void fdb_probe_actor_create(const char* name, unsigned long id);
extern void fdb_probe_actor_destroy(const char* name, unsigned long id);
extern void fdb_probe_actor_enter(const char* name, unsigned long, int index);
extern void fdb_probe_actor_exit(const char* name, unsigned long, int index);
#else
#define FDB_TRACE_PROBE_STRING_CONCAT(h, t) h##t
#define FDB_TRACE_PROBE(...)
inline void fdb_probe_actor_create(const char* name, unsigned long id) {}
inline void fdb_probe_actor_destroy(const char* name, unsigned long id) {}
inline void fdb_probe_actor_enter(const char* name, unsigned long id, int index) {}
inline void fdb_probe_actor_exit(const char* name, unsigned long id, int index) {}
#endif

#if defined(__aarch64__)
#define _MM_HINT_T0 0 /* dummy -- not used */
#endif

#endif /* FLOW_PLATFORM_H */
