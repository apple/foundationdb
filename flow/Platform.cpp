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

#ifdef _WIN32
// This has to come as the first include on Win32 for rand_s() to be found
#define _CRT_RAND_S
#include <stdlib.h>
#include <math.h> // For _set_FMA3_enable workaround in platformInit
#endif

#include "flow/Platform.h"
#include "flow/Arena.h"

#include "flow/Trace.h"
#include "flow/Error.h"

#include "flow/Knobs.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <algorithm>

#include <sys/types.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "flow/UnitTest.h"
#include "flow/FaultInjection.h"

#ifdef _WIN32
#include <windows.h>
#undef max
#undef min
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

/* Needed for disk capacity */
#include <sys/statvfs.h>

/* getifaddrs */
#include <sys/socket.h>
#include <ifaddrs.h>
#include <arpa/inet.h>

#include "flow/stacktrace.h"

#ifdef __linux__
/* Needed for memory allocation */
#include <linux/mman.h>
/* Needed for processor affinity */
#include <sched.h>
/* Needed for getProcessorTime* and setpriority */
#include <sys/syscall.h>
/* Needed for setpriority */
#include <sys/resource.h>
/* Needed for crash handler */
#include <signal.h>
/* Needed for gnu_dev_{major,minor} */
#include <sys/sysmacros.h>
#endif

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

std::string removeWhitespace(const std::string &t)
{
	static const std::string ws(" \t\r");
	std::string str = t;
	size_t found = str.find_last_not_of(ws);
	if (found != std::string::npos)
		str.erase(found + 1);
	else
		str.clear();			// str is all whitespace
	found = str.find_first_not_of(ws);
	if (found != std::string::npos)
		str.erase(0, found);
	else
		str.clear();			// str is all whitespace

	return str;
}

#ifdef _WIN32
#define ALLOC_FAIL NULL
#elif defined(__unixish__)
#define ALLOC_FAIL MAP_FAILED
#else
#error What platform is this?
#endif

using std::cout;
using std::endl;

#if defined(_WIN32)
__int64 FiletimeAsInt64 (FILETIME &t){
	return *(__int64*)&t;
}
#endif

#ifdef _WIN32
bool handlePdhStatus(const PDH_STATUS& status, std::string message) {
	if (status != ERROR_SUCCESS) {
		TraceEvent(SevWarnAlways, message.c_str()).GetLastError().detail("Status", status);
		return false;
	}
	return true;
}

bool setPdhString(int id, std::string &out) {
	char buf[512];
	DWORD sz = 512;
	if (!handlePdhStatus(PdhLookupPerfNameByIndex(NULL, id, buf, &sz), "PdhLookupPerfByNameIndex"))
		return false;
	out = buf;
	return true;
}
#endif

#ifdef __unixish__
static double getProcessorTimeGeneric(int who) {
	struct rusage r_usage;

	if (getrusage(who, &r_usage)) {
		TraceEvent(SevError, "GetCPUTime").detail("Who", who).GetLastError();
		throw platform_error();
	}

	return (r_usage.ru_utime.tv_sec + (r_usage.ru_utime.tv_usec / double(1e6)) +
			r_usage.ru_stime.tv_sec + (r_usage.ru_stime.tv_usec / double(1e6)));
}
#endif

double getProcessorTimeThread() {
	INJECT_FAULT( platform_error, "getProcessorTimeThread" );
#if defined(_WIN32)
	FILETIME ftCreate, ftExit, ftKernel, ftUser;
	if (!GetThreadTimes(GetCurrentThread(), &ftCreate, &ftExit, &ftKernel, &ftUser)) {
		TraceEvent(SevError, "GetThreadCPUTime").GetLastError();
		throw platform_error();
	}
	return FiletimeAsInt64(ftKernel) / double(1e7) + FiletimeAsInt64(ftUser) / double(1e7);
#elif defined(__linux__)
	return getProcessorTimeGeneric(RUSAGE_THREAD);
#elif defined(__APPLE__)
	/* No RUSAGE_THREAD so we use the lower level interface */
	struct thread_basic_info info;
	mach_msg_type_number_t info_count = THREAD_BASIC_INFO_COUNT;
	if (KERN_SUCCESS != thread_info(mach_thread_self(), THREAD_BASIC_INFO, (thread_info_t)&info, &info_count)) {
		TraceEvent(SevError, "GetThreadCPUTime").GetLastError();
		throw platform_error();
	}
	return (info.user_time.seconds + (info.user_time.microseconds / double(1e6)) +
			info.system_time.seconds + (info.system_time.microseconds / double(1e6)));
#else
	#warning getProcessorTimeThread unimplemented on this platform
	return 0.0;
#endif
}

double getProcessorTimeProcess() {
	INJECT_FAULT( platform_error, "getProcessorTimeProcess" );
#if defined(_WIN32)
	FILETIME ftCreate, ftExit, ftKernel, ftUser;
	if (!GetProcessTimes(GetCurrentProcess(), &ftCreate, &ftExit, &ftKernel, &ftUser)) {
		TraceEvent(SevError, "GetProcessCPUTime").GetLastError();
		throw platform_error();
	}
	return FiletimeAsInt64(ftKernel) / double(1e7) + FiletimeAsInt64(ftUser) / double(1e7);
#elif defined(__unixish__)
	return getProcessorTimeGeneric(RUSAGE_SELF);
#else
	#warning getProcessorTimeProcess unimplemented on this platform
	return 0.0;
#endif
}

uint64_t getResidentMemoryUsage() {
#if defined(__linux__)
	uint64_t rssize = 0;

	std::ifstream stat_stream("/proc/self/statm", std::ifstream::in);
	std::string ignore;

	if(!stat_stream.good()) {
		TraceEvent(SevError, "GetResidentMemoryUsage").GetLastError();
		throw platform_error();
	}

	stat_stream >> ignore;
	stat_stream >> rssize;

	rssize *= sysconf(_SC_PAGESIZE);

	return rssize;
#elif defined(_WIN32)
	PROCESS_MEMORY_COUNTERS_EX pmc;
	if(!GetProcessMemoryInfo(GetCurrentProcess(), (PPROCESS_MEMORY_COUNTERS)&pmc, sizeof(pmc))) {
		TraceEvent(SevError, "GetResidentMemoryUsage").GetLastError();
		throw platform_error();
	}
	return pmc.WorkingSetSize;
#elif defined(__APPLE__)
	struct task_basic_info info;
	mach_msg_type_number_t info_count = TASK_BASIC_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&info, &info_count)) {
		TraceEvent(SevError, "GetResidentMemoryUsage").GetLastError();
		throw platform_error();
	}
	return info.resident_size;
#else
	#warning getMemoryUsage unimplemented on this platform
	return 0;
#endif
}

uint64_t getMemoryUsage() {
#if defined(__linux__)
	uint64_t vmsize = 0;

	std::ifstream stat_stream("/proc/self/statm", std::ifstream::in);

	if(!stat_stream.good()) {
		TraceEvent(SevError, "GetMemoryUsage").GetLastError();
		throw platform_error();
	}

	stat_stream >> vmsize;

	vmsize *= sysconf(_SC_PAGESIZE);

	return vmsize;
#elif defined(_WIN32)
	PROCESS_MEMORY_COUNTERS_EX pmc;
	if(!GetProcessMemoryInfo(GetCurrentProcess(), (PPROCESS_MEMORY_COUNTERS)&pmc, sizeof(pmc))) {
		TraceEvent(SevError, "GetMemoryUsage").GetLastError();
		throw platform_error();
	}
	return pmc.PagefileUsage;
#elif defined(__APPLE__)
	struct task_basic_info info;
	mach_msg_type_number_t info_count = TASK_BASIC_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&info, &info_count)) {
		TraceEvent(SevError, "GetMemoryUsage").GetLastError();
		throw platform_error();
	}
	return info.virtual_size;
#else
	#warning getMemoryUsage unimplemented on this platform
	return 0;
#endif
}

#if defined(__linux__)
void getMemoryInfo(std::map<StringRef, int64_t>& request, std::stringstream& memInfoStream) {
	size_t count = request.size();
	if (count == 0)
		return;

	while (count > 0 && !memInfoStream.eof()) {
		std::string key;

		memInfoStream >> key;
		auto item = request.find(StringRef(key));
		if (item != request.end()){
			int64_t value;
			memInfoStream >> value;
			item->second = value;
			count--;
		}
		memInfoStream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
	}
}

int64_t getLowWatermark(std::stringstream& zoneInfoStream) {
	int64_t lowWatermark = 0;
	while(!zoneInfoStream.eof()) {
		std::string key;
		zoneInfoStream >> key;

		if(key == "low") {
			int64_t value;
			zoneInfoStream >> value;
			lowWatermark += value;
		}

		zoneInfoStream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
	}

	return lowWatermark;
}
#endif

void getMachineRAMInfo(MachineRAMInfo& memInfo) {
#if defined(__linux__)
	std::ifstream zoneInfoFileStream("/proc/zoneinfo", std::ifstream::in);
	int64_t lowWatermark = 0;
	if(!zoneInfoFileStream.good()) {
		TraceEvent(SevWarnAlways, "GetMachineZoneInfo").GetLastError();
	}
	else {
		std::stringstream zoneInfoStream;
		zoneInfoStream << zoneInfoFileStream.rdbuf();
		lowWatermark = getLowWatermark(zoneInfoStream) * 4; // Convert from 4K pages to KB
	}

	std::ifstream fileStream("/proc/meminfo", std::ifstream::in);
	if (!fileStream.good()) {
		TraceEvent(SevError, "GetMachineMemInfo").GetLastError();
		throw platform_error();
	}

	std::map<StringRef, int64_t> request = {
		{ LiteralStringRef("MemTotal:"), 0 },
		{ LiteralStringRef("MemFree:"), 0 },
		{ LiteralStringRef("MemAvailable:"), -1 },
		{ LiteralStringRef("Active(file):"), 0 },
		{ LiteralStringRef("Inactive(file):"), 0 },
		{ LiteralStringRef("SwapTotal:"), 0 },
		{ LiteralStringRef("SwapFree:"), 0 },
		{ LiteralStringRef("SReclaimable:"), 0 },
	};

	std::stringstream memInfoStream;
	memInfoStream << fileStream.rdbuf();
	getMemoryInfo( request, memInfoStream );

	int64_t memFree = request[LiteralStringRef("MemFree:")];
	int64_t pageCache = request[LiteralStringRef("Active(file):")] + request[LiteralStringRef("Inactive(file):")];
	int64_t slabReclaimable = request[LiteralStringRef("SReclaimable:")];
	int64_t usedSwap = request[LiteralStringRef("SwapTotal:")] - request[LiteralStringRef("SwapFree:")];

	memInfo.total = 1024 * request[LiteralStringRef("MemTotal:")];
	if(request[LiteralStringRef("MemAvailable:")] != -1) {
		memInfo.available = 1024 * (request[LiteralStringRef("MemAvailable:")] - usedSwap);
	}
	else {
		memInfo.available = 1024 * (std::max<int64_t>(0, (memFree-lowWatermark) + std::max(pageCache-lowWatermark, pageCache/2) + std::max(slabReclaimable-lowWatermark, slabReclaimable/2)) - usedSwap);
	}

	memInfo.committed = memInfo.total - memInfo.available;
#elif defined(_WIN32)
	MEMORYSTATUSEX mem_status;
	mem_status.dwLength = sizeof(mem_status);
	if (!GlobalMemoryStatusEx(&mem_status)) {
		TraceEvent(SevError, "WindowsGetMemStatus").GetLastError();
		throw platform_error();
	}

	PERFORMACE_INFORMATION perf;
	if (!GetPerformanceInfo(&perf, sizeof(perf))) {
		TraceEvent(SevError, "WindowsGetMemPerformanceInfo").GetLastError();
		throw platform_error();
	}

	memInfo.total = mem_status.ullTotalPhys;
	memInfo.committed = perf.PageSize*perf.CommitTotal;
	memInfo.available = memInfo.total - memInfo.committed;
#elif defined(__APPLE__)
	vm_statistics_data_t vm_stat;
	vm_size_t pagesize;
	mach_msg_type_number_t host_size = sizeof(vm_statistics_data_t) / sizeof(integer_t);
	if (KERN_SUCCESS != host_statistics(mach_host_self(), HOST_VM_INFO, (host_info_t)&vm_stat, &host_size)) {
		TraceEvent(SevError, "GetMachineMemInfo").GetLastError();
		throw platform_error();
	}
	host_page_size(mach_host_self(), &pagesize);

	memInfo.total = pagesize * (vm_stat.free_count + vm_stat.active_count + vm_stat.inactive_count + vm_stat.wire_count);
	memInfo.available = pagesize * vm_stat.free_count;
	memInfo.committed = memInfo.total - memInfo.available;
#else
	#warning getMachineRAMInfo unimplemented on this platform
#endif
}

Error systemErrorCodeToError() {
#if defined(_WIN32)
	if(GetLastError() == ERROR_IO_DEVICE) {
		return io_error();
	}
#elif defined(__unixish__)
	if(errno == EIO || errno == EROFS) {
		return io_error();
	}
#else
	#error Port me!
#endif

	return platform_error();
}

void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) {
	INJECT_FAULT( platform_error, "getDiskBytes" );
#if defined(__unixish__)
#ifdef __linux__
	struct statvfs buf;
	if (statvfs(directory.c_str(), &buf)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "GetDiskBytesStatvfsError").detail("Directory", directory).GetLastError().error(e);
		throw e;
	}

	uint64_t blockSize = buf.f_frsize;
#elif defined(__APPLE__)
	struct statfs buf;
	if (statfs(directory.c_str(), &buf)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "GetDiskBytesStatfsError").detail("Directory", directory).GetLastError().error(e);
		throw e;
	}

	uint64_t blockSize = buf.f_bsize;
#else
#error Unknown unix
#endif

	free = std::min( (uint64_t) std::numeric_limits<int64_t>::max(), buf.f_bavail * blockSize );
	total = std::min( (uint64_t) std::numeric_limits<int64_t>::max(), buf.f_blocks * blockSize );

#elif defined(_WIN32)
	std::string fullPath = abspath(directory);
	//TraceEvent("FullDiskPath").detail("Path", fullPath).detail("Disk", (char)toupper(fullPath[0]));

	ULARGE_INTEGER freeSpace;
	ULARGE_INTEGER totalSpace;
	ULARGE_INTEGER totalFreeSpace;
	if( !GetDiskFreeSpaceEx( fullPath.c_str(), &freeSpace, &totalSpace, &totalFreeSpace ) ) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "DiskFreeError").detail("Path", fullPath).GetLastError().error(e);
		throw e;
	}
	total = std::min( (uint64_t) std::numeric_limits<int64_t>::max(), totalSpace.QuadPart );
	free = std::min( (uint64_t) std::numeric_limits<int64_t>::max(), freeSpace.QuadPart );
#else
	#warning getDiskBytes unimplemented on this platform
	free = 1LL<<50;
	total = 1LL<<50;
#endif
}

#ifdef __unixish__
const char* getInterfaceName(uint32_t _ip) {
	INJECT_FAULT( platform_error, "getInterfaceName" );
	static char iname[20];

	struct ifaddrs* interfaces = NULL;
	const char* ifa_name = NULL;

	if (getifaddrs(&interfaces)) {
		TraceEvent(SevWarnAlways, "GetInterfaceAddrs").GetLastError();
		throw platform_error();
	}

	for (struct ifaddrs* iter = interfaces; iter; iter = iter->ifa_next) {
		if(!iter->ifa_addr)
			continue;
		if (iter->ifa_addr->sa_family == AF_INET) {
			uint32_t ip = ntohl(((struct sockaddr_in*)iter->ifa_addr)->sin_addr.s_addr);
			if (ip == _ip) {
				ifa_name = iter->ifa_name;
				break;
			}
		}
	}

	if (ifa_name) {
		strncpy(iname, ifa_name, 19);
		iname[19] = 0;
	}

	freeifaddrs(interfaces);

	if (ifa_name)
		return iname;
	else
		return NULL;
}
#endif

#if defined(__linux__)
void getNetworkTraffic(uint32_t ip, uint64_t& bytesSent, uint64_t& bytesReceived,
					   uint64_t& outSegs, uint64_t& retransSegs) {
	INJECT_FAULT( platform_error, "getNetworkTraffic" ); // Even though this function doesn't throw errors, the equivalents for other platforms do, and since all of our simulation testing is on Linux...
	const char* ifa_name = nullptr;
	try {
		ifa_name = getInterfaceName(ip);
	}
	catch(Error &e) {
		if(e.code() != error_code_platform_error) {
			throw;
		}
	}

	if (!ifa_name)
		return;

	std::ifstream dev_stream("/proc/net/dev", std::ifstream::in);
	dev_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
	dev_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

	std::string iface;
	std::string ignore;

	uint64_t bytesSentSum = 0;
	uint64_t bytesReceivedSum = 0;

	while (dev_stream.good()) {
		dev_stream >> iface;
		if (dev_stream.eof()) break;
		if (!strncmp(iface.c_str(), ifa_name, strlen(ifa_name))) {
			uint64_t sent = 0, received = 0;

			dev_stream >> received;
			for (int i = 0; i < 7; i++) dev_stream >> ignore;
			dev_stream >> sent;

			bytesSentSum += sent;
			bytesReceivedSum += received;

			dev_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
		}
	}

	if(bytesSentSum > 0) {
		bytesSent = bytesSentSum;
	}
	if(bytesReceivedSum > 0) {
		bytesReceived = bytesReceivedSum;
	}

	std::ifstream snmp_stream("/proc/net/snmp", std::ifstream::in);

	std::string label;

	while (snmp_stream.good()) {
		snmp_stream >> label;
		snmp_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
		if (label == "Tcp:")
			break;
	}

	/* Ignore the first 11 columns of the Tcp line */
	for (int i = 0; i < 11; i++)
		snmp_stream >> ignore;

	snmp_stream >> outSegs;
	snmp_stream >> retransSegs;
}

void getMachineLoad(uint64_t& idleTime, uint64_t& totalTime) {
	INJECT_FAULT( platform_error, "getMachineLoad" ); // Even though this function doesn't throw errors, the equivalents for other platforms do, and since all of our simulation testing is on Linux...
	std::ifstream stat_stream("/proc/stat", std::ifstream::in);

	std::string ignore;
	stat_stream >> ignore;

	uint64_t t_user, t_nice, t_system, t_idle, t_iowait, t_irq, t_softirq, t_steal, t_guest;
	stat_stream >> t_user >> t_nice >> t_system >> t_idle >> t_iowait >> t_irq >> t_softirq >> t_steal >> t_guest;

	totalTime = t_user+t_nice+t_system+t_idle+t_iowait+t_irq+t_softirq+t_steal+t_guest;
	idleTime = t_idle+t_iowait;

	if( !DEBUG_DETERMINISM )
		TraceEvent("MachineLoadDetail").detail("User", t_user).detail("Nice", t_nice).detail("System", t_system).detail("Idle", t_idle).detail("IOWait", t_iowait).detail("IRQ", t_irq).detail("SoftIRQ", t_softirq).detail("Steal", t_steal).detail("Guest", t_guest);
}

void getDiskStatistics(std::string const& directory, uint64_t& currentIOs, uint64_t& busyTicks, uint64_t& reads, uint64_t& writes, uint64_t& writeSectors, uint64_t& readSectors) {
	INJECT_FAULT( platform_error, "getDiskStatistics" );
	currentIOs = 0;

	struct stat buf;
	if (stat(directory.c_str(), &buf)) {
		TraceEvent(SevError, "GetDiskStatisticsStatError").detail("Directory", directory).GetLastError();
		throw platform_error();
	}

	std::ifstream proc_stream("/proc/diskstats", std::ifstream::in);
	while (proc_stream.good()) {
		std::string line;
		getline(proc_stream, line);
		std::istringstream disk_stream(line, std::istringstream::in);

		unsigned int majorId;
		unsigned int minorId;
		disk_stream >> majorId;
		disk_stream >> minorId;
		if(majorId == (unsigned int) gnu_dev_major(buf.st_dev) && minorId == (unsigned int) gnu_dev_minor(buf.st_dev)) {
			std::string ignore;
			uint64_t rd_ios;	/* # of reads completed */
			//	    This is the total number of reads completed successfully.

			uint64_t rd_merges;	/* # of reads merged */
			//	    Reads and writes which are adjacent to each other may be merged for
			//	    efficiency.  Thus two 4K reads may become one 8K read before it is
			//	    ultimately handed to the disk, and so it will be counted (and queued)
			//	    as only one I/O.  This field lets you know how often this was done.

			uint64_t rd_sectors; /*# of sectors read */
			//	    This is the total number of sectors read successfully.

			uint64_t rd_ticks;	/* # of milliseconds spent reading */
			//	    This is the total number of milliseconds spent by all reads (as
			//	    measured from __make_request() to end_that_request_last()).

			uint64_t wr_ios;	/* # of writes completed */
			//	    This is the total number of writes completed successfully.

			uint64_t wr_merges;	/* # of writes merged */
			//	    Reads and writes which are adjacent to each other may be merged for
			//	    efficiency.  Thus two 4K reads may become one 8K read before it is
			//	    ultimately handed to the disk, and so it will be counted (and queued)
			//	    as only one I/O.  This field lets you know how often this was done.

			uint64_t wr_sectors; /* # of sectors written */
			//	    This is the total number of sectors written successfully.

			uint64_t wr_ticks;	/* # of milliseconds spent writing */
			//	    This is the total number of milliseconds spent by all writes (as
			//	    measured from __make_request() to end_that_request_last()).

			uint64_t cur_ios;	/* # of I/Os currently in progress */
			//	    The only field that should go to zero. Incremented as requests are
			//	    given to appropriate struct request_queue and decremented as they finish.

			uint64_t ticks;	/* # of milliseconds spent doing I/Os */
			//	    This field increases so long as field 9 is nonzero.

			uint64_t aveq;	/* weighted # of milliseconds spent doing I/Os */
			//	    This field is incremented at each I/O start, I/O completion, I/O
			//	    merge, or read of these stats by the number of I/Os in progress
			//	    (field 9) times the number of milliseconds spent doing I/O since the
			//	    last update of this field.  This can provide an easy measure of both
			//	    I/O completion time and the backlog that may be accumulating.

			disk_stream >> ignore;
			disk_stream >> rd_ios;
			disk_stream >> rd_merges;
			disk_stream >> rd_sectors;
			disk_stream >> rd_ticks;
			disk_stream >> wr_ios;
			disk_stream >> wr_merges;
			disk_stream >> wr_sectors;
			disk_stream >> wr_ticks;
			disk_stream >> cur_ios;
			disk_stream >> ticks;
			disk_stream >> aveq;

			currentIOs = cur_ios;
			busyTicks = ticks;
			reads = rd_ios;
			writes = wr_ios;
			writeSectors = wr_sectors;
			readSectors = rd_sectors;

			//TraceEvent("DiskMetricsRaw").detail("Input", line).detail("Ignore", ignore).detail("RdIos", rd_ios)
			//	.detail("RdMerges", rd_merges).detail("RdSectors", rd_sectors).detail("RdTicks", rd_ticks).detail("WrIos", wr_ios).detail("WrMerges", wr_merges)
			//	.detail("WrSectors", wr_sectors).detail("WrTicks", wr_ticks).detail("CurIos", cur_ios).detail("Ticks", ticks).detail("Aveq", aveq)
			//	.detail("CurrentIOs", currentIOs).detail("BusyTicks", busyTicks).detail("Reads", reads).detail("Writes", writes).detail("WriteSectors", writeSectors)
			//  .detail("ReadSectors", readSectors);
			return;
		} else
			disk_stream.ignore( std::numeric_limits<std::streamsize>::max(), '\n');
	}

	if(!g_network->isSimulated()) TraceEvent(SevWarn, "GetDiskStatisticsDeviceNotFound").detail("Directory", directory);
}

dev_t getDeviceId(std::string path) {
	struct stat statInfo;

	while (true) {
		int returnValue = stat(path.c_str(), &statInfo);
		if (!returnValue) break;

		if (errno == ENOENT) {
			path = parentDirectory(path);
		} else {
			TraceEvent(SevError, "GetDeviceIdError").detail("Path", path).GetLastError();
			throw platform_error();
		}
	}

	return statInfo.st_dev;
}

#endif

#ifdef __APPLE__
void getNetworkTraffic(uint32_t ip, uint64_t& bytesSent, uint64_t& bytesReceived,
					   uint64_t& outSegs, uint64_t& retransSegs) {
	INJECT_FAULT( platform_error, "getNetworkTraffic" );

	const char* ifa_name = nullptr;
	try {
		ifa_name = getInterfaceName(ip);
	}
	catch(Error &e) {
		if(e.code() != error_code_platform_error) {
			throw;
		}
	}

	if (!ifa_name)
		return;

	int mib[] = {
		CTL_NET,
		PF_ROUTE,
		0,
		AF_INET,
		NET_RT_IFLIST2,
		0 /* If we could get an interface index instead of name, we would pass it here */
	};

	size_t len;

	if (sysctl(mib, 6, NULL, &len, NULL, 0) < 0) {
		TraceEvent(SevError, "GetNetworkTrafficError").GetLastError();
		throw platform_error();
	}

	char *buf = (char*)malloc(len);

	if (sysctl(mib, 6, buf, &len, NULL, 0) < 0) {
		free(buf);
		TraceEvent(SevError, "GetNetworkTrafficReadInterfacesError").GetLastError();
		throw platform_error();
	}

	char *lim = buf + len;

	for (char *next = buf; next < lim; ) {
		struct if_msghdr* ifm = (struct if_msghdr*)next;
		next += ifm->ifm_msglen;

		if ((ifm->ifm_type = RTM_IFINFO2)) {
			struct if_msghdr2* if2m = (struct if_msghdr2*)ifm;
			struct sockaddr_dl *sdl = (struct sockaddr_dl*)(if2m + 1);

			if (sdl->sdl_nlen == strlen(ifa_name) && !strncmp(ifa_name, sdl->sdl_data, sdl->sdl_nlen)) {
				bytesSent = if2m->ifm_data.ifi_obytes;
				bytesReceived = if2m->ifm_data.ifi_ibytes;
				outSegs = if2m->ifm_data.ifi_opackets;
				retransSegs = 0;
				break;
			}
		}
	}

	free(buf);
}

void getMachineLoad(uint64_t& idleTime, uint64_t& totalTime) {
	INJECT_FAULT( platform_error, "getMachineLoad" );
	mach_msg_type_number_t count = HOST_CPU_LOAD_INFO_COUNT;
	host_cpu_load_info_data_t r_load;

	if (host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info_t)&r_load, &count) != KERN_SUCCESS) {
		TraceEvent(SevError, "GetMachineLoad").GetLastError();
		throw platform_error();
	}

	idleTime = r_load.cpu_ticks[CPU_STATE_IDLE];
	totalTime = r_load.cpu_ticks[CPU_STATE_IDLE] + r_load.cpu_ticks[CPU_STATE_USER] + r_load.cpu_ticks[CPU_STATE_NICE] + r_load.cpu_ticks[CPU_STATE_SYSTEM];
}

void getDiskStatistics(std::string const& directory, uint64_t& currentIOs, uint64_t& busyTicks, uint64_t& reads, uint64_t& writes, uint64_t& writeSectors, uint64_t& readSectors) {
	INJECT_FAULT( platform_error, "getDiskStatistics" );
	currentIOs = 0;
	busyTicks = 0;
	writeSectors = 0;
	readSectors = 0;

	const int kMaxDiskNameSize = 64;

	struct statfs buf;
	if (statfs(directory.c_str(), &buf)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "GetDiskStatisticsStatfsError").detail("Directory", directory).GetLastError().error(e);
		throw e;
	}

	const char* dev = strrchr(buf.f_mntfromname, '/');
	if (!dev) {
		TraceEvent(SevError, "GetDiskStatisticsStrrchrError").detail("Directory", directory).GetLastError();
		throw platform_error();
	}
	dev++;

	io_iterator_t disk_list;

	// According to Apple docs, if this gets passed to IOServiceGetMatchingServices, we aren't responsible for the memory anymore,
	// the only case where it isn't passed is if it's null, in which case we also aren't responsible. So no need to call CFRelease
	// on this variable.
	CFMutableDictionaryRef match = IOBSDNameMatching(kIOMasterPortDefault, kNilOptions, dev);

	if(!match) {
		TraceEvent(SevError, "IOBSDNameMatching");
		throw platform_error();
	}

	if (IOServiceGetMatchingServices(kIOMasterPortDefault, match, &disk_list) != kIOReturnSuccess) {
		TraceEvent(SevError, "IOServiceGetMatchingServices");
		throw platform_error();
	}

	io_registry_entry_t disk = IOIteratorNext(disk_list);
	if (!disk) {
		IOObjectRelease(disk_list);
		TraceEvent(SevError, "IOIteratorNext");
		throw platform_error();
	}

	io_registry_entry_t tdisk = disk;
	while (!IOObjectConformsTo(disk, "IOBlockStorageDriver")) {
		IORegistryEntryGetParentEntry(disk, kIOServicePlane, &tdisk);
		IOObjectRelease(disk);
		disk = tdisk;
	}

	CFDictionaryRef disk_dict = NULL;
	if (IORegistryEntryCreateCFProperties(disk, (CFMutableDictionaryRef*)&disk_dict, kCFAllocatorDefault, kNilOptions) != kIOReturnSuccess) {
		IOObjectRelease(disk);
		IOObjectRelease(disk_list);
		TraceEvent(SevError, "IORegistryEntryCreateCFProperties");
		throw platform_error();
	}

	// Here and below, note that memory returned by CFDictionaryGetValue() is not owned by us, and should not be CFRelease()'d by us.
	CFDictionaryRef stats_dict = (CFDictionaryRef)CFDictionaryGetValue(disk_dict, CFSTR(kIOBlockStorageDriverStatisticsKey));

	if (stats_dict == NULL) {
		CFRelease(disk_dict);
		IOObjectRelease(disk);
		IOObjectRelease(disk_list);
		TraceEvent(SevError, "CFDictionaryGetValue");
		throw platform_error();
	}

	CFNumberRef number;

	if ((number = (CFNumberRef)CFDictionaryGetValue(stats_dict, CFSTR(kIOBlockStorageDriverStatisticsReadsKey)))) {
		CFNumberGetValue(number, kCFNumberSInt64Type, &reads);
	}

	if ((number = (CFNumberRef)CFDictionaryGetValue(stats_dict, CFSTR(kIOBlockStorageDriverStatisticsWritesKey)))) {
		CFNumberGetValue(number, kCFNumberSInt64Type, &writes);
	}

	CFRelease(disk_dict);
	IOObjectRelease(disk);
	IOObjectRelease(disk_list);
}
#endif

#if defined(_WIN32)
std::vector<std::string> expandWildcardPath(const char *wildcardPath)
{
	PDH_STATUS Status;
	char *EndOfPaths;
	char *Paths = NULL;
	DWORD BufferSize = 0;
	std::vector<std::string> results;

	Status = PdhExpandCounterPath(wildcardPath, Paths, &BufferSize);
	if (Status != PDH_MORE_DATA) {
		TraceEvent(SevWarn, "PdhExpandCounterPathError").detail("Reason", "Expand Path call made no sense").detail("Status", Status);
		goto Cleanup;
	}

	Paths = (char *)malloc(BufferSize);
	Status = PdhExpandCounterPath(wildcardPath, Paths, &BufferSize);

	if (Status != ERROR_SUCCESS) {
		TraceEvent(SevWarn, "PdhExpandCounterPathError").detail("Reason", "Expand Path call failed").detail("Status", Status);
		goto Cleanup;
	}

	if (Paths == NULL) {
		TraceEvent("WindowsPdhExpandCounterPathError").detail("Reason", "Path could not be expanded");
		goto Cleanup;
	}

	EndOfPaths = Paths + BufferSize;

	for (char *p = Paths; ((p != EndOfPaths) && (*p != '\0')); p += strlen(p) + 1) {
		results.push_back( p );
		//printf("Counter: %s\n", p);
	}

Cleanup:
	if (Paths)
	{
		free(Paths);
	}
	return results;
}

std::vector<HCOUNTER> addCounters( HQUERY Query, const char *path ) {
	std::vector<HCOUNTER> counters;

	std::vector<std::string> paths = expandWildcardPath( path );

	for(int i = 0; i < paths.size(); i++) {
		HCOUNTER counter;
		handlePdhStatus( PdhAddCounter(Query, paths[i].c_str(), 0, &counter), "PdhAddCounter" );
		counters.push_back( counter );
	}
	return counters;
}
#endif

struct SystemStatisticsState {
	double lastTime;
	double lastClockThread;
	double lastClockProcess;
	uint64_t processLastSent;
	uint64_t processLastReceived;
#if defined(_WIN32)
	struct {
		std::string diskDevice;
		std::string physicalDisk;
		std::string processor;
		std::string networkDevice;
		std::string tcpv4;
		std::string pctIdle;
		std::string diskQueueLength;
		std::string diskReadsPerSec;
		std::string diskWritesPerSec;
		std::string diskWriteBytesPerSec;
		std::string bytesSentPerSec;
		std::string bytesRecvPerSec;
		std::string segmentsOutPerSec;
		std::string segmentsRetransPerSec;
	} pdhStrings;
	PDH_STATUS Status;
	HQUERY Query;
	HCOUNTER QueueLengthCounter;
	HCOUNTER DiskTimeCounter;
	HCOUNTER ReadsCounter;
	HCOUNTER WritesCounter;
	HCOUNTER WriteBytesCounter;
	std::vector<HCOUNTER> SendCounters;
	std::vector<HCOUNTER> ReceiveCounters;
	HCOUNTER SegmentsOutCounter;
	HCOUNTER SegmentsRetransCounter;
	HCOUNTER ProcessorIdleCounter;
	SystemStatisticsState() : Query(NULL), QueueLengthCounter(NULL), DiskTimeCounter(NULL),
		ReadsCounter(NULL), WritesCounter(NULL), WriteBytesCounter(NULL), ProcessorIdleCounter(NULL),
#elif defined(__unixish__)
	uint64_t machineLastSent, machineLastReceived;
	uint64_t machineLastOutSegs, machineLastRetransSegs;
	uint64_t lastBusyTicks, lastReads, lastWrites, lastWriteSectors, lastReadSectors;
	uint64_t lastClockIdleTime, lastClockTotalTime;
	SystemStatisticsState() : machineLastSent(0), machineLastReceived(0), machineLastOutSegs(0), machineLastRetransSegs(0),
		lastBusyTicks(0), lastReads(0), lastWrites(0), lastWriteSectors(0), lastReadSectors(0), lastClockIdleTime(0), lastClockTotalTime(0),
#else
	#error Port me!
#endif
		lastTime(0), lastClockThread(0), lastClockProcess(0), processLastSent(0), processLastReceived(0) {}
};

#if defined(_WIN32)
void initPdhStrings(SystemStatisticsState *state, std::string dataFolder) {
	if (setPdhString(234, state->pdhStrings.physicalDisk) &&
		setPdhString(238, state->pdhStrings.processor) &&
		setPdhString(510, state->pdhStrings.networkDevice) &&
		setPdhString(638, state->pdhStrings.tcpv4) &&
		setPdhString(1482, state->pdhStrings.pctIdle) &&
		setPdhString(198, state->pdhStrings.diskQueueLength) &&
		setPdhString(214, state->pdhStrings.diskReadsPerSec) &&
		setPdhString(216, state->pdhStrings.diskWritesPerSec) &&
		setPdhString(222, state->pdhStrings.diskWriteBytesPerSec) &&
		setPdhString(506, state->pdhStrings.bytesSentPerSec) &&
		setPdhString(264, state->pdhStrings.bytesRecvPerSec) &&
		setPdhString(654, state->pdhStrings.segmentsOutPerSec) &&
		setPdhString(656, state->pdhStrings.segmentsRetransPerSec)) {

		if (!dataFolder.empty()) {
			dataFolder = abspath(dataFolder);
			char buf[512], buf2[512];
			DWORD sz = 512, sz2 = 512;

			if (!GetVolumePathName(dataFolder.c_str(), buf, 512)) {
				TraceEvent(SevWarn, "GetVolumePathName").GetLastError().detail("Path", dataFolder);
				return;
			}

			if (!GetVolumeNameForVolumeMountPoint(buf, buf2, 512)) {
				TraceEvent(SevWarn, "GetVolumeNameForVolumeMountPoint").GetLastError().detail("Path", dataFolder);
				return;
			}

			if (!strlen(buf2)) {
				TraceEvent(SevWarn, "WinDiskStatsGetPathError").detail("Path", dataFolder);
				return;
			}

			if (buf2[strlen(buf2) - 1] == '\\')
				buf2[strlen(buf2) - 1] = 0;

			HANDLE hDevice = CreateFile(buf2, 0, 0, NULL, OPEN_EXISTING, 0, NULL);
			if (hDevice == INVALID_HANDLE_VALUE) {
				TraceEvent(SevWarn, "CreateFile").GetLastError().detail("Path", dataFolder);
				return;
			}

			STORAGE_DEVICE_NUMBER storage_device;
			if (!DeviceIoControl(hDevice, IOCTL_STORAGE_GET_DEVICE_NUMBER, NULL, 0,
								 &storage_device, sizeof(storage_device), &sz, NULL)) {
				TraceEvent(SevWarn, "DeviceIoControl").GetLastError().detail("Path", dataFolder);
				return;
			}

			// Find the drive letter involved!
			sz = 512;
			if (handlePdhStatus(PdhEnumObjectItems(NULL, NULL, state->pdhStrings.physicalDisk.c_str(),
								buf2, &sz2, buf, &sz, PERF_DETAIL_NOVICE, 0), "PdhEnumObjectItems")) {
				char *ptr = buf;
				while (*ptr) {
					if (isdigit(*ptr) && atoi(ptr) == storage_device.DeviceNumber) {
						state->pdhStrings.diskDevice = ptr;
						break;
					}
					ptr += strlen(ptr) + 1;
				}
			}

			if (state->pdhStrings.diskDevice.empty()) {
				TraceEvent(SevWarn, "WinDiskStatsGetPathError").detail("Path", dataFolder);
				return;
			}
		}
	}
}
#endif

SystemStatistics getSystemStatistics(std::string dataFolder, uint32_t ip, SystemStatisticsState **statState) {
	if( (*statState) == NULL )
		(*statState) = new SystemStatisticsState();
	SystemStatistics returnStats;

	double nowTime = timer();
	double nowClockProcess = getProcessorTimeProcess();
	double nowClockThread = getProcessorTimeThread();
	returnStats.elapsed = nowTime - (*statState)->lastTime;

	returnStats.initialized = (*statState)->lastTime != 0;
	if( returnStats.initialized ) {
		returnStats.processCPUSeconds = (nowClockProcess - (*statState)->lastClockProcess);
		returnStats.mainThreadCPUSeconds = (nowClockThread - (*statState)->lastClockThread);
	}

	returnStats.processMemory = getMemoryUsage();
	returnStats.processResidentMemory = getResidentMemoryUsage();

	MachineRAMInfo memInfo;
	getMachineRAMInfo(memInfo);
	returnStats.machineTotalRAM = memInfo.total;
	returnStats.machineCommittedRAM = memInfo.committed;
	returnStats.machineAvailableRAM = memInfo.available;

	if(dataFolder != "") {
		int64_t diskTotal, diskFree;
		getDiskBytes(dataFolder, diskFree, diskTotal);
		returnStats.processDiskTotalBytes = diskTotal;
		returnStats.processDiskFreeBytes = diskFree;
	}

#if defined(_WIN32)
	if((*statState)->Query == NULL) {
		initPdhStrings(*statState, dataFolder);

		TraceEvent("SetupQuery");
		handlePdhStatus( PdhOpenQuery(NULL, NULL, &(*statState)->Query), "PdhOpenQuery" );

		if( !(*statState)->pdhStrings.diskDevice.empty() ) {
			handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.physicalDisk + "(" + (*statState)->pdhStrings.diskDevice + ")\\" + (*statState)->pdhStrings.pctIdle).c_str(), 0, &(*statState)->DiskTimeCounter), "PdhAddCounter");
			handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.physicalDisk + "(" + (*statState)->pdhStrings.diskDevice + ")\\" + (*statState)->pdhStrings.diskQueueLength).c_str(), 0, &(*statState)->QueueLengthCounter), "PdhAddCounter");
			handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.physicalDisk + "(" + (*statState)->pdhStrings.diskDevice + ")\\" + (*statState)->pdhStrings.diskReadsPerSec).c_str(), 0, &(*statState)->ReadsCounter), "PdhAddCounter");
			handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.physicalDisk + "(" + (*statState)->pdhStrings.diskDevice + ")\\" + (*statState)->pdhStrings.diskWritesPerSec).c_str(), 0, &(*statState)->WritesCounter), "PdhAddCounter");
			handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.physicalDisk + "(" + (*statState)->pdhStrings.diskDevice + ")\\" + (*statState)->pdhStrings.diskWriteBytesPerSec).c_str(), 0, &(*statState)->WriteBytesCounter), "PdhAddCounter");
		}
		(*statState)->SendCounters = addCounters((*statState)->Query, ("\\" + (*statState)->pdhStrings.networkDevice + "(*)\\" + (*statState)->pdhStrings.bytesSentPerSec).c_str());
		(*statState)->ReceiveCounters = addCounters((*statState)->Query, ("\\" + (*statState)->pdhStrings.networkDevice + "(*)\\" + (*statState)->pdhStrings.bytesRecvPerSec).c_str());
		handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.tcpv4 + "\\" + (*statState)->pdhStrings.segmentsOutPerSec).c_str(), 0, &(*statState)->SegmentsOutCounter), "PdhAddCounter");
		handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.tcpv4 + "\\" + (*statState)->pdhStrings.segmentsRetransPerSec).c_str(), 0, &(*statState)->SegmentsRetransCounter), "PdhAddCounter");
		handlePdhStatus(PdhAddCounter((*statState)->Query, ("\\" + (*statState)->pdhStrings.processor + "(*)\\" + (*statState)->pdhStrings.pctIdle).c_str(), 0, &(*statState)->ProcessorIdleCounter), "PdhAddCounter");
	}
	handlePdhStatus( PdhCollectQueryData((*statState)->Query), "PdhCollectQueryData" );

	PDH_FMT_COUNTERVALUE DisplayValue;
	if (returnStats.initialized) {
		if (!(*statState)->pdhStrings.diskDevice.empty()) {
			if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->DiskTimeCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "DiskTimeCounter" ) )
				returnStats.processDiskIdleSeconds = DisplayValue.doubleValue * returnStats.elapsed / 100.0;
			if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->QueueLengthCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "QueueLengthCounter" ) )
				returnStats.processDiskQueueDepth = DisplayValue.doubleValue;
			if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->ReadsCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "ReadsCounter" ) )
				returnStats.processDiskRead = DisplayValue.doubleValue * returnStats.elapsed;
			if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->WritesCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "WritesCounter" ) )
				returnStats.processDiskWrite = DisplayValue.doubleValue * returnStats.elapsed;
			if (handlePdhStatus(PdhGetFormattedCounterValue((*statState)->WriteBytesCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "WriteBytesCounter"))
				returnStats.processDiskWriteSectors = DisplayValue.doubleValue * returnStats.elapsed / 512.0;
		}
		returnStats.machineMegabitsSent = 0.0;
		for( int i = 0; i < (*statState)->SendCounters.size(); i++ )
			if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->SendCounters[i], PDH_FMT_DOUBLE, 0, &DisplayValue), "SendCounter" ) )
				returnStats.machineMegabitsSent += DisplayValue.doubleValue * 7.62939453e-6;
		returnStats.machineMegabitsSent *= returnStats.elapsed;

		returnStats.machineMegabitsReceived = 0.0;
		for( int i = 0; i < (*statState)->ReceiveCounters.size(); i++ )
			if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->ReceiveCounters[i], PDH_FMT_DOUBLE, 0, &DisplayValue), "ReceiveCounter" ) )
				returnStats.machineMegabitsReceived += DisplayValue.doubleValue * 7.62939453e-6;
		returnStats.machineMegabitsReceived *= returnStats.elapsed;

		if (handlePdhStatus(PdhGetFormattedCounterValue((*statState)->SegmentsOutCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "SegmentsOutCounter"))
			returnStats.machineOutSegs = DisplayValue.doubleValue * returnStats.elapsed;
		if (handlePdhStatus(PdhGetFormattedCounterValue((*statState)->SegmentsRetransCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "SegmentsRetransCounter"))
			returnStats.machineRetransSegs = DisplayValue.doubleValue * returnStats.elapsed;

		if( handlePdhStatus( PdhGetFormattedCounterValue((*statState)->ProcessorIdleCounter, PDH_FMT_DOUBLE, 0, &DisplayValue), "ProcessorIdleCounter" ) )
			returnStats.machineCPUSeconds = (100 - DisplayValue.doubleValue) * returnStats.elapsed / 100.0;
	}
#elif defined(__unixish__)
	uint64_t machineNowSent = (*statState)->machineLastSent;
	uint64_t machineNowReceived = (*statState)->machineLastReceived;
	uint64_t machineOutSegs = (*statState)->machineLastOutSegs;
	uint64_t machineRetransSegs = (*statState)->machineLastRetransSegs;

	getNetworkTraffic(ip, machineNowSent, machineNowReceived, machineOutSegs, machineRetransSegs);
	if( returnStats.initialized ) {
		returnStats.machineMegabitsSent = ((machineNowSent - (*statState)->machineLastSent) * 8e-6);
		returnStats.machineMegabitsReceived = ((machineNowReceived - (*statState)->machineLastReceived) * 8e-6);
		returnStats.machineOutSegs = machineOutSegs - (*statState)->machineLastOutSegs;
		returnStats.machineRetransSegs = machineRetransSegs - (*statState)->machineLastRetransSegs;
	}
	(*statState)->machineLastSent = machineNowSent;
	(*statState)->machineLastReceived = machineNowReceived;
	(*statState)->machineLastOutSegs = machineOutSegs;
	(*statState)->machineLastRetransSegs = machineRetransSegs;

	uint64_t currentIOs;
	uint64_t nowBusyTicks = (*statState)->lastBusyTicks;
	uint64_t nowReads = (*statState)->lastReads;
	uint64_t nowWrites = (*statState)->lastWrites;
	uint64_t nowWriteSectors = (*statState)->lastWriteSectors; 
	uint64_t nowReadSectors = (*statState)->lastReadSectors;

	if(dataFolder != "") {
		getDiskStatistics(dataFolder, currentIOs, nowBusyTicks, nowReads, nowWrites, nowWriteSectors, nowReadSectors);
		returnStats.processDiskQueueDepth = currentIOs;
		returnStats.processDiskReadCount = nowReads;
		returnStats.processDiskWriteCount = nowWrites;
		if( returnStats.initialized ) {
			returnStats.processDiskIdleSeconds = std::max<double>(0, returnStats.elapsed - std::min<double>(returnStats.elapsed, (nowBusyTicks - (*statState)->lastBusyTicks) / 1000.0));
			returnStats.processDiskRead = (nowReads - (*statState)->lastReads);
			returnStats.processDiskWrite = (nowWrites - (*statState)->lastWrites);
			returnStats.processDiskWriteSectors = (nowWriteSectors - (*statState)->lastWriteSectors);
			returnStats.processDiskReadSectors = (nowReadSectors - (*statState)->lastReadSectors);
		}
		(*statState)->lastBusyTicks = nowBusyTicks;
		(*statState)->lastReads = nowReads;
		(*statState)->lastWrites = nowWrites;
		(*statState)->lastWriteSectors = nowWriteSectors;
		(*statState)->lastReadSectors = nowReadSectors;
	}

	uint64_t clockIdleTime = (*statState)->lastClockIdleTime;
	uint64_t clockTotalTime = (*statState)->lastClockTotalTime;

	getMachineLoad(clockIdleTime, clockTotalTime);
	returnStats.machineCPUSeconds = clockTotalTime - (*statState)->lastClockTotalTime != 0 ? ( 1 - ((clockIdleTime - (*statState)->lastClockIdleTime) / ((double)(clockTotalTime - (*statState)->lastClockTotalTime)))) * returnStats.elapsed : 0;
	(*statState)->lastClockIdleTime = clockIdleTime;
	(*statState)->lastClockTotalTime = clockTotalTime;
#endif
	(*statState)->lastTime = nowTime;
	(*statState)->lastClockProcess = nowClockProcess;
	(*statState)->lastClockThread = nowClockThread;
	return returnStats;
}

#ifdef _WIN32
struct OffsetTimer {
	double secondsPerCount, offset;

	static const int64_t FILETIME_C_EPOCH = 11644473600LL * 10000000LL;	// Difference between FILETIME epoch (1601) and Unix epoch (1970) in 100ns FILETIME ticks

	OffsetTimer() {
		long long countsPerSecond;
		if (!QueryPerformanceFrequency( (LARGE_INTEGER*)&countsPerSecond))
			throw performance_counter_error();
		secondsPerCount = 1.0 / countsPerSecond;

		FILETIME fileTime;

		offset = 0;
		double timer = now();
		GetSystemTimeAsFileTime(&fileTime);
		static_assert( sizeof(fileTime) == sizeof(uint64_t), "FILETIME size wrong" );
		offset = (*(uint64_t*)&fileTime - FILETIME_C_EPOCH) * 100e-9 - timer;
	}

	double now() {
		long long count;
		if (!QueryPerformanceCounter( (LARGE_INTEGER*)&count ))
			throw performance_counter_error();
		return offset + count * secondsPerCount;
	}
};
#elif defined(__linux__)
#define DOUBLETIME(ts) (double(ts.tv_sec) + (ts.tv_nsec * 1e-9))
#ifndef CLOCK_MONOTONIC_RAW
#define CLOCK_MONOTONIC_RAW 4 // Confirmed safe to do with glibc >= 2.11 and kernel >= 2.6.28. No promises with older glibc. Older kernel definitely breaks it.
#endif
struct OffsetTimer {
	double offset;

	OffsetTimer() {
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);
		offset = DOUBLETIME(ts);
		clock_gettime(CLOCK_MONOTONIC, &ts);
		offset -= DOUBLETIME(ts);
	}

	double now() {
		struct timespec ts;
		clock_gettime(CLOCK_MONOTONIC, &ts);
		return (offset + DOUBLETIME(ts));
	}
};

#elif defined(__APPLE__)

#include <mach/mach.h>
#include <mach/mach_time.h>

struct OffsetTimer {
	mach_timebase_info_data_t timebase_info;
	uint64_t offset;
	double offset_seconds;

	OffsetTimer() {
		mach_timebase_info(&timebase_info);
		offset = mach_absolute_time();

		struct timeval tv;
		gettimeofday(&tv, NULL);

		offset_seconds = tv.tv_sec + 1e-6 * tv.tv_usec;
	}

	double now() {
		uint64_t elapsed = mach_absolute_time() - offset;
		return offset_seconds + double((elapsed * timebase_info.numer) / timebase_info.denom) * 1e-9;
	}
};

#else
#error Port me!
#endif

double timer_monotonic() {
	static OffsetTimer theTimer;
	return theTimer.now();
}

double timer() {
#ifdef _WIN32
	static const int64_t FILETIME_C_EPOCH = 11644473600LL * 10000000LL;	// Difference between FILETIME epoch (1601) and Unix epoch (1970) in 100ns FILETIME ticks
	FILETIME fileTime;
	GetSystemTimeAsFileTime(&fileTime);
	static_assert( sizeof(fileTime) == sizeof(uint64_t), "FILETIME size wrong" );
	return (*(uint64_t*)&fileTime - FILETIME_C_EPOCH) * 100e-9;
#elif defined(__linux__)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return double(ts.tv_sec) + (ts.tv_nsec * 1e-9);
#elif defined(__APPLE__)
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return double(tv.tv_sec) + (tv.tv_usec * 1e-6);
#else
#error Port me!
#endif
};

uint64_t timer_int() {
#ifdef _WIN32
	static const int64_t FILETIME_C_EPOCH = 11644473600LL * 10000000LL;	// Difference between FILETIME epoch (1601) and Unix epoch (1970) in 100ns FILETIME ticks
	FILETIME fileTime;
	GetSystemTimeAsFileTime(&fileTime);
	static_assert( sizeof(fileTime) == sizeof(uint64_t), "FILETIME size wrong" );
	return (*(uint64_t*)&fileTime - FILETIME_C_EPOCH);
#elif defined(__linux__)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return uint64_t(ts.tv_sec) * 1e9 + ts.tv_nsec;
#elif defined(__APPLE__)
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return uint64_t(tv.tv_sec) * 1e9 + (tv.tv_usec * 1e3);
#else
#error Port me!
#endif
};

void getLocalTime(const time_t *timep, struct tm *result) {
#ifdef _WIN32
	if(localtime_s(result, timep) != 0) {
		TraceEvent(SevError, "GetLocalTimeError").GetLastError();
		throw platform_error();
	}
#elif defined(__unixish__)
	if(localtime_r(timep, result) == NULL) {
		TraceEvent(SevError, "GetLocalTimeError").GetLastError();
		throw platform_error();
	}
#else
#error Port me!
#endif
}

void setMemoryQuota( size_t limit ) {
	INJECT_FAULT( platform_error, "setMemoryQuota" );
#if defined(_WIN32)
	HANDLE job = CreateJobObject( NULL, NULL );
	if (!job) {
		TraceEvent(SevError, "WinCreateJobError").GetLastError();
		throw platform_error();
	}
	JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits;
	limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_JOB_MEMORY;
	limits.JobMemoryLimit = limit;
	if (!SetInformationJobObject( job, JobObjectExtendedLimitInformation, &limits, sizeof(limits) )) {
		TraceEvent(SevError, "FailedToSetInfoOnJobObject").detail("Limit", limit).GetLastError();
		throw platform_error();
	}
	if (!AssignProcessToJobObject( job, GetCurrentProcess() ))
		TraceEvent(SevWarn, "FailedToSetMemoryLimit").GetLastError();
#elif defined(__linux__)
	struct rlimit rlim;
	if (getrlimit(RLIMIT_AS, &rlim)) {
		TraceEvent(SevError, "GetMemoryLimit").GetLastError();
		throw platform_error();
	} else if (limit > rlim.rlim_max) {
		TraceEvent(SevError, "MemoryLimitTooHigh").detail("Limit", limit).detail("ResidentMaxLimit", rlim.rlim_max);
		throw platform_error();
	}
	rlim.rlim_cur = limit;
	if (setrlimit(RLIMIT_AS, &rlim)) {
		TraceEvent(SevError, "SetMemoryLimit").detail("Limit", limit).GetLastError();
		throw platform_error();
	}
#endif
}

#ifdef _WIN32
static int ModifyPrivilege( const char* szPrivilege, bool fEnable )
{
	HRESULT hr = S_OK;
	TOKEN_PRIVILEGES NewState;
	LUID luid;
	HANDLE hToken = NULL;

	// Open the process token for this process.
	if (!OpenProcessToken( GetCurrentProcess(),
						   TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY,
						   &hToken ))
	{
		TraceEvent( SevWarn, "OpenProcessTokenError" ).error(large_alloc_failed()).GetLastError();
		return ERROR_FUNCTION_FAILED;
	}

	// Get the local unique ID for the privilege.
	if ( !LookupPrivilegeValue( NULL,
								szPrivilege,
								&luid ))
	{
		CloseHandle( hToken );
		TraceEvent( SevWarn, "LookupPrivilegeValue" ).error(large_alloc_failed()).GetLastError();
		return ERROR_FUNCTION_FAILED;
	}

	//cout << luid.HighPart << " " << luid.LowPart << endl;

	// Assign values to the TOKEN_PRIVILEGE structure.
	NewState.PrivilegeCount = 1;
	NewState.Privileges[0].Luid = luid;
	NewState.Privileges[0].Attributes =
		(fEnable ? SE_PRIVILEGE_ENABLED : 0);

	// Adjust the token privilege.
	if (!AdjustTokenPrivileges(hToken,
							   FALSE,
							   &NewState,
							   0,
							   NULL,
							   NULL))
	{
		TraceEvent( SevWarn, "AdjustTokenPrivileges" ).error(large_alloc_failed()).GetLastError();
		hr = ERROR_FUNCTION_FAILED;
	}

	// Close the handle.
	CloseHandle(hToken);

	return hr;
}
#endif

static bool largePagesPrivilegeEnabled = false;

static void enableLargePages() {
	if (largePagesPrivilegeEnabled)
		return;
#ifdef _WIN32
	ModifyPrivilege(SE_LOCK_MEMORY_NAME, true);
	largePagesPrivilegeEnabled = true;
#else
	// SOMEDAY: can/should we teach the client how to enable large pages
	// on Linux? Or just rely on the system to have been configured as
	// desired?
#endif
}

static void *allocateInternal(size_t length, bool largePages) {
	void *block = NULL;

#ifdef _WIN32
	DWORD allocType = MEM_COMMIT|MEM_RESERVE;

	if (largePages)
		allocType |= MEM_LARGE_PAGES;

	return VirtualAlloc(NULL, length, allocType, PAGE_READWRITE);
#elif defined(__linux__)
	int flags = MAP_PRIVATE|MAP_ANONYMOUS;

	if (largePages)
		flags |= MAP_HUGETLB;

	return mmap(NULL, length, PROT_READ|PROT_WRITE, flags, -1, 0);
#elif defined(__APPLE__)
	int flags = MAP_PRIVATE|MAP_ANON;

	return mmap(NULL, length, PROT_READ|PROT_WRITE, flags, -1, 0);
#else
#error Port me!
#endif
}

static bool largeBlockFail = false;
void *allocate(size_t length, bool allowLargePages) {
	if (allowLargePages)
		enableLargePages();

	void *block = ALLOC_FAIL;

	if (allowLargePages && !largeBlockFail) {
		block = allocateInternal(length, true);
		if (block == ALLOC_FAIL) largeBlockFail = true;
	}

	if (block == ALLOC_FAIL)
		block = allocateInternal(length, false);

	// FIXME: SevWarnAlways trace if "close" to out of memory

	if (block == ALLOC_FAIL)
		platform::outOfMemory();

	return block;
}

#if 0
void* numaAllocate(size_t size) {
	void* thePtr = (void*)0xA00000000LL;
	enableLargePages();

	size_t vaPageSize = 2<<20;//64<<10;
	int nVAPages = size / vaPageSize;

	int nodes;
	if (!GetNumaHighestNodeNumber((PULONG)&nodes)) {
		TraceEvent(SevError, "GetNumaHighestNodeNumber").getLastError();
		throw platform_error();
	}
	++nodes;

	for(int i=0; i<nodes; i++) {
		char* p = (char*)thePtr + i*nVAPages/nodes*vaPageSize;
		char* e = (char*)thePtr + (i+1)*nVAPages/nodes*vaPageSize;
		//printf("  %p + %lld\n", p, e-p);
		// SOMEDAY: removed NUMA extensions for compatibity with Windows Server 2003 -- make execution dynamic
		if (!VirtualAlloc/*ExNuma*/(/*GetCurrentProcess(),*/ p, e-p, MEM_COMMIT|MEM_RESERVE|MEM_LARGE_PAGES, PAGE_READWRITE/*, i*/)) {
			Error e = platform_error();
			TraceEvent(e, "VirtualAlloc").GetLastError();
			throw e;
		}
	}
	return thePtr;
}
#endif

void setAffinity(int proc) {
#if defined(_WIN32)
	/*if (SetProcessAffinityMask(GetCurrentProcess(), 0x5555))//0x5555555555555555UL))
		printf("Set affinity mask\n");
	else
		printf("Failed to set affinity mask: error %d\n", GetLastError());*/
	SetThreadAffinityMask( GetCurrentThread(), 1UL<<proc );
#elif defined(__linux__)
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(proc, &set);
	sched_setaffinity(0, sizeof(cpu_set_t), &set);
#endif
}


namespace platform {

int getRandomSeed() {
	INJECT_FAULT( platform_error, "getRandomSeed" );
	int randomSeed;
	int retryCount = 0;

#ifdef _WIN32
	do {
		retryCount++;
		if( rand_s( (unsigned int *)&randomSeed ) != 0 ) {
			TraceEvent(SevError, "WindowsRandomSeedError");
			throw platform_error();
		}
	} while (randomSeed == 0 && retryCount < FLOW_KNOBS->RANDOMSEED_RETRY_LIMIT);	// randomSeed cannot be 0 since we use mersenne twister in DeterministicRandom. Get a new one if randomSeed is 0.
#else
	int devRandom = open("/dev/urandom", O_RDONLY);
	do {
		retryCount++;
		if (read(devRandom, &randomSeed, sizeof(randomSeed)) != sizeof(randomSeed) ) {
			TraceEvent(SevError, "OpenURandom").GetLastError();
			throw platform_error();	
		}
	} while (randomSeed == 0 && retryCount < FLOW_KNOBS->RANDOMSEED_RETRY_LIMIT);
	close(devRandom);
#endif

	if (randomSeed == 0) {
		TraceEvent(SevError, "RandomSeedZeroError");
		throw platform_error();
	}
	return randomSeed;
}
}; // namespace platform

std::string joinPath( std::string const& directory, std::string const& filename ) {
	auto d = directory;
	auto f = filename;
	while (f.size() && (f[0] == '/' || f[0] == CANONICAL_PATH_SEPARATOR))
		f = f.substr(1);
	while (d.size() && (d.back() == '/' || d.back() == CANONICAL_PATH_SEPARATOR))
		d = d.substr(0, d.size()-1);
	return d + CANONICAL_PATH_SEPARATOR + f;
}

void renamedFile() {
	INJECT_FAULT( io_error, "renameFile" );
}

void renameFile( std::string const& fromPath, std::string const& toPath ) {
	INJECT_FAULT( io_error, "renameFile" );
#ifdef _WIN32
	if (MoveFile( fromPath.c_str(), toPath.c_str() )) {
		//renamedFile();
		return;
	}
#elif (defined(__linux__) || defined(__APPLE__))
	if (!rename( fromPath.c_str(), toPath.c_str() )) {
		//FIXME: We cannot inject faults after renaming the file, because we could end up with two asyncFileNonDurable open for the same file
		//renamedFile();
		return;
	}
#else
	#error Port me!
#endif
	TraceEvent(SevError, "RenameFile").detail("FromPath", fromPath).detail("ToPath", toPath).GetLastError();
	throw io_error();
}

void atomicReplace( std::string const& path, std::string const& content, bool textmode ) {
	FILE* f = 0;
	try {
		INJECT_FAULT( io_error, "atomicReplace" );

		std::string tempfilename = parentDirectory(path) + CANONICAL_PATH_SEPARATOR + g_random->randomUniqueID().toString() + ".tmp";
		f = textmode ? fopen( tempfilename.c_str(), "wt" ) : fopen(tempfilename.c_str(), "wb");
		if(!f)
			throw io_error();

		if( textmode && fprintf( f, "%s", content.c_str() ) < 0)
			throw io_error();

		if (!textmode && fwrite(content.c_str(), sizeof(uint8_t), content.size(), f) != content.size())
			throw io_error();

		if(fflush(f) != 0)
			throw io_error();

	#ifdef _WIN32
		HANDLE h = (HANDLE)_get_osfhandle(_fileno(f));
		if(!g_network->isSimulated()) {
			if(!FlushFileBuffers(h))
				throw io_error();
		}

		if(fclose(f) != 0) {
			f = 0;
			throw io_error();
		}
		f = 0;

		if(!ReplaceFile( path.c_str(), tempfilename.c_str(), NULL, NULL, NULL, NULL ))
			throw io_error();
	#elif defined(__unixish__)
		if(!g_network->isSimulated()) {
			if(fsync( fileno(f) ) != 0)
				throw io_error();
		}

		if(fclose(f) != 0) {
			f = 0;
			throw io_error();
		}
		f = 0;

		if(rename( tempfilename.c_str(), path.c_str() ) != 0)
			throw io_error();
	#else
	#error Port me!
	#endif

		INJECT_FAULT( io_error, "atomicReplace" );
	}
	catch(Error &e) {
		TraceEvent(SevWarn, "AtomicReplace").error(e).detail("Path", path).GetLastError();
		if (f) fclose(f);
		throw;
	}
}

static bool deletedFile() {
	INJECT_FAULT( platform_error, "deleteFile" );
	return true;
}

bool deleteFile( std::string const& filename ) {
	INJECT_FAULT( platform_error, "deleteFile" );
#ifdef _WIN32
	if (DeleteFile(filename.c_str()))
		return deletedFile();
	if (GetLastError() == ERROR_FILE_NOT_FOUND)
		return false;
#elif defined(__unixish__)
	if (!unlink( filename.c_str() ))
		return deletedFile();
	if (errno == ENOENT)
		return false;
#else
	#error Port me!
#endif
	Error e = systemErrorCodeToError();
	TraceEvent(SevError, "DeleteFile").detail("Filename", filename).GetLastError().error(e);
	throw errno;
}

static void createdDirectory() { INJECT_FAULT( platform_error, "createDirectory" ); }

namespace platform {

bool createDirectory( std::string const& directory ) {
	INJECT_FAULT( platform_error, "createDirectory" );

#ifdef _WIN32
	if (CreateDirectory( directory.c_str(), NULL )) {
		createdDirectory();
		return true;
	}
	if (GetLastError() == ERROR_ALREADY_EXISTS)
		return false;
	if (GetLastError() == ERROR_PATH_NOT_FOUND) {
		size_t delim = directory.find_last_of("/\\");
		if (delim != std::string::npos) {
			createDirectory( directory.substr(0, delim) );
			return createDirectory( directory );
		}
	}
	Error e = systemErrorCodeToError();
	TraceEvent(SevError, "CreateDirectory").detail("Directory", directory).GetLastError().error(e);
	throw e;
#elif (defined(__linux__) || defined(__APPLE__))
	size_t sep = 0;
	do {
		sep = directory.find_first_of('/', sep + 1);
		if ( mkdir( directory.substr(0, sep).c_str(), 0755 ) != 0 ) {
			if (errno == EEXIST)
				continue;

			Error e;
			if(errno == EACCES) {
				e = file_not_writable();
			}
			else {
				e = systemErrorCodeToError();
			}

			TraceEvent(SevError, "CreateDirectory").detail("Directory", directory).GetLastError().error(e);
			throw e;
		}
		createdDirectory();
	} while (sep != std::string::npos && sep != directory.length() - 1);
	return true;
#else
	#error Port me!
#endif
}

}; // namespace platform

std::string abspath( std::string const& filename ) {
	// Returns an absolute path canonicalized to use only CANONICAL_PATH_SEPARATOR
	INJECT_FAULT( platform_error, "abspath" );

#ifdef _WIN32
	char nameBuffer[MAX_PATH];
	if (!GetFullPathName(filename.c_str(), MAX_PATH, nameBuffer, NULL)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "AbsolutePathError").detail("Filename", filename).GetLastError().error(e);
		throw e;
	}
	// Not totally obvious from the help whether GetFullPathName canonicalizes slashes, so let's do it...
	for(char*x = nameBuffer; *x; x++)
		if (*x == '/')
			*x = CANONICAL_PATH_SEPARATOR;
	return nameBuffer;
#elif (defined(__linux__) || defined(__APPLE__))
	char result[PATH_MAX];
	auto r = realpath( filename.c_str(), result );
	if (!r) {
		if (errno == ENOENT) {
			int sep = filename.find_last_of( CANONICAL_PATH_SEPARATOR );
			if (sep != std::string::npos) {
				return joinPath( abspath( filename.substr(0, sep) ), filename.substr(sep) );
			}
			else if (filename.find("~") == std::string::npos) {
				return joinPath( abspath( "." ), filename );
			}
		}
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "AbsolutePathError").detail("Filename", filename).GetLastError().error(e);
		throw e;
	}
	return std::string(r);
#else
	#error Port me!
#endif
}

std::string basename( std::string const& filename ) {
	auto abs = abspath(filename);
	size_t sep = abs.find_last_of( CANONICAL_PATH_SEPARATOR );
	if (sep == std::string::npos) return filename;
	return abs.substr(sep+1);
}

std::string parentDirectory( std::string const& filename ) {
	auto abs = abspath(filename);
	size_t sep = abs.find_last_of( CANONICAL_PATH_SEPARATOR );
	if (sep == std::string::npos) {
		TraceEvent(SevError, "GetParentDirectoryOfFile")
			.detail("File", filename)
			.GetLastError();
		throw platform_error();
	}
	return abs.substr(0, sep);
}

std::string getUserHomeDirectory() {
#if defined(__unixish__)
	const char* ret = getenv( "HOME" );
	if ( !ret ) {
		if ( struct passwd *pw = getpwuid( getuid() ) ) {
			ret = pw->pw_dir;
		}
	}
	return ret;
#elif defined(_WIN32)
	TCHAR szPath[MAX_PATH];
	if( SHGetFolderPath(NULL, CSIDL_PROFILE, NULL, 0, szPath)  != S_OK ) {
		TraceEvent(SevError, "GetUserHomeDirectory").GetLastError();
		throw platform_error();
	}
	std::string path(szPath);
	return path;
#else
#error Port me!
#endif
}

#ifdef _WIN32
#define FILE_ATTRIBUTE_DATA DWORD
#elif (defined(__linux__) || defined(__APPLE__))
#define FILE_ATTRIBUTE_DATA mode_t
#else
#error Port me!
#endif

bool acceptFile( FILE_ATTRIBUTE_DATA fileAttributes, std::string name, std::string extension ) {
#ifdef _WIN32
	return !(fileAttributes & FILE_ATTRIBUTE_DIRECTORY) && StringRef(name).endsWith(extension);
#elif (defined(__linux__) || defined(__APPLE__))
	return S_ISREG(fileAttributes) && StringRef(name).endsWith(extension);
#else
	#error Port me!
#endif
}

bool acceptDirectory( FILE_ATTRIBUTE_DATA fileAttributes, std::string name, std::string extension ) {
#ifdef _WIN32
	return (fileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
#elif (defined(__linux__) || defined(__APPLE__))
	return S_ISDIR(fileAttributes);
#else
	#error Port me!
#endif
}

std::vector<std::string> findFiles( std::string const& directory, std::string const& extension,
		bool (*accept_file)(FILE_ATTRIBUTE_DATA, std::string, std::string)) {
	INJECT_FAULT( platform_error, "findFiles" );
	std::vector<std::string> result;

#ifdef _WIN32
	WIN32_FIND_DATA fd;
	HANDLE h = FindFirstFile( (directory + "/*" + extension).c_str(), &fd );
	if (h == INVALID_HANDLE_VALUE) {
		if (GetLastError() != ERROR_FILE_NOT_FOUND && GetLastError() != ERROR_PATH_NOT_FOUND) {
			TraceEvent(SevError, "FindFirstFile").detail("Directory", directory).detail("Extension", extension).GetLastError();
			throw platform_error();
		}
	} else {
		while (true) {
			std::string name = fd.cFileName;
			if ((*accept_file)(fd.dwFileAttributes, name, extension)) {
				result.push_back( name );
			}
			if (!FindNextFile( h, &fd ))
				break;
		}
		if (GetLastError() != ERROR_NO_MORE_FILES) {
			TraceEvent(SevError, "FindNextFile").detail("Directory", directory).detail("Extension", extension).GetLastError();
			FindClose(h);
			throw platform_error();
		}
		FindClose(h);
	}
#elif (defined(__linux__) || defined(__APPLE__))
	DIR *dip;

	if ((dip = opendir(directory.c_str())) != NULL) {
		struct dirent *dit;
		while ((dit = readdir(dip)) != NULL) {
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
			if ((*accept_file)(buf.st_mode, name, extension))
				result.push_back( name );
		}

		closedir(dip);
	}
#else
	#error Port me!
#endif
	std::sort(result.begin(), result.end());
	return result;
}


namespace platform {

std::vector<std::string> listFiles( std::string const& directory, std::string const& extension ) {
	return findFiles( directory, extension, &acceptFile );
}

std::vector<std::string> listDirectories( std::string const& directory ) {
	return findFiles( directory, "", &acceptDirectory );
}

void findFilesRecursively(std::string path, std::vector<std::string> &out) {
	// Add files to output, prefixing path
	std::vector<std::string> files = platform::listFiles(path);
	for(auto const &f : files)
		out.push_back(joinPath(path, f));

	// Recurse for directories
	std::vector<std::string> directories = platform::listDirectories(path);
	for(auto const &dir : directories) {
		if(dir != "." && dir != "..")
			findFilesRecursively(joinPath(path, dir), out);
	}
};

}; // namespace platform


void threadSleep( double seconds ) {
#ifdef _WIN32
	Sleep( (DWORD)(seconds * 1e3) );
#elif (defined(__linux__) || defined(__APPLE__))
	struct timespec req, rem;

	req.tv_sec = seconds;
	req.tv_nsec = (seconds - req.tv_sec) * 1e9L;

	while (nanosleep(&req, &rem) == -1 && errno == EINTR) {
		req.tv_sec = rem.tv_sec;
		req.tv_nsec = rem.tv_nsec;
	}
#else
	#error Port me!
#endif
}

void threadYield() {
#ifdef _WIN32
	Sleep(0);
#elif defined( __unixish__ )
	sched_yield();
#else
#error Port me!
#endif
}

namespace platform {

void makeTemporary( const char* filename ) {
#ifdef _WIN32
	SetFileAttributes(filename, FILE_ATTRIBUTE_TEMPORARY);
#endif
}
}; // namespace platform

#ifdef _WIN32
THREAD_HANDLE startThread(void (*func) (void *), void *arg) {
	return (void *)_beginthread(func, 0, arg);
}
#elif (defined(__linux__) || defined(__APPLE__))
THREAD_HANDLE startThread(void *(*func) (void *), void *arg) {
	pthread_t t;
	pthread_create(&t, NULL, func, arg);
	return t;
}
#else
	#error Port me!
#endif

void waitThread(THREAD_HANDLE thread) {
#ifdef _WIN32
	WaitForSingleObject(thread, INFINITE);
#elif (defined(__linux__) || defined(__APPLE__))
	pthread_join(thread, NULL);
#else
	#error Port me!
#endif
}

void deprioritizeThread() {
#ifdef __linux__
	int tid = syscall(SYS_gettid);
	setpriority( PRIO_PROCESS, tid, 10 );
#elif defined(_WIN32)
#endif
}

bool fileExists(std::string const& filename) {
	FILE* f = fopen(filename.c_str(), "rb");
	if (!f) return false;
	fclose(f);
	return true;
}

bool directoryExists(std::string const& path) {
#ifdef _WIN32
	DWORD bits = ::GetFileAttributes(path.c_str());
	return bits != INVALID_FILE_ATTRIBUTES && (bits & FILE_ATTRIBUTE_DIRECTORY);
#else
	DIR *d = opendir(path.c_str());
	if(d == nullptr)
		return false;
	closedir(d);
	return true;
#endif
}

int64_t fileSize(std::string const& filename) {
#ifdef _WIN32
	struct _stati64 file_status;
	if(_stati64(filename.c_str(), &file_status) != 0)
		return 0;
	else
		return file_status.st_size;
#elif (defined(__linux__) || defined(__APPLE__))
	struct stat file_status;
	if(stat(filename.c_str(), &file_status) != 0)
		return 0;
	else
		return file_status.st_size;
#else
	#error Port me!
#endif
}

std::string readFileBytes( std::string const& filename, int maxSize ) {
	std::string s;
	FILE* f = fopen(filename.c_str(), "rb");
	if (!f) throw file_not_readable();
	try {
		fseek(f, 0, SEEK_END);
		size_t size = ftell(f);
		if (size > maxSize)
			throw file_too_large();
		s.resize( size );
		fseek(f, 0, SEEK_SET);
		if (!fread( &s[0], size, 1, f ))
			throw file_not_readable();
	} catch (...) {
		fclose(f);
		throw;
	}
	fclose(f);
	return s;
}

void writeFileBytes(std::string const& filename, const uint8_t* data, size_t count) {
	FILE* f = fopen(filename.c_str(), "wb");
	if (!f)
	{
		TraceEvent(SevError, "WriteFileBytes").detail("Filename", filename).GetLastError();
		throw file_not_writable();
	}

	try {
		size_t length = fwrite(data, sizeof(uint8_t), count, f);
		if (length != count)
		{
			TraceEvent(SevError, "WriteFileBytes").detail("Filename", filename).detail("WrittenLength", length).GetLastError();
			throw file_not_writable();
		}
	}
	catch (...) {
		fclose(f);
		throw;
	}
	fclose(f);
}

void writeFile(std::string const& filename, std::string const& content) {
	writeFileBytes(filename, (const uint8_t*)(content.c_str()), content.size());
}


namespace platform {

bool getEnvironmentVar(const char* name, std::string& value) {
#if defined(__unixish__)
	char* val = getenv(name);
	if (val) {
		value = std::string(val);
		return true;
	}
	return false;
#elif defined(_WIN32)
	int len = GetEnvironmentVariable(name, NULL, 0);
	if (len == 0) {
		if (GetLastError() == ERROR_ENVVAR_NOT_FOUND) {
			return false;
		}
		TraceEvent(SevError, "GetEnvironmentVariable").detail("Name", name).GetLastError();
		throw platform_error();
	}
	value.resize(len);
	int rc = GetEnvironmentVariable(name, &value[0], len);
	if (rc + 1 != len) {
		TraceEvent(SevError, "WrongEnvVarLength")
			.detail("ExpectedLength", len)
			.detail("ReceivedLength", rc + 1);
		throw platform_error();
	}
	value.resize(len-1);
	return true;
#else
#error Port me!
#endif
}

int setEnvironmentVar(const char *name, const char *value, int overwrite)
{
#if defined(_WIN32)
	int errcode = 0;
	if(!overwrite) {
		size_t envsize = 0;
		errcode = getenv_s(&envsize, NULL, 0, name);
		if(errcode || envsize) return errcode;
	}
	return _putenv_s(name, value);
#else
	return setenv(name, value, overwrite);
#endif
}

#if defined(_WIN32)
#define getcwd(buf, maxlen) _getcwd(buf, maxlen)
#endif
std::string getWorkingDirectory() {
	char *buf;
	if( (buf = getcwd(NULL, 0)) == NULL ) {
		TraceEvent(SevWarnAlways, "GetWorkingDirectoryError").GetLastError();
		throw platform_error();
	}
	std::string result(buf);
	free(buf);
	return result;
}

}; // namespace platform

extern std::string format( const char *form, ... );


namespace platform {

std::string getDefaultPluginPath( const char* plugin_name ) {
#ifdef _WIN32
	std::string installPath;
	if(!platform::getEnvironmentVar("FOUNDATIONDB_INSTALL_PATH", installPath)) {
		// This is relying of the DLL search order to load the plugin,
		//  starting in the same directory as the executable.
		return plugin_name;
	}
	return format( "%splugins\\%s.dll", installPath.c_str(), plugin_name );
#elif defined(__linux__)
	return format( "/usr/lib/foundationdb/plugins/%s.so", plugin_name );
#elif defined(__APPLE__)
	return format( "/usr/local/foundationdb/plugins/%s.dylib", plugin_name );
#else
	#error Port me!
#endif
}
}; // namespace platform

#ifdef ALLOC_INSTRUMENTATION
#define TRACEALLOCATOR( size ) TraceEvent("MemSample").detail("Count", FastAllocator<size>::getApproximateMemoryUnused()/size).detail("TotalSize", FastAllocator<size>::getApproximateMemoryUnused()).detail("SampleCount", 1).detail("Hash", "FastAllocatedUnused" #size ).detail("Bt", "na")
#ifdef __linux__
#include <cxxabi.h>
#endif
uint8_t *g_extra_memory;
#endif

namespace platform {

void outOfMemory() {
#ifdef ALLOC_INSTRUMENTATION
	delete [] g_extra_memory;
	std::vector< std::pair<std::string, const char*> > typeNames;
	for( auto i = allocInstr.begin(); i != allocInstr.end(); ++i ) {
		std::string s;
#ifdef __linux__
		char *demangled = abi::__cxa_demangle(i->first, NULL, NULL, NULL);
		if (demangled) {
			s = demangled;
			if (StringRef(s).startsWith(LiteralStringRef("(anonymous namespace)::")))
				s = s.substr(LiteralStringRef("(anonymous namespace)::").size());
			free(demangled);
		} else
			s = i->first;
#else
		s = i->first;
		if (StringRef(s).startsWith(LiteralStringRef("class `anonymous namespace'::")))
			s = s.substr(LiteralStringRef("class `anonymous namespace'::").size());
		else if (StringRef(s).startsWith(LiteralStringRef("class ")))
			s = s.substr(LiteralStringRef("class ").size());
		else if (StringRef(s).startsWith(LiteralStringRef("struct ")))
			s = s.substr(LiteralStringRef("struct ").size());
#endif
		typeNames.push_back( std::make_pair(s, i->first) );
	}
	std::sort(typeNames.begin(), typeNames.end());
	for(int i=0; i<typeNames.size(); i++) {
		const char* n = typeNames[i].second;
		auto& f = allocInstr[n];
		if(f.maxAllocated > 10000)
			TraceEvent("AllocInstrument").detail("CurrentAlloc", f.allocCount-f.deallocCount)
				.detail("Name", typeNames[i].first.c_str());
	}

	std::unordered_map<uint32_t, BackTraceAccount> traceCounts;
	size_t memSampleSize;
	memSample_entered = true;
	{
		ThreadSpinLockHolder holder( memLock );
		traceCounts = backTraceLookup;
		memSampleSize = memSample.size();
	}
	memSample_entered = false;

	TraceEvent("MemSampleSummary")
		.detail("InverseByteSampleRatio", SAMPLE_BYTES)
		.detail("MemorySamples", memSampleSize)
		.detail("BackTraces", traceCounts.size());

	for( auto i = traceCounts.begin(); i != traceCounts.end(); ++i ) {
		char buf[1024];
		std::vector<void *> *frames = i->second.backTrace;
		std::string backTraceStr;
#if defined(_WIN32)
		for (int j = 1; j < frames->size(); j++) {
			_snprintf(buf, 1024, "%p ", frames->at(j));
			backTraceStr += buf;
		}
#else
		backTraceStr = format_backtrace(&(*frames)[0], frames->size());
#endif
		TraceEvent("MemSample")
			.detail("Count", (int64_t)i->second.count)
			.detail("TotalSize", i->second.totalSize)
			.detail("SampleCount", i->second.sampleCount)
			.detail("Hash", format("%lld", i->first))
			.detail("Bt", backTraceStr);
	}

	TraceEvent("MemSample")
		.detail("Count", traceCounts.size())
		.detail("TotalSize", traceCounts.size() * ((int)(sizeof(uint32_t) + sizeof(size_t) + sizeof(size_t))))
		.detail("SampleCount", traceCounts.size())
		.detail("Hash", "backTraces")
		.detail("Bt", "na");

	TraceEvent("MemSample")
		.detail("Count", memSampleSize)
		.detail("TotalSize", memSampleSize * ((int)(sizeof(void*) + sizeof(uint32_t) + sizeof(size_t))))
		.detail("SapmleCount", memSampleSize)
		.detail("Hash", "memSamples")
		.detail("Bt", "na");
	TRACEALLOCATOR(16);
	TRACEALLOCATOR(32);
	TRACEALLOCATOR(64);
	TRACEALLOCATOR(128);
	TRACEALLOCATOR(256);
	TRACEALLOCATOR(512);
	TRACEALLOCATOR(1024);
	TRACEALLOCATOR(2048);
	TRACEALLOCATOR(4096);
	g_traceBatch.dump();
#endif

	criticalError(FDB_EXIT_NO_MEM, "OutOfMemory", "Out of memory");
}
}; // namespace platform

extern "C" void criticalError(int exitCode, const char *type, const char *message) {
	// Be careful!  This function may be called asynchronously from a thread or in other weird conditions

	fprintf(stderr, "ERROR: %s\n", message);

	if (g_network && !g_network->isSimulated())
	{
		TraceEvent ev(SevError, type);
		ev.detail("Message", message);
	}

	flushAndExit(exitCode);
}

extern void flushTraceFileVoid();

extern "C" void flushAndExit(int exitCode) {
	flushTraceFileVoid();
	fflush(stdout);
	closeTraceFile();
#ifdef _WIN32
	// This function is documented as being asynchronous, but we suspect it might actually be synchronous in the
	// case that it is passed a handle to the current process. If not, then there may be cases where we escalate
	// to the crashAndDie call below.
	TerminateProcess(GetCurrentProcess(), exitCode);
#else
	_exit(exitCode);
#endif
	// should never reach here, but you never know
	crashAndDie();
}

#ifdef __unixish__
#include <dlfcn.h>

#ifdef __linux__
#include <link.h>
#endif

struct ImageInfo {
	void *offset;
	std::string symbolFileName;

	ImageInfo() : offset(NULL), symbolFileName("") {}
};

ImageInfo getImageInfo(const void *symbol) {
	Dl_info info;
	ImageInfo imageInfo;

#ifdef __linux__
	link_map *linkMap;
	int res = dladdr1(symbol, &info, (void**)&linkMap, RTLD_DL_LINKMAP);
#else
	int res = dladdr(symbol, &info);
#endif

	if(res != 0) {
		std::string imageFile = basename(info.dli_fname);
		// If we have a client library that doesn't end in the appropriate extension, we will get the wrong debug suffix. This should only be a cosmetic problem, though.
#ifdef __linux__
		imageInfo.offset = (void*)linkMap->l_addr;
		if(imageFile.length() >= 3 && imageFile.rfind(".so") == imageFile.length()-3) {
#else
		imageInfo.offset = info.dli_fbase;
		if(imageFile.length() >= 6 && imageFile.rfind(".dylib") == imageFile.length()-6) {
#endif
			imageInfo.symbolFileName = imageFile + "-debug";
		}
		else {
			imageInfo.symbolFileName = imageFile + ".debug";
		}
	}
	else {
		imageInfo.symbolFileName = "unknown";
	}

	return imageInfo;
}

ImageInfo getCachedImageInfo() {
	// The use of "getCachedImageInfo" is arbitrary and was a best guess at a good way to get the image of the
	//  most likely candidate for the "real" flow library or binary
	static ImageInfo info = getImageInfo((const void *)&getCachedImageInfo);
	return info;
}

#include <execinfo.h>

namespace platform {
void* getImageOffset() {
	return getCachedImageInfo().offset;
}

size_t raw_backtrace(void** addresses, int maxStackDepth) {
#if !defined(__APPLE__)
	// absl::GetStackTrace doesn't have an implementation for MacOS.
	return absl::GetStackTrace(addresses, maxStackDepth, 0);
#else
	return backtrace(addresses, maxStackDepth);
#endif
}

std::string format_backtrace(void **addresses, int numAddresses) {
	ImageInfo const& imageInfo = getCachedImageInfo();
#ifdef __APPLE__
	std::string s = format("atos -o %s -arch x86_64 -l %p", imageInfo.symbolFileName.c_str(), imageInfo.offset);
	for(int i = 1; i < numAddresses; i++) {
		s += format(" %p", addresses[i]);
	}
#else
	std::string s = format("addr2line -e %s -p -C -f -i", imageInfo.symbolFileName.c_str());
	for(int i = 1; i < numAddresses; i++) {
		s += format(" %p", (char*)addresses[i]-(char*)imageInfo.offset);
	}
#endif
	return s;
}

std::string get_backtrace() {
	void *addresses[50];
	size_t size = raw_backtrace(addresses, 50);
	return format_backtrace(addresses, size);
}
}; // namespace platform
#else

namespace platform {
std::string get_backtrace() { return std::string(); }
std::string format_backtrace(void **addresses, int numAddresses) { return std::string(); }
void* getImageOffset() { return NULL; }
}; // namespace platform
#endif

bool isLibraryLoaded(const char* lib_path) {
#if !defined(__linux__) && !defined(__APPLE__) && !defined(_WIN32)
#error Port me!
#endif

	void* dlobj = NULL;

#if defined(__unixish__)
	dlobj = dlopen( lib_path, RTLD_NOLOAD | RTLD_LAZY );
#else
	dlobj = GetModuleHandle( lib_path );
#endif

	return dlobj != NULL;
}

void* loadLibrary(const char* lib_path) {
#if !defined(__linux__) && !defined(__APPLE__) && !defined(_WIN32)
#error Port me!
#endif

	void* dlobj = NULL;

#if defined(__unixish__)
	dlobj = dlopen( lib_path, RTLD_LAZY | RTLD_LOCAL );
	if(dlobj == NULL) {
		TraceEvent(SevWarn, "LoadLibraryFailed").detail("Library", lib_path).detail("Error", dlerror());
	}
#else
	dlobj = LoadLibrary( lib_path );
	if(dlobj == NULL) {
		TraceEvent(SevWarn, "LoadLibraryFailed").detail("Library", lib_path).GetLastError();
	}
#endif

	return dlobj;
}

void* loadFunction(void* lib, const char* func_name) {
	void* dlfcn = NULL;

#if defined(__unixish__)
	dlfcn = dlsym( lib, func_name );
	if(dlfcn == NULL) {
		TraceEvent(SevWarn, "LoadFunctionFailed").detail("Function", func_name).detail("Error", dlerror());
	}
#else
	dlfcn = GetProcAddress( (HINSTANCE)lib, func_name );
	if(dlfcn == NULL) {
		TraceEvent(SevWarn, "LoadFunctionFailed").detail("Function", func_name).GetLastError();
	}
#endif

	return dlfcn;
}

void platformInit() {
#ifdef WIN32
	_set_FMA3_enable(0); // Workaround for VS 2013 code generation bug. See https://connect.microsoft.com/VisualStudio/feedback/details/811093/visual-studio-2013-rtm-c-x64-code-generation-bug-for-avx2-instructions
#endif
#ifdef __linux__
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		criticalError(FDB_EXIT_ERROR, "MonotonicTimeUnavailable", "clock_gettime(CLOCK_MONOTONIC, ...) returned an error. Check your kernel and glibc versions.");
	}
#endif
}

void crashHandler(int sig) {
#ifdef __linux__
	// Pretty much all of this handler is risking undefined behavior and hangs,
	//  but the idea is that we're about to crash anyway...
	std::string backtrace = platform::get_backtrace();

	bool error = (sig != SIGUSR2);

	fflush(stdout);
	TraceEvent(error ? SevError : SevInfo, error ? "Crash" : "ProcessTerminated")
		.detail("Signal", sig)
		.detail("Name", strsignal(sig))
		.detail("Trace", backtrace);
	flushTraceFileVoid();

	fprintf(stderr, "SIGNAL: %s (%d)\n", strsignal(sig), sig);
	fprintf(stderr, "Trace: %s\n", backtrace.c_str());

	_exit(128 + sig);
#else
	// No crash handler for other platforms!
#endif
}

void registerCrashHandler() {
#ifdef __linux__
	// For these otherwise fatal errors, attempt to log a trace of
	// what was happening and then exit
	struct sigaction action;
	action.sa_handler = crashHandler;
	sigfillset( &action.sa_mask );
	action.sa_flags = 0;

	sigaction(SIGILL, &action, NULL);
	sigaction(SIGFPE, &action, NULL);
	sigaction(SIGSEGV, &action, NULL);
	sigaction(SIGBUS, &action, NULL);
	sigaction(SIGUSR2, &action, NULL);
#else
	// No crash handler for other platforms!
#endif
}

#ifdef __linux__
extern volatile void** net2backtraces;
extern volatile size_t net2backtraces_offset;
extern volatile size_t net2backtraces_max;
extern volatile bool net2backtraces_overflow;
extern volatile int net2backtraces_count;
extern volatile double net2liveness;
extern volatile int profilingEnabled;
extern void initProfiling();

volatile thread_local bool profileThread = false;
#endif

volatile int profilingEnabled = 1;

void setProfilingEnabled(int enabled) { 
	profilingEnabled = enabled; 
}

void profileHandler(int sig) {
#ifdef __linux__
	if (!profileThread || !profilingEnabled) {
		return;
	}

	net2backtraces_count++;
	if (!net2backtraces || net2backtraces_max - net2backtraces_offset < 50) {
		net2backtraces_overflow = true;
		return;
	}

	// We are casting away the volatile-ness of the backtrace array, but we believe that should be reasonably safe in the signal handler
	ProfilingSample* ps = const_cast<ProfilingSample*>((volatile ProfilingSample*)(net2backtraces + net2backtraces_offset));

	ps->timestamp = timer();

	// SOMEDAY: should we limit the maximum number of frames from
	// backtrace beyond just available space?
	size_t size = backtrace(ps->frames, net2backtraces_max - net2backtraces_offset - 2);

	ps->length = size;

	net2backtraces_offset += size + 2;
#else
	// No slow task profiling for other platforms!
#endif
}

void* checkThread(void *arg) {
#ifdef __linux__
	pthread_t mainThread = *(pthread_t*)arg;
	free(arg);

	double lastValue = net2liveness;
	double lastSignal = 0;
	double logInterval = FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL;
	while(true) {
		threadSleep(FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL);
		if(lastValue == net2liveness) {
			double t = timer();
			if(lastSignal == 0 || t - lastSignal >= logInterval) {
				if(lastSignal > 0) {
					logInterval = std::min(FLOW_KNOBS->SLOWTASK_PROFILING_MAX_LOG_INTERVAL, FLOW_KNOBS->SLOWTASK_PROFILING_LOG_BACKOFF * logInterval);
				}

				lastSignal = t;
				pthread_kill(mainThread, SIGPROF);
			}
		}
		else {
			lastSignal = 0;
			logInterval = FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL;
		}
		lastValue = net2liveness;
	}
	return NULL;
#else
	// No slow task profiling for other platforms!
	return NULL;
#endif
}

void setupSlowTaskProfiler() {
#ifdef __linux__
	if(FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL > 0) {
		TraceEvent("StartingSlowTaskProfilingThread").detail("Interval", FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL);
		initProfiling();
		profileThread = true;

		struct sigaction action;
		action.sa_handler = profileHandler;
		sigfillset(&action.sa_mask);
		action.sa_flags = 0;
		sigaction(SIGPROF, &action, NULL);

		// Start a thread which will use signals to log stacks on long events
		pthread_t *mainThread = (pthread_t*)malloc(sizeof(pthread_t));
		*mainThread = pthread_self();
		startThread(&checkThread, (void*)mainThread);
	}
#else
	// No slow task profiling for other platforms!
#endif
}

#ifdef __linux__
// There's no good place to put this, so it's here.
// Ubuntu's packaging of libstdc++_pic offers different symbols than libstdc++.  Go figure.
// Notably, it's missing a definition of std::istream::ignore(long), which causes compilation errors
// in the bindings.  Thus, we provide weak versions of their definitions, so that if the
// linked-against libstdc++ is missing their definitions, we'll be able to use the provided
// ignore(long, int) version.
#include <istream>
namespace std {
typedef basic_istream<char, std::char_traits<char>> char_basic_istream;
template <>
char_basic_istream& __attribute__((weak)) char_basic_istream::ignore(streamsize count) {
  return ignore(count, std::char_traits<char>::eof());
}
typedef basic_istream<wchar_t, std::char_traits<wchar_t>> wchar_basic_istream;
template <>
wchar_basic_istream& __attribute__((weak)) wchar_basic_istream::ignore(streamsize count) {
  return ignore(count, std::char_traits<wchar_t>::eof());
}
}
#endif

// UnitTest for getMemoryInfo
#ifdef __linux__
TEST_CASE("/flow/Platform/getMemoryInfo") {

	printf("UnitTest flow/Platform/getMemoryInfo 1\n");
	std::string memString =
		"MemTotal:       24733228 kB\n"
		"MemFree:         2077580 kB\n"
		"Buffers:          266940 kB\n"
		"Cached:         16798292 kB\n"
		"SwapCached:       210240 kB\n"
		"Active:         12447724 kB\n"
		"Inactive:        9175508 kB\n"
		"Active(anon):    3458596 kB\n"
		"Inactive(anon):  1102948 kB\n"
		"Active(file):    8989128 kB\n"
		"Inactive(file):  8072560 kB\n"
		"Unevictable:           0 kB\n"
		"Mlocked:               0 kB\n"
		"SwapTotal:      25165820 kB\n"
		"SwapFree:       23680228 kB\n"
		"Dirty:               200 kB\n"
		"Writeback:             0 kB\n"
		"AnonPages:       4415148 kB\n"
		"Mapped:            62804 kB\n"
		"Shmem:              3544 kB\n"
		"Slab:             620144 kB\n"
		"SReclaimable:     556640 kB\n"
		"SUnreclaim:        63504 kB\n"
		"KernelStack:        5240 kB\n"
		"PageTables:        47292 kB\n"
		"NFS_Unstable:          0 kB\n"
		"Bounce:                0 kB\n"
		"WritebackTmp:          0 kB\n"
		"CommitLimit:    37532432 kB\n"
		"Committed_AS:    8603484 kB\n"
		"VmallocTotal:   34359738367 kB\n"
		"VmallocUsed:      410576 kB\n";

	std::map<StringRef, int64_t> request = {
		{ LiteralStringRef("MemTotal:"), 0 },
		{ LiteralStringRef("MemFree:"), 0 },
		{ LiteralStringRef("MemAvailable:"), 0 },
		{ LiteralStringRef("Buffers:"), 0 },
		{ LiteralStringRef("Cached:"), 0 },
		{ LiteralStringRef("SwapTotal:"), 0 },
		{ LiteralStringRef("SwapFree:"), 0 },
	};

	std::stringstream memInfoStream(memString);
	getMemoryInfo(request, memInfoStream);
	ASSERT(request[LiteralStringRef("MemTotal:")] == 24733228);
	ASSERT(request[LiteralStringRef("MemFree:")] == 2077580);
	ASSERT(request[LiteralStringRef("MemAvailable:")] == 0);
	ASSERT(request[LiteralStringRef("Buffers:")] == 266940);
	ASSERT(request[LiteralStringRef("Cached:")] == 16798292);
	ASSERT(request[LiteralStringRef("SwapTotal:")] == 25165820);
	ASSERT(request[LiteralStringRef("SwapFree:")] == 23680228);
	for (auto & item : request) {
		printf("%s:%ld\n", item.first.toString().c_str(), item.second );
	}

	printf("UnitTest flow/Platform/getMemoryInfo 2\n");
	std::string memString1 =
		"Slab:             192816 kB\n"
		"SReclaimable:     158404 kB\n"
		"SUnreclaim:        34412 kB\n"
		"KernelStack:        7152 kB\n"
		"PageTables:        45284 kB\n"
		"NFS_Unstable:          0 kB\n"
		"Bounce:                0 kB\n"
		"WritebackTmp:          0 kB\n"
		"MemTotal:       31856496 kB\n"
		"MemFree:        25492716 kB\n"
		"MemAvailable:   28470756 kB\n"
		"Buffers:          313644 kB\n"
		"Cached:          2956444 kB\n"
		"SwapCached:            0 kB\n"
		"Active:          3708432 kB\n"
		"Inactive:        2163752 kB\n"
		"Active(anon):    2604524 kB\n"
		"Inactive(anon):   199896 kB\n"
		"Active(file):    1103908 kB\n"
		"Inactive(file):  1963856 kB\n"
		"Unevictable:           0 kB\n"
		"Mlocked:               0 kB\n"
		"SwapTotal:             0 kB\n"
		"SwapFree:              0 kB\n"
		"Dirty:                 0 kB\n"
		"Writeback:             0 kB\n"
		"AnonPages:       2602108 kB\n"
		"Mapped:           361088 kB\n"
		"Shmem:            202332 kB\n"
		"CommitLimit:    15928248 kB\n"
		"Committed_AS:    5556756 kB\n"
		"VmallocTotal:   34359738367 kB\n"
		"VmallocUsed:      427528 kB\n"
		"VmallocChunk:   34359283752 kB\n"
		"HardwareCorrupted:     0 kB\n"
		"AnonHugePages:   1275904 kB\n";

	std::stringstream memInfoStream1(memString1);
	getMemoryInfo(request, memInfoStream1);
	ASSERT(request[LiteralStringRef("MemTotal:")] == 31856496);
	ASSERT(request[LiteralStringRef("MemFree:")] == 25492716);
	ASSERT(request[LiteralStringRef("MemAvailable:")] == 28470756);
	ASSERT(request[LiteralStringRef("Buffers:")] == 313644);
	ASSERT(request[LiteralStringRef("Cached:")] == 2956444);
	ASSERT(request[LiteralStringRef("SwapTotal:")] == 0);
	ASSERT(request[LiteralStringRef("SwapFree:")] == 0);
	for (auto & item : request) {
		printf("%s:%ld\n", item.first.toString().c_str(), item.second);
	}

	return Void();
}
#endif
