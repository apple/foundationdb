/*
 * Platform.actor.cpp
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

#include "flow/Platform.h"

#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include <boost/format.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

#include "fmt/format.h"

#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/FaultInjection.h"
#include "flow/Knobs.h"
#include "flow/Platform.actor.h"
#include "flow/ScopeExit.h"
#include "flow/StreamCipher.h"
#include "flow/Trace.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/Util.h"

// boost uses either std::array or boost::asio::detail::array to store the IPv6 Addresses.
// Enforce the format of IPAddressStore, which is declared in IPAddress.h, is using the same type
// to boost.
static_assert(std::is_same<boost::asio::ip::address_v6::bytes_type, std::array<uint8_t, 16>>::value,
              "IPAddressStore must be std::array<uint8_t, 16>");

#define CANONICAL_PATH_SEPARATOR '/'

#include <dirent.h>
#include <ftw.h>
#include <pwd.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/statvfs.h> /* Needed for disk capacity */

#if !defined(__aarch64__)
#include <cpuid.h>
#endif

/* getifaddrs */
#include <sys/socket.h>
#include <ifaddrs.h>
#include <arpa/inet.h>

#include "stacktrace/stacktrace.h"

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
#endif // __linux__

#ifdef __APPLE__
/* Needed for cross-platform 'environ' */
#include <crt_externs.h>
#include <mach-o/dyld.h>
#include <mach/mach.h>
#include <net/if_dl.h>
#include <net/if.h>
#include <net/route.h>
#include <netinet/in.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <sys/sysctl.h>
#include <sys/syslimits.h>
#include <sys/uio.h>

#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/storage/IOBlockStorageDriver.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/IOBSD.h>
#endif // __APPLE_

#include "flow/actorcompiler.h" // This must be the last #include.

std::string removeWhitespace(const std::string& t) {
	static const std::string ws(" \t\r");
	std::string str = t;
	size_t found = str.find_last_not_of(ws);
	if (found != std::string::npos)
		str.erase(found + 1);
	else
		str.clear(); // str is all whitespace
	found = str.find_first_not_of(ws);
	if (found != std::string::npos)
		str.erase(0, found);
	else
		str.clear(); // str is all whitespace

	return str;
}

#define ALLOC_FAIL MAP_FAILED

static double getProcessorTimeGeneric(int who) {
	struct rusage r_usage;

	if (getrusage(who, &r_usage)) {
		TraceEvent(SevError, "GetCPUTime").detail("Who", who).GetLastError();
		throw platform_error();
	}

	return (r_usage.ru_utime.tv_sec + (r_usage.ru_utime.tv_usec / double(1e6)) + r_usage.ru_stime.tv_sec +
	        (r_usage.ru_stime.tv_usec / double(1e6)));
}

double getProcessorTimeThread() {
	INJECT_FAULT(platform_error, "getProcessorTimeThread"); // Get Thread CPU Time failed
#if defined(__linux__)
	return getProcessorTimeGeneric(RUSAGE_THREAD);
#elif defined(__APPLE__)
	/* No RUSAGE_THREAD so we use the lower level interface */
	struct thread_basic_info info;
	mach_msg_type_number_t info_count = THREAD_BASIC_INFO_COUNT;
	if (KERN_SUCCESS != thread_info(mach_thread_self(), THREAD_BASIC_INFO, (thread_info_t)&info, &info_count)) {
		TraceEvent(SevError, "GetThreadCPUTime").GetLastError();
		throw platform_error();
	}
	return (info.user_time.seconds + (info.user_time.microseconds / double(1e6)) + info.system_time.seconds +
	        (info.system_time.microseconds / double(1e6)));
#else
#warning getProcessorTimeThread unimplemented on this platform
	return 0.0;
#endif
}

double getProcessorTimeProcess() {
	INJECT_FAULT(platform_error, "getProcessorTimeProcess"); // Get CPU Process Time failed
	return getProcessorTimeGeneric(RUSAGE_SELF);
}

uint64_t getResidentMemoryUsage() {
#if defined(__linux__)
	uint64_t rssize = 0;

	std::ifstream stat_stream("/proc/self/statm", std::ifstream::in);
	std::string ignore;

	if (!stat_stream.good()) {
		TraceEvent(SevError, "GetResidentMemoryUsage").GetLastError();
		throw platform_error();
	}

	stat_stream >> ignore;
	stat_stream >> rssize;

	rssize *= sysconf(_SC_PAGESIZE);

	return rssize;
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

	if (!stat_stream.good()) {
		TraceEvent(SevError, "GetMemoryUsage").GetLastError();
		throw platform_error();
	}

	stat_stream >> vmsize;

	vmsize *= sysconf(_SC_PAGESIZE);

	return vmsize;
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

#ifdef __linux__
namespace linux_os {

namespace {

void getMemoryInfo(std::map<StringRef, int64_t>& request, std::stringstream& memInfoStream) {
	size_t count = request.size();
	if (count == 0)
		return;

	keyValueReader<std::string, int64_t>(memInfoStream, [&](const std::string& key, const int64_t& value) -> bool {
		auto item = request.find(StringRef(key));
		if (item != std::end(request)) {
			item->second = value;
			--count;
		}
		return count != 0;
	});
}

int64_t getLowWatermark(std::stringstream& zoneInfoStream) {
	int64_t lowWatermark = 0;
	while (!zoneInfoStream.eof()) {
		std::string key;
		zoneInfoStream >> key;

		if (key == "low") {
			int64_t value;
			zoneInfoStream >> value;
			lowWatermark += value;
		}

		zoneInfoStream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
	}

	return lowWatermark;
}

void getMachineRAMInfoImpl(MachineRAMInfo& memInfo) {
	std::ifstream zoneInfoFileStream("/proc/zoneinfo", std::ifstream::in);
	int64_t lowWatermark = 0;
	if (!zoneInfoFileStream.good()) {
		TraceEvent(SevWarnAlways, "GetMachineZoneInfo").GetLastError();
	} else {
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
		{ "MemTotal:"_sr, 0 },       { "MemFree:"_sr, 0 },   { "MemAvailable:"_sr, -1 }, { "Active(file):"_sr, 0 },
		{ "Inactive(file):"_sr, 0 }, { "SwapTotal:"_sr, 0 }, { "SwapFree:"_sr, 0 },      { "SReclaimable:"_sr, 0 },
	};

	std::stringstream memInfoStream;
	memInfoStream << fileStream.rdbuf();
	getMemoryInfo(request, memInfoStream);

	int64_t memFree = request["MemFree:"_sr];
	int64_t pageCache = request["Active(file):"_sr] + request["Inactive(file):"_sr];
	int64_t slabReclaimable = request["SReclaimable:"_sr];
	int64_t usedSwap = request["SwapTotal:"_sr] - request["SwapFree:"_sr];

	memInfo.total = 1024 * request["MemTotal:"_sr];
	if (request["MemAvailable:"_sr] != -1) {
		memInfo.available = 1024 * (request["MemAvailable:"_sr] - usedSwap);
	} else {
		memInfo.available =
		    1024 * (std::max<int64_t>(0,
		                              (memFree - lowWatermark) + std::max(pageCache - lowWatermark, pageCache / 2) +
		                                  std::max(slabReclaimable - lowWatermark, slabReclaimable / 2)) -
		            usedSwap);
	}

	memInfo.committed = memInfo.total - memInfo.available;
}

} // anonymous namespace

std::map<std::string, int64_t> reportCGroupCpuStat() {
	// Default path to the cpu,cpuacct
	// See manpages for cgroup
	static const std::string PATH_TO_CPU_CPUACCT = "/sys/fs/cgroup/cpu,cpuacct/cpu.stat";

	std::map<std::string, int64_t> result;
	std::ifstream ifs(PATH_TO_CPU_CPUACCT);
	if (!ifs.is_open()) {
		return result;
	}

	keyValueReader<std::string, int64_t>(ifs, [&](const std::string& key, const int64_t& value) -> bool {
		result[key] = value;
		return true;
	});

	return result;
}

} // namespace linux_os
#endif // #ifdef __linux__

void getMachineRAMInfo(MachineRAMInfo& memInfo) {
#if defined(__linux__)
	linux_os::getMachineRAMInfoImpl(memInfo);
#elif defined(__APPLE__)
	vm_statistics_data_t vm_stat;
	vm_size_t pagesize;
	mach_msg_type_number_t host_size = sizeof(vm_statistics_data_t) / sizeof(integer_t);
	if (KERN_SUCCESS != host_statistics(mach_host_self(), HOST_VM_INFO, (host_info_t)&vm_stat, &host_size)) {
		TraceEvent(SevError, "GetMachineMemInfo").GetLastError();
		throw platform_error();
	}
	host_page_size(mach_host_self(), &pagesize);

	memInfo.total =
	    pagesize * (vm_stat.free_count + vm_stat.active_count + vm_stat.inactive_count + vm_stat.wire_count);
	memInfo.available = pagesize * vm_stat.free_count;
	memInfo.committed = memInfo.total - memInfo.available;
#else
#warning getMachineRAMInfo unimplemented on this platform
#endif
}

Error systemErrorCodeToError() {
	if (errno == EIO || errno == EROFS) {
		return io_error();
	}
	return platform_error();
}

void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) {
	INJECT_FAULT(platform_error, "getDiskBytes"); // Get disk bytes failed
#if defined(__linux__)
	struct statvfs buf;
	if (statvfs(directory.c_str(), &buf)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "GetDiskBytesStatvfsError").error(e).detail("Directory", directory).GetLastError();
		throw e;
	}

	uint64_t blockSize = buf.f_frsize;
#elif defined(__APPLE__)
	struct statfs buf;
	if (statfs(directory.c_str(), &buf)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "GetDiskBytesStatfsError").error(e).detail("Directory", directory).GetLastError();
		throw e;
	}

	uint64_t blockSize = buf.f_bsize;
#else
#error Unknown unix
#endif

	free = std::min((uint64_t)std::numeric_limits<int64_t>::max(), buf.f_bavail * blockSize);

	// f_blocks is the total fs space but (f_bfree - f_bavail) is space only available to privileged users
	// so that amount will be subtracted from the reported total since FDB can't use it.
	total = std::min((uint64_t)std::numeric_limits<int64_t>::max(),
	                 (buf.f_blocks - (buf.f_bfree - buf.f_bavail)) * blockSize);
}

const char* getInterfaceName(const IPAddress& _ip) {
	INJECT_FAULT(platform_error, "getInterfaceName"); // Get interface name failed
	static char iname[20];

	struct ifaddrs* interfaces = nullptr;
	const char* ifa_name = nullptr;

	if (getifaddrs(&interfaces)) {
		TraceEvent(SevWarnAlways, "GetInterfaceAddrs").GetLastError();
		throw platform_error();
	}

	for (struct ifaddrs* iter = interfaces; iter; iter = iter->ifa_next) {
		if (!iter->ifa_addr)
			continue;
		if (iter->ifa_addr->sa_family == AF_INET && _ip.isV4()) {
			uint32_t ip = ntohl((reinterpret_cast<struct sockaddr_in*>(iter->ifa_addr))->sin_addr.s_addr);
			if (ip == _ip.toV4()) {
				ifa_name = iter->ifa_name;
				break;
			}
		} else if (iter->ifa_addr->sa_family == AF_INET6 && _ip.isV6()) {
			struct sockaddr_in6* ifa_addr = reinterpret_cast<struct sockaddr_in6*>(iter->ifa_addr);
			if (memcmp(_ip.toV6().data(), &ifa_addr->sin6_addr, 16) == 0) {
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
		return nullptr;
}

#if defined(__linux__)
void getNetworkTraffic(const IPAddress& ip,
                       uint64_t& bytesSent,
                       uint64_t& bytesReceived,
                       uint64_t& outSegs,
                       uint64_t& retransSegs) {
	INJECT_FAULT(
	    platform_error,
	    "getNetworkTraffic"); // getNetworkTraffic: Even though this function doesn't throw errors, the equivalents for
	                          // other platforms do, and since all of our simulation testing is on Linux...
	const char* ifa_name = nullptr;
	try {
		ifa_name = getInterfaceName(ip);
	} catch (Error& e) {
		if (e.code() != error_code_platform_error) {
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
		if (dev_stream.eof())
			break;
		if (!strncmp(iface.c_str(), ifa_name, strlen(ifa_name))) {
			uint64_t sent = 0, received = 0;

			dev_stream >> received;
			for (int i = 0; i < 7; i++)
				dev_stream >> ignore;
			dev_stream >> sent;

			bytesSentSum += sent;
			bytesReceivedSum += received;

			dev_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
		}
	}

	if (bytesSentSum > 0) {
		bytesSent = bytesSentSum;
	}
	if (bytesReceivedSum > 0) {
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

void getMachineLoad(uint64_t& idleTime, uint64_t& totalTime, bool logDetails) {
	INJECT_FAULT(platform_error,
	             "getMachineLoad"); // getMachineLoad: Even though this function doesn't throw errors, the equivalents
	                                // for other platforms do, and since all of our simulation testing is on Linux...
	std::ifstream stat_stream("/proc/stat", std::ifstream::in);

	std::string ignore;
	stat_stream >> ignore;

	uint64_t t_user, t_nice, t_system, t_idle, t_iowait, t_irq, t_softirq, t_steal, t_guest;
	stat_stream >> t_user >> t_nice >> t_system >> t_idle >> t_iowait >> t_irq >> t_softirq >> t_steal >> t_guest;

	totalTime = t_user + t_nice + t_system + t_idle + t_iowait + t_irq + t_softirq + t_steal + t_guest;
	idleTime = t_idle + t_iowait;

	if (!DEBUG_DETERMINISM && logDetails)
		TraceEvent("MachineLoadDetail")
		    .detail("User", t_user)
		    .detail("Nice", t_nice)
		    .detail("System", t_system)
		    .detail("Idle", t_idle)
		    .detail("IOWait", t_iowait)
		    .detail("IRQ", t_irq)
		    .detail("SoftIRQ", t_softirq)
		    .detail("Steal", t_steal)
		    .detail("Guest", t_guest);
}

void getDiskStatistics(std::string const& directory,
                       uint64_t& currentIOs,
                       uint64_t& readMilliSecs,
                       uint64_t& writeMilliSecs,
                       uint64_t& IOMilliSecs,
                       uint64_t& reads,
                       uint64_t& writes,
                       uint64_t& writeSectors,
                       uint64_t& readSectors) {
	INJECT_FAULT(platform_error, "getDiskStatistics"); // Getting disks statistics failed
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
		if (majorId == (unsigned int)gnu_dev_major(buf.st_dev) && minorId == (unsigned int)gnu_dev_minor(buf.st_dev)) {
			std::string ignore;
			uint64_t rd_ios; /* # of reads completed */
			//	    This is the total number of reads completed successfully.

			uint64_t rd_merges; /* # of reads merged */
			//	    Reads and writes which are adjacent to each other may be merged for
			//	    efficiency.  Thus two 4K reads may become one 8K read before it is
			//	    ultimately handed to the disk, and so it will be counted (and queued)
			//	    as only one I/O.  This field lets you know how often this was done.

			uint64_t rd_sectors; /*# of sectors read */
			//	    This is the total number of sectors read successfully.

			uint64_t rd_ticks; /* # of milliseconds spent reading */
			//	    This is the total number of milliseconds spent by all reads (as
			//	    measured from __make_request() to end_that_request_last()).

			uint64_t wr_ios; /* # of writes completed */
			//	    This is the total number of writes completed successfully.

			uint64_t wr_merges; /* # of writes merged */
			//	    Reads and writes which are adjacent to each other may be merged for
			//	    efficiency.  Thus two 4K reads may become one 8K read before it is
			//	    ultimately handed to the disk, and so it will be counted (and queued)
			//	    as only one I/O.  This field lets you know how often this was done.

			uint64_t wr_sectors; /* # of sectors written */
			//	    This is the total number of sectors written successfully.

			uint64_t wr_ticks; /* # of milliseconds spent writing */
			//	    This is the total number of milliseconds spent by all writes (as
			//	    measured from __make_request() to end_that_request_last()).

			uint64_t cur_ios; /* # of I/Os currently in progress */
			//	    The only field that should go to zero. Incremented as requests are
			//	    given to appropriate struct request_queue and decremented as they finish.

			uint64_t ticks; /* # of milliseconds spent doing I/Os */
			//	    This field increases so long as field 9 is nonzero.

			uint64_t aveq; /* weighted # of milliseconds spent doing I/Os */
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
			readMilliSecs = rd_ticks;
			writeMilliSecs = wr_ticks;
			IOMilliSecs = ticks;
			reads = rd_ios;
			writes = wr_ios;
			writeSectors = wr_sectors;
			readSectors = rd_sectors;

			//TraceEvent("DiskMetricsRaw").detail("Input", line).detail("Ignore", ignore).detail("RdIos", rd_ios)
			//	.detail("RdMerges", rd_merges).detail("RdSectors", rd_sectors).detail("RdTicks",
			// rd_ticks).detail("WrIos", wr_ios).detail("WrMerges", wr_merges) 	.detail("WrSectors",
			// wr_sectors).detail("WrTicks", wr_ticks).detail("CurIos", cur_ios).detail("Ticks", ticks).detail("Aveq",
			// aveq) 	.detail("CurrentIOs", currentIOs).detail("BusyTicks", busyTicks).detail("Reads",
			// reads).detail("Writes", writes).detail("WriteSectors", writeSectors)
			//  .detail("ReadSectors", readSectors);
			return;
		} else
			disk_stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
	}

	if (!g_network->isSimulated())
		TraceEvent(SevWarn, "GetDiskStatisticsDeviceNotFound").detail("Directory", directory);
}

dev_t getDeviceId(std::string path) {
	struct stat statInfo;

	while (true) {
		int returnValue = stat(path.c_str(), &statInfo);
		if (!returnValue)
			break;

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
void getNetworkTraffic(const IPAddress& ip,
                       uint64_t& bytesSent,
                       uint64_t& bytesReceived,
                       uint64_t& outSegs,
                       uint64_t& retransSegs) {
	INJECT_FAULT(platform_error, "getNetworkTraffic"); // Get network traffic failed (macOS)

	const char* ifa_name = nullptr;
	try {
		ifa_name = getInterfaceName(ip);
	} catch (Error& e) {
		if (e.code() != error_code_platform_error) {
			throw;
		}
	}

	if (!ifa_name)
		return;

	int mib[] = {
		CTL_NET, PF_ROUTE,       0,
		AF_INET, NET_RT_IFLIST2, 0 /* If we could get an interface index instead of name, we would pass it here */
	};

	size_t len;

	if (sysctl(mib, 6, nullptr, &len, nullptr, 0) < 0) {
		TraceEvent(SevError, "GetNetworkTrafficError").GetLastError();
		throw platform_error();
	}

	char* buf = (char*)malloc(len);

	if (sysctl(mib, 6, buf, &len, nullptr, 0) < 0) {
		free(buf);
		TraceEvent(SevError, "GetNetworkTrafficReadInterfacesError").GetLastError();
		throw platform_error();
	}

	char* lim = buf + len;

	for (char* next = buf; next < lim;) {
		struct if_msghdr* ifm = (struct if_msghdr*)next;
		next += ifm->ifm_msglen;

		if ((ifm->ifm_type = RTM_IFINFO2)) {
			struct if_msghdr2* if2m = (struct if_msghdr2*)ifm;
			struct sockaddr_dl* sdl = (struct sockaddr_dl*)(if2m + 1);

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

void getMachineLoad(uint64_t& idleTime, uint64_t& totalTime, bool logDetails) {
	INJECT_FAULT(platform_error, "getMachineLoad"); // Getting machine load filed (macOS)
	mach_msg_type_number_t count = HOST_CPU_LOAD_INFO_COUNT;
	host_cpu_load_info_data_t r_load;

	if (host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info_t)&r_load, &count) != KERN_SUCCESS) {
		TraceEvent(SevError, "GetMachineLoad").GetLastError();
		throw platform_error();
	}

	idleTime = r_load.cpu_ticks[CPU_STATE_IDLE];
	totalTime = r_load.cpu_ticks[CPU_STATE_IDLE] + r_load.cpu_ticks[CPU_STATE_USER] + r_load.cpu_ticks[CPU_STATE_NICE] +
	            r_load.cpu_ticks[CPU_STATE_SYSTEM];
}

void getDiskStatistics(std::string const& directory,
                       uint64_t& currentIOs,
                       uint64_t& readMilliSecs,
                       uint64_t& writeMilliSecs,
                       uint64_t& IOMilliSecs,
                       uint64_t& reads,
                       uint64_t& writes,
                       uint64_t& writeSectors,
                       uint64_t& readSectors) {
	INJECT_FAULT(platform_error, "getDiskStatistics"); // Getting disk stats failed (macOS)
	currentIOs = 0; // This will not be used because we cannot get its value.
	readMilliSecs = 0;
	writeMilliSecs = 0;
	IOMilliSecs = 0;
	writeSectors = 0;
	readSectors = 0;

	struct statfs buf;
	if (statfs(directory.c_str(), &buf)) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "GetDiskStatisticsStatfsError").error(e).detail("Directory", directory).GetLastError();
		throw e;
	}

	const char* dev = strrchr(buf.f_mntfromname, '/');
	if (!dev) {
		TraceEvent(SevError, "GetDiskStatisticsStrrchrError").detail("Directory", directory).GetLastError();
		throw platform_error();
	}
	dev++;

	io_iterator_t disk_list;

	// According to Apple docs, if this gets passed to IOServiceGetMatchingServices, we aren't responsible for the
	// memory anymore, the only case where it isn't passed is if it's null, in which case we also aren't responsible. So
	// no need to call CFRelease on this variable.
	CFMutableDictionaryRef match = IOBSDNameMatching(kIOMasterPortDefault, kNilOptions, dev);

	if (!match) {
		TraceEvent(SevError, "IOBSDNameMatching").log();
		throw platform_error();
	}

	if (IOServiceGetMatchingServices(kIOMasterPortDefault, match, &disk_list) != kIOReturnSuccess) {
		TraceEvent(SevError, "IOServiceGetMatchingServices").log();
		throw platform_error();
	}

	io_registry_entry_t disk = IOIteratorNext(disk_list);
	if (!disk) {
		IOObjectRelease(disk_list);
		TraceEvent(SevError, "IOIteratorNext").log();
		throw platform_error();
	}

	io_registry_entry_t tdisk = disk;
	while (!IOObjectConformsTo(disk, "IOBlockStorageDriver")) {
		IORegistryEntryGetParentEntry(disk, kIOServicePlane, &tdisk);
		IOObjectRelease(disk);
		disk = tdisk;
	}

	CFDictionaryRef disk_dict = nullptr;
	if (IORegistryEntryCreateCFProperties(
	        disk, (CFMutableDictionaryRef*)&disk_dict, kCFAllocatorDefault, kNilOptions) != kIOReturnSuccess) {
		IOObjectRelease(disk);
		IOObjectRelease(disk_list);
		TraceEvent(SevError, "IORegistryEntryCreateCFProperties").log();
		throw platform_error();
	}

	// Here and below, note that memory returned by CFDictionaryGetValue() is not owned by us, and should not be
	// CFRelease()'d by us.
	CFDictionaryRef stats_dict =
	    (CFDictionaryRef)CFDictionaryGetValue(disk_dict, CFSTR(kIOBlockStorageDriverStatisticsKey));

	if (stats_dict == nullptr) {
		CFRelease(disk_dict);
		IOObjectRelease(disk);
		IOObjectRelease(disk_list);
		TraceEvent(SevError, "CFDictionaryGetValue").log();
		throw platform_error();
	}

	CFNumberRef number;

	if ((number = (CFNumberRef)CFDictionaryGetValue(stats_dict, CFSTR(kIOBlockStorageDriverStatisticsReadsKey)))) {
		CFNumberGetValue(number, kCFNumberSInt64Type, &reads);
	}

	if ((number = (CFNumberRef)CFDictionaryGetValue(stats_dict, CFSTR(kIOBlockStorageDriverStatisticsWritesKey)))) {
		CFNumberGetValue(number, kCFNumberSInt64Type, &writes);
	}

	uint64_t nanoSecs;
	if ((number =
	         (CFNumberRef)CFDictionaryGetValue(stats_dict, CFSTR(kIOBlockStorageDriverStatisticsTotalReadTimeKey)))) {
		CFNumberGetValue(number, kCFNumberSInt64Type, &nanoSecs);
		readMilliSecs += nanoSecs;
		IOMilliSecs += nanoSecs;
	}
	if ((number =
	         (CFNumberRef)CFDictionaryGetValue(stats_dict, CFSTR(kIOBlockStorageDriverStatisticsTotalWriteTimeKey)))) {
		CFNumberGetValue(number, kCFNumberSInt64Type, &nanoSecs);
		writeMilliSecs += nanoSecs;
		IOMilliSecs += nanoSecs;
	}
	// nanoseconds to milliseconds
	readMilliSecs /= 1000000;
	writeMilliSecs /= 1000000;
	IOMilliSecs /= 1000000;

	CFRelease(disk_dict);
	IOObjectRelease(disk);
	IOObjectRelease(disk_list);
}
#endif

struct SystemStatisticsState {
	double lastTime;
	double lastClockThread;
	double lastClockProcess;
	uint64_t processLastSent;
	uint64_t processLastReceived;
	uint64_t machineLastSent, machineLastReceived;
	uint64_t machineLastOutSegs, machineLastRetransSegs;
	uint64_t lastReadMilliSecs, lastWriteMilliSecs, lastIOMilliSecs, lastReads, lastWrites, lastWriteSectors,
	    lastReadSectors;
	uint64_t lastClockIdleTime, lastClockTotalTime;
	SystemStatisticsState()
	  : lastTime(0), lastClockThread(0), lastClockProcess(0), processLastSent(0), processLastReceived(0),
	    machineLastSent(0), machineLastReceived(0), machineLastOutSegs(0), machineLastRetransSegs(0),
	    lastReadMilliSecs(0), lastWriteMilliSecs(0), lastIOMilliSecs(0), lastReads(0), lastWrites(0),
	    lastWriteSectors(0), lastReadSectors(0), lastClockIdleTime(0), lastClockTotalTime(0) {}
};

SystemStatistics getSystemStatistics(std::string const& dataFolder,
                                     const IPAddress* ip,
                                     SystemStatisticsState** statState,
                                     bool logDetails) {
	if ((*statState) == nullptr)
		(*statState) = new SystemStatisticsState();
	SystemStatistics returnStats;

	double nowTime = timer();
	double nowClockProcess = getProcessorTimeProcess();
	double nowClockThread = getProcessorTimeThread();
	returnStats.elapsed = nowTime - (*statState)->lastTime;

	returnStats.initialized = (*statState)->lastTime != 0;
	if (returnStats.initialized) {
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

	if (dataFolder != "") {
		int64_t diskTotal, diskFree;
		getDiskBytes(dataFolder, diskFree, diskTotal);
		returnStats.processDiskTotalBytes = diskTotal;
		returnStats.processDiskFreeBytes = diskFree;
	}

	uint64_t machineNowSent = (*statState)->machineLastSent;
	uint64_t machineNowReceived = (*statState)->machineLastReceived;
	uint64_t machineOutSegs = (*statState)->machineLastOutSegs;
	uint64_t machineRetransSegs = (*statState)->machineLastRetransSegs;

	getNetworkTraffic(*ip, machineNowSent, machineNowReceived, machineOutSegs, machineRetransSegs);
	if (returnStats.initialized) {
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
	uint64_t nowReadMilliSecs = (*statState)->lastReadMilliSecs;
	uint64_t nowWriteMilliSecs = (*statState)->lastWriteMilliSecs;
	uint64_t nowIOMilliSecs = (*statState)->lastIOMilliSecs;
	uint64_t nowReads = (*statState)->lastReads;
	uint64_t nowWrites = (*statState)->lastWrites;
	uint64_t nowWriteSectors = (*statState)->lastWriteSectors;
	uint64_t nowReadSectors = (*statState)->lastReadSectors;

	if (dataFolder != "") {
		getDiskStatistics(dataFolder,
		                  currentIOs,
		                  nowReadMilliSecs,
		                  nowWriteMilliSecs,
		                  nowIOMilliSecs,
		                  nowReads,
		                  nowWrites,
		                  nowWriteSectors,
		                  nowReadSectors);
		returnStats.processDiskQueueDepth = currentIOs;
		returnStats.processDiskReadCount = nowReads;
		returnStats.processDiskWriteCount = nowWrites;
		if (returnStats.initialized) {
			returnStats.processDiskIdleSeconds = std::max<double>(
			    0,
			    returnStats.elapsed -
			        std::min<double>(returnStats.elapsed, (nowIOMilliSecs - (*statState)->lastIOMilliSecs) / 1000.0));
			returnStats.processDiskReadSeconds =
			    std::min<double>(returnStats.elapsed, (nowReadMilliSecs - (*statState)->lastReadMilliSecs) / 1000.0);
			returnStats.processDiskWriteSeconds =
			    std::min<double>(returnStats.elapsed, (nowWriteMilliSecs - (*statState)->lastWriteMilliSecs) / 1000.0);
			returnStats.processDiskRead = (nowReads - (*statState)->lastReads);
			returnStats.processDiskWrite = (nowWrites - (*statState)->lastWrites);
			returnStats.processDiskWriteSectors = (nowWriteSectors - (*statState)->lastWriteSectors);
			returnStats.processDiskReadSectors = (nowReadSectors - (*statState)->lastReadSectors);
		}
		(*statState)->lastIOMilliSecs = nowIOMilliSecs;
		(*statState)->lastReadMilliSecs = nowReadMilliSecs;
		(*statState)->lastWriteMilliSecs = nowWriteMilliSecs;
		(*statState)->lastReads = nowReads;
		(*statState)->lastWrites = nowWrites;
		(*statState)->lastWriteSectors = nowWriteSectors;
		(*statState)->lastReadSectors = nowReadSectors;
	}

	uint64_t clockIdleTime = (*statState)->lastClockIdleTime;
	uint64_t clockTotalTime = (*statState)->lastClockTotalTime;

	getMachineLoad(clockIdleTime, clockTotalTime, logDetails);
	returnStats.machineCPUSeconds = clockTotalTime - (*statState)->lastClockTotalTime != 0
	                                    ? (1 - ((clockIdleTime - (*statState)->lastClockIdleTime) /
	                                            ((double)(clockTotalTime - (*statState)->lastClockTotalTime)))) *
	                                          returnStats.elapsed
	                                    : 0;
	(*statState)->lastClockIdleTime = clockIdleTime;
	(*statState)->lastClockTotalTime = clockTotalTime;
	(*statState)->lastTime = nowTime;
	(*statState)->lastClockProcess = nowClockProcess;
	(*statState)->lastClockThread = nowClockThread;
	return returnStats;
}

#if defined(__linux__)
#define DOUBLETIME(ts) (double(ts.tv_sec) + (ts.tv_nsec * 1e-9))
#ifndef CLOCK_MONOTONIC_RAW
#define CLOCK_MONOTONIC_RAW                                                                                            \
	4 // Confirmed safe to do with glibc >= 2.11 and kernel >= 2.6.28. No promises with older glibc. Older kernel
	  // definitely breaks it.
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
		gettimeofday(&tv, nullptr);

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
#if defined(__linux__)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return double(ts.tv_sec) + (ts.tv_nsec * 1e-9);
#elif defined(__APPLE__)
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return double(tv.tv_sec) + (tv.tv_usec * 1e-6);
#else
#error Port me!
#endif
};

uint64_t timer_int() {
#if defined(__linux__)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return uint64_t(ts.tv_sec) * 1e9 + ts.tv_nsec;
#elif defined(__APPLE__)
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return uint64_t(tv.tv_sec) * 1e9 + (tv.tv_usec * 1e3);
#else
#error Port me!
#endif
};

void getLocalTime(const time_t* timep, struct tm* result) {
	if (localtime_r(timep, result) == nullptr) {
		TraceEvent(SevError, "GetLocalTimeError").GetLastError();
		throw platform_error();
	}
}

// Outputs a GMT time string for the given epoch seconds, which looks like
// 2013-04-28 20:57:01.000 +0000
std::string epochsToGMTString(double epochs) {
	auto time = (time_t)epochs;

	char buff[50];
	auto size = strftime(buff, 50, "%Y-%m-%d %H:%M:%S", gmtime(&time));
	std::string timeString = std::string(std::begin(buff), std::begin(buff) + size);

	// Add fractional seconds and GMT timezone.
	double integerPart;
	timeString += format(".%03.3d +0000", (int)(1000 * modf(epochs, &integerPart)));

	return timeString;
}

std::vector<std::string> getEnvironmentKnobOptions() {
	constexpr const size_t ENVKNOB_PREFIX_LEN = sizeof(ENVIRONMENT_KNOB_OPTION_PREFIX) - 1;
	std::vector<std::string> knobOptions;
	char** e = nullptr;
#ifdef __linux__
	e = environ;
#elif defined(__APPLE__)
	e = *_NSGetEnviron();
#else
#error Port me!
#endif
	for (; e && *e; e++) {
		std::string_view envOption(*e);
		if (boost::starts_with(envOption, ENVIRONMENT_KNOB_OPTION_PREFIX)) {
			knobOptions.emplace_back(envOption.substr(ENVKNOB_PREFIX_LEN));
		}
	}
	return knobOptions;
}

void setMemoryQuota(size_t limit) {
	if (limit == 0) {
		return;
	}
#if defined(USE_SANITIZER)
	// ASAN doesn't work with memory quotas: https://github.com/google/sanitizers/wiki/AddressSanitizer#ulimit--v
	return;
#endif
	INJECT_FAULT(platform_error, "setMemoryQuota"); // setting memory quota failed
#if defined(__linux__)
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

static bool largePagesPrivilegeEnabled = false;

static void enableLargePages() {
	if (largePagesPrivilegeEnabled)
		return;
	// SOMEDAY: can/should we teach the client how to enable large pages
	// on Linux? Or just rely on the system to have been configured as
	// desired?
}

static void* mmapSafe(void* addr, size_t len, int prot, int flags, int fd, off_t offset) {
	void* result = mmap(addr, len, prot, flags, fd, offset);
	if (result == MAP_FAILED) {
		int err = errno;
		fprintf(stderr,
		        "Error calling mmap(%p, %zu, %d, %d, %d, %jd): %s\n",
		        addr,
		        len,
		        prot,
		        flags,
		        fd,
		        (intmax_t)offset,
		        strerror(err));
		fflush(stderr);
		std::abort();
	}
	return result;
}

static void mprotectSafe(void* p, size_t s, int prot) {
	if (mprotect(p, s, prot) != 0) {
		int err = errno;
		fprintf(stderr, "Error calling mprotect(%p, %zu, %d): %s\n", p, s, prot, strerror(err));
		fflush(stderr);
		std::abort();
	}
}

static void* mmapInternal(size_t length, int flags, bool guardPages) {
	if (guardPages && FLOW_KNOBS->FAST_ALLOC_ALLOW_GUARD_PAGES) {
		static size_t pageSize = sysconf(_SC_PAGESIZE);
		length = RightAlign(length, pageSize);
		length += 2 * pageSize; // Map enough for the guard pages
		void* resultWithGuardPages = mmapSafe(nullptr, length, PROT_READ | PROT_WRITE, flags, -1, 0);
		// left guard page
		mprotectSafe(resultWithGuardPages, pageSize, PROT_NONE);
		// right guard page
		mprotectSafe((void*)(uintptr_t(resultWithGuardPages) + length - pageSize), pageSize, PROT_NONE);
		return (void*)(uintptr_t(resultWithGuardPages) + pageSize);
	} else {
		return mmapSafe(nullptr, length, PROT_READ | PROT_WRITE, flags, -1, 0);
	}
}

static void* allocateInternal(size_t length, bool largePages, bool guardPages) {

#if defined(__linux__)
	int flags = MAP_PRIVATE | MAP_ANONYMOUS;

	if (largePages)
		flags |= MAP_HUGETLB;

	return mmapInternal(length, flags, guardPages);
#elif defined(__APPLE__)
	int flags = MAP_PRIVATE | MAP_ANON;

	return mmapInternal(length, flags, guardPages);
#else
#error Port me!
#endif
}

static bool largeBlockFail = false;
void* allocate(size_t length, bool allowLargePages, bool includeGuardPages) {
	if (allowLargePages)
		enableLargePages();

	void* block = ALLOC_FAIL;

	if (allowLargePages && !largeBlockFail) {
		block = allocateInternal(length, true, includeGuardPages);
		if (block == ALLOC_FAIL)
			largeBlockFail = true;
	}

	if (block == ALLOC_FAIL)
		block = allocateInternal(length, false, includeGuardPages);

	// FIXME: SevWarnAlways trace if "close" to out of memory

	if (block == ALLOC_FAIL)
		platform::outOfMemory();

	return block;
}

void setAffinity(int proc) {
#if defined(__linux__)
	cpu_set_t set;
	CPU_ZERO(&set);
	CPU_SET(proc, &set);
	sched_setaffinity(0, sizeof(cpu_set_t), &set);
#endif
}

namespace platform {

int getRandomSeed() {
	INJECT_FAULT(platform_error, "getRandomSeed"); // getting a random seed failed
	int randomSeed;

	int devRandom = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
	if (read(devRandom, &randomSeed, sizeof(randomSeed)) != sizeof(randomSeed)) {
		TraceEvent(SevError, "OpenURandom").GetLastError();
		throw platform_error();
	}
	close(devRandom);

	return randomSeed;
}
} // namespace platform

std::string joinPath(std::string const& directory, std::string const& filename) {
	auto d = directory;
	auto f = filename;
	while (f.size() && (f[0] == '/' || f[0] == CANONICAL_PATH_SEPARATOR))
		f = f.substr(1);
	while (d.size() && (d.back() == '/' || d.back() == CANONICAL_PATH_SEPARATOR))
		d.resize(d.size() - 1);
	return d + CANONICAL_PATH_SEPARATOR + f;
}

void renamedFile() {
	INJECT_FAULT(io_error, "renameFile"); // renaming file failed
}

void renameFile(std::string const& fromPath, std::string const& toPath) {
	INJECT_FAULT(io_error, "renameFile"); // rename file failed
#if (defined(__linux__) || defined(__APPLE__))
	if (!rename(fromPath.c_str(), toPath.c_str())) {
		// FIXME: We cannot inject faults after renaming the file, because we could end up with two asyncFileNonDurable
		// open for the same file renamedFile();
		return;
	}
#else
#error Port me!
#endif
	TraceEvent(SevError, "RenameFile").detail("FromPath", fromPath).detail("ToPath", toPath).GetLastError();
	throw io_error();
}

#if defined(__linux__)
#define FOPEN_CLOEXEC_MODE "e"
#else
#define FOPEN_CLOEXEC_MODE ""
#endif

void atomicReplace(std::string const& path, std::string const& content, bool textmode) {
	FILE* f = 0;
	try {
		INJECT_FAULT(io_error, "atomicReplace"); // atomic rename failed

		std::string tempfilename =
		    joinPath(parentDirectory(path), deterministicRandom()->randomUniqueID().toString() + ".tmp");
		f = textmode ? fopen(tempfilename.c_str(), "wt" FOPEN_CLOEXEC_MODE) : fopen(tempfilename.c_str(), "wb");
		if (!f)
			throw io_error();
		// get the uid/gid/mode bits of old file and set it on new file, else fail
		struct stat info;
		bool exists = true;
		if (stat(path.c_str(), &info) < 0) {
			if (errno == ENOENT) {
				exists = false;
			} else {
				TraceEvent("StatFailed").detail("Path", path);
				throw io_error();
			}
		}
		if (exists && chown(tempfilename.c_str(), info.st_uid, info.st_gid) < 0) {
			TraceEvent("ChownFailed")
			    .detail("TempFilename", tempfilename)
			    .detail("OriginalFile", path)
			    .detail("Uid", info.st_uid)
			    .detail("Gid", info.st_gid);
			deleteFile(tempfilename);
			throw io_error();
		}
		if (exists && chmod(tempfilename.c_str(), info.st_mode) < 0) {
			TraceEvent("ChmodFailed")
			    .detail("TempFilename", tempfilename)
			    .detail("OriginalFile", path)
			    .detail("Mode", info.st_mode);
			deleteFile(tempfilename);
			throw io_error();
		}

		if (textmode && fprintf(f, "%s", content.c_str()) < 0)
			throw io_error();

		if (!textmode && fwrite(content.c_str(), sizeof(uint8_t), content.size(), f) != content.size())
			throw io_error();

		if (fflush(f) != 0)
			throw io_error();

		if (!g_network->isSimulated()) {
			if (fsync(fileno(f)) != 0)
				throw io_error();
		}

		if (fclose(f) != 0) {
			f = 0;
			throw io_error();
		}
		f = 0;

		if (rename(tempfilename.c_str(), path.c_str()) != 0)
			throw io_error();

		INJECT_FAULT(io_error, "atomicReplace"); // io_error after atomic rename
	} catch (Error& e) {
		TraceEvent(SevWarn, "AtomicReplace").error(e).detail("Path", path).GetLastError();
		if (f)
			fclose(f);
		throw;
	}
}

static bool deletedFile() {
	INJECT_FAULT(platform_error, "deleteFile"); // delete file failed
	return true;
}

bool deleteFile(std::string const& filename) {
	INJECT_FAULT(platform_error, "deleteFile"); // file deletion failed
	if (!unlink(filename.c_str()))
		return deletedFile();
	if (errno == ENOENT)
		return false;
	Error e = systemErrorCodeToError();
	TraceEvent(SevError, "DeleteFile").error(e).detail("Filename", filename).GetLastError();
	throw e;
}

static void createdDirectory() {
	INJECT_FAULT(platform_error, "createDirectory"); // create dir (noargs) failed
}

namespace platform {

bool createDirectory(std::string const& directory) {
	INJECT_FAULT(platform_error, "createDirectory"); // create dir failed

#if (defined(__linux__) || defined(__APPLE__))
	size_t sep = 0;
	do {
		sep = directory.find_first_of('/', sep + 1);
		if (mkdir(directory.substr(0, sep).c_str(), 0755) != 0) {
			if (errno == EEXIST)
				continue;
			auto mkdirErrno = errno;

			// check if directory already exists
			// necessary due to old kernel bugs
			struct stat s;
			const char* dirname = directory.c_str();
			if (stat(dirname, &s) != -1 && S_ISDIR(s.st_mode)) {
				TraceEvent("DirectoryAlreadyExists").detail("Directory", dirname).detail("IgnoredError", mkdirErrno);
				continue;
			}

			Error e;
			if (mkdirErrno == EACCES) {
				e = file_not_writable();
			} else {
				e = systemErrorCodeToError();
			}

			TraceEvent(SevError, "CreateDirectory")
			    .error(e)
			    .detail("Directory", directory)
			    .detailf("UnixErrorCode", "%x", errno)
			    .detail("UnixError", strerror(mkdirErrno));
			throw e;
		}
		createdDirectory();
	} while (sep != std::string::npos && sep != directory.length() - 1);
	return true;
#else
#error Port me!
#endif
}

} // namespace platform

const uint8_t separatorChar = CANONICAL_PATH_SEPARATOR;
StringRef separator(&separatorChar, 1);
StringRef dotdot = ".."_sr;

std::string cleanPath(std::string const& path) {
	std::vector<StringRef> finalParts;
	bool absolute = !path.empty() && path[0] == CANONICAL_PATH_SEPARATOR;

	StringRef p(path);

	while (p.size() != 0) {
		StringRef part = p.eat(separator);
		if (part.size() == 0 || (part.size() == 1 && part[0] == '.'))
			continue;
		if (part == dotdot) {
			if (!finalParts.empty() && finalParts.back() != dotdot) {
				finalParts.pop_back();
				continue;
			}
			if (absolute) {
				continue;
			}
		}
		finalParts.push_back(part);
	}

	std::string result;
	result.reserve(PATH_MAX);
	if (absolute) {
		result.append(1, CANONICAL_PATH_SEPARATOR);
	}

	for (int i = 0; i < finalParts.size(); ++i) {
		if (i != 0) {
			result.append(1, CANONICAL_PATH_SEPARATOR);
		}
		result.append((const char*)finalParts[i].begin(), finalParts[i].size());
	}

	return result.empty() ? "." : result;
}

std::string popPath(const std::string& path) {
	int i = path.size() - 1;
	// Skip over any trailing separators
	while (i >= 0 && path[i] == CANONICAL_PATH_SEPARATOR) {
		--i;
	}
	// Skip over non separators
	while (i >= 0 && path[i] != CANONICAL_PATH_SEPARATOR) {
		--i;
	}
	// Skip over trailing separators again
	bool foundSeparator = false;
	while (i >= 0 && path[i] == CANONICAL_PATH_SEPARATOR) {
		--i;
		foundSeparator = true;
	}

	if (foundSeparator) {
		++i;
	} else {
		// If absolute then we popped off the only path component so return "/"
		if (!path.empty() && path.front() == CANONICAL_PATH_SEPARATOR) {
			return "/";
		}
	}
	return path.substr(0, i + 1);
}

std::string abspath(std::string const& path_, bool resolveLinks, bool mustExist) {
	if (path_.empty()) {
		Error e = platform_error();
		Severity sev = e.code() == error_code_io_error ? SevError : SevWarnAlways;
		TraceEvent(sev, "AbsolutePathError").error(e).detail("Path", path_);
		throw e;
	}
	std::string path = path_.back() == '\\' ? path_.substr(0, path_.size() - 1) : path_;
	// Returns an absolute path canonicalized to use only CANONICAL_PATH_SEPARATOR
	INJECT_FAULT(platform_error, "abspath"); // abspath failed

	if (!resolveLinks) {
		// TODO:  Not resolving symbolic links does not yet behave well on Windows because of drive letters
		// and network names, so it's not currently allowed here (but it is allowed in fdbmonitor which is unix-only)
		ASSERT(false);
		// Treat paths starting with ~ or separator as absolute, meaning they shouldn't be appended to the current
		// working dir
		bool absolute = !path.empty() && (path[0] == CANONICAL_PATH_SEPARATOR || path[0] == '~');
		std::string clean = cleanPath(absolute ? path : joinPath(platform::getWorkingDirectory(), path));
		if (mustExist && !fileExists(clean)) {
			Error e = systemErrorCodeToError();
			Severity sev = e.code() == error_code_io_error ? SevError : SevWarnAlways;
			TraceEvent(sev, "AbsolutePathError").error(e).detail("Path", path).GetLastError();
			throw e;
		}
		return clean;
	}

#if (defined(__linux__) || defined(__APPLE__))
	char result[PATH_MAX];
	// Must resolve links, so first try realpath on the whole thing
	const char* r = realpath(path.c_str(), result);
	if (r == nullptr) {
		// If the error was ENOENT and the path doesn't have to exist,
		// try to resolve symlinks in progressively shorter prefixes of the path
		if (errno == ENOENT && !mustExist) {
			std::string prefix = popPath(path);
			std::string suffix = path.substr(prefix.size());
			if (prefix.empty() && (suffix.empty() || suffix[0] != '~')) {
				prefix = ".";
			}
			if (!prefix.empty()) {
				return cleanPath(joinPath(abspath(prefix, true, false), suffix));
			}
		}
		Error e = systemErrorCodeToError();
		Severity sev = e.code() == error_code_io_error ? SevError : SevWarnAlways;
		TraceEvent(sev, "AbsolutePathError").error(e).detail("Path", path).GetLastError();
		throw e;
	}
	return std::string(r);
#else
#error Port me!
#endif
}

std::string parentDirectory(std::string const& path, bool resolveLinks, bool mustExist) {
	return popPath(abspath(path, resolveLinks, mustExist));
}

std::string basename(std::string const& filename) {
	auto abs = abspath(filename);
	size_t sep = abs.find_last_of(CANONICAL_PATH_SEPARATOR);
	if (sep == std::string::npos)
		return filename;
	return abs.substr(sep + 1);
}

std::string getUserHomeDirectory() {
	const char* ret = getenv("HOME");
	if (!ret) {
		if (struct passwd* pw = getpwuid(getuid())) {
			ret = pw->pw_dir;
		}
	}
	return ret;
}

#if (defined(__linux__) || defined(__APPLE__))
#define FILE_ATTRIBUTE_DATA mode_t

bool acceptFile(FILE_ATTRIBUTE_DATA fileAttributes, std::string const& name, std::string const& extension) {
	return S_ISREG(fileAttributes) && StringRef(name).endsWith(extension);
}

bool acceptDirectory(FILE_ATTRIBUTE_DATA fileAttributes, std::string const& name, std::string const& extension) {
	return S_ISDIR(fileAttributes);
}

ACTOR Future<std::vector<std::string>> findFiles(std::string directory,
                                                 std::string extension,
                                                 bool directoryOnly,
                                                 bool async) {
	INJECT_FAULT(platform_error, "findFiles"); // findFiles failed
	state std::vector<std::string> result;
	state int64_t tsc_begin = timestampCounter();

	state DIR* dip = nullptr;

	if ((dip = opendir(directory.c_str())) != nullptr) {
		loop {
			struct dirent* dit;
			dit = readdir(dip);
			if (dit == nullptr) {
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
				if (isError)
					throw platform_error();
				else
					continue;
			}

			if ((directoryOnly && acceptDirectory(buf.st_mode, name, extension)) ||
			    (!directoryOnly && acceptFile(buf.st_mode, name, extension))) {
				result.push_back(name);
			}
			if (async && timestampCounter() - tsc_begin > FLOW_KNOBS->TSC_YIELD_TIME && !g_network->isSimulated()) {
				wait(yield());
				tsc_begin = timestampCounter();
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

namespace platform {

std::vector<std::string> listFiles(std::string const& directory, std::string const& extension) {
	return findFiles(directory, extension, false /* directoryOnly */, false).get();
}

Future<std::vector<std::string>> listFilesAsync(std::string const& directory, std::string const& extension) {
	return findFiles(directory, extension, false /* directoryOnly */, true);
}

std::vector<std::string> listDirectories(std::string const& directory) {
	return findFiles(directory, "", true /* directoryOnly */, false).get();
}

Future<std::vector<std::string>> listDirectoriesAsync(std::string const& directory) {
	return findFiles(directory, "", true /* directoryOnly */, true);
}

void findFilesRecursively(std::string const& path, std::vector<std::string>& out) {
	// Add files to output, prefixing path
	std::vector<std::string> files = platform::listFiles(path);
	for (auto const& f : files)
		out.push_back(joinPath(path, f));

	// Recurse for directories
	std::vector<std::string> directories = platform::listDirectories(path);
	for (auto const& dir : directories) {
		if (dir != "." && dir != "..")
			findFilesRecursively(joinPath(path, dir), out);
	}
}

ACTOR Future<Void> findFilesRecursivelyAsync(std::string path, std::vector<std::string>* out) {
	// Add files to output, prefixing path
	state std::vector<std::string> files = wait(listFilesAsync(path, ""));
	for (auto const& f : files)
		out->push_back(joinPath(path, f));

	// Recurse for directories
	state std::vector<std::string> directories = wait(listDirectoriesAsync(path));
	for (auto const& dir : directories) {
		if (dir != "." && dir != "..")
			wait(findFilesRecursivelyAsync(joinPath(path, dir), out));
	}
	return Void();
}

} // namespace platform

void threadSleep(double seconds) {
#if (defined(__linux__) || defined(__APPLE__))
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
	sched_yield();
}

namespace platform {

void setCloseOnExec(int fd) {
	int options = fcntl(fd, F_GETFD);
	if (options != -1) {
		options = fcntl(fd, F_SETFD, options | FD_CLOEXEC);
	}
	if (options == -1) {
		TraceEvent(SevWarnAlways, "PlatformSetCloseOnExecError").suppressFor(60).GetLastError();
	}
}

} // namespace platform

#if (defined(__linux__) || defined(__APPLE__))
THREAD_HANDLE startThread(void* (*func)(void*), void* arg, int stackSize, const char* name) {
	pthread_t t;
	pthread_attr_t attr;

	pthread_attr_init(&attr);
	if (stackSize != 0) {
		if (pthread_attr_setstacksize(&attr, stackSize) != 0) {
			// If setting the stack size fails the default stack size will be used, so failure to set
			// the stack size is treated as a warning.
			// Logging a trace event here is a bit risky because startThread() could be used early
			// enough that TraceEvent can't be used yet, though currently it is not used with a nonzero
			// stack size that early in execution.
			TraceEvent(SevWarnAlways, "StartThreadInvalidStackSize").detail("StackSize", stackSize);
		};
	}

	pthread_create(&t, &attr, func, arg);
	pthread_attr_destroy(&attr);

#if defined(__linux__)
	if (name != nullptr) {
		// TODO: Should this just truncate?
		int retVal = pthread_setname_np(t, name);
		if (!retVal)
			return t;
		// In simulation and unit testing a thread may return before the name can be set, this will
		// return ENOENT or ESRCH. We'll log when ENOENT or ESRCH is encountered and continue, otherwise we'll log and
		// throw a platform_error.
		if (errno == ENOENT || errno == ESRCH) {
			TraceEvent(SevWarn, "PthreadSetNameNp").detail("Name", name).detail("ReturnCode", retVal).GetLastError();
		} else {
			TraceEvent(SevError, "PthreadSetNameNp").detail("Name", name).detail("ReturnCode", retVal).GetLastError();
			throw platform_error();
		}
	}
#endif

	return t;
}
#else
#error Port me!
#endif

void waitThread(THREAD_HANDLE thread) {
#if (defined(__linux__) || defined(__APPLE__))
	pthread_join(thread, nullptr);
#else
#error Port me!
#endif
}

void setThreadPriority(int pri) {
#ifdef __linux__
	int tid = syscall(SYS_gettid);
	setpriority(PRIO_PROCESS, tid, pri);
#endif
}

bool fileExists(std::string const& filename) {
	FILE* f = fopen(filename.c_str(), "rb" FOPEN_CLOEXEC_MODE);
	if (!f)
		return false;
	fclose(f);
	return true;
}

bool directoryExists(std::string const& path) {
	DIR* d = opendir(path.c_str());
	if (d == nullptr)
		return false;
	closedir(d);
	return true;
}

int64_t fileSize(std::string const& filename) {
#if (defined(__linux__) || defined(__APPLE__))
	struct stat file_status;
	if (stat(filename.c_str(), &file_status) != 0)
		return 0;
	else
		return file_status.st_size;
#else
#error Port me!
#endif
}

size_t readFileBytes(std::string const& filename, uint8_t* buff, size_t len) {
	std::fstream ifs(filename, std::fstream::in | std::fstream::binary);
	if (!ifs.good()) {
		TraceEvent("ileBytes_FileOpenError").detail("Filename", filename).GetLastError();
		throw io_error();
	}

	size_t bytesRead = len;
	ifs.seekg(0, std::fstream::beg);
	ifs.read((char*)buff, len);
	if (!ifs) {
		bytesRead = ifs.gcount();
		TraceEvent("ReadFileBytes_ShortRead")
		    .detail("Filename", filename)
		    .detail("Requested", len)
		    .detail("Actual", bytesRead);
	}

	return bytesRead;
}

std::string readFileBytes(std::string const& filename, int maxSize) {
	if (!fileExists(filename)) {
		TraceEvent("ReadFileBytes_FileNotFound").detail("Filename", filename);
		throw file_not_found();
	}

	size_t size = fileSize(filename);
	if (size > maxSize) {
		TraceEvent("ReadFileBytes_FileTooLarge").detail("Filename", filename);
		throw file_too_large();
	}

	std::string ret;
	ret.resize(size);
	readFileBytes(filename, (uint8_t*)ret.data(), size);

	return ret;
}

void writeFileBytes(std::string const& filename, const uint8_t* data, size_t count) {
	std::ofstream ofs(filename, std::fstream::out | std::fstream::binary);
	if (!ofs.good()) {
		TraceEvent("WriteFileBytes_FileOpenError").detail("Filename", filename).GetLastError();
		throw io_error();
	}

	ofs.write((const char*)data, count);
}

void writeFile(std::string const& filename, std::string const& content) {
	writeFileBytes(filename, (const uint8_t*)(content.c_str()), content.size());
}

namespace platform {

bool getEnvironmentVar(const char* name, std::string& value) {
	char* val = getenv(name);
	if (val) {
		value = std::string(val);
		return true;
	}
	return false;
}

int setEnvironmentVar(const char* name, const char* value, int overwrite) {
	return setenv(name, value, overwrite);
}

std::string getWorkingDirectory() {
	char* buf;
	if ((buf = getcwd(nullptr, 0)) == nullptr) {
		TraceEvent(SevWarnAlways, "GetWorkingDirectoryError").GetLastError();
		throw platform_error();
	}
	std::string result(buf);
	free(buf);
	return result;
}

} // namespace platform

extern std::string format(const char* form, ...);

namespace platform {
std::string getDefaultConfigPath() {
#if defined(__linux__)
	return "/etc/foundationdb";
#elif defined(__APPLE__)
	return "/usr/local/etc/foundationdb";
#else
#error Port me!
#endif
}

std::string getDefaultClusterFilePath() {
	return joinPath(getDefaultConfigPath(), "fdb.cluster");
}
} // namespace platform

#ifdef ALLOC_INSTRUMENTATION
#define TRACEALLOCATOR(size)                                                                                           \
	TraceEvent("MemSample")                                                                                            \
	    .detail("Count", FastAllocator<size>::getApproximateMemoryUnused() / size)                                     \
	    .detail("TotalSize", FastAllocator<size>::getApproximateMemoryUnused())                                        \
	    .detail("SampleCount", 1)                                                                                      \
	    .detail("Hash", "FastAllocatedUnused" #size)                                                                   \
	    .detail("Bt", "na")
#ifdef __linux__
#include <cxxabi.h>
#endif
uint8_t* g_extra_memory;
#endif

namespace platform {

void outOfMemory() {
#ifdef ALLOC_INSTRUMENTATION
	delete[] g_extra_memory;
	std::vector<std::pair<std::string, const char*>> typeNames;
	for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
		std::string s;
#ifdef __linux__
		char* demangled = abi::__cxa_demangle(i->first, nullptr, nullptr, nullptr);
		if (demangled) {
			s = demangled;
			if (StringRef(s).startsWith("(anonymous namespace)::"_sr))
				s = s.substr("(anonymous namespace)::"_sr.size());
			free(demangled);
		} else
			s = i->first;
#else
		s = i->first;
		if (StringRef(s).startsWith("class `anonymous namespace'::"_sr))
			s = s.substr("class `anonymous namespace'::"_sr.size());
		else if (StringRef(s).startsWith("class "_sr))
			s = s.substr("class "_sr.size());
		else if (StringRef(s).startsWith("struct "_sr))
			s = s.substr("struct "_sr.size());
#endif
		typeNames.emplace_back(s, i->first);
	}
	std::sort(typeNames.begin(), typeNames.end());
	for (int i = 0; i < typeNames.size(); i++) {
		const char* n = typeNames[i].second;
		auto& f = allocInstr[n];
		if (f.maxAllocated > 10000)
			TraceEvent("AllocInstrument")
			    .detail("CurrentAlloc", f.allocCount - f.deallocCount)
			    .detail("Name", typeNames[i].first.c_str());
	}

	std::unordered_map<uint32_t, BackTraceAccount> traceCounts;
	size_t memSampleSize;
	memSample_entered = true;
	{
		ThreadSpinLockHolder holder(memLock);
		traceCounts = backTraceLookup;
		memSampleSize = memSample.size();
	}
	memSample_entered = false;

	TraceEvent("MemSampleSummary")
	    .detail("InverseByteSampleRatio", SAMPLE_BYTES)
	    .detail("MemorySamples", memSampleSize)
	    .detail("BackTraces", traceCounts.size());

	for (auto i = traceCounts.begin(); i != traceCounts.end(); ++i) {
		char buf[1024];
		std::vector<void*>* frames = i->second.backTrace;
		std::string backTraceStr;
		backTraceStr = format_backtrace(&(*frames)[0], frames->size());
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
	TRACEALLOCATOR(96);
	TRACEALLOCATOR(128);
	TRACEALLOCATOR(256);
	TRACEALLOCATOR(512);
	TRACEALLOCATOR(1024);
	TRACEALLOCATOR(2048);
	TRACEALLOCATOR(4096);
	TRACEALLOCATOR(8192);
	g_traceBatch.dump();
#endif

	criticalError(FDB_EXIT_NO_MEM, "OutOfMemory", "Out of memory");
}

// Because the lambda used with nftw below cannot capture
int __eraseDirectoryRecursiveCount;

int eraseDirectoryRecursive(std::string const& dir) {
	__eraseDirectoryRecursiveCount = 0;
#if defined(__linux__) || defined(__APPLE__)
	int error = nftw(
	    dir.c_str(),
	    [](const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf) -> int {
		    int r = remove(fpath);
		    if (r == 0)
			    ++__eraseDirectoryRecursiveCount;
		    return r;
	    },
	    64,
	    FTW_DEPTH | FTW_PHYS);
	/* Looks like calling code expects this to continue silently if
	   the directory we're deleting doesn't exist in the first
	   place */
	if (error && errno != ENOENT) {
		Error e = systemErrorCodeToError();
		TraceEvent(SevError, "EraseDirectoryRecursiveError").error(e).detail("Directory", dir).GetLastError();
		throw e;
	}
#else
#error Port me!
#endif
	// INJECT_FAULT( platform_error, "eraseDirectoryRecursive" );
	return __eraseDirectoryRecursiveCount;
}

TmpFile::TmpFile() : filename("") {
	createTmpFile(boost::filesystem::temp_directory_path().string(), TmpFile::defaultPrefix);
}

TmpFile::TmpFile(const std::string& tmpDir) : filename("") {
	std::string dir = removeWhitespace(tmpDir);
	createTmpFile(dir, TmpFile::defaultPrefix);
}

TmpFile::TmpFile(const std::string& tmpDir, const std::string& prefix) : filename("") {
	std::string dir = removeWhitespace(tmpDir);
	createTmpFile(dir, prefix);
}

TmpFile::~TmpFile() {
	if (!filename.empty()) {
		destroyFile();
	}
}

void TmpFile::createTmpFile(const std::string_view dir, const std::string_view prefix) {
	std::string modelPattern = "%%%%-%%%%-%%%%-%%%%";
	boost::format fmter("%s/%s-%s");
	std::string modelPath = boost::str(boost::format(fmter % dir % prefix % modelPattern));
	boost::filesystem::path filePath = boost::filesystem::unique_path(modelPath);

	filename = filePath.string();

	// Create empty tmp file
	std::fstream tmpFile(filename, std::fstream::out);
	if (!tmpFile.good()) {
		TraceEvent("TmpFile_CreateFileError").detail("Filename", filename);
		throw io_error();
	}
	TraceEvent("TmpFile_CreateSuccess").detail("Filename", filename);
}

size_t TmpFile::read(uint8_t* buff, size_t len) {
	return readFileBytes(filename, buff, len);
}

void TmpFile::write(const uint8_t* buff, size_t len) {
	writeFileBytes(filename, buff, len);
}

bool TmpFile::destroyFile() {
	bool deleted = deleteFile(filename);
	if (deleted) {
		TraceEvent("TmpFileDestory_Success").detail("Filename", filename);
	} else {
		TraceEvent("TmpFileDestory_Failed").detail("Filename", filename);
	}
	return deleted;
}

} // namespace platform

extern "C" void criticalError(int exitCode, const char* type, const char* message) {
	// Be careful!  This function may be called asynchronously from a thread or in other weird conditions

	fprintf(stderr, "ERROR: %s\n", message);

	if (g_network && !g_network->isSimulated()) {
		TraceEvent ev(SevError, type);
		ev.detail("Message", message);
	}

	flushAndExit(exitCode);
}

extern void flushTraceFileVoid();

#ifdef USE_GCOV
extern "C" void __gcov_flush();
#endif

extern "C" void flushAndExit(int exitCode) {
	flushTraceFileVoid();
	fflush(stdout);
	closeTraceFile();

	// Flush all output streams. The original intent is to flush the outfile for contrib/debug_determinism.
	fflush(nullptr);

#ifdef USE_GCOV
	__gcov_flush();
#endif
	_exit(exitCode);
	// should never reach here, but you never know
	crashAndDie();
}

#include <dlfcn.h>

#ifdef __linux__
#include <link.h>
#endif

platform::ImageInfo getImageInfo(const void* symbol) {
	Dl_info info;
	platform::ImageInfo imageInfo;

#ifdef __linux__
	link_map* linkMap = nullptr;
	int res = dladdr1(symbol, &info, (void**)&linkMap, RTLD_DL_LINKMAP);
#else
	int res = dladdr(symbol, &info);
#endif

	if (res != 0) {
		imageInfo.fileName = info.dli_fname;
		std::string imageFile = basename(info.dli_fname);
		// If we have a client library that doesn't end in the appropriate extension, we will get the wrong debug
		// suffix. This should only be a cosmetic problem, though.
#ifdef __linux__
		imageInfo.offset = (void*)linkMap->l_addr;
		if (imageFile.length() >= 3 && imageFile.rfind(".so") == imageFile.length() - 3) {
			imageInfo.symbolFileName = imageFile + "-debug";
		}
#else
		imageInfo.offset = info.dli_fbase;
		if (imageFile.length() >= 6 && imageFile.rfind(".dylib") == imageFile.length() - 6) {
			imageInfo.symbolFileName = imageFile + "-debug";
		}
#endif
		else {
			imageInfo.symbolFileName = imageFile + ".debug";
		}
	}

	return imageInfo;
}

platform::ImageInfo getCachedImageInfo() {
	// The use of "getCachedImageInfo" is arbitrary and was a best guess at a good way to get the image of the
	//  most likely candidate for the "real" flow library or binary
	static platform::ImageInfo info = getImageInfo((const void*)&getCachedImageInfo);
	return info;
}

#include <execinfo.h>

namespace platform {
ImageInfo getImageInfo() {
	return getCachedImageInfo();
}

size_t raw_backtrace(void** addresses, int maxStackDepth) {
#if !defined(__APPLE__)
	// absl::GetStackTrace doesn't have an implementation for MacOS.
	return absl::GetStackTrace(addresses, maxStackDepth, 0);
#else
	return backtrace(addresses, maxStackDepth);
#endif
}

std::string format_backtrace(void** addresses, int numAddresses) {
	ImageInfo const& imageInfo = getCachedImageInfo();
#ifdef __APPLE__
	std::string s = format("atos -o %s -arch x86_64 -l %p", imageInfo.symbolFileName.c_str(), imageInfo.offset);
	for (int i = 1; i < numAddresses; i++) {
		s += format(" %p", addresses[i]);
	}
#else
	std::string s = format("addr2line -e %s -p -C -f -i", imageInfo.symbolFileName.c_str());
	for (int i = 1; i < numAddresses; i++) {
		s += format(" %p", (char*)addresses[i] - (char*)imageInfo.offset);
	}
#endif
	return s;
}

std::string get_backtrace() {
	void* addresses[50];
	size_t size = raw_backtrace(addresses, 50);
	return format_backtrace(addresses, size);
}
} // namespace platform

bool isLibraryLoaded(const char* lib_path) {
#if !defined(__linux__) && !defined(__APPLE__)
#error Port me!
#endif

	void* dlobj = nullptr;

	dlobj = dlopen(lib_path, RTLD_NOLOAD | RTLD_LAZY);

	return dlobj != nullptr;
}

void* loadLibrary(const char* lib_path) {
#if !defined(__linux__) && !defined(__APPLE__)
#error Port me!
#endif

	void* dlobj = nullptr;

	dlobj = dlopen(lib_path,
	               RTLD_LAZY | RTLD_LOCAL
#ifdef USE_SANITIZER // Keep alive dlopen()-ed libs for symbolized XSAN backtrace
	                   | RTLD_NODELETE
#endif
	);
	if (dlobj == nullptr) {
		TraceEvent(SevWarn, "LoadLibraryFailed").detail("Library", lib_path).detail("Error", dlerror());
	}

	return dlobj;
}

void* loadFunction(void* lib, const char* func_name) {
	void* dlfcn = nullptr;

	dlfcn = dlsym(lib, func_name);
	if (dlfcn == nullptr) {
		TraceEvent(SevWarn, "LoadFunctionFailed").detail("Function", func_name).detail("Error", dlerror());
	}

	return dlfcn;
}

void closeLibrary(void* handle) {
	dlclose(handle);
}

std::string exePath() {
#if defined(__linux__)
	std::unique_ptr<char[]> buf(new char[PATH_MAX]);
	auto len = readlink("/proc/self/exe", buf.get(), PATH_MAX);
	if (len > 0 && len < PATH_MAX) {
		buf[len] = '\0';
		return std::string(buf.get());
	} else {
		throw platform_error();
	}
#elif defined(__APPLE__)
	uint32_t bufSize = 1024;
	std::unique_ptr<char[]> buf(new char[bufSize]);
	while (true) {
		auto res = _NSGetExecutablePath(buf.get(), &bufSize);
		if (res == -1) {
			buf.reset(new char[bufSize]);
		} else {
			return std::string(buf.get());
		}
	}
#else
#error Port me!
#endif
}

void platformInit() {
#ifdef __linux__
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		criticalError(FDB_EXIT_ERROR,
		              "MonotonicTimeUnavailable",
		              "clock_gettime(CLOCK_MONOTONIC, ...) returned an error. Check your kernel and glibc versions.");
	}
#endif
}

std::vector<std::function<void()>> g_crashHandlerCallbacks;

void registerCrashHandlerCallback(void (*f)()) {
	g_crashHandlerCallbacks.push_back(f);
}

// The crashHandler function is registered to handle signals before the process terminates.
// Basic information about the crash is printed/traced, and stdout and trace events are flushed.
void crashHandler(int sig) {
#ifdef __linux__
	// Pretty much all of this handler is risking undefined behavior and hangs,
	//  but the idea is that we're about to crash anyway...
	std::string backtrace = platform::get_backtrace();

	bool error = (sig != SIGUSR2 && sig != SIGTERM);

	StreamCipherKey::cleanup();
	StreamCipher::cleanup();

	for (auto& f : g_crashHandlerCallbacks) {
		f();
	}

	fprintf(error ? stderr : stdout, "SIGNAL: %s (%d)\n", strsignal(sig), sig);
	if (error) {
		fprintf(stderr, "Trace: %s\n", backtrace.c_str());
	}

	fflush(stdout);
	{
		TraceEvent te(error ? SevError : SevInfo, error ? "Crash" : "ProcessTerminated");
		te.detail("Signal", sig).detail("Name", strsignal(sig)).detail("Trace", backtrace);
		if (error) {
			te.setErrorKind(ErrorKind::BugDetected);
		}
	}
	flushTraceFileVoid();

#ifdef USE_GCOV
	__gcov_flush();
#endif

	struct sigaction sa;
	sa.sa_handler = SIG_DFL;
	if (sigemptyset(&sa.sa_mask)) {
		int err = errno;
		fprintf(stderr, "sigemptyset failed: %s\n", strerror(err));
		_exit(sig + 128);
	}
	sa.sa_flags = 0;
	if (sigaction(sig, &sa, nullptr)) {
		int err = errno;
		fprintf(stderr, "sigaction failed: %s\n", strerror(err));
		_exit(sig + 128);
	}
	if (kill(getpid(), sig)) {
		int err = errno;
		fprintf(stderr, "kill failed: %s\n", strerror(err));
		_exit(sig + 128);
	}
	// Rely on kill to end the process
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
	sigfillset(&action.sa_mask);
	action.sa_flags = 0;

	sigaction(SIGILL, &action, nullptr);
	sigaction(SIGFPE, &action, nullptr);
	sigaction(SIGSEGV, &action, nullptr);
	sigaction(SIGBUS, &action, nullptr);
	sigaction(SIGUSR2, &action, nullptr);
#ifdef USE_GCOV
	// SIGTERM is the "graceful" way to end an fdbserver process, so we actually
	// don't want to invoke crashHandler. crashHandler is not actually
	// async-signal-safe, so we can only justify calling it if we were going to
	// crash anyway. For USE_GCOV though we need to flush coverage info, which
	// we do through crashHandler.
	sigaction(SIGTERM, &action, nullptr);
#endif
	sigaction(SIGABRT, &action, nullptr);
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
extern std::atomic<int64_t> net2RunLoopIterations;
extern std::atomic<int64_t> net2RunLoopSleeps;
extern void initProfiling();

namespace {

std::atomic<double> checkThreadTime;
std::mutex loopProfilerThreadMutex;
std::optional<pthread_t> loopProfilerThread;
std::atomic<bool> loopProfilerStopRequested = false;

} // namespace
#endif

// True if this thread is the thread being profiled. Not to be used from the signal handler.
thread_local bool profileThread = false;

// The thread ID of the profiled thread. This can be compared against the current thread ID
// to see if we are on the profiled thread. Can be used in the signal handler.
volatile int64_t profileThreadId = -1;

#ifdef __linux__
struct sigaction chainedAction;
#endif

volatile bool profilingEnabled = 1;
volatile thread_local bool flowProfilingEnabled = 1;

volatile int64_t numProfilesDisabled = 0;
volatile int64_t numProfilesOverflowed = 0;
volatile int64_t numProfilesCaptured = 0;

int64_t getNumProfilesDisabled() {
	if (profileThread) {
		return numProfilesDisabled;
	} else {
		return 0;
	}
}

int64_t getNumProfilesOverflowed() {
	if (profileThread) {
		return numProfilesOverflowed;
	} else {
		return 0;
	}
}

int64_t getNumProfilesCaptured() {
	if (profileThread) {
		return numProfilesCaptured;
	} else {
		return 0;
	}
}

void profileHandler(int sig) {
#ifdef __linux__
	if (chainedAction.sa_handler != SIG_DFL && chainedAction.sa_handler != SIG_IGN &&
	    chainedAction.sa_handler != nullptr) {
		chainedAction.sa_handler(sig);
	}

	// This is not documented in the POSIX list of signal-safe functions, but numbered syscalls are reported to be
	// async safe in Linux.
	if (profileThreadId != syscall(__NR_gettid)) {
		return;
	} else if (!profilingEnabled) {
		++numProfilesDisabled;
		return;
	}

	++net2backtraces_count;

	if (!net2backtraces || net2backtraces_max - net2backtraces_offset < 50) {
		++numProfilesOverflowed;
		net2backtraces_overflow = true;
		return;
	}

	++numProfilesCaptured;

	// We are casting away the volatile-ness of the backtrace array, but we believe that should be reasonably safe in
	// the signal handler
	ProfilingSample* ps =
	    const_cast<ProfilingSample*>((volatile ProfilingSample*)(net2backtraces + net2backtraces_offset));

	// We can only read the check thread time in a signal handler if the atomic is lock free.
	// We can't get the time from a timer() call because it's not signal safe.
	ps->timestamp = checkThreadTime.is_lock_free() ? checkThreadTime.load() : 0;

#if defined(USE_SANITIZER)
	// In sanitizer builds the workaround implemented in SignalSafeUnwind.cpp is disabled
	// so calling backtrace may cause a deadlock
	size_t size = 0;
#else
	// SOMEDAY: should we limit the maximum number of frames from backtrace beyond just available space?
	size_t size = backtrace(ps->frames, net2backtraces_max - net2backtraces_offset - 2);
#endif

	ps->length = size;

	net2backtraces_offset += size + 2;
#else
// No slow task profiling for other platforms!
#endif
}

void setProfilingEnabled(int enabled) {
#ifdef __linux__
	if (profileThread) {
		profilingEnabled = enabled;
	}
	flowProfilingEnabled = enabled;
#else
	// No profiling for other platforms!
#endif
}

void* checkThread(void* arg) {
#ifdef __linux__
	pthread_t mainThread = *(pthread_t*)arg;
	free(arg);

	int64_t lastRunLoopIterations = net2RunLoopIterations.load();
	int64_t lastRunLoopSleeps = net2RunLoopSleeps.load();

	double slowTaskStart = 0;
	double lastSlowTaskSignal = 0;
	double lastSaturatedSignal = 0;
	double lastSlowTaskBlockedLog = 0;

	const double minSlowTaskLogInterval =
	    std::max(FLOW_KNOBS->SLOWTASK_PROFILING_LOG_INTERVAL, FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL);
	const double minSaturationLogInterval =
	    std::max(FLOW_KNOBS->SATURATION_PROFILING_LOG_INTERVAL, FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL);

	double slowTaskLogInterval = minSlowTaskLogInterval;
	double saturatedLogInterval = minSaturationLogInterval;

	while (!loopProfilerStopRequested) {
		threadSleep(FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL);

		int64_t currentRunLoopIterations = net2RunLoopIterations.load();
		int64_t currentRunLoopSleeps = net2RunLoopSleeps.load();

		bool slowTask = lastRunLoopIterations == currentRunLoopIterations;
		bool saturated = lastRunLoopSleeps == currentRunLoopSleeps;

		if (slowTask) {
			double t = timer();
			bool newSlowTask = lastSlowTaskSignal == 0;

			if (newSlowTask) {
				slowTaskStart = t;
			} else if (t - std::max(slowTaskStart, lastSlowTaskBlockedLog) > FLOW_KNOBS->SLOWTASK_BLOCKED_INTERVAL) {
				lastSlowTaskBlockedLog = t;
				// When this gets logged, it will be with a current timestamp (using timer()). If the network thread
				// unblocks, it will log any slow task related events at an earlier timestamp. That means the order of
				// events during this sequence will not match their timestamp order.
				TraceEvent(SevWarnAlways, "RunLoopBlocked").detail("Duration", t - slowTaskStart);
			}

			if (newSlowTask || t - lastSlowTaskSignal >= slowTaskLogInterval) {
				if (lastSlowTaskSignal > 0) {
					slowTaskLogInterval = std::min(FLOW_KNOBS->SLOWTASK_PROFILING_MAX_LOG_INTERVAL,
					                               FLOW_KNOBS->SLOWTASK_PROFILING_LOG_BACKOFF * slowTaskLogInterval);
				}

				lastSlowTaskSignal = t;
				checkThreadTime.store(lastSlowTaskSignal);
				pthread_kill(mainThread, SIGPROF);
			}
		} else {
			slowTaskStart = 0;
			lastSlowTaskSignal = 0;
			lastRunLoopIterations = currentRunLoopIterations;
			slowTaskLogInterval = minSlowTaskLogInterval;
		}

		if (saturated) {
			double t = timer();
			if (lastSaturatedSignal == 0 || t - lastSaturatedSignal >= saturatedLogInterval) {
				if (lastSaturatedSignal > 0) {
					saturatedLogInterval =
					    std::min(FLOW_KNOBS->SATURATION_PROFILING_MAX_LOG_INTERVAL,
					             FLOW_KNOBS->SATURATION_PROFILING_LOG_BACKOFF * saturatedLogInterval);
				}

				lastSaturatedSignal = t;

				if (!slowTask) {
					checkThreadTime.store(lastSaturatedSignal);
					pthread_kill(mainThread, SIGPROF);
				}
			}
		} else {
			lastSaturatedSignal = 0;
			lastRunLoopSleeps = currentRunLoopSleeps;
			saturatedLogInterval = minSaturationLogInterval;
		}
	}
	return nullptr;
#else
	// No slow task profiling for other platforms!
	return nullptr;
#endif
}

#if defined(DTRACE_PROBES)
void fdb_probe_actor_create(const char* name, unsigned long id) {
	FDB_TRACE_PROBE(actor_create, name, id);
}
void fdb_probe_actor_destroy(const char* name, unsigned long id) {
	FDB_TRACE_PROBE(actor_destroy, name, id);
}
void fdb_probe_actor_enter(const char* name, unsigned long id, int index) {
	FDB_TRACE_PROBE(actor_enter, name, id, index);
}
void fdb_probe_actor_exit(const char* name, unsigned long id, int index) {
	FDB_TRACE_PROBE(actor_exit, name, id, index);
}
#endif

void throwExecPathError(Error e, char path[]) {
	Severity sev = e.code() == error_code_io_error ? SevError : SevWarnAlways;
	TraceEvent(sev, "GetPathError").error(e).detail("Path", path);
	throw e;
}

std::string getExecPath() {
	char path[1024];
	uint32_t size = sizeof(path);
#if defined(__APPLE__)
	if (_NSGetExecutablePath(path, &size) == 0) {
		return std::string(path);
	} else {
		throwExecPathError(platform_error(), path);
	}
#elif defined(__linux__)
	ssize_t len = ::readlink("/proc/self/exe", path, size);
	if (len != -1) {
		path[len] = '\0';
		return std::string(path);
	} else {
		throwExecPathError(platform_error(), path);
	}
#endif
	return "unsupported OS";
}

void setupRunLoopProfiler() {
#ifdef __linux__
	if (profileThreadId == -1 && FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL > 0) {
		chainedAction.sa_handler = SIG_DFL;

		TraceEvent("StartingRunLoopProfilingThread").detail("Interval", FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL);

		profileThread = true;
		profileThreadId = syscall(__NR_gettid);
		if (profileThreadId < 0) {
			TraceEvent(SevWarnAlways, "RunLoopProfilingSetupFailed")
			    .detail("Reason", "Error getting thread ID")
			    .GetLastError();
			return;
		}

		initProfiling();

		struct sigaction action;
		action.sa_handler = profileHandler;
		sigfillset(&action.sa_mask);
		action.sa_flags = 0;
		if (sigaction(SIGPROF, &action, &chainedAction)) {
			TraceEvent(SevWarnAlways, "RunLoopProfilingSetupFailed")
			    .detail("Reason", "Error configuring signal handler")
			    .GetLastError();
			return;
		}

		// Start a thread which will use signals to log stacks on long events
		pthread_t* mainThread = (pthread_t*)malloc(sizeof(pthread_t));
		*mainThread = pthread_self();
		{
			std::unique_lock<std::mutex> lock(loopProfilerThreadMutex);
			if (!loopProfilerStopRequested) {
				loopProfilerThread = startThread(&checkThread, (void*)mainThread, 0, "fdb-loopprofile");
			}
		}
	}
#else
	// No slow task profiling for other platforms!
#endif
}

void stopRunLoopProfiler() {
#ifdef __linux__
	std::unique_lock<std::mutex> lock(loopProfilerThreadMutex);
	loopProfilerStopRequested.store(true);
	if (loopProfilerThread) {
		pthread_join(loopProfilerThread.value(), NULL);
		loopProfilerThread = {};
	}
#endif
}

// UnitTest for getMemoryInfo
#ifdef __linux__
TEST_CASE("/flow/Platform/getMemoryInfo") {

	printf("UnitTest flow/Platform/getMemoryInfo 1\n");
	std::string memString = "MemTotal:       24733228 kB\n"
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
		{ "MemTotal:"_sr, 0 }, { "MemFree:"_sr, 0 },   { "MemAvailable:"_sr, 0 }, { "Buffers:"_sr, 0 },
		{ "Cached:"_sr, 0 },   { "SwapTotal:"_sr, 0 }, { "SwapFree:"_sr, 0 },
	};

	std::stringstream memInfoStream(memString);
	linux_os::getMemoryInfo(request, memInfoStream);
	ASSERT(request["MemTotal:"_sr] == 24733228);
	ASSERT(request["MemFree:"_sr] == 2077580);
	ASSERT(request["MemAvailable:"_sr] == 0);
	ASSERT(request["Buffers:"_sr] == 266940);
	ASSERT(request["Cached:"_sr] == 16798292);
	ASSERT(request["SwapTotal:"_sr] == 25165820);
	ASSERT(request["SwapFree:"_sr] == 23680228);
	for (auto& item : request) {
		fmt::print("{}:{}\n", item.first.toString().c_str(), item.second);
	}

	printf("UnitTest flow/Platform/getMemoryInfo 2\n");
	std::string memString1 = "Slab:             192816 kB\n"
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
	linux_os::getMemoryInfo(request, memInfoStream1);
	ASSERT(request["MemTotal:"_sr] == 31856496);
	ASSERT(request["MemFree:"_sr] == 25492716);
	ASSERT(request["MemAvailable:"_sr] == 28470756);
	ASSERT(request["Buffers:"_sr] == 313644);
	ASSERT(request["Cached:"_sr] == 2956444);
	ASSERT(request["SwapTotal:"_sr] == 0);
	ASSERT(request["SwapFree:"_sr] == 0);
	for (auto& item : request) {
		fmt::print("{}:{}\n", item.first.toString().c_str(), item.second);
	}

	return Void();
}
#endif

int testPathFunction(const char* name,
                     std::function<std::string(std::string)> fun,
                     std::string a,
                     ErrorOr<std::string> b) {
	ErrorOr<std::string> result;
	try {
		result = fun(a);
	} catch (Error& e) {
		result = e;
	}
	bool r = result.isError() == b.isError() && (b.isError() || b.get() == result.get()) &&
	         (!b.isError() || b.getError().code() == result.getError().code());

	printf("%s: %s('%s') -> %s",
	       r ? "PASS" : "FAIL",
	       name,
	       a.c_str(),
	       result.isError() ? result.getError().what() : format("'%s'", result.get().c_str()).c_str());
	if (!r) {
		printf("  *ERROR* expected %s", b.isError() ? b.getError().what() : format("'%s'", b.get().c_str()).c_str());
	}
	printf("\n");
	return r ? 0 : 1;
}

int testPathFunction2(const char* name,
                      std::function<std::string(std::string, bool, bool)> fun,
                      std::string a,
                      bool resolveLinks,
                      bool mustExist,
                      ErrorOr<std::string> b) {
	// Skip tests with resolveLinks set to false as the implementation is not complete
	if (resolveLinks == false) {
		printf("SKIPPED: %s('%s', %d, %d)\n", name, a.c_str(), resolveLinks, mustExist);
		return 0;
	}

	ErrorOr<std::string> result;
	try {
		result = fun(a, resolveLinks, mustExist);
	} catch (Error& e) {
		result = e;
	}
	bool r = result.isError() == b.isError() && (b.isError() || b.get() == result.get()) &&
	         (!b.isError() || b.getError().code() == result.getError().code());

	printf("%s: %s('%s', %d, %d) -> %s",
	       r ? "PASS" : "FAIL",
	       name,
	       a.c_str(),
	       resolveLinks,
	       mustExist,
	       result.isError() ? result.getError().what() : format("'%s'", result.get().c_str()).c_str());
	if (!r) {
		printf("  *ERROR* expected %s", b.isError() ? b.getError().what() : format("'%s'", b.get().c_str()).c_str());
	}
	printf("\n");
	return r ? 0 : 1;
}

void platformSpecificDirectoryOpsTests(const std::string& cwd, int& errors) {
	// Create some symlinks and test resolution (or non-resolution) of them
	ASSERT(symlink("one/two", "simfdb/backups/four") == 0);
	ASSERT(symlink("../backups/four", "simfdb/backups/five") == 0);

	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/four/../two", true, true, joinPath(cwd, "simfdb/backups/one/two"));
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../two", true, true, joinPath(cwd, "simfdb/backups/one/two"));
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../two", true, false, joinPath(cwd, "simfdb/backups/one/two"));
	errors += testPathFunction2("abspath", abspath, "simfdb/backups/five/../three", true, true, platform_error());
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../three", true, false, joinPath(cwd, "simfdb/backups/one/three"));
	errors += testPathFunction2("abspath",
	                            abspath,
	                            "simfdb/backups/five/../three/../four",
	                            true,
	                            false,
	                            joinPath(cwd, "simfdb/backups/one/four"));

	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/four/../two",
	                            true,
	                            true,
	                            joinPath(cwd, "simfdb/backups/one/"));
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/five/../two",
	                            true,
	                            true,
	                            joinPath(cwd, "simfdb/backups/one/"));
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/five/../two",
	                            true,
	                            false,
	                            joinPath(cwd, "simfdb/backups/one/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../three", true, true, platform_error());
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/five/../three",
	                            true,
	                            false,
	                            joinPath(cwd, "simfdb/backups/one/"));
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/five/../three/../four",
	                            true,
	                            false,
	                            joinPath(cwd, "simfdb/backups/one/"));
}

TEST_CASE("/flow/Platform/directoryOps") {
	int errors = 0;

	errors += testPathFunction("popPath", popPath, "a", "");
	errors += testPathFunction("popPath", popPath, "a/", "");
	errors += testPathFunction("popPath", popPath, "a///", "");
	errors += testPathFunction("popPath", popPath, "a///..", "a/");
	errors += testPathFunction("popPath", popPath, "a///../", "a/");
	errors += testPathFunction("popPath", popPath, "a///..//", "a/");
	errors += testPathFunction("popPath", popPath, "/", "/");
	errors += testPathFunction("popPath", popPath, "/a", "/");
	errors += testPathFunction("popPath", popPath, "/a/b", "/a/");
	errors += testPathFunction("popPath", popPath, "/a/b/", "/a/");
	errors += testPathFunction("popPath", popPath, "/a/b/..", "/a/b/");
	errors += testPathFunction("popPath", popPath, "/a/b///..//", "/a/b/");

	errors += testPathFunction("cleanPath", cleanPath, "/", "/");
	errors += testPathFunction("cleanPath", cleanPath, "///.///", "/");
	errors += testPathFunction("cleanPath", cleanPath, "/a/b/.././../c/./././////./d/..//", "/c");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//", "c");
	errors += testPathFunction("cleanPath", cleanPath, "..", "..");
	errors += testPathFunction("cleanPath", cleanPath, "../.././", "../..");
	errors += testPathFunction("cleanPath", cleanPath, "../a/b/..//", "../a");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//..", ".");
	errors += testPathFunction("cleanPath", cleanPath, "/..", "/");
	errors += testPathFunction("cleanPath", cleanPath, "/../foo/bar///", "/foo/bar");
	errors += testPathFunction("cleanPath", cleanPath, "/a/b/../.././../", "/");
	errors += testPathFunction("cleanPath", cleanPath, ".", ".");

	// Creating this directory in backups avoids some sanity checks
	platform::createDirectory("simfdb/backups/one/two/three");
	std::string cwd = platform::getWorkingDirectory();
	platformSpecificDirectoryOpsTests(cwd, errors);
	errors += testPathFunction2("abspath", abspath, "/", false, false, "/");
	errors += testPathFunction2("abspath", abspath, "/foo//bar//baz/.././", false, false, "/foo/bar");
	errors += testPathFunction2("abspath", abspath, "/", true, false, "/");
	errors += testPathFunction2("abspath", abspath, "", true, false, platform_error());
	errors += testPathFunction2("abspath", abspath, ".", true, false, cwd);
	errors += testPathFunction2("abspath", abspath, "/a", true, false, "/a");
	errors += testPathFunction2("abspath", abspath, "one/two/three/four", false, true, platform_error());
	errors +=
	    testPathFunction2("abspath", abspath, "one/two/three/four", false, false, joinPath(cwd, "one/two/three/four"));
	errors += testPathFunction2(
	    "abspath", abspath, "one/two/three/./four", false, false, joinPath(cwd, "one/two/three/four"));
	errors += testPathFunction2(
	    "abspath", abspath, "one/two/three/./four", false, false, joinPath(cwd, "one/two/three/four"));
	errors +=
	    testPathFunction2("abspath", abspath, "one/two/three/./four/..", false, false, joinPath(cwd, "one/two/three"));
	errors += testPathFunction2(
	    "abspath", abspath, "one/./two/../three/./four", false, false, joinPath(cwd, "one/three/four"));
	errors += testPathFunction2("abspath", abspath, "one/./two/../three/./four", false, true, platform_error());
	errors += testPathFunction2("abspath", abspath, "one/two/three/./four", false, true, platform_error());
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/one/two/three", false, true, joinPath(cwd, "simfdb/backups/one/two/three"));
	errors += testPathFunction2("abspath", abspath, "simfdb/backups/one/two/threefoo", false, true, platform_error());
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/four/../two", false, false, joinPath(cwd, "simfdb/backups/two"));
	errors += testPathFunction2("abspath", abspath, "simfdb/backups/four/../two", false, true, platform_error());
	errors += testPathFunction2("abspath", abspath, "simfdb/backups/five/../two", false, true, platform_error());
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../two", false, false, joinPath(cwd, "simfdb/backups/two"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", false, false, joinPath(cwd, "foo2/bar"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", false, true, platform_error());
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", true, false, joinPath(cwd, "foo2/bar"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", true, true, platform_error());

	errors += testPathFunction2("parentDirectory", parentDirectory, "", true, false, platform_error());
	errors += testPathFunction2("parentDirectory", parentDirectory, "/", true, false, "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, "/a", true, false, "/");
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, ".", false, false, cleanPath(joinPath(cwd, "..")) + "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, "./foo", false, false, cleanPath(cwd) + "/");
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "one/two/three/four", false, true, platform_error());
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/four", false, false, joinPath(cwd, "one/two/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/./four", false, false, joinPath(cwd, "one/two/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/./four/..", false, false, joinPath(cwd, "one/two/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/./two/../three/./four", false, false, joinPath(cwd, "one/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/./two/../three/./four", false, true, platform_error());
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "one/two/three/./four", false, true, platform_error());
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/one/two/three",
	                            false,
	                            true,
	                            joinPath(cwd, "simfdb/backups/one/two/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/one/two/threefoo", false, true, platform_error());
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/four/../two",
	                            false,
	                            false,
	                            joinPath(cwd, "simfdb/backups/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/four/../two", false, true, platform_error());
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../two", false, true, platform_error());
	errors += testPathFunction2("parentDirectory",
	                            parentDirectory,
	                            "simfdb/backups/five/../two",
	                            false,
	                            false,
	                            joinPath(cwd, "simfdb/backups/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "foo/./../foo2/./bar//", false, false, joinPath(cwd, "foo2/"));
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "foo/./../foo2/./bar//", false, true, platform_error());
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "foo/./../foo2/./bar//", true, false, joinPath(cwd, "foo2/"));
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "foo/./../foo2/./bar//", true, true, platform_error());

	printf("%d errors.\n", errors);

	ASSERT(errors == 0);
	return Void();
}
