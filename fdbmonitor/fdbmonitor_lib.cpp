/*
 * fdbmonitor_lib.cpp
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

#include <csignal>
#include <limits>
#include <sys/time.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <random>

#ifdef __linux__
#include <sys/prctl.h>
#elif defined(__FreeBSD__)
#include <sys/procctl.h>
#endif

#include <sys/wait.h>

#ifdef __FreeBSD__
#include <sys/event.h>
#define O_EVTONLY O_RDONLY
#endif

#ifdef __APPLE__
#include <sys/event.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif

#include <stdlib.h>
#include <unordered_map>
#include <unordered_set>
#include <iterator>
#include <functional>
#include <memory>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <syslog.h>
#include <stdarg.h>
#include <pwd.h>
#include <grp.h>

#include "fdbmonitor.h"
#include "fdbclient/versions.h"

namespace fdbmonitor {

bool daemonize = false;
std::string logGroup = "default";

int severity_to_priority(Severity severity) {
	switch (severity) {
	case SevError:
		return LOG_ERR;
	case SevWarnAlways:
		return LOG_WARNING;
	case SevWarn:
		return LOG_NOTICE;
	case SevDebug:
		return LOG_DEBUG;
	case SevInfo:
	default:
		return LOG_INFO;
	}
}

double timer() {
#if defined(__linux__) || defined(__FreeBSD__)
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return double(ts.tv_sec) + (ts.tv_nsec * 1e-9);
#elif defined(__APPLE__)
	mach_timebase_info_data_t timebase_info;
	mach_timebase_info(&timebase_info);
	return ((mach_absolute_time() * timebase_info.numer) / timebase_info.denom) * 1e-9;
#else
#error Port me!
#endif
};

double get_cur_timestamp() {
	struct tm tm_info;
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	localtime_r(&tv.tv_sec, &tm_info);

	return tv.tv_sec + 1e-6 * tv.tv_usec;
}

int randomInt(int min, int max) {
	static std::random_device rd;
	static std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(min, max);

	return dis(gen);
}

void vlog_process_msg(Severity severity, const char* process, const char* format, va_list args) {
	if (daemonize) {
		char buf[4096];
		int len = vsnprintf(buf, 4096, format, args);
		syslog(severity_to_priority(severity),
		       "LogGroup=\"%s\" Process=\"%s\": %.*s",
		       logGroup.c_str(),
		       process,
		       len,
		       buf);
	} else {
		fprintf(stderr,
		        "Time=\"%.6f\" Severity=\"%d\" LogGroup=\"%s\" Process=\"%s\": ",
		        get_cur_timestamp(),
		        (int)severity,
		        logGroup.c_str(),
		        process);
		vfprintf(stderr, format, args);
	}
}

void log_msg(Severity severity, const char* format, ...) {
	va_list args;
	va_start(args, format);

	vlog_process_msg(severity, "fdbmonitor", format, args);

	va_end(args);
}

void log_process_msg(Severity severity, const char* process, const char* format, ...) {
	va_list args;
	va_start(args, format);

	vlog_process_msg(severity, process, format, args);

	va_end(args);
}

void log_err(const char* func, int err, const char* format, ...) {
	va_list args;
	va_start(args, format);

	char buf[4096];
	int len = vsnprintf(buf, 4096, format, args);

	log_msg(SevError, "%.*s (%s error %d: %s)\n", len, buf, func, err, strerror(err));

	va_end(args);
}

void monitor_fd(fdb_fd_set list, int fd, int* maxfd, void* cmd) {
#ifdef __linux__
	FD_SET(fd, list);
	if (fd > *maxfd)
		*maxfd = fd;
#elif defined(__APPLE__) || defined(__FreeBSD__)
	/* ignore maxfd */
	struct kevent ev;
	EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, cmd);
	kevent(list, &ev, 1, nullptr, 0, nullptr); // FIXME: check?
#endif
}

void unmonitor_fd(fdb_fd_set list, int fd) {
#ifdef __linux__
	FD_CLR(fd, list);
#elif defined(__APPLE__) || defined(__FreeBSD__)
	struct kevent ev;
	EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
	kevent(list, &ev, 1, nullptr, 0, nullptr); // FIXME: check?
#endif
}

const char* get_value_multi(const CSimpleIni& ini, const char* key, ...) {
	const char* ret = nullptr;
	const char* section = nullptr;

	std::string keyWithUnderscores(key);
	for (int i = keyWithUnderscores.size() - 1; i >= 0; --i) {
		if (keyWithUnderscores[i] == '-') {
			keyWithUnderscores.at(i) = '_';
		}
	}

	va_list ap;
	va_start(ap, key);
	while (!ret && (section = va_arg(ap, const char*))) {
		ret = ini.GetValue(section, key, nullptr);
		if (!ret) {
			ret = ini.GetValue(section, keyWithUnderscores.c_str(), nullptr);
		}
	}
	va_end(ap);
	return ret;
}

bool isParameterNameEqual(const char* str, const char* target) {
	if (!str || !target) {
		return false;
	}
	while (*str && *target) {
		char curStr = *str, curTarget = *target;
		if (curStr == '-') {
			curStr = '_';
		}
		if (curTarget == '-') {
			curTarget = '_';
		}
		if (curStr != curTarget) {
			return false;
		}
		str++;
		target++;
	}
	return !(*str || *target);
}

// Parse size value with same format as parse_with_suffix in flow.h
uint64_t parseWithSuffix(const char* to_parse, const char* default_unit) {
	char* end_ptr = nullptr;
	uint64_t ret = strtoull(to_parse, &end_ptr, 10);
	if (end_ptr == to_parse) {
		// failed to parse
		return 0;
	}
	const char* unit = default_unit;
	if (*end_ptr != 0) {
		unit = end_ptr;
	}
	if (unit == nullptr) {
		// no unit found
		return 0;
	}
	if (strcmp(end_ptr, "B") == 0) {
		// do nothing
	} else if (strcmp(unit, "KB") == 0) {
		ret *= static_cast<uint64_t>(1e3);
	} else if (strcmp(unit, "KiB") == 0) {
		ret *= 1ull << 10;
	} else if (strcmp(unit, "MB") == 0) {
		ret *= static_cast<uint64_t>(1e6);
	} else if (strcmp(unit, "MiB") == 0) {
		ret *= 1ull << 20;
	} else if (strcmp(unit, "GB") == 0) {
		ret *= static_cast<uint64_t>(1e9);
	} else if (strcmp(unit, "GiB") == 0) {
		ret *= 1ull << 30;
	} else if (strcmp(unit, "TB") == 0) {
		ret *= static_cast<uint64_t>(1e12);
	} else if (strcmp(unit, "TiB") == 0) {
		ret *= 1ull << 40;
	} else {
		// unrecognized unit
		ret = 0;
	}
	return ret;
}

std::string joinPath(std::string const& directory, std::string const& filename) {
	auto d = directory;
	auto f = filename;
	while (f.size() && (f[0] == '/' || f[0] == CANONICAL_PATH_SEPARATOR))
		f = f.substr(1);
	while (d.size() && (d.back() == '/' || d.back() == CANONICAL_PATH_SEPARATOR))
		d = d.substr(0, d.size() - 1);
	return d + CANONICAL_PATH_SEPARATOR + f;
}

std::string cleanPath(std::string const& path) {
	std::vector<std::string> finalParts;
	bool absolute = !path.empty() && path[0] == CANONICAL_PATH_SEPARATOR;

	int i = 0;
	while (i < path.size()) {
		int sep = path.find((char)CANONICAL_PATH_SEPARATOR, i);
		if (sep == path.npos) {
			sep = path.size();
		}
		std::string part = path.substr(i, sep - i);
		i = sep + 1;
		if (part.size() == 0 || (part.size() == 1 && part[0] == '.'))
			continue;
		if (part == "..") {
			if (!finalParts.empty() && finalParts.back() != "..") {
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
		result.append(finalParts[i]);
	}

	return result.empty() ? "." : result;
}

// Removes the last component from a path string (if possible) and returns the result with one trailing separator.
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

std::string abspath(std::string const& path, bool resolveLinks) {
	if (path.empty()) {
		return "";
	}

	if (!resolveLinks) {
		// Treat paths starting with ~ or separator as absolute, meaning they shouldn't be appended to the current
		// working dir
		bool absolute = !path.empty() && (path[0] == CANONICAL_PATH_SEPARATOR || path[0] == '~');
		return cleanPath(absolute ? path : joinPath(abspath(".", true), path));
	}

	char result[PATH_MAX];
	// Must resolve links, so first try realpath on the whole thing
	const char* r = realpath(path.c_str(), result);
	if (r == nullptr) {
		// If the error was ENOENT and the path doesn't have to exist,
		// try to resolve symlinks in progressively shorter prefixes of the path
		if (errno == ENOENT && path != ".") {
			std::string prefix = popPath(path);
			std::string suffix = path.substr(prefix.size());
			if (prefix.empty() && (suffix.empty() || suffix[0] != '~')) {
				prefix = ".";
			}
			if (!prefix.empty()) {
				std::string abs = abspath(prefix, true);
				// An empty return from abspath() means there was an error.
				if (abs.empty())
					return abs;
				return cleanPath(joinPath(abs, suffix));
			}
		}
		log_err("realpath", errno, "Unable to get real path for %s", path.c_str());
		return "";
	}
	return std::string(r);
}

std::string parentDirectory(std::string const& path, bool resolveLinks) {
	return popPath(abspath(path, resolveLinks));
}

int mkdir(std::string const& directory) {
	size_t sep = 0;
	do {
		sep = directory.find_first_of('/', sep + 1);
		if (::mkdir(directory.substr(0, sep).c_str(), 0755) != 0) {
			if (errno == EEXIST)
				continue;

			return -1;
		}
	} while (sep != std::string::npos && sep != directory.length() - 1);

	return 0;
}

std::unordered_map<ProcessID, std::unique_ptr<Command>> id_command;
std::unordered_map<pid_t, ProcessID> pid_id;
std::unordered_map<ProcessID, pid_t> id_pid;

// Return resident memory in bytes for the given process, or 0 if error.
uint64_t getRss(ProcessID id) {
#ifndef __linux__
	// TODO: implement for non-linux
	return 0;
#else
	pid_t pid = id_pid[id];
	char stat_path[100];
	snprintf(stat_path, sizeof(stat_path), "/proc/%d/statm", pid);
	FILE* stat_file = fopen(stat_path, "r");
	if (stat_file == nullptr) {
		log_msg(SevWarn, "Unable to open stat file for %s\n", id.c_str());
		return 0;
	}
	long rss = 0;
	int ret = fscanf(stat_file, "%*s%ld", &rss);
	if (ret == 0) {
		log_msg(SevWarn, "Unable to parse rss size for %s\n", id.c_str());
		return 0;
	}
	fclose(stat_file);
	return static_cast<uint64_t>(rss) * sysconf(_SC_PAGESIZE);
#endif
}

void start_process(Command* cmd, ProcessID id, uid_t uid, gid_t gid, int delay, sigset_t* mask) {
	if (!cmd->argv)
		return;

	pid_t pid = fork();

	if (pid < 0) { /* fork error */
		cmd->last_start = timer();
		int fork_delay = cmd->get_and_update_current_restart_delay();
		cmd->fork_retry_time = cmd->last_start + fork_delay;
		log_err("fork",
		        errno,
		        "Unable to fork new %s process, restarting %s in %d seconds",
		        cmd->argv[0],
		        cmd->ssection.c_str(),
		        fork_delay);
		return;
	} else if (pid == 0) { /* we are the child */
		/* remove signal handlers from parent */
		signal(SIGHUP, SIG_DFL);
		signal(SIGINT, SIG_DFL);
		signal(SIGTERM, SIG_DFL);
#ifdef __linux__
		signal(SIGCHLD, SIG_DFL);
#endif
		sigprocmask(SIG_SETMASK, mask, nullptr);

		/* All output in this block should be to stdout (for SevInfo messages) or stderr (for SevError messages) */
		/* Using log_msg() or log_err() from the child will cause the logs to be written incorrectly */
		dup2(cmd->pipes[0][1], fileno(stdout));
		dup2(cmd->pipes[1][1], fileno(stderr));

		if (delay) {
			while ((delay = sleep(delay)) > 0) {
			}
		}

		if (!cmd->envvars.empty()) {
			size_t start = 0;
			do {
				const auto keyValueEnd = cmd->envvars.find(' ', start);
				const auto& keyValue = keyValueEnd != std::string::npos
				                           ? cmd->envvars.substr(start, keyValueEnd - start)
				                           : cmd->envvars.substr(start);
				if (!EnvVarUtils::keyValueValid(keyValue, cmd->envvars)) {
					exit(1);
				}
				const auto [key, value] = EnvVarUtils::extractKeyAndValue(keyValue);
				if (setenv(key.c_str(), value.c_str(), /* overwrite */ 1)) {
					fprintf(stderr, "setenv failed for %s in envvars\n", keyValue.c_str());
					exit(1);
				}
				start = keyValueEnd;
				if (start != std::string::npos) {
					while (cmd->envvars[start] == ' ') {
						start++;
					}
				}
			} while (start != std::string::npos && start < cmd->envvars.length());
		}

		if (!cmd->delete_envvars.empty()) {
			size_t start = 0;
			do {
				const size_t bound = cmd->delete_envvars.find(' ', start);
				const std::string& var = cmd->delete_envvars.substr(start, bound - start);
				fprintf(stdout, "Deleting parent environment variable: \'%s\'\n", var.c_str());
				fflush(stdout);
				if (unsetenv(var.c_str())) {
					fprintf(stderr,
					        "Unable to remove parent environment variable: %s (unsetenv error %d: %s)\n",
					        var.c_str(),
					        errno,
					        strerror(errno));
					exit(1);
				}
				start = bound;
				if (start != std::string::npos) {
					while (cmd->delete_envvars[start] == ' ') {
						start++;
					}
				}
			} while (start != std::string::npos && start < cmd->delete_envvars.length());
		}

#ifdef __linux__
		/* death of our parent raises SIGHUP */
		prctl(PR_SET_PDEATHSIG, SIGHUP);
		if (getppid() == 1) /* parent already died before prctl */
			exit(0);
#elif defined(__FreeBSD__)
		/* death of our parent raises SIGHUP */
		const int sig = SIGHUP;
		procctl(P_PID, 0, PROC_PDEATHSIG_CTL, (void*)&sig);
		if (getppid() == 1) /* parent already died before procctl */
			exit(0);
#endif

		if (getegid() != gid)
			if (setgid(gid) != 0) {
				fprintf(stderr, "Unable to set GID to %d (setgid error %d: %s)\n", gid, errno, strerror(errno));
				exit(1);
			}
		if (geteuid() != uid)
			if (setuid(uid) != 0) {
				fprintf(stderr, "Unable to set UID to %d (setuid error %d: %s)\n", uid, errno, strerror(errno));
				exit(1);
			}

#ifdef __linux__
		/* death of our parent raises SIGHUP */
		/* although not documented to this effect, setting uid/gid
		   appears to reset PDEATHSIG */
		prctl(PR_SET_PDEATHSIG, SIGHUP);
		if (getppid() == 1) /* parent already died before prctl */
			exit(0);
#endif

		if (!cmd->quiet) {
			fprintf(stdout, "Launching %s (%d) for %s\n", cmd->argv[0], getpid(), cmd->ssection.c_str());
			fflush(stdout);
		}

		execv(cmd->argv[0], (char* const*)cmd->argv);

		fprintf(stderr,
		        "Unable to launch %s for %s (execv error %d: %s)\n",
		        cmd->argv[0],
		        cmd->ssection.c_str(),
		        errno,
		        strerror(errno));
		_exit(0);
	}

	cmd->last_start = timer() + delay;
	cmd->fork_retry_time = -1;
	pid_id[pid] = id;
	id_pid[id] = pid;
}

void print_usage(const char* name) {
	printf("FoundationDB Process Monitor " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n"
	       "Usage: %s [OPTIONS]\n"
	       "\n"
	       "  --conffile CONFFILE\n"
	       "                 The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is\n"
	       "                 `/etc/foundationdb/foundationdb.conf'.\n"
	       "  --lockfile LOCKFILE\n"
	       "                 The path of the mutual exclusion file for this instance of\n"
	       "                 fdbmonitor. The default is `/var/run/fdbmonitor.pid'.\n"
	       "  --loggroup LOGGROUP\n"
	       "                 Sets the 'LogGroup' field with the specified value for all\n"
	       "                 entries in the log output. The default log group is 'default'.\n"
	       "  --daemonize    Background the fdbmonitor process.\n"
	       "  -h, --help     Display this help and exit.\n",
	       name);
}

bool argv_equal(const char** a1, const char** a2) {
	int i = 0;

	while (a1[i] && a2[i]) {
		if (strcmp(a1[i], a2[i]))
			return false;
		i++;
	}

	if (a1[i] != nullptr || a2[i] != nullptr)
		return false;
	return true;
}

void kill_process(ProcessID id, bool wait, bool cleanup) {
	pid_t pid = id_pid[id];

	log_msg(SevInfo, "Killing process %d\n", pid);

	kill(pid, SIGTERM);
	if (wait) {
		waitpid(pid, nullptr, 0);
	}

	if (cleanup) {
		pid_id.erase(pid);
		id_pid.erase(id);
	}
}

void load_conf(const char* confpath, uid_t& uid, gid_t& gid, sigset_t* mask, fdb_fd_set rfds, int* maxfd) {
	log_msg(SevInfo, "Loading configuration %s\n", confpath);

	CSimpleIniA ini;
	ini.SetUnicode();

	SI_Error err = ini.LoadFile(confpath);
	bool loadedConf = err >= 0;
	if (!loadedConf) {
		log_msg(SevError, "Unable to load configuration file %s (SI_Error: %d, errno: %d)\n", confpath, err, errno);
	}

	if (loadedConf) {
		uid_t _uid;
		gid_t _gid;

		const char* user = ini.GetValue("fdbmonitor", "user", nullptr);
		const char* group = ini.GetValue("fdbmonitor", "group", nullptr);

		if (user) {
			errno = 0;
			struct passwd* pw = getpwnam(user);
			if (!pw) {
				log_err("getpwnam", errno, "Unable to lookup user %s", user);
				return;
			}
			_uid = pw->pw_uid;
		} else
			_uid = geteuid();

		if (group) {
			errno = 0;
			struct group* gr = getgrnam(group);
			if (!gr) {
				log_err("getgrnam", errno, "Unable to lookup group %s", group);
				return;
			}
			_gid = gr->gr_gid;
		} else
			_gid = getegid();

		/* Any change to uid or gid requires the process to be restarted to take effect */
		if (uid != _uid || gid != _gid) {
			std::vector<ProcessID> kill_ids;
			for (auto i : id_pid) {
				if (id_command[i.first]->kill_on_configuration_change) {
					kill_ids.push_back(i.first);
				}
			}
			for (auto i : kill_ids) {
				kill_process(i);
				id_command.erase(i);
			}
		}

		uid = _uid;
		gid = _gid;
	}

	std::list<ProcessID> kill_ids;
	std::list<std::pair<ProcessID, Command*>> start_ids;

	for (auto i : id_pid) {
		if (!loadedConf || ini.GetSectionSize(id_command[i.first]->ssection.c_str()) == -1) {
			/* Process no longer configured; deconfigure it and kill it if required */
			log_msg(SevInfo, "Deconfigured %s\n", id_command[i.first]->ssection.c_str());

			id_command[i.first]->deconfigured = true;

			if (id_command[i.first]->kill_on_configuration_change) {
				kill_ids.push_back(i.first);
				id_command.erase(i.first);
			}
		} else {
			auto cmd = std::make_unique<Command>(ini, id_command[i.first]->section, i.first, rfds, maxfd);

			// If we just turned on 'kill_on_configuration_change', then kill the process to make sure we pick up any of
			// its pending config changes
			if (*(id_command[i.first]) != *cmd ||
			    (cmd->kill_on_configuration_change && !id_command[i.first]->kill_on_configuration_change)) {
				log_msg(SevInfo, "Found new configuration for %s\n", id_command[i.first]->ssection.c_str());
				auto* c = cmd.get();
				id_command[i.first] = std::move(cmd);

				if (c->kill_on_configuration_change) {
					kill_ids.push_back(i.first);
					start_ids.emplace_back(i.first, c);
				}
			} else {
				log_msg(SevInfo, "Updated configuration for %s\n", id_command[i.first]->ssection.c_str());
				id_command[i.first]->update(*cmd);
			}
		}
	}

	for (auto i : kill_ids)
		kill_process(i);

	for (auto i : start_ids) {
		start_process(i.second, i.first, uid, gid, 0, mask);
	}

	/* We've handled deconfigured sections, now look for newly
	   configured sections */
	if (loadedConf) {
		CSimpleIniA::TNamesDepend sections;
		ini.GetAllSections(sections);
		for (auto i : sections) {
			if (auto dot = strrchr(i.pItem, '.')) {
				ProcessID id = i.pItem;
				if (!id_pid.count(id)) {
					/* Found something we haven't yet started */
					Command* cmd;

					auto itr = id_command.find(id);
					if (itr != id_command.end()) {
						cmd = itr->second.get();
					} else {
						std::string section(i.pItem, dot - i.pItem);
						auto p = std::make_unique<Command>(ini, section, id, rfds, maxfd);
						cmd = p.get();
						id_command[id] = std::move(p);
					}

					if (cmd->fork_retry_time <= timer()) {
						log_msg(SevInfo, "Starting %s\n", i.pItem);
						start_process(cmd, id, uid, gid, 0, mask);
					}
				}
			}
		}
	}
}

/* cmd->pipes[pipe_idx] *must* be ready to read without blocking */
void read_child_output(Command* cmd, int pipe_idx, fdb_fd_set fds) {
	char buf[4096];

	int len = read(cmd->pipes[pipe_idx][0], buf, 4096);
	if (len == -1) {
		if (errno != EINTR) {
			/* We shouldn't get EAGAIN or EWOULDBLOCK
			   here, and if it's not EINTR then all of
			   the other alternatives seem "bad". */
			log_err("read", errno, "Error while reading from %s, no longer logging output", cmd->ssection.c_str());
			unmonitor_fd(fds, cmd->pipes[pipe_idx][0]);
		}
		return;
	}

	// pipe_idx == 0 is stdout, pipe_idx == 1 is stderr
	Severity priority = (pipe_idx == 0) ? SevInfo : SevError;

	int start = 0;
	for (int i = 0; i < len; i++) {
		if (buf[i] == '\n') {
			log_process_msg(priority, cmd->ssection.c_str(), "%.*s", i - start + 1, buf + start);
			start = i + 1;
		}
	}

	if (start < len) {
		log_process_msg(priority, cmd->ssection.c_str(), "%.*s\n", len - start, buf + start);
	}
}

#if defined(__APPLE__) || defined(__FreeBSD__)
void watch_conf_dir(int kq, int* confd_fd, std::string confdir) {
	struct kevent ev;
	std::string original = confdir;

	while (true) {
		/* If already watching, drop it and close */
		if (*confd_fd >= 0) {
			EV_SET(&ev, *confd_fd, EVFILT_VNODE, EV_DELETE, NOTE_WRITE, 0, nullptr);
			kevent(kq, &ev, 1, nullptr, 0, nullptr);
			close(*confd_fd);
		}

		confdir = original;
		std::string child = confdir;

		/* Find the nearest existing ancestor */
		while ((*confd_fd = open(confdir.c_str(), O_EVTONLY)) < 0 && errno == ENOENT) {
			child = confdir;
			confdir = parentDirectory(confdir, false);
		}

		if (*confd_fd >= 0) {
			EV_SET(&ev, *confd_fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_WRITE, 0, nullptr);
			kevent(kq, &ev, 1, nullptr, 0, nullptr);

			/* If our child appeared since we last tested it, start over from the beginning */
			if (confdir != child && (access(child.c_str(), F_OK) == 0 || errno != ENOENT)) {
				continue;
			}

			if (confdir != original) {
				log_msg(SevInfo, "Watching parent directory of missing directory %s\n", child.c_str());
			} else {
				log_msg(SevInfo, "Watching conf dir %s\n", confdir.c_str());
			}
		}

		return;
	}
}

void watch_conf_file(int kq, int* conff_fd, const char* confpath) {
	struct kevent ev;

	/* If already watching, drop it and close */
	if (*conff_fd >= 0) {
		EV_SET(&ev, *conff_fd, EVFILT_VNODE, EV_DELETE, NOTE_WRITE | NOTE_ATTRIB, 0, nullptr);
		kevent(kq, &ev, 1, nullptr, 0, nullptr);
		close(*conff_fd);
	}

	/* Open and watch */
	*conff_fd = open(confpath, O_EVTONLY);
	if (*conff_fd >= 0) {
		EV_SET(&ev, *conff_fd, EVFILT_VNODE, EV_ADD | EV_CLEAR, NOTE_WRITE | NOTE_ATTRIB, 0, nullptr);
		kevent(kq, &ev, 1, nullptr, 0, nullptr);
	}
}
#endif

#ifdef __linux__
int fdbmon_stat(const char* path, struct stat* path_stat, bool is_link) {
	return is_link ? lstat(path, path_stat) : stat(path, path_stat);
}

/* Sets watches to track changes to all symlinks on a path.
 * Also sets a watch on the last existing ancestor of a path if the full path doesn't exist. */
std::unordered_map<int, std::unordered_set<std::string>> set_watches(std::string path, int ifd) {
	std::unordered_map<int, std::unordered_set<std::string>> additional_watch_wds;
	struct stat path_stat;

	if (path.size() < 2)
		return additional_watch_wds;

	int idx = 1;
	bool exists = true;

	/* Check each level of the path, setting a watch on any symlinks.
	 * Stop checking once we get to a part of the path that doesn't exist.
	 * If we encounter a non-existing path, watch the closest existing ancestor. */
	while (idx != std::string::npos && exists) {
		idx = path.find_first_of('/', idx + 1);
		std::string subpath = path.substr(0, idx);

		int level = 0;
		while (true) {
			/* Check path existence */
			int result = fdbmon_stat(subpath.c_str(), &path_stat, true);
			if (result != 0) {
				if (errno == ENOENT) {
					exists = false;
				} else {
					log_err("lstat", errno, "Unable to stat %s", path.c_str());
					exit(1);
				}
			}

			if (exists) {
				/* Don't do anything for existing non-links */
				if (!S_ISLNK(path_stat.st_mode)) {
					break;
				} else if (level++ == 100) {
					log_msg(SevError, "Too many nested symlinks in path %s\n", path.c_str());
					exit(1);
				}
			}

			std::string parent = parentDirectory(subpath, false);

			/* Watch the parent directory of the current path for changes */
			int wd = inotify_add_watch(ifd, parent.c_str(), IN_CREATE | IN_MOVED_TO);
			if (wd < 0) {
				log_err("inotify_add_watch", errno, "Unable to add watch to parent directory %s", parent.c_str());
				exit(1);
			}

			if (exists) {
				log_msg(SevInfo, "Watching parent directory of symlink %s (%d)\n", subpath.c_str(), wd);
				additional_watch_wds[wd].insert(subpath.substr(parent.size()));
			} else {
				/* If the subpath has appeared since we set the watch, we should cancel it and resume traversing the
				 * path */
				int result = fdbmon_stat(subpath.c_str(), &path_stat, true);
				if (result == 0 || errno != ENOENT) {
					inotify_rm_watch(ifd, wd);
					continue;
				}

				log_msg(SevInfo, "Watching parent directory of missing directory %s (%d)\n", subpath.c_str(), wd);
				additional_watch_wds[wd].insert(subpath.substr(parent.size()));
				break;
			}

			/* Follow the symlink */
			char buf[PATH_MAX + 1];
			ssize_t len = readlink(subpath.c_str(), buf, PATH_MAX);
			if (len < 0) {
				log_err("readlink", errno, "Unable to follow symlink %s", subpath.c_str());
				exit(1);
			}

			buf[len] = '\0';
			if (buf[0] == '/') {
				subpath = buf;
			} else {
				subpath = joinPath(parent, buf);
			}
		}
	}

	return additional_watch_wds;
}
#endif

} // namespace fdbmonitor
