/*
 * fdbmonitor.cpp
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

#include <signal.h>
#include <stdio.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <random>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include <sys/wait.h>

#ifdef __linux__
#include <sys/inotify.h>
#include <time.h>
#include <linux/limits.h>
#endif

#ifdef __FreeBSD__
#include <sys/event.h>
#define O_EVTONLY O_RDONLY
#endif

#ifdef __APPLE__
#include <sys/event.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif

#include <sys/time.h>
#include <stdlib.h>

#include <cinttypes>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <sstream>
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

#include "flow/SimpleOpt.h"

#include "fdbclient/SimpleIni.h"
#include "fdbclient/versions.h"

#ifdef __linux__
typedef fd_set* fdb_fd_set;
#elif defined(__APPLE__) || defined(__FreeBSD__)
typedef int fdb_fd_set;
#endif

#define CANONICAL_PATH_SEPARATOR '/'

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

double get_cur_timestamp() {
	struct tm tm_info;
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	localtime_r(&tv.tv_sec, &tm_info);

	return tv.tv_sec + 1e-6 * tv.tv_usec;
}

enum Severity { SevDebug = 5, SevInfo = 10, SevWarn = 20, SevWarnAlways = 30, SevError = 40 };
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

typedef std::string ProcessID;

bool daemonize = false;
std::string logGroup = "default";

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

int randomInt(int min, int max) {
	static std::random_device rd;
	static std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(min, max);

	return dis(gen);
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

std::string abspath(std::string const& path, bool resolveLinks = true) {
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

std::string parentDirectory(std::string const& path, bool resolveLinks = true) {
	return popPath(abspath(path, resolveLinks));
}

int mkdir(std::string const& directory) {
	size_t sep = 0;
	do {
		sep = directory.find_first_of('/', sep + 1);
		if (mkdir(directory.substr(0, sep).c_str(), 0755) != 0) {
			if (errno == EEXIST)
				continue;

			return -1;
		}
	} while (sep != std::string::npos && sep != directory.length() - 1);

	return 0;
}

struct Command {
private:
	std::vector<std::string> commands;
	fdb_fd_set fds;

public:
	char** argv;
	std::string section, ssection;
	uint32_t initial_restart_delay;
	uint32_t max_restart_delay;
	double current_restart_delay;
	double restart_backoff;
	uint32_t restart_delay_reset_interval;
	double last_start;
	double fork_retry_time;
	bool quiet;
	const char* delete_envvars;
	bool deconfigured;
	bool kill_on_configuration_change;

	// one pair for each of stdout and stderr
	int pipes[2][2];

	Command() : argv(nullptr) {}
	Command(const CSimpleIni& ini, std::string _section, ProcessID id, fdb_fd_set fds, int* maxfd)
	  : fds(fds), argv(nullptr), section(_section), fork_retry_time(-1), quiet(false), delete_envvars(nullptr),
	    deconfigured(false), kill_on_configuration_change(true) {
		char _ssection[strlen(section.c_str()) + 22];
		snprintf(_ssection, strlen(section.c_str()) + 22, "%s", id.c_str());
		ssection = _ssection;

		for (auto p : pipes) {
			if ((pipe(p) == 0)) {
				monitor_fd(fds, p[0], maxfd, this);
			} else {
				log_err("pipe", errno, "Unable to construct pipe for %s", ssection.c_str());
				p[0] = -1;
				p[1] = -1;
			}
		}

		CSimpleIniA::TNamesDepend keys, skeys, gkeys;

		ini.GetAllKeys(section.c_str(), keys);
		ini.GetAllKeys(ssection.c_str(), skeys);
		ini.GetAllKeys("general", gkeys);

		keys.splice(keys.end(), skeys, skeys.begin(), skeys.end());
		keys.splice(keys.end(), gkeys, gkeys.begin(), gkeys.end());
		keys.sort(CSimpleIniA::Entry::KeyOrder());
		keys.unique([](const CSimpleIniA::Entry& lhs, const CSimpleIniA::Entry& rhs) -> bool {
			return !CSimpleIniA::Entry::KeyOrder()(lhs, rhs);
		});

		last_start = 0;

		char* endptr;
		const char* rd =
		    get_value_multi(ini, "restart-delay", ssection.c_str(), section.c_str(), "general", "fdbmonitor", nullptr);
		if (!rd) {
			log_msg(SevError, "Unable to resolve restart delay for %s\n", ssection.c_str());
			return;
		} else {
			max_restart_delay = strtoul(rd, &endptr, 10);
			if (*endptr != '\0') {
				log_msg(SevError, "Unable to parse restart delay for %s\n", ssection.c_str());
				return;
			}
		}

		const char* mrd = get_value_multi(
		    ini, "initial-restart-delay", ssection.c_str(), section.c_str(), "general", "fdbmonitor", nullptr);
		if (!mrd) {
			initial_restart_delay = 0;
		} else {
			initial_restart_delay = std::min<uint32_t>(max_restart_delay, strtoul(mrd, &endptr, 10));
			if (*endptr != '\0') {
				log_msg(SevError, "Unable to parse initial restart delay for %s\n", ssection.c_str());
				return;
			}
		}

		current_restart_delay = initial_restart_delay;

		const char* rbo = get_value_multi(
		    ini, "restart-backoff", ssection.c_str(), section.c_str(), "general", "fdbmonitor", nullptr);
		if (!rbo) {
			restart_backoff = max_restart_delay;
		} else {
			restart_backoff = strtod(rbo, &endptr);
			if (*endptr != '\0') {
				log_msg(SevError, "Unable to parse restart backoff for %s\n", ssection.c_str());
				return;
			}
			if (restart_backoff < 1.0) {
				log_msg(SevError, "Invalid restart backoff value %lf for %s\n", restart_backoff, ssection.c_str());
				return;
			}
		}

		const char* rdri = get_value_multi(
		    ini, "restart-delay-reset-interval", ssection.c_str(), section.c_str(), "general", "fdbmonitor", nullptr);
		if (!rdri) {
			restart_delay_reset_interval = max_restart_delay;
		} else {
			restart_delay_reset_interval = strtoul(rdri, &endptr, 10);
			if (*endptr != '\0') {
				log_msg(SevError, "Unable to parse restart delay reset interval for %s\n", ssection.c_str());
				return;
			}
		}

		const char* q =
		    get_value_multi(ini, "disable-lifecycle-logging", ssection.c_str(), section.c_str(), "general", nullptr);
		if (q && !strcmp(q, "true"))
			quiet = true;

		const char* del_env =
		    get_value_multi(ini, "delete-envvars", ssection.c_str(), section.c_str(), "general", nullptr);
		delete_envvars = del_env;

		const char* kocc =
		    get_value_multi(ini, "kill-on-configuration-change", ssection.c_str(), section.c_str(), "general", nullptr);
		if (kocc && strcmp(kocc, "true")) {
			kill_on_configuration_change = false;
		}

		const char* binary = get_value_multi(ini, "command", ssection.c_str(), section.c_str(), "general", nullptr);
		if (!binary) {
			log_msg(SevError, "Unable to resolve command for %s\n", ssection.c_str());
			return;
		}
		std::stringstream ss(binary);
		std::copy(std::istream_iterator<std::string>(ss),
		          std::istream_iterator<std::string>(),
		          std::back_inserter<std::vector<std::string>>(commands));

		const char* id_s = ssection.c_str() + strlen(section.c_str()) + 1;

		for (auto i : keys) {
			if (isParameterNameEqual(i.pItem, "command") || isParameterNameEqual(i.pItem, "restart-delay") ||
			    isParameterNameEqual(i.pItem, "initial-restart-delay") ||
			    isParameterNameEqual(i.pItem, "restart-backoff") ||
			    isParameterNameEqual(i.pItem, "restart-delay-reset-interval") ||
			    isParameterNameEqual(i.pItem, "disable-lifecycle-logging") ||
			    isParameterNameEqual(i.pItem, "delete-envvars") ||
			    isParameterNameEqual(i.pItem, "kill-on-configuration-change")) {
				continue;
			}

			std::string opt = get_value_multi(ini, i.pItem, ssection.c_str(), section.c_str(), "general", nullptr);

			std::size_t pos = 0;

			while ((pos = opt.find("$ID", pos)) != opt.npos)
				opt.replace(pos, 3, id_s, strlen(id_s));

			const char* flagName = i.pItem + 5;
			if ((strncmp("flag_", i.pItem, 5) == 0 || strncmp("flag-", i.pItem, 5) == 0) && strlen(flagName) > 0) {
				if (opt == "true")
					commands.push_back(std::string("--") + flagName);
				else if (opt != "false") {
					log_msg(SevError,
					        "Bad flag value, must be true/false.  Flag: '%s'  Value: '%s'\n",
					        flagName,
					        opt.c_str());
					return;
				}
			} else
				commands.push_back(std::string("--").append(i.pItem).append("=").append(opt));
		}

		argv = new char*[commands.size() + 1];
		int i = 0;
		for (auto itr : commands) {
			argv[i++] = strdup(itr.c_str());
		}
		argv[i] = nullptr;
	}
	~Command() {
		for (int i = 0; i < commands.size(); ++i) {
			free(argv[i]);
		}
		delete[] argv;
		for (auto p : pipes) {
			if (p[0] >= 0 && p[1] >= 0) {
				unmonitor_fd(fds, p[0]);
				close(p[0]);
				close(p[1]);
			}
		}
	}
	void update(const Command& other) {
		quiet = other.quiet;
		delete_envvars = other.delete_envvars;
		initial_restart_delay = other.initial_restart_delay;
		max_restart_delay = other.max_restart_delay;
		restart_backoff = other.restart_backoff;
		restart_delay_reset_interval = other.restart_delay_reset_interval;
		deconfigured = other.deconfigured;
		kill_on_configuration_change = other.kill_on_configuration_change;

		current_restart_delay = std::min<double>(max_restart_delay, current_restart_delay);
		current_restart_delay = std::max<double>(initial_restart_delay, current_restart_delay);
	}
	bool operator!=(const Command& rhs) {
		if (rhs.commands.size() != commands.size())
			return true;

		for (size_t i = 0; i < commands.size(); i++) {
			if (commands[i].compare(rhs.commands[i]) != 0)
				return true;
		}

		return false;
	}

	int get_and_update_current_restart_delay() {
		if (timer() - last_start >= restart_delay_reset_interval) {
			current_restart_delay = initial_restart_delay;
		}

		int jitter = randomInt(floor(-0.1 * current_restart_delay), ceil(0.1 * current_restart_delay));
		int delay = std::max<int>(0, round(current_restart_delay) + jitter);
		current_restart_delay =
		    std::min<double>(max_restart_delay, restart_backoff * std::max(1.0, current_restart_delay));
		return delay;
	}
};

std::unordered_map<ProcessID, std::unique_ptr<Command>> id_command;
std::unordered_map<pid_t, ProcessID> pid_id;
std::unordered_map<ProcessID, pid_t> id_pid;

enum { OPT_CONFFILE, OPT_LOCKFILE, OPT_LOGGROUP, OPT_DAEMONIZE, OPT_HELP };

CSimpleOpt::SOption g_rgOptions[] = { { OPT_CONFFILE, "--conffile", SO_REQ_SEP },
	                                  { OPT_LOCKFILE, "--lockfile", SO_REQ_SEP },
	                                  { OPT_LOGGROUP, "--loggroup", SO_REQ_SEP },
	                                  { OPT_DAEMONIZE, "--daemonize", SO_NONE },
	                                  { OPT_HELP, "-?", SO_NONE },
	                                  { OPT_HELP, "-h", SO_NONE },
	                                  { OPT_HELP, "--help", SO_NONE },
	                                  SO_END_OF_OPTIONS };

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

		/* All output in this block should be to stdout (for SevInfo messages) or stderr (for SevError messages) */
		/* Using log_msg() or log_err() from the child will cause the logs to be written incorrectly */
		dup2(cmd->pipes[0][1], fileno(stdout));
		dup2(cmd->pipes[1][1], fileno(stderr));

		if (cmd->delete_envvars != nullptr && std::strlen(cmd->delete_envvars) > 0) {
			std::string vars(cmd->delete_envvars);
			size_t start = 0;
			do {
				size_t bound = vars.find(" ", start);
				std::string var = vars.substr(start, bound - start);
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
				while (vars[start] == ' ')
					start++;
			} while (start <= vars.length());
		}

#ifdef __linux__
		signal(SIGCHLD, SIG_DFL);

		sigprocmask(SIG_SETMASK, mask, nullptr);

		/* death of our parent raises SIGHUP */
		prctl(PR_SET_PDEATHSIG, SIGHUP);
		if (getppid() == 1) /* parent already died before prctl */
			exit(0);
#endif

		if (delay)
			while ((delay = sleep(delay)) > 0) {
			}

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

volatile int exit_signal = 0;

#ifdef __linux__
void signal_handler(int sig) {
	if (sig > exit_signal)
		exit_signal = sig;
}
#endif

volatile bool child_exited = false;

#ifdef __linux__
void child_handler(int sig) {
	child_exited = true;
}
#endif

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

void kill_process(ProcessID id, bool wait = true) {
	pid_t pid = id_pid[id];

	log_msg(SevInfo, "Killing process %d\n", pid);

	kill(pid, SIGTERM);
	if (wait) {
		waitpid(pid, nullptr, 0);
	}

	pid_id.erase(pid);
	id_pid.erase(id);
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

int testPathFunction(const char* name, std::function<std::string(std::string)> fun, std::string a, std::string b) {
	std::string o = fun(a);
	bool r = b == o;
	printf("%s: %s(%s) = %s expected %s\n", r ? "PASS" : "FAIL", name, a.c_str(), o.c_str(), b.c_str());
	return r ? 0 : 1;
}

int testPathFunction2(const char* name,
                      std::function<std::string(std::string, bool)> fun,
                      std::string a,
                      bool x,
                      std::string b) {
	std::string o = fun(a, x);
	bool r = b == o;
	printf("%s: %s(%s, %d) => %s expected %s\n", r ? "PASS" : "FAIL", name, a.c_str(), x, o.c_str(), b.c_str());
	return r ? 0 : 1;
}

void testPathOps() {
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

	errors += testPathFunction("cleanPath", cleanPath, "/", "/");
	errors += testPathFunction("cleanPath", cleanPath, "..", "..");
	errors += testPathFunction("cleanPath", cleanPath, "../.././", "../..");
	errors += testPathFunction("cleanPath", cleanPath, "///.///", "/");
	errors += testPathFunction("cleanPath", cleanPath, "/a/b/.././../c/./././////./d/..//", "/c");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//", "c");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//..", ".");
	errors += testPathFunction("cleanPath", cleanPath, "a/b/.././../c/./././////./d/..//../..", "..");
	errors += testPathFunction("cleanPath", cleanPath, "../a/b/..//", "../a");
	errors += testPathFunction("cleanPath", cleanPath, "/..", "/");
	errors += testPathFunction("cleanPath", cleanPath, "/../foo/bar///", "/foo/bar");
	errors += testPathFunction("cleanPath", cleanPath, "/a/b/../.././../", "/");
	errors += testPathFunction("cleanPath", cleanPath, ".", ".");

	mkdir("simfdb/backups/one/two/three");
	std::string cwd = abspath(".", true);

	// Create some symlinks and test resolution (or non-resolution) of them
	[[maybe_unused]] int rc;
	// Ignoring return codes, if symlinks fail tests below will fail
	rc = unlink("simfdb/backups/four");
	rc = unlink("simfdb/backups/five");
	rc = symlink("one/two", "simfdb/backups/four") == 0 ? 0 : 1;
	rc = symlink("../backups/four", "simfdb/backups/five") ? 0 : 1;

	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../two", true, joinPath(cwd, "simfdb/backups/one/two"));
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../three", true, joinPath(cwd, "simfdb/backups/one/three"));
	errors += testPathFunction2(
	    "abspath", abspath, "simfdb/backups/five/../three/../four", true, joinPath(cwd, "simfdb/backups/one/four"));

	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../two", true, joinPath(cwd, "simfdb/backups/one/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../three", true, joinPath(cwd, "simfdb/backups/one/"));

	errors += testPathFunction2("abspath", abspath, "/", false, "/");
	errors += testPathFunction2("abspath", abspath, "/foo//bar//baz/.././", false, "/foo/bar");
	errors += testPathFunction2("abspath", abspath, "/", true, "/");
	errors += testPathFunction2("abspath", abspath, "", true, "");
	errors += testPathFunction2("abspath", abspath, ".", true, cwd);
	errors += testPathFunction2("abspath", abspath, "/a", true, "/a");
	errors += testPathFunction2("abspath", abspath, "one/two/three/four", false, joinPath(cwd, "one/two/three/four"));
	errors += testPathFunction2("abspath", abspath, "one/two/three/./four", false, joinPath(cwd, "one/two/three/four"));
	errors += testPathFunction2("abspath", abspath, "one/two/three/./four/..", false, joinPath(cwd, "one/two/three"));
	errors +=
	    testPathFunction2("abspath", abspath, "one/./two/../three/./four", false, joinPath(cwd, "one/three/four"));
	errors +=
	    testPathFunction2("abspath", abspath, "simfdb/backups/four/../two", false, joinPath(cwd, "simfdb/backups/two"));
	errors +=
	    testPathFunction2("abspath", abspath, "simfdb/backups/five/../two", false, joinPath(cwd, "simfdb/backups/two"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", false, joinPath(cwd, "foo2/bar"));
	errors += testPathFunction2("abspath", abspath, "foo/./../foo2/./bar//", true, joinPath(cwd, "foo2/bar"));

	errors += testPathFunction2("parentDirectory", parentDirectory, "", true, "");
	errors += testPathFunction2("parentDirectory", parentDirectory, "/", true, "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, "/a", true, "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, ".", false, cleanPath(joinPath(cwd, "..")) + "/");
	errors += testPathFunction2("parentDirectory", parentDirectory, "./foo", false, cleanPath(cwd) + "/");
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/four", false, joinPath(cwd, "one/two/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/./four", false, joinPath(cwd, "one/two/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/two/three/./four/..", false, joinPath(cwd, "one/two/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "one/./two/../three/./four", false, joinPath(cwd, "one/three/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/four/../two", false, joinPath(cwd, "simfdb/backups/"));
	errors += testPathFunction2(
	    "parentDirectory", parentDirectory, "simfdb/backups/five/../two", false, joinPath(cwd, "simfdb/backups/"));
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "foo/./../foo2/./bar//", false, joinPath(cwd, "foo2/"));
	errors +=
	    testPathFunction2("parentDirectory", parentDirectory, "foo/./../foo2/./bar//", true, joinPath(cwd, "foo2/"));

	printf("%d errors.\n", errors);
	if (errors)
		exit(-1);
}

int main(int argc, char** argv) {
	// testPathOps(); return -1;

	std::string lockfile = "/var/run/fdbmonitor.pid";
#ifdef __FreeBSD__
	std::string _confpath = "/usr/local/etc/foundationdb/foundationdb.conf";
#else
	std::string _confpath = "/etc/foundationdb/foundationdb.conf";
#endif

	std::vector<const char*> additional_watch_paths;

	CSimpleOpt args(argc, argv, g_rgOptions, SO_O_NOERR | SO_O_HYPHEN_TO_UNDERSCORE);

	while (args.Next()) {
		if (args.LastError() == SO_SUCCESS) {
			switch (args.OptionId()) {
			case OPT_CONFFILE:
				_confpath = args.OptionArg();
				break;
			case OPT_LOCKFILE:
				lockfile = args.OptionArg();
				break;
			case OPT_LOGGROUP:
				if (strchr(args.OptionArg(), '"') != nullptr) {
					log_msg(SevError, "Invalid log group '%s', cannot contain '\"'\n", args.OptionArg());
					exit(1);
				}
				logGroup = args.OptionArg();
				break;
			case OPT_DAEMONIZE:
				daemonize = true;
				break;
			case OPT_HELP:
				print_usage(argv[0]);
				exit(0);
			}
		} else {
			print_usage(argv[0]);
			exit(1);
		}
	}

	log_msg(SevInfo, "Started FoundationDB Process Monitor " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");

	// Modify _confpath to be absolute for further traversals
	if (!_confpath.empty() && _confpath[0] != '/') {
		char buf[PATH_MAX];
		if (!getcwd(buf, PATH_MAX)) {
			log_err("getcwd", errno, "Unable to get cwd");
			exit(1);
		}

		_confpath = joinPath(buf, _confpath);
	}

	// Guaranteed (if non-nullptr) to be an absolute path with no
	// symbolic link, /./ or /../ components
	char* p = realpath(_confpath.c_str(), nullptr);
	if (!p) {
		log_msg(SevError, "No configuration file at %s\n", _confpath.c_str());
		exit(1);
	}

	std::string confpath = p;
	free(p);

	// Will always succeed given an absolute path
	std::string confdir = parentDirectory(confpath, false);
	std::string conffile = confpath.substr(confdir.size());

#ifdef __linux__
	// Setup inotify
	int ifd = inotify_init();
	if (ifd < 0) {
		log_err("inotify_init", errno, "Unable to initialize inotify");
		exit(1);
	}

	int conffile_wd = -1;
	int confdir_wd = -1;
	std::unordered_map<int, std::unordered_set<std::string>> additional_watch_wds;

	bool reload_additional_watches = true;
#endif

	/* fds we're blocking on via pselect or kevent */
	fdb_fd_set watched_fds;
	/* only linux needs this, but... */
	int maxfd = 0;

#ifdef __linux__
	fd_set rfds;
	watched_fds = &rfds;

	FD_ZERO(&rfds);
	FD_SET(ifd, &rfds);
	maxfd = ifd;

	int nfds = 0;
	fd_set srfds;
#endif

	if (daemonize) {
#if defined(__APPLE__) || defined(__FreeBSD__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#endif
		if (daemon(0, 0)) {
#if defined(__APPLE__) || defined(__FreeBSD__)
#pragma GCC diagnostic pop
#endif
			log_err("daemon", errno, "Unable to daemonize");
			exit(1);
		}

		/* open syslog connection immediately, to be inherited by
		   forked children */
		openlog("fdbmonitor", LOG_PID | LOG_NDELAY, LOG_DAEMON);

		signal(SIGTSTP, SIG_IGN);
		signal(SIGTTOU, SIG_IGN);
		signal(SIGTTIN, SIG_IGN);

		/* new process group, no controlling terminal */
		/* unchecked since the only failure indicates we're already a
		 process group leader */
		setsid();
	}

	/* open and lock our lockfile for mutual exclusion */
	std::string lockfileDir = parentDirectory(lockfile, true);
	if (lockfileDir.size() == 0) {
		log_msg(SevError, "Unable to determine parent directory of lockfile %s\n", lockfile.c_str());
		exit(1);
	}

	if (mkdir(lockfileDir) < 0) {
		log_err("mkdir", errno, "Unable to create parent directory for lockfile %s", lockfile.c_str());
		exit(1);
	}

	int lockfile_fd = open(lockfile.c_str(), O_RDWR | O_CREAT, 0640);
	if (lockfile_fd < 0) {
		log_err("open", errno, "Unable to open fdbmonitor lockfile %s", lockfile.c_str());
		exit(1);
	}
	if (lockf(lockfile_fd, F_LOCK, 0) < 0) {
		log_err(
		    "lockf", errno, "Unable to lock fdbmonitor lockfile %s (is fdbmonitor already running?)", lockfile.c_str());
		exit(0);
	}

	if (chdir("/") < 0) {
		log_err("chdir", errno, "Unable to change working directory");
		exit(1);
	}

	/* write our pid to the lockfile for convenience */
	char pid_buf[16];
	snprintf(pid_buf, sizeof(pid_buf), "%d\n", getpid());
	auto ign = write(lockfile_fd, pid_buf, strlen(pid_buf));
	(void)ign;

#ifdef __linux__
	/* attempt to do clean shutdown and remove lockfile when killed */
	signal(SIGHUP, signal_handler);
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);
#elif defined(__APPLE__) || defined(__FreeBSD__)
	int kq = kqueue();
	if (kq < 0) {
		log_err("kqueue", errno, "Unable to create kqueue");
		exit(1);
	}
	watched_fds = kq;

	signal(SIGHUP, SIG_IGN);
	signal(SIGINT, SIG_IGN);
	signal(SIGTERM, SIG_IGN);

	struct kevent ev;

	EV_SET(&ev, SIGHUP, EVFILT_SIGNAL, EV_ADD, 0, 0, nullptr);
	kevent(kq, &ev, 1, nullptr, 0, nullptr);
	EV_SET(&ev, SIGINT, EVFILT_SIGNAL, EV_ADD, 0, 0, nullptr);
	kevent(kq, &ev, 1, nullptr, 0, nullptr);
	EV_SET(&ev, SIGTERM, EVFILT_SIGNAL, EV_ADD, 0, 0, nullptr);
	kevent(kq, &ev, 1, nullptr, 0, nullptr);
	EV_SET(&ev, SIGCHLD, EVFILT_SIGNAL, EV_ADD, 0, 0, nullptr);
	kevent(kq, &ev, 1, nullptr, 0, nullptr);

	int confd_fd = -1;
	int conff_fd = -1;

	// Watch the directory holding the configuration file
	watch_conf_dir(kq, &confd_fd, confdir);

#endif

#ifdef __linux__
	signal(SIGCHLD, child_handler);
#endif

	uid_t uid = 0;
	gid_t gid = 0;

	sigset_t normal_mask, full_mask;
	sigfillset(&full_mask);

#ifdef __linux__
	/* normal will be restored in our main loop in the call to
	   pselect, but none blocks all signals while processing events */
	sigprocmask(SIG_SETMASK, &full_mask, &normal_mask);
#elif defined(__APPLE__) || defined(__FreeBSD__)
	sigprocmask(0, nullptr, &normal_mask);
#endif

#if defined(__APPLE__) || defined(__FreeBSD__)
	struct stat st_buf;
	struct timespec mtimespec;

	if (stat(confpath.c_str(), &st_buf) < 0) {
		log_err("stat", errno, "Unable to stat configuration file %s", confpath.c_str());
	}

	memcpy(&mtimespec, &(st_buf.st_mtimespec), sizeof(struct timespec));
#endif

	bool reload = true;
	while (1) {
		if (reload) {
			reload = false;
#ifdef __linux__
			/* Remove existing watches on conf file and directory */
			if (confdir_wd >= 0 && inotify_rm_watch(ifd, confdir_wd) < 0) {
				log_msg(SevInfo, "Could not remove inotify conf dir watch, continuing...\n");
			}
			if (conffile_wd >= 0 && inotify_rm_watch(ifd, conffile_wd) < 0) {
				log_msg(SevInfo, "Could not remove inotify conf file watch, continuing...\n");
			}

			/* Create new watches */
			conffile_wd = inotify_add_watch(ifd, confpath.c_str(), IN_CLOSE_WRITE);
			if (conffile_wd < 0) {
				if (errno != ENOENT) {
					log_err(
					    "inotify_add_watch", errno, "Unable to set watch on configuration file %s", confpath.c_str());
					exit(1);
				} else {
					log_msg(SevInfo, "Conf file has been deleted %s\n", confpath.c_str());
				}
			} else {
				log_msg(SevInfo, "Watching conf file %s\n", confpath.c_str());
			}

			confdir_wd = inotify_add_watch(ifd, confdir.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO);
			if (confdir_wd < 0) {
				if (errno != ENOENT) {
					log_err("inotify_add_watch",
					        errno,
					        "Unable to set watch on configuration file parent directory %s",
					        confdir.c_str());
					exit(1);
				} else {
					reload_additional_watches = true;
					log_msg(SevInfo, "Conf dir has been deleted %s\n", confdir.c_str());
				}
			} else {
				log_msg(SevInfo, "Watching conf dir %s (%d)\n", confdir.c_str(), confdir_wd);
			}

			/* Reload watches on symlinks and/or the oldest existing ancestor */
			if (reload_additional_watches) {
				additional_watch_wds = set_watches(_confpath, ifd);
			}

			load_conf(confpath.c_str(), uid, gid, &normal_mask, &rfds, &maxfd);
			reload_additional_watches = false;
#elif defined(__APPLE__) || defined(__FreeBSD__)
			load_conf(confpath.c_str(), uid, gid, &normal_mask, watched_fds, &maxfd);
			watch_conf_file(kq, &conff_fd, confpath.c_str());
			watch_conf_dir(kq, &confd_fd, confdir);
#endif
		}

		double end_time = std::numeric_limits<double>::max();
		for (auto& i : id_command) {
			if (i.second->fork_retry_time >= 0) {
				end_time = std::min(i.second->fork_retry_time, end_time);
			}
		}
		struct timespec tv;
		double timeout = -1;
		if (end_time < std::numeric_limits<double>::max()) {
			timeout = std::max(0.0, end_time - timer());
			if (timeout > 0) {
				tv.tv_sec = timeout;
				tv.tv_nsec = 1e9 * (timeout - tv.tv_sec);
			}
		}

#ifdef __linux__
		/* Block until something interesting happens (while atomically
		   unblocking signals) */
		srfds = rfds;
		nfds = 0;
		if (timeout < 0) {
			nfds = pselect(maxfd + 1, &srfds, nullptr, nullptr, nullptr, &normal_mask);
		} else if (timeout > 0) {
			nfds = pselect(maxfd + 1, &srfds, nullptr, nullptr, &tv, &normal_mask);
		}

		if (nfds == 0) {
			reload = true;
		}
#elif defined(__APPLE__) || defined(__FreeBSD__)
		int nev = 0;
		if (timeout < 0) {
			nev = kevent(kq, nullptr, 0, &ev, 1, nullptr);
		} else if (timeout > 0) {
			nev = kevent(kq, nullptr, 0, &ev, 1, &tv);
		}

		if (nev == 0) {
			reload = true;
		}

		if (nev > 0) {
			switch (ev.filter) {
			case EVFILT_VNODE:
				struct kevent timeout;
				// This could be the conf dir or conf file
				if (ev.ident == confd_fd) {
					/* Changes in the directory holding the conf file; schedule a future timeout to reset watches and
					 * reload the conf */
					EV_SET(&timeout, 1, EVFILT_TIMER, EV_ADD | EV_ONESHOT, 0, 200, nullptr);
					kevent(kq, &timeout, 1, nullptr, 0, nullptr);
				} else {
					/* Direct writes to the conf file; reload! */
					reload = true;
				}
				break;
			case EVFILT_TIMER:
				reload = true;
				break;
			case EVFILT_SIGNAL:
				switch (ev.ident) {
				case SIGHUP:
				case SIGINT:
				case SIGTERM:
					exit_signal = ev.ident;
					break;
				case SIGCHLD:
					child_exited = true;
					break;
				default:
					break;
				}
				break;
			case EVFILT_READ:
				Command* cmd = (Command*)ev.udata;
				for (int i = 0; i < 2; i++) {
					if (ev.ident == cmd->pipes[i][0]) {
						read_child_output(cmd, i, watched_fds);
					}
				}
				break;
			}
		} else {
			reload = true;
		}
#endif

		/* select() could have returned because received an exit signal */
		if (exit_signal > 0) {
			switch (exit_signal) {
			case SIGHUP:
				for (auto& i : id_command) {
					i.second->current_restart_delay = i.second->initial_restart_delay;
					i.second->fork_retry_time = -1;
				}
				reload = true;
				log_msg(SevInfo,
				        "Received signal %d (%s), resetting timeouts and reloading configuration\n",
				        exit_signal,
				        strsignal(exit_signal));
				break;
			case SIGINT:
			case SIGTERM:
				log_msg(SevWarn, "Received signal %d (%s), shutting down\n", exit_signal, strsignal(exit_signal));

				/* Unblock signals */
				signal(SIGCHLD, SIG_IGN);
				sigprocmask(SIG_SETMASK, &normal_mask, nullptr);

				/* If daemonized, setsid() was called earlier so we can just kill our entire new process group */
				if (daemonize) {
					kill(0, SIGHUP);
				} else {
					/* Otherwise kill each process individually but don't wait on them yet */
					auto i = id_pid.begin();
					auto iEnd = id_pid.end();
					while (i != iEnd) {
						// Must advance i before calling kill_process() which erases the entry at i
						kill_process((i++)->first, false);
					}
				}

				/* Wait for all child processes (says POSIX.1-2001) */
				/* POSIX.1-2001 specifies that if the disposition of SIGCHLD is set to SIG_IGN, then children that
				   terminate do not become zombies and a call to wait() will block until all children have terminated,
				   and then fail with errno set to ECHILD */
				wait(nullptr);

				unlink(lockfile.c_str());
				exit(0);
			default:
				break;
			}
			exit_signal = 0;
		}

#ifdef __linux__
		/* select() could have returned because we have a fd ready to
		   read (child output or inotify on conf file) */
		if (nfds > 0) {
			int len, i = 0;

			char buf[4096];

			for (auto& itr : id_command) {
				for (int i = 0; i < 2; i++) {
					if (FD_ISSET((itr.second)->pipes[i][0], &srfds)) {
						read_child_output(itr.second.get(), i, watched_fds);
					}
				}
			}

			if (FD_ISSET(ifd, &srfds)) {
				len = read(ifd, buf, 4096);
				if (len < 0)
					log_err("read", errno, "Error reading inotify message");

				while (i < len) {
					struct inotify_event* event = (struct inotify_event*)&buf[i];

					auto search = additional_watch_wds.find(event->wd);
					if (event->wd != conffile_wd) {
						if (search != additional_watch_wds.end() && event->len && search->second.count(event->name)) {
							log_msg(SevInfo,
							        "Changes detected on watched symlink `%s': (%d, %#010x)\n",
							        event->name,
							        event->wd,
							        event->mask);

							char* redone_confpath = realpath(_confpath.c_str(), nullptr);
							if (!redone_confpath) {
								log_msg(SevInfo, "Error calling realpath on `%s', continuing...\n", _confpath.c_str());
								// exit(1);
								i += sizeof(struct inotify_event) + event->len;
								continue;
							}

							confpath = redone_confpath;

							// Will always succeed given an absolute path
							confdir = parentDirectory(confpath, false);
							conffile = confpath.substr(confdir.size());

							// Remove all the old watches
							for (auto wd : additional_watch_wds) {
								if (inotify_rm_watch(ifd, wd.first) < 0) {
									// log_err("inotify_rm_watch", errno, "Unable to remove symlink watch %d",
									// wd.first); exit(1);
									log_msg(SevInfo, "Could not remove inotify watch %d, continuing...\n", wd.first);
								}
							}

							reload = true;
							reload_additional_watches = true;
							break;
						} else if (event->wd == confdir_wd && event->len && conffile == event->name) {
							reload = true;
						}
					}

					else if (event->wd == conffile_wd) {
						reload = true;
					}

					i += sizeof(struct inotify_event) + event->len;
				}
			}
		}
#endif

		/* select() could have returned because of one or more
		   SIGCHLDs */
		if (child_exited) {
			pid_t pid;
			int child_status;
			while ((pid = waitpid(-1, &child_status, WNOHANG))) {
				if (pid < 0) {
					if (errno != ECHILD)
						log_err("waitpid", errno, "Error while waiting for child process");
					break;
				}

				ProcessID id = pid_id[pid];
				Command* cmd = id_command[id].get();

				pid_id.erase(pid);
				id_pid.erase(id);

				if (cmd->deconfigured) {
					id_command.erase(id);
				} else {
					int delay = cmd->get_and_update_current_restart_delay();
					if (!cmd->quiet) {
						if (WIFEXITED(child_status)) {
							Severity priority = (WEXITSTATUS(child_status) == 0) ? SevWarn : SevError;
							log_process_msg(priority,
							                cmd->ssection.c_str(),
							                "Process %d exited %d, restarting in %d seconds\n",
							                pid,
							                WEXITSTATUS(child_status),
							                delay);
						} else if (WIFSIGNALED(child_status))
							log_process_msg(SevWarn,
							                cmd->ssection.c_str(),
							                "Process %d terminated by signal %d, restarting in %d seconds\n",
							                pid,
							                WTERMSIG(child_status),
							                delay);
						else
							log_process_msg(SevWarnAlways,
							                cmd->ssection.c_str(),
							                "Process %d exited for unknown reason, restarting in %d seconds\n",
							                pid,
							                delay);
					}

					start_process(cmd, id, uid, gid, delay, &normal_mask);
				}
			}
			child_exited = false;
		}
	}
}
