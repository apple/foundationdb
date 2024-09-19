/*
 * fdbmonitor.h
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

#pragma once

#ifndef FDB_MONITOR_H
#define FDB_MONITOR_H

#include <cstdint>
#include <sys/select.h>
#include <sys/syslog.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <sstream>
#include <random>
#include <unordered_map>
#include <unordered_set>

#ifdef __linux__
#include <sys/inotify.h>
#include <sys/time.h>
#include <linux/limits.h>
#endif

#include "fdbclient/SimpleIni.h"

#ifdef __linux__
constexpr uint64_t DEFAULT_MEMORY_LIMIT = 8LL << 30;
#endif
constexpr double MEMORY_CHECK_INTERVAL = 2.0; // seconds

#ifdef __linux__
typedef fd_set* fdb_fd_set;
#elif defined(__APPLE__) || defined(__FreeBSD__)
typedef int fdb_fd_set;
#endif

#define CANONICAL_PATH_SEPARATOR '/'

namespace fdbmonitor {

enum Severity { SevDebug = 5, SevInfo = 10, SevWarn = 20, SevWarnAlways = 30, SevError = 40 };

extern bool daemonize;
extern std::string logGroup;

typedef std::string ProcessID;

int severity_to_priority(Severity severity);
double timer();
double get_cur_timestamp();
int randomInt(int min, int max);
void vlog_process_msg(Severity severity, const char* process, const char* format, va_list args);
void log_msg(Severity severity, const char* format, ...);
void log_process_msg(Severity severity, const char* process, const char* format, ...);
void log_err(const char* func, int err, const char* format, ...);
void monitor_fd(fdb_fd_set list, int fd, int* maxfd, void* cmd);
void unmonitor_fd(fdb_fd_set list, int fd);
const char* get_value_multi(const CSimpleIni& ini, const char* key, ...);
uint64_t parseWithSuffix(const char* to_parse, const char* default_unit = nullptr);
bool isParameterNameEqual(const char* str, const char* target);
std::string popPath(const std::string& path);
std::string cleanPath(std::string const& path);
std::string abspath(std::string const& path, bool resolveLinks = true);
std::string joinPath(std::string const& directory, std::string const& filename);
std::string parentDirectory(std::string const& path, bool resolveLinks = true);
int mkdir(std::string const& directory);
void print_usage(const char* name);
std::unordered_map<int, std::unordered_set<std::string>> set_watches(std::string path, int ifd);
void load_conf(const char* confpath, uid_t& uid, gid_t& gid, sigset_t* mask, fdb_fd_set rfds, int* maxfd);
uint64_t getRss(ProcessID id);
void kill_process(ProcessID id, bool wait = true, bool cleanup = true);

struct Command;
void read_child_output(Command* cmd, int pipe_idx, fdb_fd_set fds);
void start_process(Command* cmd, ProcessID id, uid_t uid, gid_t gid, int delay, sigset_t* mask);

#if defined(__APPLE__) || defined(__FreeBSD__)
void watch_conf_dir(int kq, int* confd_fd, std::string confdir);
void watch_conf_file(int kq, int* conff_fd, const char* confpath);
#endif

struct EnvVarUtils {
	// This utility assumes key and value are separated by one equal sign
	static std::pair<std::string, std::string> extractKeyAndValue(const std::string& keyValue) {
		const auto equalIdx = keyValue.find('=');
		if (equalIdx == std::numeric_limits<size_t>::max()) {
			log_msg(SevError, "Stopping because equalIdx has reached size_t max value\n");
			exit(1);
		}
		return { /* key */ keyValue.substr(0, equalIdx), /* value */ keyValue.substr(equalIdx + 1) };
	}

	static bool keyValueValid(const std::string& keyValue, const std::string& envvars) {
		if (keyValue.empty()) {
			log_msg(SevError, "Key-value in envvars %s can not be empty\n", envvars.c_str());
			return false;
		}
		const auto numEqualSigns = std::count(keyValue.begin(), keyValue.end(), '=');
		if (numEqualSigns != 1) {
			log_msg(SevError,
			        "Key-value %s in envvars string %s should have exactly one equal "
			        "sign\n",
			        keyValue.c_str(),
			        envvars.c_str());
			return false;
		}
		const auto [key, value] = extractKeyAndValue(keyValue);
		if (key.empty()) {
			log_msg(SevError, "Stopping because key in envvars key-value %s is empty\n", keyValue.c_str());
			return false;
		}
		if (value.empty()) {
			log_msg(SevError, "Stopping because value in envvars key-value %s is empty\n", keyValue.c_str());
			return false;
		}
		return true;
	}
};

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
	std::string envvars;
	std::string delete_envvars;
	bool deconfigured;
	bool kill_on_configuration_change;
	uint64_t memory_rss;

	// one pair for each of stdout and stderr
	int pipes[2][2];

	Command(const CSimpleIni& ini, std::string _section, ProcessID id, fdb_fd_set fds, int* maxfd)
	  : fds(fds), argv(nullptr), section(_section), fork_retry_time(-1), quiet(false), envvars(), delete_envvars(),
	    deconfigured(false), kill_on_configuration_change(true), memory_rss(0) {
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

		const char* env = get_value_multi(ini, "envvars", ssection.c_str(), section.c_str(), "general", nullptr);
		if (env) {
			envvars = env;
		}

		const char* del_env =
		    get_value_multi(ini, "delete-envvars", ssection.c_str(), section.c_str(), "general", nullptr);
		if (del_env) {
			delete_envvars = del_env;
		}

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

		const char* mem_rss = get_value_multi(ini, "memory", ssection.c_str(), section.c_str(), "general", nullptr);
#ifdef __linux__
		if (mem_rss) {
			memory_rss = parseWithSuffix(mem_rss, "MiB");
		} else {
			memory_rss = DEFAULT_MEMORY_LIMIT;
		}
#else
		if (mem_rss) {
			// While the memory check is not currently implemented on non-Linux by
			// fdbmonitor, the "memory" option is still pass to fdbserver, which will
			// crash itself if the limit is exceeded.
			log_msg(SevWarn,
			        "Memory monitoring by fdbmonitor is not supported by "
			        "current system\n");
		}
#endif

		std::stringstream ss(binary);
		std::copy(std::istream_iterator<std::string>(ss),
		          std::istream_iterator<std::string>(),
		          std::back_inserter<std::vector<std::string>>(commands));

		const char* id_s = ssection.c_str() + strlen(section.c_str()) + 1;

		const std::string pid_s = std::to_string(getpid());

		for (auto i : keys) {
			// For "memory" option, despite they are handled by fdbmonitor, we still
			// pass it to fdbserver.
			if (isParameterNameEqual(i.pItem, "command") || isParameterNameEqual(i.pItem, "restart-delay") ||
			    isParameterNameEqual(i.pItem, "initial-restart-delay") ||
			    isParameterNameEqual(i.pItem, "restart-backoff") ||
			    isParameterNameEqual(i.pItem, "restart-delay-reset-interval") ||
			    isParameterNameEqual(i.pItem, "disable-lifecycle-logging") ||
			    isParameterNameEqual(i.pItem, "envvars") || isParameterNameEqual(i.pItem, "delete-envvars") ||
			    isParameterNameEqual(i.pItem, "kill-on-configuration-change")) {
				continue;
			}

			std::string opt = get_value_multi(ini, i.pItem, ssection.c_str(), section.c_str(), "general", nullptr);

			std::size_t pos = 0;
			while ((pos = opt.find("$ID", pos)) != opt.npos)
				opt.replace(pos, 3, id_s, strlen(id_s));

			pos = 0;
			while ((pos = opt.find("$PID", pos)) != opt.npos)
				opt.replace(pos, 4, pid_s);

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
		envvars = other.envvars;
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

} // namespace fdbmonitor

#endif
