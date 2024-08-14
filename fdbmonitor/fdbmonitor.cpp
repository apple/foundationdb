/*
 * fdbmonitor.cpp
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
#include <memory>
#include <sys/wait.h>
#include <cinttypes>
#include <sys/stat.h>
#include <fcntl.h>
#include <filesystem>

#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#endif

#include "fdbmonitor.h"
#include "SimpleOpt/SimpleOpt.h"
#include "fdbclient/versions.h"

namespace fdbmonitor {

enum { OPT_CONFFILE, OPT_LOCKFILE, OPT_LOGGROUP, OPT_DAEMONIZE, OPT_HELP };

CSimpleOpt::SOption g_rgOptions[] = { { OPT_CONFFILE, "--conffile", SO_REQ_SEP },
	                                  { OPT_LOCKFILE, "--lockfile", SO_REQ_SEP },
	                                  { OPT_LOGGROUP, "--loggroup", SO_REQ_SEP },
	                                  { OPT_DAEMONIZE, "--daemonize", SO_NONE },
	                                  { OPT_HELP, "-?", SO_NONE },
	                                  { OPT_HELP, "-h", SO_NONE },
	                                  { OPT_HELP, "--help", SO_NONE },
	                                  SO_END_OF_OPTIONS };

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

extern std::unordered_map<ProcessID, std::unique_ptr<Command>> id_command;
extern std::unordered_map<pid_t, ProcessID> pid_id;
extern std::unordered_map<ProcessID, pid_t> id_pid;

} // namespace fdbmonitor

int main(int argc, char** argv) {
	using namespace fdbmonitor;

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
	double last_rss_check = timer();
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
		double now = timer();

		// True if any process has a resident memory limit
		bool need_rss_check = false;

		for (auto& i : id_command) {
			if (i.second->fork_retry_time >= 0) {
				end_time = std::min(i.second->fork_retry_time, end_time);
			}
			// If process has a resident memory limit and is currently running
			if (i.second->memory_rss > 0 && id_pid.count(i.first) > 0) {
				need_rss_check = true;
			}
		}
		bool timeout_for_rss_check = false;
		if (need_rss_check && end_time > last_rss_check + MEMORY_CHECK_INTERVAL) {
			end_time = last_rss_check + MEMORY_CHECK_INTERVAL;
			timeout_for_rss_check = true;
		}
		struct timespec tv;
		double timeout = -1;
		if (end_time < std::numeric_limits<double>::max()) {
			timeout = std::max(0.0, end_time - now);
			if (timeout > 0) {
				tv.tv_sec = timeout;
				tv.tv_nsec = 1e9 * (timeout - tv.tv_sec);
			}
		}

		bool is_timeout = false;

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
			is_timeout = true;
			if (!timeout_for_rss_check) {
				reload = true;
			}
		}
#elif defined(__APPLE__) || defined(__FreeBSD__)
		int nev = 0;
		if (timeout < 0) {
			nev = kevent(kq, nullptr, 0, &ev, 1, nullptr);
		} else if (timeout > 0) {
			nev = kevent(kq, nullptr, 0, &ev, 1, &tv);
		}

		if (nev == 0) {
			is_timeout = true;
			if (!timeout_for_rss_check) {
				reload = true;
			}
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

		if (is_timeout && timeout_for_rss_check) {
			last_rss_check = timer();
			std::vector<ProcessID> oom_ids;
			for (auto& i : id_command) {
				if (id_pid.count(i.first) == 0) {
					// process is not running
					continue;
				}
				uint64_t rss_limit = i.second->memory_rss;
				if (rss_limit == 0) {
					continue;
				}
				uint64_t current_rss = getRss(i.first);
				if (current_rss > rss_limit) {
					log_process_msg(SevWarn,
					                i.second->ssection.c_str(),
					                "Process %d being killed for exceeding resident memory limit, current %" PRIu64
					                " , limit %" PRIu64 "\n",
					                id_pid[i.first],
					                current_rss,
					                i.second->memory_rss);
					oom_ids.push_back(i.first);
				}
			}
			// kill process without waiting, and rely on the SIGCHLD handling logic below to restart the process.
			for (auto& id : oom_ids) {
				kill_process(id, false /*wait*/, false /*cleanup*/);
			}
			if (oom_ids.size() > 0) {
				child_exited = true;
			}
		}

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
