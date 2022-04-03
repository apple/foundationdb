/*
 * FDBService.cpp
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

#include "ServiceBase.h"
#include "ThreadPool.h"

#include <iostream>
#include <fstream>
#include <unordered_map>

#include <stdint.h>
#include <time.h>

#include "flow/SimpleOpt.h"
#include "fdbclient/SimpleIni.h"
#include "fdbclient/versions.h"

// For PathFileExists
#include "Shlwapi.h"
#pragma comment(lib, "Shlwapi.lib")
// For SHGetFolderPath
#include <ShlObj.h>
#pragma comment(lib, "Shell32.lib")

// To be used for logging, naming of Mutex, etc.
#ifdef __FDB_DOC_MONITOR__
#define SERVICE_NAME "fdbdocmonitor"
#define CONFIG_NAME "document\\document.conf"
#else // __FDB_KVS_MONITOR__
#define SERVICE_NAME "fdbmonitor"
#define CONFIG_NAME "foundationdb.conf"
#endif

// Set up the options parsing. We have only one option now, but this could change!
enum { OPT_CONFFILE };
CSimpleOpt::SOption g_rgOptions[] = { { OPT_CONFFILE, "--conffile", SO_REQ_SEP }, SO_END_OF_OPTIONS };

// For logging debugging messages
bool logging = false;
std::ofstream logFile;

std::string format(const char* form, ...) {
	char buf[200];
	va_list args;

	va_start(args, form);
	int size = vsnprintf(buf, sizeof(buf), form, args);
	va_end(args);

	if (size >= 0 && size < sizeof(buf)) {
		return std::string(buf, size);
	}

#ifdef _WIN32
	// Microsoft's non-standard vsnprintf doesn't return a correct size, but just an error, so determine the necessary
	// size
	va_start(args, form);
	size = _vscprintf(form, args);
	va_end(args);
#endif

	if (size < 0)
		throw std::exception("Error in format");

	std::string s;
	s.resize(size + 1);
	va_start(args, form);
	size = vsnprintf(&s[0], s.size(), form, args);
	va_end(args);
	if (size < 0 || size >= s.size())
		throw std::exception("Error in format");

	s.resize(size);
	return s;
}

void writeLogLine(std::string message) {
	if (logging && logFile.good()) {
		char datebuf[128], timebuf[128];
		_strdate_s(datebuf, 128);
		_strtime_s(timebuf, 128);
		logFile << datebuf << " " << timebuf << " - " << message << "\n";
		logFile.flush();
	}
}

class FDBService : public CServiceBase {
public:
	FDBService(bool fCanStop = true, bool fCanShutdown = true, bool fCanPauseContinue = false)
	  : CServiceBase(SERVICE_NAME, fCanStop, fCanShutdown, fCanPauseContinue), serviceStopping(false) {
		// Create a manual-reset event to signal the end of the service.
		stoppedEvent = CreateEvent(nullptr, true, false, nullptr);
		if (stoppedEvent == nullptr) {
			throw GetLastError();
		}
		// Create a manual-reset event to signal the end of the service.
		stoppingEvent = CreateEvent(nullptr, true, false, nullptr);
		if (stoppingEvent == nullptr) {
			throw GetLastError();
		}

		// Initialize child job member
		childJob = nullptr;
	}

	virtual ~FDBService(void) {
		if (stoppedEvent) {
			CloseHandle(stoppedEvent);
			stoppedEvent = nullptr;
		}
		if (stoppingEvent) {
			CloseHandle(stoppingEvent);
			stoppingEvent = nullptr;
		}
		if (childJob) {
			CloseHandle(childJob);
			childJob = nullptr;
		}
		logFile.close();
	};

	// The following method will allow the object to run
	// within the foreground
	virtual bool Run(void) {
		int status = 0;

		// Start the service
		try {
			// Define the configuration file
			confFile = GetDefaultConfigFilePath();

			// Run the worker thread
			ServiceWorkerThread();
		} catch (DWORD dwError) {
			// Log the error.
			WriteErrorLogEntry("Service Start", dwError);
			status++;
		} catch (...) {
			// Log the error.
			WriteEventLogEntry("Service failed to start.", EVENTLOG_ERROR_TYPE);
			status++;
		}

		return (status == 0);
	}

protected:
	// The following function will return the default configuration
	// file path
	std::string GetDefaultConfigFilePath() {
		TCHAR programData[MAX_PATH];
		if (SHGetFolderPath(nullptr, CSIDL_COMMON_APPDATA, nullptr, 0, programData) != S_OK) {
			errorExit("resolving CSIDL_COMMON_APPDATA");
		}

		return std::string(programData) + "\\foundationdb\\" + CONFIG_NAME;
	}

	struct Command {
		std::string binary;
		std::string args;
		std::string section;
		std::string ssection;
		int restartDelay;
		boolean quiet;
		boolean valid;

		Command() : valid(false), quiet(false), restartDelay(5) {}

		bool operator!=(const Command& rhs) { return binary != rhs.binary || args != rhs.args; }
	};

	struct Subprocess {
		// Represents a single subprocess which is in the configuration file.  It is either running
		// or waiting (on a timer) to restart.

		// Interface to event loop
		Subprocess(FDBService* svc, int id, Command const& cmd)
		  : svc(svc), id(id), command(cmd), process_or_timer(INVALID_HANDLE_VALUE), isProcess(true) {
			createProcess();
		}
		~Subprocess() {
			if (isProcess) {
				// FIXME: should this be other than 0? Should we care if this fails?
				if (!TerminateProcess(process_or_timer, 0)) {
					svc->errorExit("Terminate fdbmonitor process");
				}

				// TerminateProcess is asynchronous, wait for the process to die
				if (!command.quiet)
					svc->LogEvent(EVENTLOG_INFORMATION_TYPE, format("Waiting for process %d to terminate", id));
				DWORD signal = WaitForSingleObject(process_or_timer, INFINITE);
				if (signal != WAIT_OBJECT_0) {
					svc->errorExit("Termination wait for process");
				} else if (!command.quiet) {
					svc->LogEvent(EVENTLOG_INFORMATION_TYPE, format("Process %d has terminated", id));
				}
			}

			CloseHandle(process_or_timer);
		}
		HANDLE getHandleToWaitOn() { return process_or_timer; }
		void onHandleSignaled() {
			if (isProcess) {
				DWORD exitCode;
				if (!GetExitCodeProcess(process_or_timer, &exitCode)) {
					svc->logLastError("process get exit code");
					exitCode = 2181; // FIXME: WHAT?
				}

				if (!command.quiet)
					svc->LogEvent(EVENTLOG_INFORMATION_TYPE,
					              format("Child process %d exited with %d, restarting in %d seconds",
					                     id,
					                     exitCode,
					                     command.restartDelay));

				CloseHandle(process_or_timer);
				startTimer();
			} else {
				CloseHandle(process_or_timer);
				createProcess();
			}
		}

		// Intrusive indices for external data structures
		int id;
		size_t subprocess_index;

		// Configuration and state
		FDBService* svc;
		Command command;
		HANDLE process_or_timer;
		bool isProcess; // otherwise, is waiting to restart

		// Implementation
		void startTimer() {
			isProcess = false;
			process_or_timer = CreateWaitableTimer(nullptr, TRUE, nullptr);
			if (!process_or_timer)
				svc->errorExit(format("Error in startTimer(): CreateWaitableTimer (%d)", GetLastError()).c_str());

			svc->startTimer(process_or_timer, command.restartDelay);
		}

		void createProcess() {
			isProcess = true;

			STARTUPINFO si;
			PROCESS_INFORMATION pi;

			ZeroMemory(&si, sizeof(si));
			si.cb = sizeof(si);
			ZeroMemory(&pi, sizeof(pi));

			if (!command.quiet)
				svc->LogEvent(EVENTLOG_INFORMATION_TYPE, format("Starting child job (%s)", command.args.c_str()));

			// Start the child process.
			if (!CreateProcess(command.binary.c_str(), // Command's binary
			                   (char*)command.args.c_str(), // Command's args
			                   nullptr, // Process handle not inheritable
			                   nullptr, // Thread handle not inheritable
			                   FALSE, // Set handle inheritance to FALSE
			                   CREATE_NEW_PROCESS_GROUP, // No flags for creation
			                   nullptr, // Use parent's environment block
			                   nullptr, // Use parent's starting directory
			                   &si, // Pointer to STARTUPINFO structure
			                   &pi) // Pointer to PROCESS_INFORMATION structure
			) {
				svc->logLastError(format("Failed to create process, restarting in %d seconds (%s)",
				                         command.restartDelay,
				                         command.args.c_str())
				                      .c_str());

				// If there was an error, we set a timer right away for the restart.
				startTimer();
			} else {
				CloseHandle(pi.hThread);
				process_or_timer = pi.hProcess;

				if (!command.quiet)
					svc->LogEvent(EVENTLOG_INFORMATION_TYPE,
					              format("Child %d started with PID %d", id, pi.dwProcessId));
			}
		}

		void update(Command cmd) {
			command.quiet = cmd.quiet;
			command.restartDelay = cmd.restartDelay;
		}
	};

	void startTimer(HANDLE h, double delaySeconds) {
		LARGE_INTEGER liDueTime;
		// negative numbers are relative times, times are in 100s of ns -- see SetWaitableTimer MSDN docs
		liDueTime.QuadPart = (LONGLONG)(delaySeconds * -10000000LL);

		if (!SetWaitableTimer(h, &liDueTime, 0, nullptr, nullptr, 0))
			errorExit(format("Error in startTimer(): SetWaitableTimer (%d)", GetLastError()).c_str());
	}

	virtual void FDBService::OnStart(DWORD argc, LPSTR* argv) {
		LogEvent(EVENTLOG_INFORMATION_TYPE, SERVICE_NAME " starting (" FDB_VT_VERSION ")");

		std::string _confpath(GetDefaultConfigFilePath());

		LogEvent(EVENTLOG_INFORMATION_TYPE, format("Default config file at %s", _confpath.c_str()));

		// Parse "command line" options
		CSimpleOpt args(argc, argv, g_rgOptions, SO_O_NOERR | SO_O_HYPHEN_TO_UNDERSCORE);

		while (args.Next()) {
			if (args.LastError() == SO_SUCCESS) {
				switch (args.OptionId()) {
				case OPT_CONFFILE:
					_confpath = args.OptionArg();
					break;
				}
			} else {
				// FIXME: log error
				throw(DWORD) 1;
			}
		}

		confFile = _confpath;
		LogEvent(EVENTLOG_INFORMATION_TYPE, format("Using config file %s", confFile.c_str()));

		// Aquire the mutex for this service. There will be one mutex per config file.
		// If the user installes the executable as more than one service (not likely)
		//  we allow that both can operate so long as they are pointed at different
		//  config files.

		// For now we will not do this check...there is question about whether we need this
		/* std::size_t pos = 0;
		// mutexes cannot have a backslash in their name
		std::string escaped = confFile;
		while( ( pos = escaped.find( "\"", pos ) ) != escaped.npos )
		    escaped.replace( pos, 1, "/" );

		HANDLE hMutex = CreateMutex(
		    nullptr,                        // default security descriptor
		    TRUE,                        // attempt to create with ownership
		    format( "Global\\" SERVICE_NAME ".%s", escaped.c_str() ).c_str() );  // object name

		if (hMutex == nullptr)
		    errorExit( format( "Could not create/aquire global mutex (%s)", escaped.c_str() ).c_str() );
		else
		    // In this case ownership on creation flag is ignored. (see MSDN docs)
		    if ( GetLastError() == ERROR_ALREADY_EXISTS )
		        errorExit( "mutex created without ownership");

		LogEvent( EVENTLOG_INFORMATION_TYPE, "instance uniqueness established" ); */

		// Queue the main service function for execution in a worker thread.
		CThreadPool::QueueUserWorkItem(&FDBService::ServiceWorkerThread, this);
	}

	virtual void FDBService::OnStop() {
		LogEvent(EVENTLOG_INFORMATION_TYPE, SERVICE_NAME " shutting down");

		serviceStopping = true;
		SetEvent(stoppingEvent);

		if (WaitForSingleObject(stoppedEvent, INFINITE) != WAIT_OBJECT_0) {
			// We're exiting, so this will be logged, but an error will not be thrown
			logLastError("OnStop final wait");
		}

		// Log a service stop message to the Application log.
		LogEvent(EVENTLOG_INFORMATION_TYPE, SERVICE_NAME " stop complete");
	}

	virtual void ServiceWorkerThread(void) {
		try {
			char confFileDirectory[2048];
			char* fileNameStart;
			if (!GetFullPathName(confFile.c_str(), 2048, confFileDirectory, &fileNameStart)) {
				errorExit(format("get path of conf file (%s)", confFile.c_str()).c_str());
			}

			if (!fileNameStart) {
				errorExit(format("file name not present (%s)", confFile.c_str()).c_str());
			}

			// Test file existence
			if (!PathFileExists(confFileDirectory)) {
				errorExit(format("conf file (%s) does not exist", confFileDirectory).c_str());
			}

			// null terminate the absolute path string after the directory
			*fileNameStart = 0;
			// confFileDirectory now contains only the parent dir of the conf file

			// FALSE here as "manualReset" means that the timer will go to the unsignaled state once
			//  signalled from a "wait" call.
			HANDLE fileErrorReloadHandle = CreateWaitableTimer(nullptr, FALSE, nullptr);
			if (fileErrorReloadHandle == INVALID_HANDLE_VALUE) {
				errorExit(format("Error creating waitable timer (%d)", GetLastError()).c_str());
			}

			HANDLE fileChangeHandle =
			    FindFirstChangeNotification(confFileDirectory,
			                                FALSE, // do not watch the subtree
			                                FILE_NOTIFY_CHANGE_LAST_WRITE | FILE_NOTIFY_CHANGE_FILE_NAME);

			if (fileChangeHandle == INVALID_HANDLE_VALUE) {
				errorExit(format("FindFirstChangeNotification (%s)", confFileDirectory).c_str());
			} else {
				LogEvent(EVENTLOG_INFORMATION_TYPE, format("watching directory %s", confFileDirectory));
			}

			loadConf(confFile.c_str());

			const int subprocessWaitIndexBase = 3;

			while (!serviceStopping) {
				// The events to be waited on will have this sequence...
				//  The signals and timers are checked in the subprocesses[] map
				//  0  - the shutdown signal
				//  1  - the file change notification
				//  2  - the file load error timer
				// [n] - signals for subprocesses (process termination or restart timer)
				int eventCount = subprocessWaitIndexBase + (int)subprocesses.size();
				HANDLE* events = new HANDLE[eventCount];
				events[0] = stoppingEvent;
				events[1] = fileChangeHandle;
				events[2] = fileErrorReloadHandle;
				for (int i = 0; i < subprocesses.size(); i++)
					events[subprocessWaitIndexBase + i] = subprocesses[i]->getHandleToWaitOn();

				// FIXME: Check that eventCount < MAXIMUM_WAIT_OBJECTS, here and/or in loadConf

				// FIXME: Is 'fileChangeHandle' signalling each time around the loop?

				// Wait for any of the events to be signalled
				DWORD signalled = WaitForMultipleObjects(eventCount, events, FALSE, INFINITE);
				delete[] events;

				// Check for error cases (anything other than one of the objects being signalled)
				if (signalled < WAIT_OBJECT_0 || signalled >= WAIT_OBJECT_0 + eventCount) {
					WriteEventLogEntry(SERVICE_NAME " wait failed", EVENTLOG_ERROR_TYPE);

					// prevent fast spin
					Sleep(2000);
				} else if (signalled == WAIT_OBJECT_0 + 0) {
					WriteEventLogEntry(SERVICE_NAME " service shutdown signalled", EVENTLOG_INFORMATION_TYPE);
				} else if (signalled == WAIT_OBJECT_0 + 1) {
					if (!loadConf(confFile.c_str())) {
						// An error was encountered in loading the conf file...set a timer to force a reload
						double retrySeconds = 0.1;
						startTimer(fileErrorReloadHandle, retrySeconds);
						WriteEventLogEntry(
						    format(SERVICE_NAME " scheduling reload in %f seconds", retrySeconds).c_str(),
						    EVENTLOG_INFORMATION_TYPE);
					} else {
						// The load was a success but we may still have an error-caused reload scheduled.
						//  Cancel any outstanding timers to avoid an uneeded reload of the file.
						CancelWaitableTimer(fileErrorReloadHandle);
					}

					// Reset the signal handling.
					if (!FindNextChangeNotification(fileChangeHandle)) {
						errorExit("FindNextChangeNotification");
					}
				} else if (signalled == WAIT_OBJECT_0 + 2) {
					WriteEventLogEntry(SERVICE_NAME " attempting configuration reload after error",
					                   EVENTLOG_INFORMATION_TYPE);
					if (!loadConf(confFile.c_str())) {
						// An error was encountered in reloading the file after another error
						//  occurred, this should have a longer delay to prevent a spin loop
						startTimer(fileErrorReloadHandle, 1.0);
					}
				} else {
					Subprocess* sp = subprocesses[signalled - WAIT_OBJECT_0 - subprocessWaitIndexBase];
					sp->onHandleSignaled();
				}
			}

			for (auto s = subprocesses.begin(); s != subprocesses.end(); ++s)
				delete *s;
			subprocesses.clear();
			id_subprocess.clear();
		} catch (...) {
			LogEvent(EVENTLOG_ERROR_TYPE, SERVICE_NAME " unexpected exception thrown");
		}

		try {
			LogEvent(EVENTLOG_INFORMATION_TYPE, SERVICE_NAME " signalling stopped");
			// Signal the stopped event to allow "onStop" to complete
			SetEvent(stoppedEvent);
		} catch (...) {
			LogEvent(EVENTLOG_ERROR_TYPE, SERVICE_NAME " unexpected exception thrown while stopping");
		}
	}

public:
	// The following method stops the service when ctrl-break is pressed
	void Break() {
		LogEvent(EVENTLOG_INFORMATION_TYPE, SERVICE_NAME " shutting down");

		serviceStopping = true;
		SetEvent(stoppingEvent);

		if (WaitForSingleObject(stoppedEvent, INFINITE) != WAIT_OBJECT_0) {
			// We're exiting, so this will be logged, but an error will not be thrown
			logLastError("OnStop final wait");
		}

		// Log a service stop message to the Application log.
		LogEvent(EVENTLOG_INFORMATION_TYPE, SERVICE_NAME " stop complete");

		return;
	}

private:
	void findRemovedOrChangedSubprocesses(const CSimpleIni& ini,
	                                      std::vector<Subprocess*> const& subprocesses,
	                                      std::vector<Subprocess*>& stop_processes,
	                                      std::vector<std::pair<uint16_t, Command>>& start_ids) {
		for (auto it = subprocesses.begin(); it != subprocesses.end(); ++it) {
			Subprocess* sp = *it;
			if (ini.GetSectionSize(sp->command.ssection.c_str()) == -1) {
				/* Server on this id no longer configured; kill it */
				LogEvent(EVENTLOG_INFORMATION_TYPE, format("Deconfigured process (ID %d)", sp->id));
				stop_processes.push_back(sp);
			} else {
				/* Server still configured; let's check the config */
				Command cmd = makeCommand(ini, sp->command.section, sp->id);
				/* On error we just stop the old process but do not start a new one */
				if (!cmd.valid) {
					stop_processes.push_back(sp);
				}
				/* The inequality operation only compares the binary path and arguments */
				else if (cmd != sp->command) {
					/* Config changed; kill the old process, start a new one */
					/* Log only if one of the two are non-quiet */
					if (!cmd.quiet || !sp->command.quiet)
						LogEvent(EVENTLOG_INFORMATION_TYPE,
						         format("Found new configuration for process (ID %d)", sp->id));
					stop_processes.push_back(sp);
					start_ids.emplace_back(sp->id, cmd);
				} else if (cmd.quiet != sp->command.quiet || cmd.restartDelay != sp->command.restartDelay) {
					// Update restartDelay and quiet but do not restart running processes
					if (!cmd.quiet || !sp->command.quiet)
						LogEvent(EVENTLOG_INFORMATION_TYPE, format("Updating process (ID %d)", sp->id));
					sp->update(cmd);
				}
			}
		}
	}

	void findAddedSubprocesses(const CSimpleIni& ini,
	                           std::unordered_map<uint16_t, Subprocess*> const& id_subprocess,
	                           std::vector<std::pair<uint16_t, Command>>& start_ids) {
		CSimpleIniA::TNamesDepend sections;
		ini.GetAllSections(sections);
		for (auto it = sections.begin(); it != sections.end(); it++) {
			if (auto dot = strrchr(it->pItem, '.')) {
				char* strtol_end;
				int id = strtoul(dot + 1, &strtol_end, 10);
				if (*strtol_end != '\0' || !(id > 0)) {
					LogEvent(EVENTLOG_ERROR_TYPE, format("Found bogus id in %s", it->pItem));
				} else {
					if (!id_subprocess.count(id)) {
						/* Found something we haven't yet started */
						LogEvent(EVENTLOG_INFORMATION_TYPE, format("Found new process (ID %d)", id));
						std::string section(it->pItem, dot - it->pItem);
						Command cmd = makeCommand(ini, section, id);
						if (cmd.valid) {
							start_ids.emplace_back(id, cmd);
						} else {
							LogEvent(
							    EVENTLOG_ERROR_TYPE,
							    format(
							        "New process (ID %d) does not have a valid specification and will not be started",
							        id));
						}
					}
				}
			}
		}
	}

	// Returns 'true' on successful load, 'false' if an error is encountered
	bool loadConf(const char* confpath) {
		LogEvent(EVENTLOG_INFORMATION_TYPE, format("Loading configuration %s", confpath));

		CSimpleIniA ini;
		ini.SetUnicode();

		// Set to a negative code on error.
		SI_Error err = ini.LoadFile(confpath);
		if (err < 0) {
			// If the error was file not found, we assume that future changes to the file will
			//  come in as change notifications. This means that we will not poll, we just leave
			//  processes running as-is waiting for the file to reappear.
			if (err == SI_FILE && GetLastError() == ERROR_FILE_NOT_FOUND) {
				LogEvent(EVENTLOG_ERROR_TYPE,
				         format("Configuration file `%s' not found on load, waiting for next change", confpath));
				return true;
			}
			LogEvent(EVENTLOG_ERROR_TYPE,
			         format("Configuration file (`%s') load error: %d, %d", confpath, err, GetLastError()));
			return false;
		}

		std::vector<Subprocess*> stop_processes;
		std::vector<std::pair<uint16_t, Command>> start_ids;

		/* First check all processes (running or waiting to restart) to be sure that current config
		   matches saved file state */
		findRemovedOrChangedSubprocesses(ini, subprocesses, stop_processes, start_ids);

		/* We've handled deconfigured servers and changed configs, but now
		   we need to look for newly configured servers */
		findAddedSubprocesses(ini, id_subprocess, start_ids);

		for (auto it = stop_processes.begin(); it != stop_processes.end(); ++it) {
			removeSubprocess(*it);
			delete *it; // Destructor terminates and waits for termination of actual process (if any)
		}
		for (auto it = start_ids.begin(); it != start_ids.end(); it++)
			addSubprocess(new Subprocess(this, it->first, it->second));

		return true;
	}

	const char* getValueMulti(const CSimpleIni& ini, const char* name, ...) {
		const char* ret = nullptr;
		const char* section = nullptr;

		std::string nameWithUnderscores(name);
		for (int i = nameWithUnderscores.size() - 1; i >= 0; --i) {
			if (nameWithUnderscores[i] == '-') {
				nameWithUnderscores.at(i) = '_';
			}
		}

		va_list ap;
		va_start(ap, name);
		while (!ret && (section = va_arg(ap, const char*))) {
			ret = ini.GetValue(section, name, nullptr);
			if (!ret) {
				ret = ini.GetValue(section, nameWithUnderscores.c_str(), nullptr);
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

	Command makeCommand(const CSimpleIni& ini, std::string section, uint16_t id) {
		std::string ssection = format("%s.%d", section.c_str(), id);
		Command result;

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

		const char* rd =
		    getValueMulti(ini, "restart-delay", ssection.c_str(), section.c_str(), "general", "fdbmonitor", nullptr);
		if (!rd) {
			LogEvent(EVENTLOG_ERROR_TYPE, format("Unable to resolve restart delay for %s\n", ssection.c_str()));
			return result;
		}

		char* endptr;
		result.restartDelay = strtoul(rd, &endptr, 10);
		if (*endptr != '\0') {
			LogEvent(EVENTLOG_ERROR_TYPE, format("Unable to parse restart delay for %s\n", ssection.c_str()));
			return result;
		}

		const char* q =
		    getValueMulti(ini, "disable-lifecycle-logging", ssection.c_str(), section.c_str(), "general", nullptr);
		if (q && !strcmp(q, "true"))
			result.quiet = true;

		const char* binary = getValueMulti(ini, "command", ssection.c_str(), section.c_str(), "general", nullptr);
		if (!binary) {
			LogEvent(EVENTLOG_ERROR_TYPE, format("Unable to resolve command for %s.%d\n", section.c_str(), id));
			return result;
		}
		result.binary = binary;
		result.args = quote(binary);

		const char* id_s = ssection.c_str() + strlen(section.c_str()) + 1;

		for (auto i : keys) {
			if (isParameterNameEqual(i.pItem, "command") || isParameterNameEqual(i.pItem, "restart-delay") ||
			    isParameterNameEqual(i.pItem, "disable-lifecycle-logging")) {
				continue;
			}

			std::string opt = getValueMulti(ini, i.pItem, ssection.c_str(), section.c_str(), "general", nullptr);

			std::size_t pos = 0;
			while ((pos = opt.find("$ID", pos)) != opt.npos)
				opt.replace(pos, 3, id_s, strlen(id_s));

			pos = 0;
			std::string pid_s = format("%d", GetCurrentProcessId());
			while ((pos = opt.find("$PID", pos)) != opt.npos)
				opt.replace(pos, 4, pid_s.c_str(), pid_s.size());

			result.args += std::string(" --") + i.pItem + "=" + quote(opt);
		}

		result.section = section;
		result.ssection = ssection;
		result.valid = true;
		return result;
	}

	static std::string quote(std::string const& s) {
		std::string q;
		/* Replace every backslash whose next non-backslash character is either the
		end of the string or a double-quote by two backslashes */
		for (int i = 0; i < s.size(); i++) {
			q.append(1, s[i]);
			if (s[i] == '\\') {
				int add = true;
				for (int j = i + 1; j < s.size(); j++) {
					// If any character other than backslash is encountered, do not duplicate.
					// Start with "true" so hitting the end of the string keep "add" true.
					if (s[j] != '\\') {
						add = false;
						break;
					}
				}
				if (add)
					q.append(1, '\\');
			}
		}

		/* Replace every double-quote in the string by backslash double-quote */
		std::size_t pos = 0;
		while ((pos = q.find("\"", pos)) != q.npos)
			q.replace(pos, 1, "\\\"");
		return "\"" + q + "\"";
	}

	///////////////
	// Error handling, etc.
	///////////////
	void errorExit(const char* context) {
		logLastError(context);
		ExitProcess(GetLastError());
	}

	void LogEvent(int wType, std::string message) {
		WriteEventLogEntry(message.c_str(), wType);
		writeLogLine(message);
	}

	void logLastError(const char* context) {
		LogEvent(EVENTLOG_ERROR_TYPE, format("%s failed (%d)", context, GetLastError()));
	}

	std::string confFile;
	bool serviceStopping;
	HANDLE stoppingEvent;
	HANDLE stoppedEvent;
	HANDLE childJob;

	std::unordered_map<uint16_t, Subprocess*> id_subprocess;
	std::vector<Subprocess*> subprocesses;

	void addSubprocess(Subprocess* newsp) {
		newsp->subprocess_index = subprocesses.size();
		subprocesses.push_back(newsp);
		id_subprocess[newsp->id] = newsp;
	}
	void removeSubprocess(Subprocess* sp) {
		subprocesses.back()->subprocess_index = sp->subprocess_index;
		subprocesses[sp->subprocess_index] = subprocesses.back();
		subprocesses.pop_back();
		id_subprocess.erase(sp->id);
	}
};

static FDBService* staticService = nullptr;

bool consoleHandler(int signal) {
	// Break from the program if ctrl-c is pressed
	if ((signal == CTRL_C_EVENT) && (staticService)) {
		staticService->Break();
	}

	return true;
}

void print_usage(const char* name) {
	printf("FoundationDB Process Monitor " SERVICE_NAME " (v" FDB_VT_VERSION ")\n"
	       "Usage: %s [OPTIONS]\n"
	       "\n"
	       "  -f --foreground Run the process in the foreground and not as a service\n"
	       "  -l --logging    Enable logging\n"
	       "  -h, --help      Display this help and exit.\n",
	       name);
}

int main(int argc, LPCSTR* argv) {
	_set_FMA3_enable(
	    0); // Workaround for VS 2013 code generation bug. See
	        // https://connect.microsoft.com/VisualStudio/feedback/details/811093/visual-studio-2013-rtm-c-x64-code-generation-bug-for-avx2-instructions

	int status = 0;
	bool bBackground, bRun;

	// Initialize variables
	bBackground = true;
	bRun = true;

	// Check if argument is specified to run in foreground
	for (DWORD loop = 1; loop < argc; loop++) {
		// Ignore undefined or non-options
		if ((argv[loop] == nullptr) || (argv[loop][0] == '\0')) {

		}

		// Check, if foreground option
		else if ((!_strnicmp("-f", argv[loop], 2)) || (!_strnicmp("--f", argv[loop], 3))) {
			bBackground = false;
		}

		// Check, if logging is enabled
		else if ((!_strnicmp("-l", argv[loop], 2)) || (!_strnicmp("--l", argv[loop], 3))) {
			logging = true;
		}

		// Check, if help is requested
		else if ((!_strnicmp("-h", argv[loop], 2)) || (!_strnicmp("--h", argv[loop], 3))) {
			print_usage(argv[0]);

			// Disable execution, if displaying help
			bRun = false;
		}
	}

	// Only run, if run is still enabled
	if (bRun)
		try {
			// the "start arguments" to the service are passed to the OnStart call, not to here

			if (logging) {
				// Determine the default conf path
				char programData[2048];
				if (!GetEnvironmentVariable("ALLUSERSPROFILE", programData, 2048)) {
					throw GetLastError();
				}
				if (!CreateDirectory(format("%s\\foundationdb", programData).c_str(), nullptr)) {
					if (GetLastError() != ERROR_ALREADY_EXISTS)
						throw GetLastError();
				}
				logFile.open(programData + std::string("\\foundationdb\\servicelog.txt"));
			}

			FDBService service(SERVICE_NAME);

			// If not background, run the service in the foreground
			if (!bBackground) {
				// Store the service class
				staticService = &service;

				// Enable the ctrl handler
				SetConsoleCtrlHandler((PHANDLER_ROUTINE)consoleHandler, TRUE);

				// Run the service
				if (!service.Run()) {
					fprintf(stderr, "Service failed to run w/err 0x%08lx\n", GetLastError());
					status++;
				}
			}

			// Otherwise, run the service within the background using the service manager
			else if (!CServiceBase::Run(service)) {
				fprintf(stderr, "Service failed to run w/err 0x%08lx\n", GetLastError());
				status++;
			}

			else if (logging) {
			}
		} catch (DWORD errno) {
			writeLogLine(format("Service threw exception 0x%08lx\n", errno));
			fprintf(stderr, "Service failed to run w/err 0x%08lx\n", errno);
			status++;
		} catch (...) {
			writeLogLine(format("Service failed with unexpected error (last error: %d)\n", GetLastError()));
			fprintf(stderr, "Service failed with unexpected error (last error: %d)\n", GetLastError());
			status++;
		}

	return status;
}
