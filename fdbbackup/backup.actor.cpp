/*
 * backup.actor.cpp
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

#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/managed_shared_memory.hpp>

#include "flow/flow.h"
#include "flow/FastAlloc.h"
#include "flow/serialize.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"
#include "flow/TLSConfig.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/Status.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/BlobStore.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"

#include "flow/Platform.h"

#include <stdarg.h>
#include <stdio.h>
#include <cinttypes>
#include <algorithm>	// std::transform
#include <string>
#include <iostream>
#include <ctime>
using std::cout;
using std::endl;

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <Windows.h>
#endif
#include <time.h>

#ifdef  __linux__
#include <execinfo.h>
#ifdef ALLOC_INSTRUMENTATION
#include <cxxabi.h>
#endif
#endif

#include "fdbclient/versions.h"

#include "flow/SimpleOpt.h"
#include "flow/actorcompiler.h"  // This must be the last #include.


// Type of program being executed
enum enumProgramExe {
	EXE_AGENT,
	EXE_BACKUP,
	EXE_RESTORE,
	EXE_FASTRESTORE_AGENT,
	EXE_DR_AGENT,
	EXE_DB_BACKUP,
	EXE_UNDEFINED
};

enum enumBackupType {
	BACKUP_UNDEFINED=0, BACKUP_START, BACKUP_MODIFY, BACKUP_STATUS, BACKUP_ABORT, BACKUP_WAIT, BACKUP_DISCONTINUE, BACKUP_PAUSE, BACKUP_RESUME, BACKUP_EXPIRE, BACKUP_DELETE, BACKUP_DESCRIBE, BACKUP_LIST, BACKUP_DUMP, BACKUP_CLEANUP
};

enum enumDBType {
	DB_UNDEFINED=0, DB_START, DB_STATUS, DB_SWITCH, DB_ABORT, DB_PAUSE, DB_RESUME
};

// New fast restore reuses the type from legacy slow restore
enum enumRestoreType {
	RESTORE_UNKNOWN, RESTORE_START, RESTORE_STATUS, RESTORE_ABORT, RESTORE_WAIT
};

//
enum {
	// Backup constants
	OPT_DESTCONTAINER, OPT_SNAPSHOTINTERVAL, OPT_ERRORLIMIT, OPT_NOSTOPWHENDONE,
	OPT_EXPIRE_BEFORE_VERSION, OPT_EXPIRE_BEFORE_DATETIME, OPT_EXPIRE_DELETE_BEFORE_DAYS,
	OPT_EXPIRE_RESTORABLE_AFTER_VERSION, OPT_EXPIRE_RESTORABLE_AFTER_DATETIME, OPT_EXPIRE_MIN_RESTORABLE_DAYS,
	OPT_BASEURL, OPT_BLOB_CREDENTIALS, OPT_DESCRIBE_DEEP, OPT_DESCRIBE_TIMESTAMPS,
	OPT_DUMP_BEGIN, OPT_DUMP_END, OPT_JSON, OPT_DELETE_DATA, OPT_MIN_CLEANUP_SECONDS,
	OPT_USE_PARTITIONED_LOG,

	// Backup and Restore constants
	OPT_TAGNAME, OPT_BACKUPKEYS, OPT_WAITFORDONE,

	// Backup Modify
	OPT_MOD_ACTIVE_INTERVAL, OPT_MOD_VERIFY_UID,

	// Restore constants
	OPT_RESTORECONTAINER, OPT_RESTORE_VERSION, OPT_RESTORE_TIMESTAMP, OPT_PREFIX_ADD, OPT_PREFIX_REMOVE, OPT_RESTORE_CLUSTERFILE_DEST, OPT_RESTORE_CLUSTERFILE_ORIG,

	// Shared constants
	OPT_CLUSTERFILE, OPT_QUIET, OPT_DRYRUN, OPT_FORCE,
	OPT_HELP, OPT_DEVHELP, OPT_VERSION, OPT_PARENTPID, OPT_CRASHONERROR,
	OPT_NOBUFSTDOUT, OPT_BUFSTDOUTERR, OPT_TRACE, OPT_TRACE_DIR,
	OPT_KNOB, OPT_TRACE_LOG_GROUP, OPT_MEMLIMIT, OPT_LOCALITY,

	//DB constants
	OPT_SOURCE_CLUSTER,
	OPT_DEST_CLUSTER,
	OPT_CLEANUP,

	OPT_TRACE_FORMAT,
};

CSimpleOpt::SOption g_rgAgentOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_VERSION, "--version", SO_NONE },
	{ OPT_VERSION, "-v", SO_NONE },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_LOCALITY, "--locality_", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStartOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_WAITFORDONE,      "-w",              SO_NONE },
	{ OPT_WAITFORDONE,      "--waitfordone",   SO_NONE },
	{ OPT_NOSTOPWHENDONE,   "-z",               SO_NONE },
	{ OPT_NOSTOPWHENDONE,   "--no-stop-when-done",SO_NONE },
	{ OPT_DESTCONTAINER,    "-d",               SO_REQ_SEP },
	{ OPT_DESTCONTAINER,    "--destcontainer",  SO_REQ_SEP },
	// Enable "-p" option after GA
	// { OPT_USE_PARTITIONED_LOG, "-p",                 SO_NONE },
	{ OPT_USE_PARTITIONED_LOG, "--partitioned_log_experimental",  SO_NONE },
	{ OPT_SNAPSHOTINTERVAL, "-s",                   SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "--snapshot_interval",  SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_BACKUPKEYS,      "-k",               SO_REQ_SEP },
	{ OPT_BACKUPKEYS,      "--keys",           SO_REQ_SEP },
	{ OPT_DRYRUN,          "-n",               SO_NONE },
	{ OPT_DRYRUN,          "--dryrun",         SO_NONE },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupModifyOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_MOD_VERIFY_UID,  "--verify_uid",     SO_REQ_SEP },
	{ OPT_DESTCONTAINER,    "-d",              SO_REQ_SEP },
	{ OPT_DESTCONTAINER,    "--destcontainer", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "-s",                  SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "--snapshot_interval", SO_REQ_SEP },
	{ OPT_MOD_ACTIVE_INTERVAL, "--active_snapshot_interval", SO_REQ_SEP },

	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStatusOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_ERRORLIMIT,      "-e",               SO_REQ_SEP },
	{ OPT_ERRORLIMIT,      "--errorlimit",     SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_JSON,            "--json",           SO_NONE},
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupAbortOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupCleanupOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,       "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_DELETE_DATA,     "--delete_data",    SO_NONE },
	{ OPT_MIN_CLEANUP_SECONDS, "--min_cleanup_seconds", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDiscontinueOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_WAITFORDONE,      "-w",              SO_NONE },
	{ OPT_WAITFORDONE,      "--waitfordone",   SO_NONE },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupWaitOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_NOSTOPWHENDONE,   "-z",               SO_NONE },
	{ OPT_NOSTOPWHENDONE,   "--no-stop-when-done",SO_NONE },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupPauseOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupExpireOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,	   "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "-d",               SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "--destcontainer",  SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_FORCE,           "-f",               SO_NONE },
	{ OPT_FORCE,           "--force",          SO_NONE },
	{ OPT_EXPIRE_RESTORABLE_AFTER_VERSION,       "--restorable_after_version",               SO_REQ_SEP },
	{ OPT_EXPIRE_RESTORABLE_AFTER_DATETIME,      "--restorable_after_timestamp",             SO_REQ_SEP },
	{ OPT_EXPIRE_BEFORE_VERSION,                 "--expire_before_version",                  SO_REQ_SEP },
	{ OPT_EXPIRE_BEFORE_DATETIME,                "--expire_before_timestamp",                SO_REQ_SEP },
	{ OPT_EXPIRE_MIN_RESTORABLE_DAYS,            "--min_restorable_days",                    SO_REQ_SEP },
	{ OPT_EXPIRE_DELETE_BEFORE_DAYS,             "--delete_before_days",                     SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDeleteOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_DESTCONTAINER,   "-d",               SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "--destcontainer",  SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDescribeOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,     "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "-d",               SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "--destcontainer",  SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_DESCRIBE_DEEP,   "--deep",           SO_NONE },
	{ OPT_DESCRIBE_TIMESTAMPS, "--version_timestamps", SO_NONE },
	{ OPT_JSON,            "--json",           SO_NONE},
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDumpOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE,     "-C",               SO_REQ_SEP },
	{ OPT_CLUSTERFILE,     "--cluster_file",   SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "-d",               SO_REQ_SEP },
	{ OPT_DESTCONTAINER,   "--destcontainer",  SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_DUMP_BEGIN,      "--begin",          SO_REQ_SEP },
	{ OPT_DUMP_END,        "--end",            SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupListOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_BASEURL,         "-b",               SO_REQ_SEP },
	{ OPT_BASEURL,         "--base_url",       SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgRestoreOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_RESTORE_CLUSTERFILE_DEST,     "--dest_cluster_file", SO_REQ_SEP },
	{ OPT_RESTORE_CLUSTERFILE_ORIG,     "--orig_cluster_file", SO_REQ_SEP },
	{ OPT_RESTORE_TIMESTAMP,            "--timestamp",         SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_RESTORECONTAINER,"-r",               SO_REQ_SEP },
	{ OPT_PREFIX_ADD,      "--add_prefix",     SO_REQ_SEP },
	{ OPT_PREFIX_REMOVE,   "--remove_prefix",  SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_BACKUPKEYS,      "-k",               SO_REQ_SEP },
	{ OPT_BACKUPKEYS,      "--keys",           SO_REQ_SEP },
	{ OPT_WAITFORDONE,     "-w",               SO_NONE },
	{ OPT_WAITFORDONE,     "--waitfordone",    SO_NONE },
	{ OPT_RESTORE_VERSION, "--version",        SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "-v",               SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_DRYRUN,          "-n",               SO_NONE },
	{ OPT_DRYRUN,          "--dryrun",         SO_NONE },
	{ OPT_FORCE,           "-f",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBAgentOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER,  "-s",               SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER,  "--source",         SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "-d",               SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "--destination",    SO_REQ_SEP },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_LOCALITY,        "--locality_",      SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBStartOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER,  "-s",               SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER,  "--source",         SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "-d",               SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "--destination",    SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_BACKUPKEYS,      "-k",               SO_REQ_SEP },
	{ OPT_BACKUPKEYS,      "--keys",           SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBStatusOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER,  "-s",               SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER,  "--source",         SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "-d",               SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "--destination",    SO_REQ_SEP },
	{ OPT_ERRORLIMIT,      "-e",               SO_REQ_SEP },
	{ OPT_ERRORLIMIT,      "--errorlimit",     SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBSwitchOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER,  "-s",               SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER,  "--source",         SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "-d",               SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "--destination",    SO_REQ_SEP },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_FORCE,           "-f",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBAbortOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER,  "-s",               SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER,  "--source",         SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "-d",               SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "--destination",    SO_REQ_SEP },
	{ OPT_CLEANUP,         "--cleanup",        SO_NONE },
	{ OPT_TAGNAME,         "-t",               SO_REQ_SEP },
	{ OPT_TAGNAME,         "--tagname",        SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBPauseOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID,      "--parentpid",       SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER,  "-s",               SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER,  "--source",         SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "-d",               SO_REQ_SEP },
	{ OPT_DEST_CLUSTER,    "--destination",    SO_REQ_SEP },
	{ OPT_TRACE,           "--log",            SO_NONE },
	{ OPT_TRACE_DIR,       "--logdir",         SO_REQ_SEP },
	{ OPT_TRACE_FORMAT,    "--trace_format",   SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup",       SO_REQ_SEP },
	{ OPT_QUIET,           "-q",               SO_NONE },
	{ OPT_QUIET,           "--quiet",          SO_NONE },
	{ OPT_VERSION,         "--version",        SO_NONE },
	{ OPT_VERSION,         "-v",               SO_NONE },
	{ OPT_CRASHONERROR,    "--crash",          SO_NONE },
	{ OPT_MEMLIMIT,        "-m",               SO_REQ_SEP },
	{ OPT_MEMLIMIT,        "--memory",         SO_REQ_SEP },
	{ OPT_HELP,            "-?",               SO_NONE },
	{ OPT_HELP,            "-h",               SO_NONE },
	{ OPT_HELP,            "--help",           SO_NONE },
	{ OPT_DEVHELP,         "--dev-help",       SO_NONE },
	{ OPT_KNOB,            "--knob_",          SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	SO_END_OF_OPTIONS
};

const KeyRef exeAgent = LiteralStringRef("backup_agent");
const KeyRef exeBackup = LiteralStringRef("fdbbackup");
const KeyRef exeRestore = LiteralStringRef("fdbrestore");
const KeyRef exeFastRestoreAgent = LiteralStringRef("fastrestore_agent"); // must be lower case
const KeyRef exeDatabaseAgent = LiteralStringRef("dr_agent");
const KeyRef exeDatabaseBackup = LiteralStringRef("fdbdr");

extern const char* getSourceVersion();

#ifdef _WIN32
void parentWatcher(void *parentHandle) {
	HANDLE parent = (HANDLE)parentHandle;
	int signal = WaitForSingleObject(parent, INFINITE);
	CloseHandle(parentHandle);
	if (signal == WAIT_OBJECT_0)
		criticalError(FDB_EXIT_SUCCESS, "ParentProcessExited", "Parent process exited");
	TraceEvent(SevError, "ParentProcessWaitFailed").detail("RetCode", signal).GetLastError();
}

#endif

static void printVersion() {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("source version %s\n", getSourceVersion());
	printf("protocol %llx\n", (long long) currentProtocolVersion.version());
}

const char *BlobCredentialInfo =
	"  BLOB CREDENTIALS\n"
	"     Blob account secret keys can optionally be omitted from blobstore:// URLs, in which case they will be\n"
	"     loaded, if possible, from 1 or more blob credentials definition files.\n\n"
	"     These files can be specified with the --blob_credentials argument described above or via the environment variable\n"
	"     FDB_BLOB_CREDENTIALS, whose value is a colon-separated list of files.  The command line takes priority over\n"
	"     over the environment but all files from both sources are used.\n\n"
	"     At connect time, the specified files are read in order and the first matching account specification (user@host)\n"
	"     will be used to obtain the secret key.\n\n"
	"     The JSON schema is:\n"
	"        { \"accounts\" : { \"user@host\" : { \"secret\" : \"SECRETKEY\" }, \"user2@host2\" : { \"secret\" : \"SECRET\" } } }\n";

static void printHelpTeaser( const char *name ) {
	fprintf(stderr, "Try `%s --help' for more information.\n", name);
}

static void printAgentUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [OPTIONS]\n\n", exeAgent.toString().c_str());
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
		   "                 FoundationDB cluster. The default is first the value of the\n"
		   "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
		   "                 then `%s'.\n", platform::getDefaultClusterFilePath().c_str());
	printf("  --log          Enables trace file logging for the CLI session.\n"
		   "  --logdir PATH  Specifes the output directory for trace files. If\n"
		   "                 unspecified, defaults to the current directory. Has\n"
		   "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
		   "                 Select the format of the trace files. xml (the default) and json are supported.\n"
		   "                 Has no effect unless --log is specified.\n");
	printf("  -m SIZE, --memory SIZE\n"
		   "                 Memory limit. The default value is 8GiB. When specified\n"
		   "                 without a unit, MiB is assumed.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");

	if (devhelp) {
#ifdef _WIN32
		printf("  -n             Create a new console.\n");
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
	}

	printf("\n");
	puts(BlobCredentialInfo);

	return;
}

void printBackupContainerInfo() {
	printf("                 Backup URL forms:\n\n");
	std::vector<std::string> formats = IBackupContainer::getURLFormats();
	for(auto &f : formats)
		printf("                     %s\n", f.c_str());
	printf("\n");
}

static void printBackupUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s (start | status | abort | wait | discontinue | pause | resume | expire | delete | describe | list | cleanup) [OPTIONS]\n\n", exeBackup.toString().c_str());
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
		   "                 FoundationDB cluster. The default is first the value of the\n"
		   "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
		   "                 then `%s'.\n", platform::getDefaultClusterFilePath().c_str());
	printf("  -d, --destcontainer URL\n"
	       "                 The Backup container URL for start, modify, describe, expire, and delete operations.\n");
	printBackupContainerInfo();
	printf("  -b, --base_url BASEURL\n"
		   "                 Base backup URL for list operations.  This looks like a Backup URL but without a backup name.\n");
	printf("  --blob_credentials FILE\n"
		   "                 File containing blob credentials in JSON format.  Can be specified multiple times for multiple files.  See below for more details.\n");
	printf("  --expire_before_timestamp DATETIME\n"
		   "                 Datetime cutoff for expire operations.  Requires a cluster file and will use version/timestamp metadata\n"
		   "                 in the database to obtain a cutoff version very close to the timestamp given in %s.\n", BackupAgentBase::timeFormat().c_str());
	printf("  --expire_before_version VERSION\n"
	       "                 Version cutoff for expire operations.  Deletes data files containing no data at or after VERSION.\n");
	printf("  --delete_before_days NUM_DAYS\n"
		   "                 Another way to specify version cutoff for expire operations.  Deletes data files containing no data at or after a\n"
		   "                 version approximately NUM_DAYS days worth of versions prior to the latest log version in the backup.\n");
	printf("  --restorable_after_timestamp DATETIME\n"
		   "                 For expire operations, set minimum acceptable restorability to the version equivalent of DATETIME and later.\n");
	printf("  --restorable_after_version VERSION\n"
		   "                 For expire operations, set minimum acceptable restorability to the VERSION and later.\n");
	printf("  --min_restorable_days NUM_DAYS\n"
		   "                 For expire operations, set minimum acceptable restorability to approximately NUM_DAYS days worth of versions\n"
		   "                 prior to the latest log version in the backup.\n");
	printf("  --version_timestamps\n");
	printf("                 For describe operations, lookup versions in the database to obtain timestamps.  A cluster file is required.\n");
	printf("  -f, --force    For expire operations, force expiration even if minimum restorability would be violated.\n");
	printf("  -s, --snapshot_interval DURATION\n"
	       "                 For start or modify operations, specifies the backup's default target snapshot interval as DURATION seconds.  Defaults to %d for start operations.\n", CLIENT_KNOBS->BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC);
	printf("  --active_snapshot_interval DURATION\n"
	       "                 For modify operations, sets the desired interval for the backup's currently active snapshot, relative to the start of the snapshot.\n");
	printf("  --verify_uid UID\n"
	       "                 Specifies a UID to verify against the BackupUID of the running backup.  If provided, the UID is verified in the same transaction\n"
	       "                 which sets the new backup parameters (if the UID matches).\n");
	printf("  -e ERRORLIMIT  The maximum number of errors printed by status (default is 10).\n");
	printf("  -k KEYS        List of key ranges to backup.\n"
		   "                 If not specified, the entire database will be backed up.\n");
	printf("  --partitioned_log_experimental  Starts with new type of backup system using partitioned logs.\n");
	printf("  -n, --dryrun   For backup start or restore start, performs a trial run with no actual changes made.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
		   "  --logdir PATH  Specifes the output directory for trace files. If\n"
		   "                 unspecified, defaults to the current directory. Has\n"
		   "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
		   "                 Select the format of the trace files. xml (the default) and json are supported.\n"
		   "                 Has no effect unless --log is specified.\n");
	printf("  --max_cleanup_seconds SECONDS\n"
	       "                 Specifies the amount of time a backup or DR needs to be stale before cleanup will\n"
	       "                 remove mutations for it. By default this is set to one hour.\n");
	printf("  --delete_data\n"
		   "                 This flag will cause cleanup to remove mutations for the most stale backup or DR.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -w, --wait     Wait for the backup to complete (allowed with `start' and `discontinue').\n");
	printf("  -z, --no-stop-when-done\n"
		   "                 Do not stop backup when restorable.\n");
	printf("  -h, --help     Display this help and exit.\n");

	if (devhelp) {
#ifdef _WIN32
		printf("  -n             Create a new console.\n");
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
		printf("  --deep         For describe operations, do not use cached metadata.  Warning: Very slow\n");

	}
	printf("\n"
		   "  KEYS FORMAT:   \"<BEGINKEY> <ENDKEY>\" [...]\n");
	printf("\n");
	puts(BlobCredentialInfo);

	return;
}

static void printRestoreUsage(bool devhelp ) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s (start | status | abort | wait) [OPTIONS]\n\n", exeRestore.toString().c_str());
	//printf("  FOLDERS        Paths to folders containing the backup files.\n");
	printf("Options for all commands:\n\n");
	printf("  --dest_cluster_file CONNFILE\n");
	printf("                 The cluster file to restore data into.\n");
	printf("  -t, --tagname TAGNAME\n");
	printf("                 The restore tag to act on.  Default is 'default'\n");
	printf("Options for start:\n\n");
	printf("  -r URL         The Backup URL for the restore to read from.\n");
	printBackupContainerInfo();
	printf("  -w, --waitfordone\n");
	printf("                 Wait for the restore to complete before exiting.  Prints progress updates.\n");
	printf("  -k KEYS        List of key ranges from the backup to restore.\n");
	printf("  --remove_prefix PREFIX\n");
	printf("                 Prefix to remove from the restored keys.\n");
	printf("  --add_prefix PREFIX\n");
	printf("                 Prefix to add to the restored keys\n");
	printf("  -n, --dryrun   Perform a trial run with no changes made.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
		   "  --logdir PATH  Specifes the output directory for trace files. If\n"
		   "                 unspecified, defaults to the current directory. Has\n"
		   "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
		   "                 Select the format of the trace files. xml (the default) and json are supported.\n"
		   "                 Has no effect unless --log is specified.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
	printf("  -v DBVERSION   The version at which the database will be restored.\n");
	printf("  --timestamp    Instead of a numeric version, use this to specify a timestamp in %s\n", BackupAgentBase::timeFormat().c_str());
	printf("                 and it will be converted to a version from that time using metadata in orig_cluster_file.\n");
	printf("  --orig_cluster_file CONNFILE\n");
	printf("                 The cluster file for the original database from which the backup was created.  The original database\n");
	printf("                 is only needed to convert a --timestamp argument to a database version.\n");
	printf("  -h, --help     Display this help and exit.\n");

	if( devhelp ) {
#ifdef _WIN32
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
	}

	printf("\n"
		   "  KEYS FORMAT:   \"<BEGINKEY> <ENDKEY>\" [...]\n");
	printf("\n");
	puts(BlobCredentialInfo);

	return;
}

static void printFastRestoreUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s (start | status | abort | wait) [OPTIONS]\n\n", exeRestore.toString().c_str());
	// printf("  FOLDERS        Paths to folders containing the backup files.\n");
	printf("Options for all commands:\n\n");
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is first the value of the\n"
	       "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
	       "                 then `%s'.\n",
	       platform::getDefaultClusterFilePath().c_str());
	printf("  -t TAGNAME     The restore tag to act on.  Default is 'default'\n");
	printf("    --tagname TAGNAME\n\n");
	printf(" Options for start:\n\n");
	printf("  -r URL         The Backup URL for the restore to read from.\n");
	printBackupContainerInfo();
	printf("  -w             Wait for the restore to complete before exiting.  Prints progress updates.\n");
	printf("    --waitfordone\n");
	printf("  -k KEYS        List of key ranges from the backup to restore\n");
	printf("  --remove_prefix PREFIX   prefix to remove from the restored keys\n");
	printf("  --add_prefix PREFIX      prefix to add to the restored keys\n");
	printf("  -n, --dry-run  Perform a trial run with no changes made.\n");
	printf("  -v DBVERSION   The version at which the database will be restored.\n");
	printf("  -h, --help     Display this help and exit.\n");
	printf("NOTE: Fast restore is still under development. The options may not be fully supported.\n");

	if (devhelp) {
#ifdef _WIN32
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
	}

	printf("\n"
	       "  KEYS FORMAT:   \"<BEGINKEY> <ENDKEY>\" [...]\n");
	printf("\n");
	puts(BlobCredentialInfo);

	return;
}

static void printDBAgentUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [OPTIONS]\n\n", exeDatabaseAgent.toString().c_str());
	printf("  -d CONNFILE    The path of a file containing the connection string for the\n"
		   "                 destination FoundationDB cluster.\n");
	printf("  -s CONNFILE    The path of a file containing the connection string for the\n"
		   "                 source FoundationDB cluster.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
		   "  --logdir PATH  Specifes the output directory for trace files. If\n"
		   "                 unspecified, defaults to the current directory. Has\n"
		   "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
		   "                 Select the format of the trace files. xml (the default) and json are supported.\n"
		   "                 Has no effect unless --log is specified.\n");
	printf("  -m SIZE, --memory SIZE\n"
		   "                 Memory limit. The default value is 8GiB. When specified\n"
		   "                 without a unit, MiB is assumed.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");
	if (devhelp) {
#ifdef _WIN32
		printf("  -n             Create a new console.\n");
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
	}

	return;
}

static void printDBBackupUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s (start | status | switch | abort | pause | resume) [OPTIONS]\n\n", exeDatabaseBackup.toString().c_str());
	printf("  -d, --destination CONNFILE\n"
	       "                 The path of a file containing the connection string for the\n");
	printf("                 destination FoundationDB cluster.\n");
	printf("  -s, --source CONNFILE\n"
	       "                 The path of a file containing the connection string for the\n"
		   "                 source FoundationDB cluster.\n");
	printf("  -e ERRORLIMIT  The maximum number of errors printed by status (default is 10).\n");
	printf("  -k KEYS        List of key ranges to backup.\n"
		   "                 If not specified, the entire database will be backed up.\n");
	printf("  --cleanup      Abort will attempt to stop mutation logging on the source cluster.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
	printf("  --log          Enables trace file logging for the CLI session.\n"
		   "  --logdir PATH  Specifes the output directory for trace files. If\n"
		   "                 unspecified, defaults to the current directory. Has\n"
		   "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace_format FORMAT\n"
		   "                 Select the format of the trace files. xml (the default) and json are supported.\n"
		   "                 Has no effect unless --log is specified.\n");
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");
	printf("\n"
		   "  KEYS FORMAT:   \"<BEGINKEY> <ENDKEY>\" [...]\n");

	if (devhelp) {
#ifdef _WIN32
		printf("  -n             Create a new console.\n");
		printf("  -q             Disable error dialog on crash.\n");
		printf("  --parentpid PID\n");
		printf("                 Specify a process after whose termination to exit.\n");
#endif
	}

	return;
}

static void printUsage(enumProgramExe programExe, bool devhelp)
{

	switch (programExe)
	{
	case EXE_AGENT:
		printAgentUsage(devhelp);
		break;
	case EXE_BACKUP:
		printBackupUsage(devhelp);
		break;
	case EXE_RESTORE:
		printRestoreUsage(devhelp);
		break;
	case EXE_FASTRESTORE_AGENT:
		printFastRestoreUsage(devhelp);
		break;
	case EXE_DR_AGENT:
		printDBAgentUsage(devhelp);
		break;
	case EXE_DB_BACKUP:
		printDBBackupUsage(devhelp);
		break;
	case EXE_UNDEFINED:
	default:
		break;
	}

	return;
}

extern bool g_crashOnError;

// Return the type of program executable based on the name of executable file
enumProgramExe	getProgramType(std::string programExe)
{
	enumProgramExe	enProgramExe = EXE_UNDEFINED;

	// lowercase the string
	std::transform(programExe.begin(), programExe.end(), programExe.begin(), ::tolower);

	// Remove the extension, if Windows
#ifdef _WIN32
	size_t lastDot = programExe.find_last_of(".");
	if (lastDot != std::string::npos) {
		size_t lastSlash = programExe.find_last_of("\\");

		// Ensure last dot is after last slash, if present
		if ((lastSlash == std::string::npos)||
			(lastSlash < lastDot)			)
		{
			programExe = programExe.substr(0, lastDot);
		}
	}
#endif
	// For debugging convenience, remove .debug suffix if present.
	if(StringRef(programExe).endsWith(LiteralStringRef(".debug")))
		programExe = programExe.substr(0, programExe.size() - 6);

	// Check if backup agent
	if ((programExe.length() >= exeAgent.size())																		&&
		(programExe.compare(programExe.length()-exeAgent.size(), exeAgent.size(), (const char*) exeAgent.begin()) == 0)	)
	{
		enProgramExe = EXE_AGENT;
	}

	// Check if backup
	else if ((programExe.length() >= exeBackup.size())																	&&
		(programExe.compare(programExe.length() - exeBackup.size(), exeBackup.size(), (const char*)exeBackup.begin()) == 0))
	{
		enProgramExe = EXE_BACKUP;
	}

	// Check if restore
	else if ((programExe.length() >= exeRestore.size())																		&&
		(programExe.compare(programExe.length() - exeRestore.size(), exeRestore.size(), (const char*)exeRestore.begin()) == 0))
	{
		enProgramExe = EXE_RESTORE;
	}

	// Check if restore
	else if ((programExe.length() >= exeFastRestoreAgent.size()) &&
	         (programExe.compare(programExe.length() - exeFastRestoreAgent.size(), exeFastRestoreAgent.size(),
	                             (const char*)exeFastRestoreAgent.begin()) == 0)) {
		enProgramExe = EXE_FASTRESTORE_AGENT;
	}

	// Check if db agent
	else if ((programExe.length() >= exeDatabaseAgent.size()) &&
	         (programExe.compare(programExe.length() - exeDatabaseAgent.size(), exeDatabaseAgent.size(),
	                             (const char*)exeDatabaseAgent.begin()) == 0)) {
		enProgramExe = EXE_DR_AGENT;
	}

	// Check if db backup
	else if ((programExe.length() >= exeDatabaseBackup.size()) &&
	         (programExe.compare(programExe.length() - exeDatabaseBackup.size(), exeDatabaseBackup.size(),
	                             (const char*)exeDatabaseBackup.begin()) == 0)) {
		enProgramExe = EXE_DB_BACKUP;
	}

	return enProgramExe;
}

enumBackupType	getBackupType(std::string backupType)
{
	enumBackupType	enBackupType = BACKUP_UNDEFINED;

	// lowercase the string
	std::transform(backupType.begin(), backupType.end(), backupType.begin(), ::tolower);

	static std::map<std::string, enumBackupType> values;
	if(values.empty()) {
		values["start"] = BACKUP_START;
		values["status"] = BACKUP_STATUS;
		values["abort"] = BACKUP_ABORT;
		values["cleanup"] = BACKUP_CLEANUP;
		values["wait"] = BACKUP_WAIT;
		values["discontinue"] = BACKUP_DISCONTINUE;
		values["pause"] = BACKUP_PAUSE;
		values["resume"] = BACKUP_RESUME;
		values["expire"] = BACKUP_EXPIRE;
		values["delete"] = BACKUP_DELETE;
		values["describe"] = BACKUP_DESCRIBE;
		values["list"] = BACKUP_LIST;
		values["dump"] = BACKUP_DUMP;
		values["modify"] = BACKUP_MODIFY;
	}

	auto i = values.find(backupType);
	if(i != values.end())
		enBackupType = i->second;

	return enBackupType;
}

enumRestoreType getRestoreType(std::string name) {
	if(name == "start") return RESTORE_START;
	if(name == "abort") return RESTORE_ABORT;
	if(name == "status") return RESTORE_STATUS;
	if(name == "wait") return RESTORE_WAIT;
	return RESTORE_UNKNOWN;
}

enumDBType getDBType(std::string dbType)
{
	enumDBType enBackupType = DB_UNDEFINED;

	// lowercase the string
	std::transform(dbType.begin(), dbType.end(), dbType.begin(), ::tolower);

	static std::map<std::string, enumDBType> values;
	if(values.empty()) {
		values["start"] = DB_START;
		values["status"] = DB_STATUS;
		values["switch"] = DB_SWITCH;
		values["abort"] = DB_ABORT;
		values["pause"] = DB_PAUSE;
		values["resume"] = DB_RESUME;
	}

	auto i = values.find(dbType);
	if(i != values.end())
		enBackupType = i->second;

	return enBackupType;
}

ACTOR Future<std::string> getLayerStatus(Reference<ReadYourWritesTransaction> tr, std::string name, std::string id, enumProgramExe exe, Database dest, bool snapshot = false) {
	// This process will write a document that looks like this:
	// { backup : { $expires : {<subdoc>}, version: <version from approximately 30 seconds from now> }
	// so that the value under 'backup' will eventually expire to null and thus be ignored by
	// readers of status.  This is because if all agents die then they can no longer clean up old
	// status docs from other dead agents.

	state Version readVer = wait(tr->getReadVersion());

	state json_spirit::mValue layersRootValue;         // Will contain stuff that goes into the doc at the layers status root
	JSONDoc layersRoot(layersRootValue);               // Convenient mutator / accessor for the layers root
	JSONDoc op = layersRoot.subDoc(name);              // Operator object for the $expires operation
	// Create the $expires key which is where the rest of the status output will go

	state JSONDoc layerRoot = op.subDoc("$expires");
	// Set the version argument in the $expires operator object.
	op.create("version") = readVer + 120 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND;

	layerRoot.create("instances_running.$sum") = 1;
	layerRoot.create("last_updated.$max") = now();

	state JSONDoc o = layerRoot.subDoc("instances." + id);

	o.create("version") = FDB_VT_VERSION;
	o.create("id")      = id;
	o.create("last_updated") = now();
	o.create("memory_usage")  = (int64_t)getMemoryUsage();
	o.create("resident_size") = (int64_t)getResidentMemoryUsage();
	o.create("main_thread_cpu_seconds") = getProcessorTimeThread();
	o.create("process_cpu_seconds")     = getProcessorTimeProcess();
	o.create("configured_workers") = CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;

	if(exe == EXE_AGENT) {
		static BlobStoreEndpoint::Stats last_stats;
		static double last_ts = 0;
		BlobStoreEndpoint::Stats current_stats = BlobStoreEndpoint::s_stats;
		JSONDoc blobstats = o.create("blob_stats");
		blobstats.create("total") = current_stats.getJSON();
		BlobStoreEndpoint::Stats diff = current_stats - last_stats;
		json_spirit::mObject diffObj = diff.getJSON();
		if(last_ts > 0)
			diffObj["bytes_per_second"] = double(current_stats.bytes_sent - last_stats.bytes_sent) / (now() - last_ts);
		blobstats.create("recent") = diffObj;
		last_stats = current_stats;
		last_ts = now();

		JSONDoc totalBlobStats = layerRoot.subDoc("blob_recent_io");
		for(auto &p : diffObj)
			totalBlobStats.create(p.first + ".$sum") = p.second;

		state FileBackupAgent fba;
		state std::vector<KeyBackedTag> backupTags = wait(getAllBackupTags(tr, snapshot));
		state std::vector<Future<Version>> tagLastRestorableVersions;
		state std::vector<Future<EBackupState>> tagStates;
		state std::vector<Future<Reference<IBackupContainer>>> tagContainers;
		state std::vector<Future<int64_t>> tagRangeBytes;
		state std::vector<Future<int64_t>> tagLogBytes;
		state Future<Optional<Value>> fBackupPaused = tr->get(fba.taskBucket->getPauseKey(), snapshot);

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state std::vector<KeyBackedTag>::iterator tag;
		state std::vector<UID> backupTagUids;
		for (tag = backupTags.begin(); tag != backupTags.end(); tag++) {
			UidAndAbortedFlagT uidAndAbortedFlag = wait(tag->getOrThrow(tr, snapshot));
			BackupConfig config(uidAndAbortedFlag.first);
			backupTagUids.push_back(config.getUid());

			tagStates.push_back(config.stateEnum().getOrThrow(tr, snapshot));
			tagRangeBytes.push_back(config.rangeBytesWritten().getD(tr, snapshot, 0));
			tagLogBytes.push_back(config.logBytesWritten().getD(tr, snapshot, 0));
			tagContainers.push_back(config.backupContainer().getOrThrow(tr, snapshot));
			tagLastRestorableVersions.push_back(fba.getLastRestorable(tr, StringRef(tag->tagName), snapshot));
		}

		wait( waitForAll(tagLastRestorableVersions) && waitForAll(tagStates) && waitForAll(tagContainers) && waitForAll(tagRangeBytes) && waitForAll(tagLogBytes) && success(fBackupPaused));

		JSONDoc tagsRoot = layerRoot.subDoc("tags.$latest");
		layerRoot.create("tags.timestamp") = now();
		layerRoot.create("total_workers.$sum") = fBackupPaused.get().present() ? 0 : CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
		layerRoot.create("paused.$latest") = fBackupPaused.get().present();

		int j = 0;
		for (KeyBackedTag eachTag : backupTags) {
			Version last_restorable_version = tagLastRestorableVersions[j].get();
			double last_restorable_seconds_behind = ((double)readVer - last_restorable_version) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
			BackupAgentBase::enumState status = (BackupAgentBase::enumState)tagStates[j].get();
			const char *statusText = fba.getStateText(status);

			// The object for this backup tag inside this instance's subdocument
			JSONDoc tagRoot = tagsRoot.subDoc(eachTag.tagName);
			tagRoot.create("current_container") = tagContainers[j].get()->getURL();
			tagRoot.create("current_status") = statusText;
			tagRoot.create("last_restorable_version") = tagLastRestorableVersions[j].get();
			tagRoot.create("last_restorable_seconds_behind") = last_restorable_seconds_behind;
			tagRoot.create("running_backup") = (status == BackupAgentBase::STATE_RUNNING_DIFFERENTIAL || status == BackupAgentBase::STATE_RUNNING);
			tagRoot.create("running_backup_is_restorable") = (status == BackupAgentBase::STATE_RUNNING_DIFFERENTIAL);
			tagRoot.create("range_bytes_written") = tagRangeBytes[j].get();
			tagRoot.create("mutation_log_bytes_written") = tagLogBytes[j].get();
			tagRoot.create("mutation_stream_id") = backupTagUids[j].toString();

			j++;
		}
	}
	else if(exe == EXE_DR_AGENT) {
		state DatabaseBackupAgent dba;
		state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(dest));
		tr2->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr2->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Standalone<RangeResultRef> tagNames = wait(tr2->getRange(dba.tagNames.range(), 10000, snapshot));
		state std::vector<Future<Optional<Key>>> backupVersion;
		state std::vector<Future<int>> backupStatus;
		state std::vector<Future<int64_t>> tagRangeBytesDR;
		state std::vector<Future<int64_t>> tagLogBytesDR;
		state Future<Optional<Value>> fDRPaused = tr->get(dba.taskBucket->getPauseKey(), snapshot);

		state std::vector<UID> drTagUids;
		for(int i = 0; i < tagNames.size(); i++) {
			backupVersion.push_back(tr2->get(tagNames[i].value.withPrefix(applyMutationsBeginRange.begin), snapshot));
			UID tagUID = BinaryReader::fromStringRef<UID>(tagNames[i].value, Unversioned());
			drTagUids.push_back(tagUID);
			backupStatus.push_back(dba.getStateValue(tr2, tagUID, snapshot));
			tagRangeBytesDR.push_back(dba.getRangeBytesWritten(tr2, tagUID, snapshot));
			tagLogBytesDR.push_back(dba.getLogBytesWritten(tr2, tagUID, snapshot));
		}

		wait(waitForAll(backupStatus) && waitForAll(backupVersion) && waitForAll(tagRangeBytesDR) && waitForAll(tagLogBytesDR) && success(fDRPaused));

		JSONDoc tagsRoot = layerRoot.subDoc("tags.$latest");
		layerRoot.create("tags.timestamp") = now();
		layerRoot.create("total_workers.$sum") = fDRPaused.get().present() ? 0 : CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
		layerRoot.create("paused.$latest") = fDRPaused.get().present();

		for (int i = 0; i < tagNames.size(); i++) {
			std::string tagName = dba.sourceTagNames.unpack(tagNames[i].key).getString(0).toString();

			BackupAgentBase::enumState status = (BackupAgentBase::enumState)backupStatus[i].get();

			JSONDoc tagRoot = tagsRoot.create(tagName);
			tagRoot.create("running_backup") = (status == BackupAgentBase::STATE_RUNNING_DIFFERENTIAL || status == BackupAgentBase::STATE_RUNNING);
			tagRoot.create("running_backup_is_restorable") = (status == BackupAgentBase::STATE_RUNNING_DIFFERENTIAL);
			tagRoot.create("range_bytes_written") = tagRangeBytesDR[i].get();
			tagRoot.create("mutation_log_bytes_written") = tagLogBytesDR[i].get();
			tagRoot.create("mutation_stream_id") = drTagUids[i].toString();

			if (backupVersion[i].get().present()) {
				double seconds_behind = ((double)readVer - BinaryReader::fromStringRef<Version>(backupVersion[i].get().get(), Unversioned())) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
				tagRoot.create("seconds_behind") = seconds_behind;
				//TraceEvent("BackupMetrics").detail("SecondsBehind", seconds_behind);
			}

			tagRoot.create("backup_state") = BackupAgentBase::getStateText(status);
		}
	}

	std::string json = json_spirit::write_string(layersRootValue);
	return json;
}

// Check for unparseable or expired statuses and delete them.
// First checks the first doc in the key range, and if it is valid, alive and not "me" then
// returns.  Otherwise, checks the rest of the range as well.
ACTOR Future<Void> cleanupStatus(Reference<ReadYourWritesTransaction> tr, std::string rootKey, std::string name, std::string id, int limit = 1) {
	state Standalone<RangeResultRef> docs = wait(tr->getRange(KeyRangeRef(rootKey, strinc(rootKey)), limit, true));
	state bool readMore = false;
	state int i;
	for(i = 0; i < docs.size(); ++i) {
		json_spirit::mValue docValue;
		try {
			json_spirit::read_string(docs[i].value.toString(), docValue);
			JSONDoc doc(docValue);
			// Update the reference version for $expires
			JSONDoc::expires_reference_version = tr->getReadVersion().get();
			// Evaluate the operators in the document, which will reduce to nothing if it is expired.
			doc.cleanOps();
			if(!doc.has(name + ".last_updated"))
				throw Error();

			// Alive and valid.
			// If limit == 1 and id is present then read more
			if(limit == 1 && doc.has(name + ".instances." + id))
				readMore = true;
		} catch(Error &e) {
			// If doc can't be parsed or isn't alive, delete it.
			TraceEvent(SevWarn, "RemovedDeadBackupLayerStatus").detail("Key", docs[i].key);
			tr->clear(docs[i].key);
			// If limit is 1 then read more.
			if(limit == 1)
				readMore = true;
		}
		if(readMore) {
			limit = 10000;
			Standalone<RangeResultRef> docs2 = wait(tr->getRange(KeyRangeRef(rootKey, strinc(rootKey)), limit, true));
			docs = std::move(docs2);
			readMore = false;
		}
	}

	return Void();
}

// Get layer status document for just this layer
ACTOR Future<json_spirit::mObject> getLayerStatus(Database src, std::string rootKey) {
	state Transaction tr(src);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Standalone<RangeResultRef> kvPairs =
			    wait(tr.getRange(KeyRangeRef(rootKey, strinc(rootKey)), GetRangeLimits::ROW_LIMIT_UNLIMITED));
			json_spirit::mObject statusDoc;
			JSONDoc modifier(statusDoc);
			for(auto &kv : kvPairs) {
				json_spirit::mValue docValue;
				json_spirit::read_string(kv.value.toString(), docValue);
				modifier.absorb(docValue);
			}
			JSONDoc::expires_reference_version = (uint64_t)tr.getReadVersion().get();
			modifier.cleanOps();
			return statusDoc;
		}
		catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Read layer status for this layer and get the total count of agent processes (instances) then adjust the poll delay based on that and BACKUP_AGGREGATE_POLL_RATE
ACTOR Future<Void> updateAgentPollRate(Database src, std::string rootKey, std::string name, double *pollDelay) {
	loop {
		try {
			json_spirit::mObject status = wait(getLayerStatus(src, rootKey));
			int64_t processes = 0;
			// If instances count is present and greater than 0 then update pollDelay
			if(JSONDoc(status).tryGet<int64_t>(name + ".instances_running", processes) && processes > 0) {
				// The aggregate poll rate is the target poll rate for all agent processes in the cluster
				// The poll rate (polls/sec) for a single processes is aggregate poll rate / processes, and pollDelay is the inverse of that
				*pollDelay = (double)processes / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
			}
		} catch(Error &e) {
			TraceEvent(SevWarn, "BackupAgentPollRateUpdateError").error(e);
		}
		wait(delay(CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE_UPDATE_INTERVAL));
	}
}

ACTOR Future<Void> statusUpdateActor(Database statusUpdateDest, std::string name, enumProgramExe exe, double *pollDelay, Database taskDest = Database(), 
										std::string id = nondeterministicRandom()->randomUniqueID().toString()) {
	state std::string metaKey = layerStatusMetaPrefixRange.begin.toString() + "json/" + name;
	state std::string rootKey = backupStatusPrefixRange.begin.toString() + name + "/json";
	state std::string instanceKey = rootKey + "/" + "agent-" + id;
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(statusUpdateDest));
	state Future<Void> pollRateUpdater;

	// Register the existence of this layer in the meta key space
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->set(metaKey, rootKey);
			wait(tr->commit());
			break;
		}
		catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// Write status periodically
	loop {
		tr->reset();
		try {
			loop{
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					state Future<std::string> futureStatusDoc = getLayerStatus(tr, name, id, exe, taskDest, true);
					wait(cleanupStatus(tr, rootKey, name, id));
					std::string statusdoc = wait(futureStatusDoc);
					tr->set(instanceKey, statusdoc);
					wait(tr->commit());
					break;
				}
				catch (Error& e) {
					wait(tr->onError(e));
				}
			}

			wait(delay(CLIENT_KNOBS->BACKUP_STATUS_DELAY * ( ( 1.0 - CLIENT_KNOBS->BACKUP_STATUS_JITTER ) + 2 * deterministicRandom()->random01() * CLIENT_KNOBS->BACKUP_STATUS_JITTER )));

			// Now that status was written at least once by this process (and hopefully others), start the poll rate control updater if it wasn't started yet
			if(!pollRateUpdater.isValid() && pollDelay != nullptr)
				pollRateUpdater = updateAgentPollRate(statusUpdateDest, rootKey, name, pollDelay);
		}
		catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToWriteStatus").error(e);
			wait(delay(10.0));
		}
	}
}

ACTOR Future<Void> runDBAgent(Database src, Database dest) {
	state double pollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
	std::string id = nondeterministicRandom()->randomUniqueID().toString();
	state Future<Void> status = statusUpdateActor(src, "dr_backup", EXE_DR_AGENT, &pollDelay, dest, id);
	state Future<Void> status_other = statusUpdateActor(dest, "dr_backup_dest", EXE_DR_AGENT, &pollDelay, dest, id);

	state DatabaseBackupAgent backupAgent(src);

	loop {
		try {
			wait(backupAgent.run(dest, &pollDelay, CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT));
			break;
		}
		catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				throw;

			TraceEvent(SevError, "DA_runAgent").error(e);
			fprintf(stderr, "ERROR: DR agent encountered fatal error `%s'\n", e.what());

			wait( delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
		}
	}

	return Void();
}

ACTOR Future<Void> runAgent(Database db) {
	state double pollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
	state Future<Void> status = statusUpdateActor(db, "backup", EXE_AGENT, &pollDelay);

	state FileBackupAgent backupAgent;

	loop {
		try {
			wait(backupAgent.run(db, &pollDelay, CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT));
			break;
		}
		catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				throw;

			TraceEvent(SevError, "BA_runAgent").error(e);
			fprintf(stderr, "ERROR: backup agent encountered fatal error `%s'\n", e.what());

			wait( delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY) );
		}
	}

	return Void();
}

ACTOR Future<Void> submitDBBackup(Database src, Database dest, Standalone<VectorRef<KeyRangeRef>> backupRanges, std::string tagName) {
	try
	{
		state DatabaseBackupAgent backupAgent(src);

		// Backup everything, if no ranges were specified
		if (backupRanges.size() == 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		}


		wait(backupAgent.submitBackup(dest, KeyRef(tagName), backupRanges, false, StringRef(), StringRef(), true));

		// Check if a backup agent is running
		bool agentRunning = wait(backupAgent.checkActive(dest));

		if (!agentRunning) {
			printf("The DR on tag `%s' was successfully submitted but no DR agents are responding.\n", printable(StringRef(tagName)).c_str());

			// Throw an error that will not display any additional information
			throw actor_cancelled();
		}
		else {
			printf("The DR on tag `%s' was successfully submitted.\n", printable(StringRef(tagName)).c_str());
		}
	}

	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code())
		{
			case error_code_backup_error:
				fprintf(stderr, "ERROR: An error was encountered during submission\n");
			break;
			case error_code_backup_duplicate:
				fprintf(stderr, "ERROR: A DR is already running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
			default:
				fprintf(stderr, "ERROR: %s\n", e.what());
			break;
		}

		throw backup_error();
	}

	return Void();
}

ACTOR Future<Void> submitBackup(Database db, std::string url, int snapshotIntervalSeconds,
                                Standalone<VectorRef<KeyRangeRef>> backupRanges, std::string tagName, bool dryRun,
                                bool waitForCompletion, bool stopWhenDone, bool usePartitionedLog) {
	try {
		state FileBackupAgent backupAgent;

		// Backup everything, if no ranges were specified
		if (backupRanges.size() == 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		}

		if (dryRun) {
			state KeyBackedTag tag = makeBackupTag(tagName);
			Optional<UidAndAbortedFlagT> uidFlag = wait(tag.get(db));

			if (uidFlag.present()) {
				BackupConfig config(uidFlag.get().first);
				EBackupState backupStatus = wait(config.stateEnum().getOrThrow(db));

				// Throw error if a backup is currently running until we support parallel backups
				if (BackupAgentBase::isRunnable((BackupAgentBase::enumState)backupStatus)) {
					throw backup_duplicate();
				}
			}

			if (waitForCompletion) {
				printf("Submitted and now waiting for the backup on tag `%s' to complete. (DRY RUN)\n", printable(StringRef(tagName)).c_str());
			}

			else {
				// Check if a backup agent is running
				bool agentRunning = wait(backupAgent.checkActive(db));

				if (!agentRunning) {
					printf("The backup on tag `%s' was successfully submitted but no backup agents are responding. (DRY RUN)\n", printable(StringRef(tagName)).c_str());

					// Throw an error that will not display any additional information
					throw actor_cancelled();
				}
				else {
					printf("The backup on tag `%s' was successfully submitted. (DRY RUN)\n", printable(StringRef(tagName)).c_str());
				}
			}
		}

		else {
			wait(backupAgent.submitBackup(db, KeyRef(url), snapshotIntervalSeconds, tagName, backupRanges, stopWhenDone,
			                              usePartitionedLog));

			// Wait for the backup to complete, if requested
			if (waitForCompletion) {
				printf("Submitted and now waiting for the backup on tag `%s' to complete.\n", printable(StringRef(tagName)).c_str());
				wait(success(backupAgent.waitBackup(db, tagName)));
			}
			else {
				// Check if a backup agent is running
				bool agentRunning = wait(backupAgent.checkActive(db));

				if (!agentRunning) {
					printf("The backup on tag `%s' was successfully submitted but no backup agents are responding.\n", printable(StringRef(tagName)).c_str());

					// Throw an error that will not display any additional information
					throw actor_cancelled();
				}
				else {
					printf("The backup on tag `%s' was successfully submitted.\n", printable(StringRef(tagName)).c_str());
				}
			}
		}
	} catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code())
		{
			case error_code_backup_error:
				fprintf(stderr, "ERROR: An error was encountered during submission\n");
			break;
			case error_code_backup_duplicate:
				fprintf(stderr, "ERROR: A backup is already running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
			default:
				fprintf(stderr, "ERROR: %s\n", e.what());
			break;
		}

		throw backup_error();
	}

	return Void();
}

ACTOR Future<Void> switchDBBackup(Database src, Database dest, Standalone<VectorRef<KeyRangeRef>> backupRanges, std::string tagName, bool forceAction) {
	try
	{
		state DatabaseBackupAgent backupAgent(src);

		// Backup everything, if no ranges were specified
		if (backupRanges.size() == 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		}


		wait(backupAgent.atomicSwitchover(dest, KeyRef(tagName), backupRanges, StringRef(), StringRef(), forceAction));
		printf("The DR on tag `%s' was successfully switched.\n", printable(StringRef(tagName)).c_str());
	}

	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code())
		{
			case error_code_backup_error:
				fprintf(stderr, "ERROR: An error was encountered during submission\n");
			break;
			case error_code_backup_duplicate:
				fprintf(stderr, "ERROR: A DR is already running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
			default:
				fprintf(stderr, "ERROR: %s\n", e.what());
			break;
		}

		throw backup_error();
	}

	return Void();
}

ACTOR Future<Void> statusDBBackup(Database src, Database dest, std::string tagName, int errorLimit) {
	try
	{
		state DatabaseBackupAgent backupAgent(src);

		std::string	statusText = wait(backupAgent.getStatus(dest, errorLimit, StringRef(tagName)));
		printf("%s\n", statusText.c_str());
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> statusBackup(Database db, std::string tagName, bool showErrors, bool json) {
	try
	{
		state FileBackupAgent backupAgent;

		std::string statusText = wait(json ? backupAgent.getStatusJSON(db, tagName) : backupAgent.getStatus(db, showErrors, tagName));
		printf("%s\n", statusText.c_str());
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> abortDBBackup(Database src, Database dest, std::string tagName, bool partial) {
	try
	{
		state DatabaseBackupAgent backupAgent(src);

		wait(backupAgent.abortBackup(dest, Key(tagName), partial));
		wait(backupAgent.unlockBackup(dest, Key(tagName)));

		printf("The DR on tag `%s' was successfully aborted.\n", printable(StringRef(tagName)).c_str());
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code())
		{
			case error_code_backup_error:
				fprintf(stderr, "ERROR: An error was encountered during submission\n");
			break;
			case error_code_backup_unneeded:
				fprintf(stderr, "ERROR: A DR was not running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
			default:
				fprintf(stderr, "ERROR: %s\n", e.what());
			break;
		}
		throw;
	}

	return Void();
}

ACTOR Future<Void> abortBackup(Database db, std::string tagName) {
	try
	{
		state FileBackupAgent backupAgent;

		wait(backupAgent.abortBackup(db, tagName));

		printf("The backup on tag `%s' was successfully aborted.\n", printable(StringRef(tagName)).c_str());
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code())
		{
			case error_code_backup_error:
				fprintf(stderr, "ERROR: An error was encountered during submission\n");
			break;
			case error_code_backup_unneeded:
				fprintf(stderr, "ERROR: A backup was not running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
			default:
				fprintf(stderr, "ERROR: %s\n", e.what());
			break;
		}
		throw;
	}

	return Void();
}

ACTOR Future<Void> cleanupMutations(Database db, bool deleteData) {
	try
	{
		wait(cleanupBackup(db, deleteData));
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> waitBackup(Database db, std::string tagName, bool stopWhenDone) {
	try
	{
		state FileBackupAgent backupAgent;

		int status = wait(backupAgent.waitBackup(db, tagName, stopWhenDone));

		printf("The backup on tag `%s' %s.\n", printable(StringRef(tagName)).c_str(),
			BackupAgentBase::getStateText((BackupAgentBase::enumState) status));
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> discontinueBackup(Database db, std::string tagName, bool waitForCompletion) {
	try
	{
		state FileBackupAgent backupAgent;

		wait(backupAgent.discontinueBackup(db, StringRef(tagName)));

		// Wait for the backup to complete, if requested
		if (waitForCompletion) {
			printf("Discontinued and now waiting for the backup on tag `%s' to complete.\n", printable(StringRef(tagName)).c_str());
			wait(success(backupAgent.waitBackup(db, tagName)));
		}
		else {
			printf("The backup on tag `%s' was successfully discontinued.\n", printable(StringRef(tagName)).c_str());
		}

	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code())
		{
			case error_code_backup_error:
				fprintf(stderr, "ERROR: An encounter was error during submission\n");
			break;
			case error_code_backup_unneeded:
				fprintf(stderr, "ERROR: A backup in not running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
			case error_code_backup_duplicate:
				fprintf(stderr, "ERROR: The backup on tag `%s' is already discontinued\n", printable(StringRef(tagName)).c_str());
			break;
			default:
				fprintf(stderr, "ERROR: %s\n", e.what());
			break;
		}
		throw;
	}

	return Void();
}

ACTOR Future<Void> changeBackupResumed(Database db, bool pause) {
	try {
		FileBackupAgent backupAgent;
		wait(backupAgent.changePause(db, pause));
		printf("All backup agents have been %s.\n", pause ? "paused" : "resumed");
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> changeDBBackupResumed(Database src, Database dest, bool pause) {
	try {
		state DatabaseBackupAgent backupAgent(src);
		wait(backupAgent.taskBucket->changePause(dest, pause));
		printf("All DR agents have been %s.\n", pause ? "paused" : "resumed");
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

Reference<IBackupContainer> openBackupContainer(const char *name, std::string destinationContainer) {
	// Error, if no dest container was specified
	if (destinationContainer.empty()) {
		fprintf(stderr, "ERROR: No backup destination was specified.\n");
		printHelpTeaser(name);
		throw backup_error();
	}

	Reference<IBackupContainer> c;
	try {
		c = IBackupContainer::openContainer(destinationContainer);
	}
	catch (Error& e) {
		std::string msg = format("ERROR: '%s' on URL '%s'", e.what(), destinationContainer.c_str());
		if(e.code() == error_code_backup_invalid_url && !IBackupContainer::lastOpenError.empty()) {
			msg += format(": %s", IBackupContainer::lastOpenError.c_str());
		}
		fprintf(stderr, "%s\n", msg.c_str());
		printHelpTeaser(name);
		throw;
	}

	return c;
}

ACTOR Future<Void> runRestore(Database db, std::string originalClusterFile, std::string tagName, std::string container, Standalone<VectorRef<KeyRangeRef>> ranges, Version targetVersion, std::string targetTimestamp, bool performRestore, bool verbose, bool waitForDone, std::string addPrefix, std::string removePrefix) {
	if(ranges.empty()) {
		ranges.push_back_deep(ranges.arena(), normalKeys);
	}

	if(targetVersion != invalidVersion && !targetTimestamp.empty()) {
		fprintf(stderr, "Restore target version and target timestamp cannot both be specified\n");
		throw restore_error();
	}

	state Optional<Database> origDb;

	// Resolve targetTimestamp if given
	if(!targetTimestamp.empty()) {
		if(originalClusterFile.empty()) {
			fprintf(stderr, "An original cluster file must be given in order to resolve restore target timestamp '%s'\n", targetTimestamp.c_str());
			throw restore_error();
		}

		if(!fileExists(originalClusterFile)) {
			fprintf(stderr, "Original source database cluster file '%s' does not exist.\n", originalClusterFile.c_str());
			throw restore_error();
		}

		origDb = Database::createDatabase(originalClusterFile, Database::API_VERSION_LATEST);
		Version v = wait(timeKeeperVersionFromDatetime(targetTimestamp, origDb.get()));
		printf("Timestamp '%s' resolves to version %" PRId64 "\n", targetTimestamp.c_str(), v);
		targetVersion = v;
	}

	try {
		state FileBackupAgent backupAgent;

		state Reference<IBackupContainer> bc = openBackupContainer(exeRestore.toString().c_str(), container);

		// If targetVersion is unset then use the maximum restorable version from the backup description
		if(targetVersion == invalidVersion) {
			if(verbose)
				printf("No restore target version given, will use maximum restorable version from backup description.\n");

			BackupDescription desc = wait(bc->describeBackup());

			if(!desc.maxRestorableVersion.present()) {
				fprintf(stderr, "The specified backup is not restorable to any version.\n");
				throw restore_error();
			}

			targetVersion = desc.maxRestorableVersion.get();

			if(verbose)
				printf("Using target restore version %" PRId64 "\n", targetVersion);
		}

		if (performRestore) {
			Version restoredVersion = wait(backupAgent.restore(db, origDb, KeyRef(tagName), KeyRef(container), ranges, waitForDone, targetVersion, verbose, KeyRef(addPrefix), KeyRef(removePrefix)));

			if(waitForDone && verbose) {
				// If restore is now complete then report version restored
				printf("Restored to version %" PRId64 "\n", restoredVersion);
			}
		}
		else {
			state Optional<RestorableFileSet> rset = wait(bc->getRestoreSet(targetVersion));

			if(!rset.present()) {
				fprintf(stderr, "Insufficient data to restore to version %" PRId64 ".  Describe backup for more information.\n", targetVersion);
				throw restore_invalid_version();
			}

			printf("Backup can be used to restore to version %" PRId64 "\n", targetVersion);
		}

	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

// Fast restore agent that kicks off the restore: send restore requests to restore workers.
ACTOR Future<Void> runFastRestoreAgent(Database db, std::string tagName, std::string container,
                                       Standalone<VectorRef<KeyRangeRef>> ranges, Version dbVersion,
                                       bool performRestore, bool verbose, bool waitForDone) {
	try {
		state FileBackupAgent backupAgent;
		state Version restoreVersion = invalidVersion;

		if (ranges.size() > 1) {
			fprintf(stdout, "[WARNING] Currently only a single restore range is tested!\n");
		}

		if (ranges.size() == 0) {
			ranges.push_back(ranges.arena(), normalKeys);
		}

		printf("[INFO] runFastRestoreAgent: restore_ranges:%d first range:%s\n", ranges.size(),
		       ranges.front().toString().c_str());

		if (performRestore) {
			if (dbVersion == invalidVersion) {
				TraceEvent("FastRestoreAgent").detail("TargetRestoreVersion", "Largest restorable version");
				BackupDescription desc = wait(IBackupContainer::openContainer(container)->describeBackup());
				if (!desc.maxRestorableVersion.present()) {
					fprintf(stderr, "The specified backup is not restorable to any version.\n");
					throw restore_error();
				}

				dbVersion = desc.maxRestorableVersion.get();
				TraceEvent("FastRestoreAgent").detail("TargetRestoreVersion", dbVersion);
			}
			state UID randomUID = deterministicRandom()->randomUniqueID();
			TraceEvent("FastRestoreAgent")
			    .detail("SubmitRestoreRequests", ranges.size())
			    .detail("RestoreUID", randomUID);
			wait(backupAgent.submitParallelRestore(db, KeyRef(tagName), ranges, KeyRef(container), dbVersion, true,
			                                       randomUID, LiteralStringRef(""), LiteralStringRef("")));
			// TODO: Support addPrefix and removePrefix
			if (waitForDone) {
				// Wait for parallel restore to finish and unlock DB after that
				TraceEvent("FastRestoreAgent").detail("BackupAndParallelRestore", "WaitForRestoreToFinish");
				wait(backupAgent.parallelRestoreFinish(db, randomUID));
				TraceEvent("FastRestoreAgent").detail("BackupAndParallelRestore", "RestoreFinished");
			} else {
				TraceEvent("FastRestoreAgent")
				    .detail("RestoreUID", randomUID)
				    .detail("OperationGuide", "Manually unlock DB when restore finishes");
				printf("WARNING: DB will be in locked state after restore. Need UID:%s to unlock DB\n",
				       randomUID.toString().c_str());
			}

			restoreVersion = dbVersion;
		} else {
			state Reference<IBackupContainer> bc = IBackupContainer::openContainer(container);
			state BackupDescription description = wait(bc->describeBackup());

			if (dbVersion <= 0) {
				wait(description.resolveVersionTimes(db));
				if (description.maxRestorableVersion.present())
					restoreVersion = description.maxRestorableVersion.get();
				else {
					fprintf(stderr, "Backup is not restorable\n");
					throw restore_invalid_version();
				}
			} else {
				restoreVersion = dbVersion;
			}

			state Optional<RestorableFileSet> rset = wait(bc->getRestoreSet(restoreVersion));
			if (!rset.present()) {
				fprintf(stderr, "Insufficient data to restore to version %" PRId64 "\n", restoreVersion);
				throw restore_invalid_version();
			}

			// Display the restore information, if requested
			if (verbose) {
				printf("[DRY RUN] Restoring backup to version: %" PRId64 "\n", restoreVersion);
				printf("%s\n", description.toString().c_str());
			}
		}

		if (waitForDone && verbose) {
			// If restore completed then report version restored
			printf("Restored to version %" PRId64 "%s\n", restoreVersion, (performRestore) ? "" : " (DRY RUN)");
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> dumpBackupData(const char *name, std::string destinationContainer, Version beginVersion, Version endVersion) {
	state Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);

	if(beginVersion < 0 || endVersion < 0) {
		BackupDescription desc = wait(c->describeBackup());

		if(!desc.maxLogEnd.present()) {
			fprintf(stderr, "ERROR: Backup must have log data in order to use relative begin/end versions.\n");
			throw backup_invalid_info();
		}

		if(beginVersion < 0) {
			beginVersion += desc.maxLogEnd.get();
		}

		if(endVersion < 0) {
			endVersion += desc.maxLogEnd.get();
		}
	}

	printf("Scanning version range %" PRId64 " to %" PRId64 "\n", beginVersion, endVersion);
	BackupFileList files = wait(c->dumpFileList(beginVersion, endVersion));
	files.toStream(stdout);

	return Void();
}

ACTOR Future<Void> expireBackupData(const char *name, std::string destinationContainer, Version endVersion, std::string endDatetime, Database db, bool force, Version restorableAfterVersion, std::string restorableAfterDatetime) {
	if (!endDatetime.empty()) {
		Version v = wait( timeKeeperVersionFromDatetime(endDatetime, db) );
		endVersion = v;
	}

	if (!restorableAfterDatetime.empty()) {
		Version v = wait( timeKeeperVersionFromDatetime(restorableAfterDatetime, db) );
		restorableAfterVersion = v;
	}

	if (endVersion == invalidVersion) {
		fprintf(stderr, "ERROR: No version or date/time is specified.\n");
		printHelpTeaser(name);
		throw backup_error();;
	}

	try {
		Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);

		state IBackupContainer::ExpireProgress progress;
		state std::string lastProgress;
		state Future<Void> expire = c->expireData(endVersion, force, &progress, restorableAfterVersion);

		loop {
			choose {
				when(wait(delay(5))) {
					std::string p = progress.toString();
					if(p != lastProgress) {
						int spaces = lastProgress.size() - p.size();
						printf("\r%s%s", p.c_str(), (spaces > 0 ? std::string(spaces, ' ').c_str() : "") );
						lastProgress = p;
					}
				}
				when(wait(expire)) {
					break;
				}
			}
		}

		std::string p = progress.toString();
		int spaces = lastProgress.size() - p.size();
		printf("\r%s%s\n", p.c_str(), (spaces > 0 ? std::string(spaces, ' ').c_str() : "") );

		if(endVersion < 0)
			printf("All data before %" PRId64 " versions (%" PRId64 " days) prior to latest backup log has been deleted.\n", -endVersion, -endVersion / ((int64_t)24 * 3600 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
		else
			printf("All data before version %" PRId64 " has been deleted.\n", endVersion);
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		if(e.code() == error_code_backup_cannot_expire)
			fprintf(stderr, "ERROR: Requested expiration would be unsafe.  Backup would not meet minimum restorability.  Use --force to delete data anyway.\n");
		else
			fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> deleteBackupContainer(const char *name, std::string destinationContainer) {
	try {
		state Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);
		state int numDeleted = 0;
		state Future<Void> done = c->deleteContainer(&numDeleted);

		state int lastUpdate = -1;
		printf("Deleting %s...\n", destinationContainer.c_str());

		loop {
			choose {
				when ( wait(done) ) {
					break;
				}
				when ( wait(delay(5)) ) {
					if(numDeleted != lastUpdate) {
						printf("\r%d...", numDeleted);
						lastUpdate = numDeleted;
					}
				}
			}
		}
		printf("\r%d objects deleted\n", numDeleted);
		printf("The entire container has been deleted.\n");
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> describeBackup(const char *name, std::string destinationContainer, bool deep, Optional<Database> cx, bool json) {
	try {
		Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);
		state BackupDescription desc = wait(c->describeBackup(deep));
		if(cx.present())
			wait(desc.resolveVersionTimes(cx.get()));
		printf("%s\n", (json ? desc.toJSON() : desc.toString()).c_str());
	}
	catch (Error& e) {
		if(e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> listBackup(std::string baseUrl) {
	try {
		std::vector<std::string> containers = wait(IBackupContainer::listContainers(baseUrl));
		for (std::string container : containers) {
			printf("%s\n", container.c_str());
		}
	}
	catch (Error& e) {
		std::string msg = format("ERROR: %s", e.what());
		if(e.code() == error_code_backup_invalid_url && !IBackupContainer::lastOpenError.empty()) {
			msg += format(": %s", IBackupContainer::lastOpenError.c_str());
		}
		fprintf(stderr, "%s\n", msg.c_str());
		throw;
	}

	return Void();
}

struct BackupModifyOptions {
	Optional<std::string> verifyUID;
	Optional<std::string> destURL;
	Optional<int> snapshotIntervalSeconds;
	Optional<int> activeSnapshotIntervalSeconds;
	bool hasChanges() const {
		return destURL.present() || snapshotIntervalSeconds.present() || activeSnapshotIntervalSeconds.present();
	}
};

ACTOR Future<Void> modifyBackup(Database db, std::string tagName, BackupModifyOptions options) {
	if(!options.hasChanges()) {
		fprintf(stderr, "No changes were specified, nothing to do!\n");
		throw backup_error();
	}

	state KeyBackedTag tag = makeBackupTag(tagName);

	state Reference<IBackupContainer> bc;
	if(options.destURL.present()) {
		bc = openBackupContainer(exeBackup.toString().c_str(), options.destURL.get());
		try {
			wait(timeoutError(bc->create(), 30));
		} catch(Error &e) {
			if(e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr, "ERROR: Could not create backup container at '%s': %s\n", options.destURL.get().c_str(), e.what());
			throw backup_error();
		}
	}
	
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(db));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state Optional<UidAndAbortedFlagT> uidFlag = wait(tag.get(db));

			if(!uidFlag.present()) {
				fprintf(stderr, "No backup exists on tag '%s'\n", tagName.c_str());
				throw backup_error();
			}

			if(uidFlag.get().second) {
				fprintf(stderr, "Cannot modify aborted backup on tag '%s'\n", tagName.c_str());
				throw backup_error();
			}

			state BackupConfig config(uidFlag.get().first);
			EBackupState s = wait(config.stateEnum().getOrThrow(tr, false, backup_invalid_info()));
			if(!FileBackupAgent::isRunnable(s)) {
				fprintf(stderr, "Backup on tag '%s' is not runnable.\n", tagName.c_str());
				throw backup_error();
			}

			if(options.verifyUID.present() && options.verifyUID.get() != uidFlag.get().first.toString()) {
				fprintf(stderr, "UID verification failed, backup on tag '%s' is '%s' but '%s' was specified.\n", tagName.c_str(), uidFlag.get().first.toString().c_str(), options.verifyUID.get().c_str());
				throw backup_error();
			}

			if(options.snapshotIntervalSeconds.present()) {
				config.snapshotIntervalSeconds().set(tr, options.snapshotIntervalSeconds.get());
			}

			if(options.activeSnapshotIntervalSeconds.present()) {
				Version begin = wait(config.snapshotBeginVersion().getOrThrow(tr, false, backup_error()));
				config.snapshotTargetEndVersion().set(tr, begin + ((int64_t)options.activeSnapshotIntervalSeconds.get() * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
			}

			if(options.destURL.present()) {
				config.backupContainer().set(tr, bc);
			}

			wait(tr->commit());
			break;
		}
		catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

static std::vector<std::vector<StringRef>> parseLine(std::string &line, bool& err, bool& partial)
{
	err = false;
	partial = false;

	bool quoted = false;
	std::vector<StringRef> buf;
	std::vector<std::vector<StringRef>> ret;

	size_t i = line.find_first_not_of(' ');
	size_t offset = i;

	bool forcetoken = false;

	while (i <= line.length()) {
		switch (line[i]) {
		case ';':
			if (!quoted) {
				if (i > offset)
					buf.push_back(StringRef((uint8_t*)(line.data() + offset), i - offset));
				ret.push_back(std::move(buf));
				offset = i = line.find_first_not_of(' ', i + 1);
			}
			else
				i++;
			break;
		case '"':
			quoted = !quoted;
			line.erase(i, 1);
			if (quoted)
				forcetoken = true;
			break;
		case ' ':
			if (!quoted) {
				buf.push_back(StringRef((uint8_t *)(line.data() + offset),
					i - offset));
				offset = i = line.find_first_not_of(' ', i);
				forcetoken = false;
			}
			else
				i++;
			break;
		case '\\':
			if (i + 2 > line.length()) {
				err = true;
				ret.push_back(std::move(buf));
				return ret;
			}
			switch (line[i + 1]) {
				char ent, save;
			case '"':
			case '\\':
			case ' ':
			case ';':
				line.erase(i, 1);
				break;
			case 'x':
				if (i + 4 > line.length()) {
					err = true;
					ret.push_back(std::move(buf));
					return ret;
				}
				char *pEnd;
				save = line[i + 4];
				line[i + 4] = 0;
				ent = char(strtoul(line.data() + i + 2, &pEnd, 16));
				if (*pEnd) {
					err = true;
					ret.push_back(std::move(buf));
					return ret;
				}
				line[i + 4] = save;
				line.replace(i, 4, 1, ent);
				break;
			default:
				err = true;
				ret.push_back(std::move(buf));
				return ret;
			}
		default:
			i++;
		}
	}

	i -= 1;
	if (i > offset || forcetoken)
		buf.push_back(StringRef((uint8_t*)(line.data() + offset), i - offset));

	ret.push_back(std::move(buf));

	if (quoted)
		partial = true;

	return ret;
}

static void addKeyRange(std::string optionValue, Standalone<VectorRef<KeyRangeRef>>& keyRanges)
{
	bool	err = false, partial = false;
	int	tokenArray = 0, tokenIndex = 0;

	auto parsed = parseLine(optionValue, err, partial);

	for (auto tokens : parsed)
	{
		tokenArray++;
		tokenIndex = 0;

		/*
		for (auto token : tokens)
		{
			tokenIndex++;

			printf("%4d token #%2d: %s\n", tokenArray, tokenIndex, printable(token).c_str());
		}
		*/

		// Process the keys
		// <begin> [end]
		switch (tokens.size())
		{
			// empty
		case 0:
			break;

			// single key range
		case 1:
				keyRanges.push_back_deep(keyRanges.arena(), KeyRangeRef(tokens.at(0), strinc(tokens.at(0))));
			break;

			// full key range
		case 2:
			try {
				keyRanges.push_back_deep(keyRanges.arena(), KeyRangeRef(tokens.at(0), tokens.at(1)));
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: Invalid key range `%s %s' reported error %s\n",
					tokens.at(0).toString().c_str(), tokens.at(1).toString().c_str(), e.what());
				throw invalid_option_value();
			}
			break;

			// Too many keys
		default:
			fprintf(stderr, "ERROR: Invalid key range identified with %ld keys", tokens.size());
			throw invalid_option_value();
			break;
		}
	}

	return;
}

Version parseVersion(const char *str) {
	StringRef s((const uint8_t *)str, strlen(str));

	if(s.endsWith(LiteralStringRef("days")) || s.endsWith(LiteralStringRef("d"))) {
		float days;
		if(sscanf(str, "%f", &days) != 1) {
			fprintf(stderr, "Could not parse version: %s\n", str);
			flushAndExit(FDB_EXIT_ERROR);
		}
		return (double)CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 24 * 3600 * -days;
	}

	Version ver;
	if(sscanf(str, "%" SCNd64, &ver) != 1) {
		fprintf(stderr, "Could not parse version: %s\n", str);
		flushAndExit(FDB_EXIT_ERROR);
	}
	return ver;
}

#ifdef ALLOC_INSTRUMENTATION
extern uint8_t *g_extra_memory;
#endif

int main(int argc, char* argv[]) {
	platformInit();

	int status = FDB_EXIT_SUCCESS;

	std::string commandLine;
	for(int a=0; a<argc; a++) {
		if (a) commandLine += ' ';
		commandLine += argv[a];
	}

	try {
#ifdef ALLOC_INSTRUMENTATION
		g_extra_memory = new uint8_t[1000000];
#endif
		registerCrashHandler();

		// Set default of line buffering standard out and error
		setvbuf(stdout, NULL, _IONBF, 0);
		setvbuf(stderr, NULL, _IONBF, 0);

		enumProgramExe programExe = getProgramType(argv[0]);
		enumBackupType backupType = BACKUP_UNDEFINED;
		enumRestoreType restoreType = RESTORE_UNKNOWN;
		enumDBType dbType = DB_UNDEFINED;

		CSimpleOpt* args = NULL;

		switch (programExe)
		{
		case EXE_AGENT:
			args = new CSimpleOpt(argc, argv, g_rgAgentOptions, SO_O_EXACT);
			break;
		case EXE_DR_AGENT:
			args = new CSimpleOpt(argc, argv, g_rgDBAgentOptions, SO_O_EXACT);
			break;
		case EXE_BACKUP:
			// Display backup help, if no arguments
			if (argc < 2) {
				printBackupUsage(false);
				return FDB_EXIT_ERROR;
			}
			else {
				// Get the backup type
				backupType = getBackupType(argv[1]);

				// Create the appropriate simple opt
				switch (backupType)
				{
				case BACKUP_START:
					args = new CSimpleOpt(argc-1, &argv[1], g_rgBackupStartOptions, SO_O_EXACT);
					break;
				case BACKUP_STATUS:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupStatusOptions, SO_O_EXACT);
					break;
				case BACKUP_ABORT:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupAbortOptions, SO_O_EXACT);
					break;
				case BACKUP_CLEANUP:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupCleanupOptions, SO_O_EXACT);
					break;
				case BACKUP_WAIT:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupWaitOptions, SO_O_EXACT);
					break;
				case BACKUP_DISCONTINUE:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupDiscontinueOptions, SO_O_EXACT);
					break;
				case BACKUP_PAUSE:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupPauseOptions, SO_O_EXACT);
					break;
				case BACKUP_RESUME:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupPauseOptions, SO_O_EXACT);
					break;
				case BACKUP_EXPIRE:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupExpireOptions, SO_O_EXACT);
					break;
				case BACKUP_DELETE:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupDeleteOptions, SO_O_EXACT);
					break;
				case BACKUP_DESCRIBE:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupDescribeOptions, SO_O_EXACT);
					break;
				case BACKUP_DUMP:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupDumpOptions, SO_O_EXACT);
					break;
				case BACKUP_LIST:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupListOptions, SO_O_EXACT);
					break;
				case BACKUP_MODIFY:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgBackupModifyOptions, SO_O_EXACT);
					break;
				case BACKUP_UNDEFINED:
				default:
					// Display help, if requested
					if ((strcmp(argv[1], "-h") == 0)		||
						(strcmp(argv[1], "--help") == 0)	)
					{
						printBackupUsage(false);
						return FDB_EXIT_ERROR;
					}
					else {
						fprintf(stderr, "ERROR: Unsupported backup action %s\n", argv[1]);
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
					break;
				}
			}
			break;
		case EXE_DB_BACKUP:
			// Display backup help, if no arguments
			if (argc < 2) {
				printDBBackupUsage(false);
				return FDB_EXIT_ERROR;
			}
			else {
				// Get the backup type
				dbType = getDBType(argv[1]);

				// Create the appropriate simple opt
				switch (dbType)
				{
				case DB_START:
					args = new CSimpleOpt(argc-1, &argv[1], g_rgDBStartOptions, SO_O_EXACT);
					break;
				case DB_STATUS:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgDBStatusOptions, SO_O_EXACT);
					break;
				case DB_SWITCH:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgDBSwitchOptions, SO_O_EXACT);
					break;
				case DB_ABORT:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgDBAbortOptions, SO_O_EXACT);
					break;
				case DB_PAUSE:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgDBPauseOptions, SO_O_EXACT);
					break;
				case DB_RESUME:
					args = new CSimpleOpt(argc - 1, &argv[1], g_rgDBPauseOptions, SO_O_EXACT);
					break;
				case DB_UNDEFINED:
				default:
					// Display help, if requested
					if ((strcmp(argv[1], "-h") == 0)		||
						(strcmp(argv[1], "--help") == 0)	)
					{
						printDBBackupUsage(false);
						return FDB_EXIT_ERROR;
					}
					else {
						fprintf(stderr, "ERROR: Unsupported dr action %s %d\n", argv[1], dbType);
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
					break;
				}
			}
			break;
		case EXE_RESTORE:
			if (argc < 2) {
				printRestoreUsage(false);
				return FDB_EXIT_ERROR;
			}
			// Get the restore operation type
			restoreType = getRestoreType(argv[1]);
			if(restoreType == RESTORE_UNKNOWN) {
				// Display help, if requested
				if ((strcmp(argv[1], "-h") == 0)		||
					(strcmp(argv[1], "--help") == 0)	)
				{
					printRestoreUsage(false);
					return FDB_EXIT_ERROR;
				}
				else {
					fprintf(stderr, "ERROR: Unsupported restore command: '%s'\n", argv[1]);
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
			}
			args = new CSimpleOpt(argc - 1, argv + 1, g_rgRestoreOptions, SO_O_EXACT);
			break;
		case EXE_FASTRESTORE_AGENT:
			if (argc < 2) {
				printFastRestoreUsage(false);
				return FDB_EXIT_ERROR;
			}
			// Get the restore operation type
			restoreType = getRestoreType(argv[1]);
			if (restoreType == RESTORE_UNKNOWN) {
				// Display help, if requested
				if ((strcmp(argv[1], "-h") == 0) || (strcmp(argv[1], "--help") == 0)) {
					printFastRestoreUsage(false);
					return FDB_EXIT_ERROR;
				} else {
					fprintf(stderr, "ERROR: Unsupported restore command: '%s'\n", argv[1]);
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
			}
			args = new CSimpleOpt(argc - 1, argv + 1, g_rgRestoreOptions, SO_O_EXACT);
			break;
		case EXE_UNDEFINED:
		default:
			fprintf(stderr, "FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
			fprintf(stderr, "ERROR: Unable to determine program type based on executable `%s'\n", argv[0]);
			return FDB_EXIT_ERROR;
			break;
		}

		std::string destinationContainer;
		bool describeDeep = false;
		bool describeTimestamps = false;
		int snapshotIntervalSeconds = CLIENT_KNOBS->BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC;
		std::string clusterFile;
		std::string sourceClusterFile;
		std::string baseUrl;
		std::string expireDatetime;
		Version expireVersion = invalidVersion;
		std::string expireRestorableAfterDatetime;
		Version expireRestorableAfterVersion = std::numeric_limits<Version>::max();
		std::vector<std::pair<std::string, std::string>> knobs;
		std::string tagName = BackupAgentBase::getDefaultTag().toString();
		bool tagProvided = false;
		std::string restoreContainer;
		std::string addPrefix;
		std::string removePrefix;
		Standalone<VectorRef<KeyRangeRef>> backupKeys;
		int maxErrors = 20;
		Version restoreVersion = invalidVersion;
		std::string restoreTimestamp;
		bool waitForDone = false;
		bool stopWhenDone = true;
		bool usePartitionedLog = false; // Set to true to use new backup system
		bool forceAction = false;
		bool trace = false;
		bool quietDisplay = false;
		bool dryRun = false;
		std::string traceDir = "";
		std::string traceFormat = "";
		std::string traceLogGroup;
		uint64_t traceRollSize = TRACE_DEFAULT_ROLL_SIZE;
		uint64_t traceMaxLogsSize = TRACE_DEFAULT_MAX_LOGS_SIZE;
		ESOError	lastError;
		bool partial = true;
		LocalityData localities;
		uint64_t memLimit = 8LL << 30;
		Optional<uint64_t> ti;
		std::vector<std::string> blobCredentials;
		std::string tlsCertPath, tlsKeyPath, tlsCAPath, tlsPassword, tlsVerifyPeers;
		Version dumpBegin = 0;
		Version dumpEnd = std::numeric_limits<Version>::max();
		std::string restoreClusterFileDest;
		std::string restoreClusterFileOrig;
		bool jsonOutput = false;
		bool deleteData = false;

		BackupModifyOptions modifyOptions;

		if( argc == 1 ) {
			printUsage(programExe, false);
			return FDB_EXIT_ERROR;
		}

	#ifdef _WIN32
		// Windows needs a gentle nudge to format floats correctly
		//_set_output_format(_TWO_DIGIT_EXPONENT);
	#endif

		while (args->Next()) {
			lastError = args->LastError();

			switch (lastError)
			{
			case SO_SUCCESS:
				break;

			case SO_ARG_INVALID_DATA:
				fprintf(stderr, "ERROR: invalid argument to option `%s'\n", args->OptionText());
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case SO_ARG_INVALID:
				fprintf(stderr, "ERROR: argument given for option `%s'\n", args->OptionText());
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case SO_ARG_MISSING:
				fprintf(stderr, "ERROR: missing argument for option `%s'\n", args->OptionText());
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;

			case SO_OPT_INVALID:
				fprintf(stderr, "ERROR: unknown option `%s'\n", args->OptionText());
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			default:
				fprintf(stderr, "ERROR: argument given for option `%s'\n", args->OptionText());
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;
			}

			int optId = args->OptionId();
			switch (optId) {
				case OPT_HELP:
					printUsage(programExe, false);
					return FDB_EXIT_SUCCESS;
					break;
				case OPT_DEVHELP:
					printUsage(programExe, true);
					return FDB_EXIT_SUCCESS;
					break;
				case OPT_VERSION:
					printVersion();
					return FDB_EXIT_SUCCESS;
					break;
				case OPT_NOBUFSTDOUT:
					setvbuf(stdout, NULL, _IONBF, 0);
					setvbuf(stderr, NULL, _IONBF, 0);
					break;
				case OPT_BUFSTDOUTERR:
					setvbuf(stdout, NULL, _IOFBF, BUFSIZ);
					setvbuf(stderr, NULL, _IOFBF, BUFSIZ);
					break;
				case OPT_QUIET:
					quietDisplay = true;
					break;
				case OPT_DRYRUN:
					dryRun = true;
					break;
				case OPT_DELETE_DATA:
					deleteData = true;
					break;
				case OPT_MIN_CLEANUP_SECONDS:
					knobs.push_back( std::make_pair( "min_cleanup_seconds", args->OptionArg() ) );
					break;
				case OPT_FORCE:
					forceAction = true;
					break;
				case OPT_TRACE:
					trace = true;
					break;
				case OPT_TRACE_DIR:
					trace = true;
					traceDir = args->OptionArg();
					break;
				case OPT_TRACE_FORMAT:
					if (!validateTraceFormat(args->OptionArg())) {
						fprintf(stderr, "WARNING: Unrecognized trace format `%s'\n", args->OptionArg());
					}
					traceFormat = args->OptionArg();
					break;
				case OPT_TRACE_LOG_GROUP:
					traceLogGroup = args->OptionArg();
					break;
				case OPT_LOCALITY: {
					std::string syn = args->OptionSyntax();
					if (!StringRef(syn).startsWith(LiteralStringRef("--locality_"))) {
						fprintf(stderr, "ERROR: unable to parse locality key '%s'\n", syn.c_str());
						return FDB_EXIT_ERROR;
					}
					syn = syn.substr(11);
					std::transform(syn.begin(), syn.end(), syn.begin(), ::tolower);
					localities.set(Standalone<StringRef>(syn), Standalone<StringRef>(std::string(args->OptionArg())));
					break;
					}
				case OPT_EXPIRE_BEFORE_DATETIME:
					expireDatetime = args->OptionArg();
					break;
				case OPT_EXPIRE_RESTORABLE_AFTER_DATETIME:
					expireRestorableAfterDatetime = args->OptionArg();
					break;
				case OPT_EXPIRE_BEFORE_VERSION:
				case OPT_EXPIRE_RESTORABLE_AFTER_VERSION:
				case OPT_EXPIRE_MIN_RESTORABLE_DAYS:
				case OPT_EXPIRE_DELETE_BEFORE_DAYS:
				{
					const char* a = args->OptionArg();
					long long ver = 0;
					if (!sscanf(a, "%lld", &ver)) {
						fprintf(stderr, "ERROR: Could not parse expiration version `%s'\n", a);
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}

					// Interpret the value as days worth of versions relative to now (negative)
					if(optId == OPT_EXPIRE_MIN_RESTORABLE_DAYS || optId == OPT_EXPIRE_DELETE_BEFORE_DAYS) {
						ver = -ver * 24 * 60 * 60 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
					}

					if(optId == OPT_EXPIRE_BEFORE_VERSION || optId == OPT_EXPIRE_DELETE_BEFORE_DAYS)
						expireVersion = ver;
					else
						expireRestorableAfterVersion = ver;
					break;
				}
				case OPT_RESTORE_TIMESTAMP:
					restoreTimestamp = args->OptionArg();
					break;
				case OPT_BASEURL:
					baseUrl = args->OptionArg();
					break;
				case OPT_RESTORE_CLUSTERFILE_DEST:
					restoreClusterFileDest = args->OptionArg();
					break;
				case OPT_RESTORE_CLUSTERFILE_ORIG:
					restoreClusterFileOrig = args->OptionArg();
					break;
				case OPT_CLUSTERFILE:
					clusterFile = args->OptionArg();
					break;
				case OPT_DEST_CLUSTER:
					clusterFile = args->OptionArg();
					break;
				case OPT_SOURCE_CLUSTER:
					sourceClusterFile = args->OptionArg();
					break;
				case OPT_CLEANUP:
					partial = false;
					break;
				case OPT_KNOB: {
					std::string syn = args->OptionSyntax();
					if (!StringRef(syn).startsWith(LiteralStringRef("--knob_"))) {
						fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", syn.c_str());
						return FDB_EXIT_ERROR;
					}
					syn = syn.substr(7);
					knobs.push_back( std::make_pair( syn, args->OptionArg() ) );
					break;
					}
				case OPT_BACKUPKEYS:
					try {
						addKeyRange(args->OptionArg(), backupKeys);
					}
					catch (Error &) {
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
					break;
				case OPT_DESTCONTAINER:
					destinationContainer = args->OptionArg();
					// If the url starts with '/' then prepend "file://" for backwards compatibility
					if(StringRef(destinationContainer).startsWith(LiteralStringRef("/")))
						destinationContainer = std::string("file://") + destinationContainer;
					modifyOptions.destURL = destinationContainer;
					break;
				case OPT_SNAPSHOTINTERVAL:
				case OPT_MOD_ACTIVE_INTERVAL:
				{
					const char* a = args->OptionArg();
					int seconds;
					if (!sscanf(a, "%d", &seconds)) {
						fprintf(stderr, "ERROR: Could not parse snapshot interval `%s'\n", a);
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
					if(optId == OPT_SNAPSHOTINTERVAL) {
						snapshotIntervalSeconds = seconds;
						modifyOptions.snapshotIntervalSeconds = seconds;
					}
					else if(optId == OPT_MOD_ACTIVE_INTERVAL) {
						modifyOptions.activeSnapshotIntervalSeconds = seconds;
					}
					break;
				}
				case OPT_MOD_VERIFY_UID:
					modifyOptions.verifyUID = args->OptionArg();
					break;
				case OPT_WAITFORDONE:
					waitForDone = true;
					break;
				case OPT_NOSTOPWHENDONE:
					stopWhenDone = false;
					break;
				case OPT_USE_PARTITIONED_LOG:
					usePartitionedLog = true;
					break;
				case OPT_RESTORECONTAINER:
					restoreContainer = args->OptionArg();
					// If the url starts with '/' then prepend "file://" for backwards compatibility
					if(StringRef(restoreContainer).startsWith(LiteralStringRef("/")))
						restoreContainer = std::string("file://") + restoreContainer;
					break;
				case OPT_DESCRIBE_DEEP:
					describeDeep = true;
					break;
				case OPT_DESCRIBE_TIMESTAMPS:
					describeTimestamps = true;
					break;
				case OPT_PREFIX_ADD:
					addPrefix = args->OptionArg();
					break;
				case OPT_PREFIX_REMOVE:
					removePrefix = args->OptionArg();
					break;
				case OPT_ERRORLIMIT: {
					const char* a = args->OptionArg();
					if (!sscanf(a, "%d", &maxErrors)) {
						fprintf(stderr, "ERROR: Could not parse max number of errors `%s'\n", a);
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
					break;
				}
				case OPT_RESTORE_VERSION: {
					const char* a = args->OptionArg();
					long long ver = 0;
					if (!sscanf(a, "%lld", &ver)) {
						fprintf(stderr, "ERROR: Could not parse database version `%s'\n", a);
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
					restoreVersion = ver;
					break;
				}
	#ifdef _WIN32
				case OPT_PARENTPID: {
					auto pid_str = args->OptionArg();
					int parent_pid = atoi(pid_str);
					auto pHandle = OpenProcess( SYNCHRONIZE, FALSE, parent_pid );
					if( !pHandle ) {
						TraceEvent("ParentProcessOpenError").GetLastError();
						fprintf(stderr, "Could not open parent process at pid %d (error %d)", parent_pid, GetLastError());
						throw platform_error();
					}
					startThread(&parentWatcher, pHandle);
					break;
				}
	#endif
				case OPT_TAGNAME:
					tagName = args->OptionArg();
					tagProvided = true;
					break;
				case OPT_CRASHONERROR:
					g_crashOnError = true;
					break;
				case OPT_MEMLIMIT:
					ti = parse_with_suffix(args->OptionArg(), "MiB");
					if (!ti.present()) {
						fprintf(stderr, "ERROR: Could not parse memory limit from `%s'\n", args->OptionArg());
						printHelpTeaser(argv[0]);
						flushAndExit(FDB_EXIT_ERROR);
					}
					memLimit = ti.get();
					break;
				case OPT_BLOB_CREDENTIALS:
					blobCredentials.push_back(args->OptionArg());
					break;
#ifndef TLS_DISABLED
				case TLSConfig::OPT_TLS_PLUGIN:
					args->OptionArg();
					break;
				case TLSConfig::OPT_TLS_CERTIFICATES:
					tlsCertPath = args->OptionArg();
					break;
				case TLSConfig::OPT_TLS_PASSWORD:
					tlsPassword = args->OptionArg();
					break;
				case TLSConfig::OPT_TLS_CA_FILE:
					tlsCAPath = args->OptionArg();
					break;
				case TLSConfig::OPT_TLS_KEY:
					tlsKeyPath = args->OptionArg();
					break;
				case TLSConfig::OPT_TLS_VERIFY_PEERS:
					tlsVerifyPeers = args->OptionArg();
					break;
#endif
				case OPT_DUMP_BEGIN:
					dumpBegin = parseVersion(args->OptionArg());
					break;
				case OPT_DUMP_END:
					dumpEnd = parseVersion(args->OptionArg());
					break;
				case OPT_JSON:
					jsonOutput = true;
					break;
			}
		}

		// Process the extra arguments
		for (int argLoop = 0; argLoop < args->FileCount(); argLoop++)
		{
			switch (programExe)
			{
			case EXE_AGENT:
				fprintf(stderr, "ERROR: Backup Agent does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

				// Add the backup key range
			case EXE_BACKUP:
				// Error, if the keys option was not specified
				if (backupKeys.size() == 0) {
					fprintf(stderr, "ERROR: Unknown backup option value `%s'\n", args->File(argLoop));
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				// Otherwise, assume the item is a key range
				else {
					try {
						addKeyRange(args->File(argLoop), backupKeys);
					}
					catch (Error& ) {
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
				}
				break;

			case EXE_RESTORE:
				fprintf(stderr, "ERROR: FDB Restore does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case EXE_FASTRESTORE_AGENT:
				fprintf(stderr, "ERROR: FDB Fast Restore Agent does not support argument value `%s'\n",
				        args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case EXE_DR_AGENT:
				fprintf(stderr, "ERROR: DR Agent does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case EXE_DB_BACKUP:
				// Error, if the keys option was not specified
				if (backupKeys.size() == 0) {
					fprintf(stderr, "ERROR: Unknown DR option value `%s'\n", args->File(argLoop));
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				// Otherwise, assume the item is a key range
				else {
					try {
						addKeyRange(args->File(argLoop), backupKeys);
					}
					catch (Error& ) {
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
				}
				break;

			case EXE_UNDEFINED:
			default:
				return FDB_EXIT_ERROR;
			}
		}

		// Delete the simple option object, if defined
		if (args)
		{
			delete args;
			args = NULL;
		}

		delete FLOW_KNOBS;
		FlowKnobs* flowKnobs = new FlowKnobs;
		FLOW_KNOBS = flowKnobs;

		delete CLIENT_KNOBS;
		ClientKnobs* clientKnobs = new ClientKnobs;
		CLIENT_KNOBS = clientKnobs;

		for(auto k=knobs.begin(); k!=knobs.end(); ++k) {
			try {
				if (!flowKnobs->setKnob( k->first, k->second ) &&
					!clientKnobs->setKnob( k->first, k->second ))
				{
					fprintf(stderr, "WARNING: Unrecognized knob option '%s'\n", k->first.c_str());
					TraceEvent(SevWarnAlways, "UnrecognizedKnobOption").detail("Knob", printable(k->first));
				}
			} catch (Error& e) {
				if (e.code() == error_code_invalid_option_value) {
					fprintf(stderr, "WARNING: Invalid value '%s' for knob option '%s'\n", k->second.c_str(), k->first.c_str());
					TraceEvent(SevWarnAlways, "InvalidKnobValue").detail("Knob", printable(k->first)).detail("Value", printable(k->second));
				}
				else {
					fprintf(stderr, "ERROR: Failed to set knob option '%s': %s\n", k->first.c_str(), e.what());
					TraceEvent(SevError, "FailedToSetKnob").detail("Knob", printable(k->first)).detail("Value", printable(k->second)).error(e);
					throw;
				}
			}
		}

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		flowKnobs->initialize(true);
		clientKnobs->initialize(true);

		if (trace) {
			if(!traceLogGroup.empty())
				setNetworkOption(FDBNetworkOptions::TRACE_LOG_GROUP, StringRef(traceLogGroup));

			if (traceDir.empty())
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE);
			else
				setNetworkOption(FDBNetworkOptions::TRACE_ENABLE, StringRef(traceDir));
			if (!traceFormat.empty()) {
				setNetworkOption(FDBNetworkOptions::TRACE_FORMAT, StringRef(traceFormat));
			}

			setNetworkOption(FDBNetworkOptions::ENABLE_SLOW_TASK_PROFILING);
		}
		setNetworkOption(FDBNetworkOptions::DISABLE_CLIENT_STATISTICS_LOGGING);

		// deferred TLS options
		if (tlsCertPath.size()) {
			try {
				setNetworkOption(FDBNetworkOptions::TLS_CERT_PATH, tlsCertPath);
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: cannot set TLS certificate path to `%s' (%s)\n", tlsCertPath.c_str(), e.what());
				return 1;
			}
		}

		if (tlsCAPath.size()) {
			try {
				setNetworkOption(FDBNetworkOptions::TLS_CA_PATH, tlsCAPath);
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: cannot set TLS CA path to `%s' (%s)\n", tlsCAPath.c_str(), e.what());
				return 1;
			}
		}
		if (tlsKeyPath.size()) {
			try {
				if (tlsPassword.size())
					setNetworkOption(FDBNetworkOptions::TLS_PASSWORD, tlsPassword);

				setNetworkOption(FDBNetworkOptions::TLS_KEY_PATH, tlsKeyPath);
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: cannot set TLS key path to `%s' (%s)\n", tlsKeyPath.c_str(), e.what());
				return 1;
			}
		}
		if (tlsVerifyPeers.size()) {
			try {
				setNetworkOption(FDBNetworkOptions::TLS_VERIFY_PEERS, tlsVerifyPeers);
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: cannot set TLS peer verification to `%s' (%s)\n", tlsVerifyPeers.c_str(), e.what());
				return 1;
			}
		}

		Error::init();
		std::set_new_handler( &platform::outOfMemory );
		setMemoryQuota( memLimit );

		Reference<ClusterConnectionFile> ccf;
		Database db;
		Reference<ClusterConnectionFile> sourceCcf;
		Database sourceDb;
		FileBackupAgent ba;
		Key tag;
		Future<Optional<Void>> f;
		Future<Optional<int>> fstatus;
		Reference<IBackupContainer> c;

		try {
			setupNetwork(0, true);
		}
		catch (Error& e) {
			fprintf(stderr, "ERROR: %s\n", e.what());
			return FDB_EXIT_ERROR;
		}

		TraceEvent("ProgramStart")
			.setMaxEventLength(12000)
			.detail("SourceVersion", getSourceVersion())
			.detail("Version", FDB_VT_VERSION )
			.detail("PackageName", FDB_VT_PACKAGE_NAME)
			.detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
			.setMaxFieldLength(10000)
			.detail("CommandLine", commandLine)
			.setMaxFieldLength(0)
			.detail("MemoryLimit", memLimit)
			.trackLatest("ProgramStart");

		// Ordinarily, this is done when the network is run. However, network thread should be set before TraceEvents are logged. This thread will eventually run the network, so call it now.
		TraceEvent::setNetworkThread();

		// Add blob credentials files from the environment to the list collected from the command line.
		const char *blobCredsFromENV = getenv("FDB_BLOB_CREDENTIALS");
		if(blobCredsFromENV != nullptr) {
			StringRef t((uint8_t*)blobCredsFromENV, strlen(blobCredsFromENV));
			do {
				StringRef file = t.eat(":");
				if(file.size() != 0)
				blobCredentials.push_back(file.toString());
			} while(t.size() != 0);
		}

		// Update the global blob credential files list
		std::vector<std::string> *pFiles = (std::vector<std::string> *)g_network->global(INetwork::enBlobCredentialFiles);
		if(pFiles != nullptr) {
			for(auto &f : blobCredentials) {
				pFiles->push_back(f);
			}
		}

		// Opens a trace file if trace is set (and if a trace file isn't already open)
		// For most modes, initCluster() will open a trace file, but some fdbbackup operations do not require
		// a cluster so they should use this instead.
		auto initTraceFile = [&]() {
			if(trace)
				openTraceFile(NetworkAddress(), traceRollSize, traceMaxLogsSize, traceDir, "trace", traceLogGroup);
		};

		auto initCluster = [&](bool quiet = false) {
			auto resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(clusterFile);
			try {
				ccf = Reference<ClusterConnectionFile>(new ClusterConnectionFile(resolvedClusterFile.first));
			}
			catch (Error& e) {
				if(!quiet)
					fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedClusterFile, e).c_str());
				return false;
			}

			try {
				db = Database::createDatabase(ccf, -1, true, localities);
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: %s\n", e.what());
				fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n", ccf->getFilename().c_str());
				return false;
			}

			return true;
		};

		if(sourceClusterFile.size()) {
			auto resolvedSourceClusterFile = ClusterConnectionFile::lookupClusterFileName(sourceClusterFile);
			try {
				sourceCcf = Reference<ClusterConnectionFile>(new ClusterConnectionFile(resolvedSourceClusterFile.first));
			}
			catch (Error& e) {
				fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedSourceClusterFile, e).c_str());
				return FDB_EXIT_ERROR;
			}

			try {
				sourceDb = Database::createDatabase(sourceCcf, -1, true, localities);
			}
			catch (Error& e) {
				fprintf(stderr, "ERROR: %s\n", e.what());
				fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n", sourceCcf->getFilename().c_str());
				return FDB_EXIT_ERROR;
			}
		}

		switch (programExe)
		{
		case EXE_AGENT:
			if(!initCluster())
				return FDB_EXIT_ERROR;
			f = stopAfter(runAgent(db));
			break;
		case EXE_BACKUP:
			switch (backupType)
			{
			case BACKUP_START:
			{
				if(!initCluster())
					return FDB_EXIT_ERROR;
				// Test out the backup url to make sure it parses.  Doesn't test to make sure it's actually writeable.
				openBackupContainer(argv[0], destinationContainer);
				f = stopAfter(submitBackup(db, destinationContainer, snapshotIntervalSeconds, backupKeys, tagName,
				                           dryRun, waitForDone, stopWhenDone, usePartitionedLog));
				break;
			}

			case BACKUP_MODIFY:
			{
				if(!initCluster())
					return FDB_EXIT_ERROR;

				f = stopAfter( modifyBackup(db, tagName, modifyOptions) );
				break;
			}

			case BACKUP_STATUS:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( statusBackup(db, tagName, true, jsonOutput) );
				break;

			case BACKUP_ABORT:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( abortBackup(db, tagName) );
				break;

			case BACKUP_CLEANUP:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( cleanupMutations(db, deleteData) );
				break;

			case BACKUP_WAIT:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( waitBackup(db, tagName, stopWhenDone) );
				break;

			case BACKUP_DISCONTINUE:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( discontinueBackup(db, tagName, waitForDone) );
				break;

			case BACKUP_PAUSE:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( changeBackupResumed(db, true) );
				break;

			case BACKUP_RESUME:
				if(!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter( changeBackupResumed(db, false) );
				break;

			case BACKUP_EXPIRE:
				initTraceFile();
				// Must have a usable cluster if either expire DateTime options were used
				if(!expireDatetime.empty() || !expireRestorableAfterDatetime.empty()) {
					if(!initCluster())
						return FDB_EXIT_ERROR;
				}
				f = stopAfter( expireBackupData(argv[0], destinationContainer, expireVersion, expireDatetime, db, forceAction, expireRestorableAfterVersion, expireRestorableAfterDatetime) );
				break;

			case BACKUP_DELETE:
				initTraceFile();
				f = stopAfter( deleteBackupContainer(argv[0], destinationContainer) );
				break;

			case BACKUP_DESCRIBE:
				initTraceFile();
				// If timestamp lookups are desired, require a cluster file
				if(describeTimestamps && !initCluster())
					return FDB_EXIT_ERROR;

				// Only pass database optionDatabase Describe will lookup version timestamps if a cluster file was given, but quietly skip them if not.
				f = stopAfter( describeBackup(argv[0], destinationContainer, describeDeep, describeTimestamps ? Optional<Database>(db) : Optional<Database>(), jsonOutput) );
				break;

			case BACKUP_LIST:
				initTraceFile();
				f = stopAfter( listBackup(baseUrl) );
				break;

			case BACKUP_DUMP:
				initTraceFile();
				f = stopAfter( dumpBackupData(argv[0], destinationContainer, dumpBegin, dumpEnd) );
				break;

			case BACKUP_UNDEFINED:
			default:
				fprintf(stderr, "ERROR: Unsupported backup action %s\n", argv[1]);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;
			}

			break;
		case EXE_RESTORE:
			if(dryRun) {
				if(restoreType != RESTORE_START) {
					fprintf(stderr, "Restore dry run only works for 'start' command\n");
					return FDB_EXIT_ERROR;
				}

				// Must explicitly call trace file options handling if not calling Database::createDatabase()
				initTraceFile();
			}
			else {
				if(restoreClusterFileDest.empty()) {
					fprintf(stderr, "Restore destination cluster file must be specified explicitly.\n");
					return FDB_EXIT_ERROR;
				}

				if(!fileExists(restoreClusterFileDest)) {
					fprintf(stderr, "Restore destination cluster file '%s' does not exist.\n", restoreClusterFileDest.c_str());
					return FDB_EXIT_ERROR;
				}

				try {
					db = Database::createDatabase(restoreClusterFileDest, Database::API_VERSION_LATEST);
				} catch(Error &e) {
					fprintf(stderr, "Restore destination cluster file '%s' invalid: %s\n", restoreClusterFileDest.c_str(), e.what());
					return FDB_EXIT_ERROR;
				}
			}

			switch(restoreType) {
				case RESTORE_START:
					f = stopAfter( runRestore(db, restoreClusterFileOrig, tagName, restoreContainer, backupKeys, restoreVersion, restoreTimestamp, !dryRun, !quietDisplay, waitForDone, addPrefix, removePrefix) );
					break;
				case RESTORE_WAIT:
					f = stopAfter( success(ba.waitRestore(db, KeyRef(tagName), true)) );
					break;
				case RESTORE_ABORT:
				    f = stopAfter(
				        map(ba.abortRestore(db, KeyRef(tagName)), [tagName](FileBackupAgent::ERestoreState s) -> Void {
					        printf("RESTORE_ABORT Tag: %s  State: %s\n", tagName.c_str(),
					               FileBackupAgent::restoreStateText(s).toString().c_str());
					        return Void();
				        }));
				    break;
				case RESTORE_STATUS:
					// If no tag is specifically provided then print all tag status, don't just use "default"
					if(tagProvided)
						tag = tagName;
					f = stopAfter( map(ba.restoreStatus(db, KeyRef(tag)), [](std::string s) -> Void {
						printf("%s\n", s.c_str());
						return Void();
					}) );
					break;
				default:
					throw restore_error();
			}
			break;
		case EXE_FASTRESTORE_AGENT:
			// TODO: We have not implmented the code commented out in this case
			if (!initCluster()) return FDB_EXIT_ERROR;
			switch (restoreType) {
			case RESTORE_START:
				f = stopAfter(runFastRestoreAgent(db, tagName, restoreContainer, backupKeys, restoreVersion, !dryRun,
				                                  !quietDisplay, waitForDone));
				break;
			case RESTORE_WAIT:
				printf("[TODO][ERROR] FastRestore does not support RESTORE_WAIT yet!\n");
				throw restore_error();
				//					f = stopAfter( success(ba.waitRestore(db, KeyRef(tagName), true)) );
				break;
			case RESTORE_ABORT:
				printf("[TODO][ERROR] FastRestore does not support RESTORE_ABORT yet!\n");
				throw restore_error();
				//					f = stopAfter( map(ba.abortRestore(db, KeyRef(tagName)),
				//[tagName](FileBackupAgent::ERestoreState s) -> Void { 						printf("Tag: %s  State: %s\n", tagName.c_str(),
				//FileBackupAgent::restoreStateText(s).toString().c_str()); 						return Void();
				//					}) );
				break;
			case RESTORE_STATUS:
				printf("[TODO][ERROR] FastRestore does not support RESTORE_STATUS yet!\n");
				throw restore_error();
				// If no tag is specifically provided then print all tag status, don't just use "default"
				if (tagProvided) tag = tagName;
				//					f = stopAfter( map(ba.restoreStatus(db, KeyRef(tag)), [](std::string s) -> Void {
				//						printf("%s\n", s.c_str());
				//						return Void();
				//					}) );
				break;
			default:
				throw restore_error();
			}
			break;
		case EXE_DR_AGENT:
			if(!initCluster())
				return FDB_EXIT_ERROR;
			f = stopAfter( runDBAgent(sourceDb, db) );
			break;
		case EXE_DB_BACKUP:
			if(!initCluster())
				return FDB_EXIT_ERROR;
			switch (dbType)
			{
			case DB_START:
				f = stopAfter( submitDBBackup(sourceDb, db, backupKeys, tagName) );
				break;
			case DB_STATUS:
				f = stopAfter( statusDBBackup(sourceDb, db, tagName, maxErrors) );
				break;
			case DB_SWITCH:
				f = stopAfter( switchDBBackup(sourceDb, db, backupKeys, tagName, forceAction) );
				break;
			case DB_ABORT:
				f = stopAfter( abortDBBackup(sourceDb, db, tagName, partial) );
				break;
			case DB_PAUSE:
				f = stopAfter( changeDBBackupResumed(sourceDb, db, true) );
				break;
			case DB_RESUME:
				f = stopAfter( changeDBBackupResumed(sourceDb, db, false) );
				break;
			case DB_UNDEFINED:
			default:
				fprintf(stderr, "ERROR: Unsupported DR action %s\n", argv[1]);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;
			}
			break;
		case EXE_UNDEFINED:
		default:
			return FDB_EXIT_ERROR;
		}

		runNetwork();

		if(f.isValid() && f.isReady() && !f.isError() && !f.get().present()) {
			status = FDB_EXIT_ERROR;
		}

		if(fstatus.isValid() && fstatus.isReady() && !fstatus.isError() && fstatus.get().present()) {
			status = fstatus.get().get();
		}

		#ifdef ALLOC_INSTRUMENTATION
		{
			cout << "Page Counts: "
				<< FastAllocator<16>::pageCount << " "
				<< FastAllocator<32>::pageCount << " "
				<< FastAllocator<64>::pageCount << " "
				<< FastAllocator<128>::pageCount << " "
				<< FastAllocator<256>::pageCount << " "
				<< FastAllocator<512>::pageCount << " "
				<< FastAllocator<1024>::pageCount << " "
				<< FastAllocator<2048>::pageCount << " "
				<< FastAllocator<4096>::pageCount << " "
				<< FastAllocator<8192>::pageCount << endl;

			vector< std::pair<std::string, const char*> > typeNames;
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
				printf("%+d\t%+d\t%d\t%d\t%s\n", f.allocCount, -f.deallocCount, f.allocCount-f.deallocCount, f.maxAllocated, typeNames[i].first.c_str());
			}

			// We're about to exit and clean up data structures, this will wreak havoc on allocation recording
			memSample_entered = true;
		}
		#endif
	} catch (Error& e) {
		TraceEvent(SevError, "MainError").error(e);
		status = FDB_EXIT_MAIN_ERROR;
	} catch (boost::system::system_error& e) {
		if (g_network) {
			TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		} else {
			fprintf(stderr, "ERROR: %s (%d)\n", e.what(), e.code().value());
		}
		status = FDB_EXIT_MAIN_EXCEPTION;
	} catch (std::exception& e) {
		TraceEvent(SevError, "MainError").error(unknown_error()).detail("RootException", e.what());
		status = FDB_EXIT_MAIN_EXCEPTION;
	}

	flushAndExit(status);
}
