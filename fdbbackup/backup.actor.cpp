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

#include "fdbbackup/BackupTLSConfig.h"
#include "fdbclient/JsonBuilder.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/Trace.h"
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
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/S3BlobStore.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"

#include "flow/Platform.h"

#include <stdarg.h>
#include <stdio.h>
#include <cinttypes>
#include <algorithm> // std::transform
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

#ifdef __linux__
#include <execinfo.h>
#ifdef ALLOC_INSTRUMENTATION
#include <cxxabi.h>
#endif
#endif

#include "fdbbackup/BackupRestoreCommon.h"
#include "fdbclient/versions.h"

#include "flow/SimpleOpt.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Type of program being executed
enum class ProgramExe { BACKUP, DB_BACKUP, UNDEFINED };

enum class BackupType {
	UNDEFINED = 0,
	START,
	MODIFY,
	STATUS,
	ABORT,
	WAIT,
	DISCONTINUE,
	PAUSE,
	RESUME,
	EXPIRE,
	DELETE_BACKUP,
	DESCRIBE,
	LIST,
	QUERY,
	DUMP,
	CLEANUP
};

enum class DBType { UNDEFINED = 0, START, STATUS, SWITCH, ABORT, PAUSE, RESUME };

CSimpleOpt::SOption g_rgBackupStartOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_WAITFORDONE, "-w", SO_NONE },
	{ OPT_WAITFORDONE, "--waitfordone", SO_NONE },
	{ OPT_NOSTOPWHENDONE, "-z", SO_NONE },
	{ OPT_NOSTOPWHENDONE, "--no-stop-when-done", SO_NONE },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	// Enable "-p" option after GA
	// { OPT_USE_PARTITIONED_LOG, "-p",                 SO_NONE },
	{ OPT_USE_PARTITIONED_LOG, "--partitioned_log_experimental", SO_NONE },
	{ OPT_SNAPSHOTINTERVAL, "-s", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "--snapshot_interval", SO_REQ_SEP },
	{ OPT_INITIAL_SNAPSHOT_INTERVAL, "--initial_snapshot_interval", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
	{ OPT_DRYRUN, "-n", SO_NONE },
	{ OPT_DRYRUN, "--dryrun", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_INCREMENTALONLY, "--incremental", SO_NONE },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupModifyOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_MOD_VERIFY_UID, "--verify_uid", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "-s", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "--snapshot_interval", SO_REQ_SEP },
	{ OPT_MOD_ACTIVE_INTERVAL, "--active_snapshot_interval", SO_REQ_SEP },

	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStatusOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_ERRORLIMIT, "-e", SO_REQ_SEP },
	{ OPT_ERRORLIMIT, "--errorlimit", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_JSON, "--json", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupAbortOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupCleanupOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_DELETE_DATA, "--delete_data", SO_NONE },
	{ OPT_MIN_CLEANUP_SECONDS, "--min_cleanup_seconds", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDiscontinueOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_WAITFORDONE, "-w", SO_NONE },
	{ OPT_WAITFORDONE, "--waitfordone", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupWaitOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_NOSTOPWHENDONE, "-z", SO_NONE },
	{ OPT_NOSTOPWHENDONE, "--no-stop-when-done", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupPauseOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupExpireOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_FORCE, "-f", SO_NONE },
	{ OPT_FORCE, "--force", SO_NONE },
	{ OPT_EXPIRE_RESTORABLE_AFTER_VERSION, "--restorable_after_version", SO_REQ_SEP },
	{ OPT_EXPIRE_RESTORABLE_AFTER_DATETIME, "--restorable_after_timestamp", SO_REQ_SEP },
	{ OPT_EXPIRE_BEFORE_VERSION, "--expire_before_version", SO_REQ_SEP },
	{ OPT_EXPIRE_BEFORE_DATETIME, "--expire_before_timestamp", SO_REQ_SEP },
	{ OPT_EXPIRE_MIN_RESTORABLE_DAYS, "--min_restorable_days", SO_REQ_SEP },
	{ OPT_EXPIRE_DELETE_BEFORE_DAYS, "--delete_before_days", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDeleteOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDescribeOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_DESCRIBE_DEEP, "--deep", SO_NONE },
	{ OPT_DESCRIBE_TIMESTAMPS, "--version_timestamps", SO_NONE },
	{ OPT_JSON, "--json", SO_NONE },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDumpOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster_file", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
	{ OPT_DUMP_BEGIN, "--begin", SO_REQ_SEP },
	{ OPT_DUMP_END, "--end", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupListOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_BASEURL, "-b", SO_REQ_SEP },
	{ OPT_BASEURL, "--base_url", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupQueryOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_RESTORE_TIMESTAMP, "--query_restore_timestamp", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "-qrv", SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "--query_restore_version", SO_REQ_SEP },
	{ OPT_BACKUPKEYS_FILTER, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS_FILTER, "--keys", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_VERSION, "-v", SO_NONE },
	{ OPT_VERSION, "--version", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob_credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBStartOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBStatusOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_ERRORLIMIT, "-e", SO_REQ_SEP },
	{ OPT_ERRORLIMIT, "--errorlimit", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBSwitchOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_FORCE, "-f", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBAbortOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_CLEANUP, "--cleanup", SO_NONE },
	{ OPT_DSTONLY, "--dstonly", SO_NONE },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBPauseOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace_format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob_", SO_REQ_SEP },
#ifndef TLS_DISABLED
	TLS_OPTION_FLAGS
#endif
	    SO_END_OF_OPTIONS
};

const KeyRef exeBackup = "fdbbackup"_sr;
const KeyRef exeDatabaseBackup = "fdbdr"_sr;

#ifdef _WIN32
void parentWatcher(void* parentHandle) {
	HANDLE parent = (HANDLE)parentHandle;
	int signal = WaitForSingleObject(parent, INFINITE);
	CloseHandle(parentHandle);
	if (signal == WAIT_OBJECT_0)
		criticalError(FDB_EXIT_SUCCESS, "ParentProcessExited", "Parent process exited");
	TraceEvent(SevError, "ParentProcessWaitFailed").detail("RetCode", signal).GetLastError();
}

#endif

static void printBackupUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [TOP_LEVEL_OPTIONS] (start | status | abort | wait | discontinue | pause | resume | expire | "
	       "delete | describe | list | query | cleanup) [ACTION_OPTIONS]\n\n",
	       exeBackup.toString().c_str());
	printf(" TOP LEVEL OPTIONS:\n");
	printf("  --build_flags  Print build information and exit.\n");
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");
	printf("\n");

	printf(" ACTION OPTIONS:\n");
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is first the value of the\n"
	       "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
	       "                 then `%s'.\n",
	       platform::getDefaultClusterFilePath().c_str());
	printf("  -d, --destcontainer URL\n"
	       "                 The Backup container URL for start, modify, describe, query, expire, and delete "
	       "operations.\n");
	printBackupContainerInfo();
	printf("  -b, --base_url BASEURL\n"
	       "                 Base backup URL for list operations.  This looks like a Backup URL but without a backup "
	       "name.\n");
	printf("  --blob_credentials FILE\n"
	       "                 File containing blob credentials in JSON format.  Can be specified multiple times for "
	       "multiple files.  See below for more details.\n");
	printf("  --expire_before_timestamp DATETIME\n"
	       "                 Datetime cutoff for expire operations.  Requires a cluster file and will use "
	       "version/timestamp metadata\n"
	       "                 in the database to obtain a cutoff version very close to the timestamp given in %s.\n",
	       BackupAgentBase::timeFormat().c_str());
	printf("  --expire_before_version VERSION\n"
	       "                 Version cutoff for expire operations.  Deletes data files containing no data at or after "
	       "VERSION.\n");
	printf("  --delete_before_days NUM_DAYS\n"
	       "                 Another way to specify version cutoff for expire operations.  Deletes data files "
	       "containing no data at or after a\n"
	       "                 version approximately NUM_DAYS days worth of versions prior to the latest log version in "
	       "the backup.\n");
	printf("  -qrv --query_restore_version VERSION\n"
	       "                 For query operations, set target version for restoring a backup. Set -1 for maximum\n"
	       "                 restorable version (default) and -2 for minimum restorable version.\n");
	printf(
	    "  --query_restore_timestamp DATETIME\n"
	    "                 For query operations, instead of a numeric version, use this to specify a timestamp in %s\n",
	    BackupAgentBase::timeFormat().c_str());
	printf(
	    "                 and it will be converted to a version from that time using metadata in the cluster file.\n");
	printf("  --restorable_after_timestamp DATETIME\n"
	       "                 For expire operations, set minimum acceptable restorability to the version equivalent of "
	       "DATETIME and later.\n");
	printf("  --restorable_after_version VERSION\n"
	       "                 For expire operations, set minimum acceptable restorability to the VERSION and later.\n");
	printf("  --min_restorable_days NUM_DAYS\n"
	       "                 For expire operations, set minimum acceptable restorability to approximately NUM_DAYS "
	       "days worth of versions\n"
	       "                 prior to the latest log version in the backup.\n");
	printf("  --version_timestamps\n");
	printf("                 For describe operations, lookup versions in the database to obtain timestamps.  A cluster "
	       "file is required.\n");
	printf(
	    "  -f, --force    For expire operations, force expiration even if minimum restorability would be violated.\n");
	printf("  -s, --snapshot_interval DURATION\n"
	       "                 For start or modify operations, specifies the backup's default target snapshot interval "
	       "as DURATION seconds.  Defaults to %d for start operations.\n",
	       CLIENT_KNOBS->BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC);
	printf("  --active_snapshot_interval DURATION\n"
	       "                 For modify operations, sets the desired interval for the backup's currently active "
	       "snapshot, relative to the start of the snapshot.\n");
	printf("  --verify_uid UID\n"
	       "                 Specifies a UID to verify against the BackupUID of the running backup.  If provided, the "
	       "UID is verified in the same transaction\n"
	       "                 which sets the new backup parameters (if the UID matches).\n");
	printf("  -e ERRORLIMIT  The maximum number of errors printed by status (default is 10).\n");
	printf("  -k KEYS        List of key ranges to backup or to filter the backup in query operations.\n"
	       "                 If not specified, the entire database will be backed up or no filter will be applied.\n");
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
	printf("  --incremental\n"
	       "                 Performs incremental backup without the base backup.\n"
	       "                 This option indicates to the backup agent that it will only need to record the log files, "
	       "and ignore the range files.\n");
#ifndef TLS_DISABLED
	printf(TLS_HELP);
#endif
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

static void printDBBackupUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [TOP_LEVEL_OPTIONS] (start | status | switch | abort | pause | resume) [OPTIONS]\n\n",
	       exeDatabaseBackup.toString().c_str());

	printf(" TOP LEVEL OPTIONS:\n");
	printf("  --build_flags  Print build information and exit.\n");
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");
	printf("\n");

	printf(" ACTION OPTIONS:\n");
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
	printf("  --dstonly      Abort will not make any changes on the source cluster.\n");
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

static void printUsage(ProgramExe programExe, bool devhelp) {
	switch (programExe) {
	case ProgramExe::BACKUP:
		printBackupUsage(devhelp);
		break;
	case ProgramExe::DB_BACKUP:
		printDBBackupUsage(devhelp);
		break;
	case ProgramExe::UNDEFINED:
	default:
		break;
	}
	return;
}

extern bool g_crashOnError;

// Return the type of program executable based on the name of executable file
ProgramExe getProgramType(std::string programExe) {
	ProgramExe enProgramExe = ProgramExe::UNDEFINED;

	// lowercase the string
	std::transform(programExe.begin(), programExe.end(), programExe.begin(), ::tolower);

	// Remove the extension, if Windows
#ifdef _WIN32
	size_t lastDot = programExe.find_last_of(".");
	if (lastDot != std::string::npos) {
		size_t lastSlash = programExe.find_last_of("\\");

		// Ensure last dot is after last slash, if present
		if ((lastSlash == std::string::npos) || (lastSlash < lastDot)) {
			programExe = programExe.substr(0, lastDot);
		}
	}
#endif
	// For debugging convenience, remove .debug suffix if present.
	if (StringRef(programExe).endsWith(LiteralStringRef(".debug")))
		programExe = programExe.substr(0, programExe.size() - 6);

	// Check if backup
	else if ((programExe.length() >= exeBackup.size()) &&
	         (programExe.compare(
	              programExe.length() - exeBackup.size(), exeBackup.size(), (const char*)exeBackup.begin()) == 0)) {
		enProgramExe = ProgramExe::BACKUP;
	}

	// Check if db backup
	else if ((programExe.length() >= exeDatabaseBackup.size()) &&
	         (programExe.compare(programExe.length() - exeDatabaseBackup.size(),
	                             exeDatabaseBackup.size(),
	                             (const char*)exeDatabaseBackup.begin()) == 0)) {
		enProgramExe = ProgramExe::DB_BACKUP;
	}

	return enProgramExe;
}

BackupType getBackupType(std::string backupType) {
	BackupType enBackupType = BackupType::UNDEFINED;

	// lowercase the string
	std::transform(backupType.begin(), backupType.end(), backupType.begin(), ::tolower);

	static std::map<std::string, BackupType> values;
	if (values.empty()) {
		values["start"] = BackupType::START;
		values["status"] = BackupType::STATUS;
		values["abort"] = BackupType::ABORT;
		values["cleanup"] = BackupType::CLEANUP;
		values["wait"] = BackupType::WAIT;
		values["discontinue"] = BackupType::DISCONTINUE;
		values["pause"] = BackupType::PAUSE;
		values["resume"] = BackupType::RESUME;
		values["expire"] = BackupType::EXPIRE;
		values["delete"] = BackupType::DELETE_BACKUP;
		values["describe"] = BackupType::DESCRIBE;
		values["list"] = BackupType::LIST;
		values["query"] = BackupType::QUERY;
		values["dump"] = BackupType::DUMP;
		values["modify"] = BackupType::MODIFY;
	}

	auto i = values.find(backupType);
	if (i != values.end())
		enBackupType = i->second;

	return enBackupType;
}

DBType getDBType(std::string dbType) {
	DBType enBackupType = DBType::UNDEFINED;

	// lowercase the string
	std::transform(dbType.begin(), dbType.end(), dbType.begin(), ::tolower);

	static std::map<std::string, DBType> values;
	if (values.empty()) {
		values["start"] = DBType::START;
		values["status"] = DBType::STATUS;
		values["switch"] = DBType::SWITCH;
		values["abort"] = DBType::ABORT;
		values["pause"] = DBType::PAUSE;
		values["resume"] = DBType::RESUME;
	}

	auto i = values.find(dbType);
	if (i != values.end())
		enBackupType = i->second;

	return enBackupType;
}

ACTOR Future<Void> submitDBBackup(Database src,
                                  Database dest,
                                  Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                  std::string tagName) {
	try {
		state DatabaseBackupAgent backupAgent(src);

		// Backup everything, if no ranges were specified
		if (backupRanges.size() == 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		}

		wait(backupAgent.submitBackup(dest, KeyRef(tagName), backupRanges, false, StringRef(), StringRef(), true));

		// Check if a backup agent is running
		bool agentRunning = wait(backupAgent.checkActive(dest));

		if (!agentRunning) {
			printf("The DR on tag `%s' was successfully submitted but no DR agents are responding.\n",
			       printable(StringRef(tagName)).c_str());

			// Throw an error that will not display any additional information
			throw actor_cancelled();
		} else {
			printf("The DR on tag `%s' was successfully submitted.\n", printable(StringRef(tagName)).c_str());
		}
	}

	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code()) {
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

ACTOR Future<Void> submitBackup(Database db,
                                std::string url,
                                int initialSnapshotIntervalSeconds,
                                int snapshotIntervalSeconds,
                                Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                std::string tagName,
                                bool dryRun,
                                bool waitForCompletion,
                                bool stopWhenDone,
                                bool usePartitionedLog,
                                bool incrementalBackupOnly) {
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
				if (BackupAgentBase::isRunnable(backupStatus)) {
					throw backup_duplicate();
				}
			}

			if (waitForCompletion) {
				printf("Submitted and now waiting for the backup on tag `%s' to complete. (DRY RUN)\n",
				       printable(StringRef(tagName)).c_str());
			}

			else {
				// Check if a backup agent is running
				bool agentRunning = wait(backupAgent.checkActive(db));

				if (!agentRunning) {
					printf("The backup on tag `%s' was successfully submitted but no backup agents are responding. "
					       "(DRY RUN)\n",
					       printable(StringRef(tagName)).c_str());

					// Throw an error that will not display any additional information
					throw actor_cancelled();
				} else {
					printf("The backup on tag `%s' was successfully submitted. (DRY RUN)\n",
					       printable(StringRef(tagName)).c_str());
				}
			}
		}

		else {
			wait(backupAgent.submitBackup(db,
			                              KeyRef(url),
			                              initialSnapshotIntervalSeconds,
			                              snapshotIntervalSeconds,
			                              tagName,
			                              backupRanges,
			                              stopWhenDone,
			                              usePartitionedLog,
			                              incrementalBackupOnly));

			// Wait for the backup to complete, if requested
			if (waitForCompletion) {
				printf("Submitted and now waiting for the backup on tag `%s' to complete.\n",
				       printable(StringRef(tagName)).c_str());
				wait(success(backupAgent.waitBackup(db, tagName)));
			} else {
				// Check if a backup agent is running
				bool agentRunning = wait(backupAgent.checkActive(db));

				if (!agentRunning) {
					printf("The backup on tag `%s' was successfully submitted but no backup agents are responding.\n",
					       printable(StringRef(tagName)).c_str());

					// Throw an error that will not display any additional information
					throw actor_cancelled();
				} else {
					printf("The backup on tag `%s' was successfully submitted.\n",
					       printable(StringRef(tagName)).c_str());
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code()) {
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

ACTOR Future<Void> switchDBBackup(Database src,
                                  Database dest,
                                  Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                  std::string tagName,
                                  bool forceAction) {
	try {
		state DatabaseBackupAgent backupAgent(src);

		// Backup everything, if no ranges were specified
		if (backupRanges.size() == 0) {
			backupRanges.push_back_deep(backupRanges.arena(), normalKeys);
		}

		wait(backupAgent.atomicSwitchover(dest, KeyRef(tagName), backupRanges, StringRef(), StringRef(), forceAction));
		printf("The DR on tag `%s' was successfully switched.\n", printable(StringRef(tagName)).c_str());
	}

	catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code()) {
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
	try {
		state DatabaseBackupAgent backupAgent(src);

		std::string statusText = wait(backupAgent.getStatus(dest, errorLimit, StringRef(tagName)));
		printf("%s\n", statusText.c_str());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> statusBackup(Database db, std::string tagName, bool showErrors, bool json) {
	try {
		state FileBackupAgent backupAgent;

		std::string statusText =
		    wait(json ? backupAgent.getStatusJSON(db, tagName) : backupAgent.getStatus(db, showErrors, tagName));
		printf("%s\n", statusText.c_str());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> abortDBBackup(Database src, Database dest, std::string tagName, bool partial, bool dstOnly) {
	try {
		state DatabaseBackupAgent backupAgent(src);

		wait(backupAgent.abortBackup(dest, Key(tagName), partial, false, dstOnly));
		wait(backupAgent.unlockBackup(dest, Key(tagName)));

		printf("The DR on tag `%s' was successfully aborted.\n", printable(StringRef(tagName)).c_str());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code()) {
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
	try {
		state FileBackupAgent backupAgent;

		wait(backupAgent.abortBackup(db, tagName));

		printf("The backup on tag `%s' was successfully aborted.\n", printable(StringRef(tagName)).c_str());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code()) {
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
	try {
		wait(cleanupBackup(db, deleteData));
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> waitBackup(Database db, std::string tagName, bool stopWhenDone) {
	try {
		state FileBackupAgent backupAgent;

		EBackupState status = wait(backupAgent.waitBackup(db, tagName, stopWhenDone));

		printf("The backup on tag `%s' %s.\n",
		       printable(StringRef(tagName)).c_str(),
		       BackupAgentBase::getStateText(status));
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> discontinueBackup(Database db, std::string tagName, bool waitForCompletion) {
	try {
		state FileBackupAgent backupAgent;

		wait(backupAgent.discontinueBackup(db, StringRef(tagName)));

		// Wait for the backup to complete, if requested
		if (waitForCompletion) {
			printf("Discontinued and now waiting for the backup on tag `%s' to complete.\n",
			       printable(StringRef(tagName)).c_str());
			wait(success(backupAgent.waitBackup(db, tagName)));
		} else {
			printf("The backup on tag `%s' was successfully discontinued.\n", printable(StringRef(tagName)).c_str());
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		switch (e.code()) {
		case error_code_backup_error:
			fprintf(stderr, "ERROR: An encounter was error during submission\n");
			break;
		case error_code_backup_unneeded:
			fprintf(stderr, "ERROR: A backup in not running on tag `%s'\n", printable(StringRef(tagName)).c_str());
			break;
		case error_code_backup_duplicate:
			fprintf(stderr,
			        "ERROR: The backup on tag `%s' is already discontinued\n",
			        printable(StringRef(tagName)).c_str());
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
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
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
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> dumpBackupData(const char* name,
                                  std::string destinationContainer,
                                  Version beginVersion,
                                  Version endVersion) {
	state Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);

	if (beginVersion < 0 || endVersion < 0) {
		BackupDescription desc = wait(c->describeBackup());

		if (!desc.maxLogEnd.present()) {
			fprintf(stderr, "ERROR: Backup must have log data in order to use relative begin/end versions.\n");
			throw backup_invalid_info();
		}

		if (beginVersion < 0) {
			beginVersion += desc.maxLogEnd.get();
		}

		if (endVersion < 0) {
			endVersion += desc.maxLogEnd.get();
		}
	}

	printf("Scanning version range %" PRId64 " to %" PRId64 "\n", beginVersion, endVersion);
	BackupFileList files = wait(c->dumpFileList(beginVersion, endVersion));
	files.toStream(stdout);

	return Void();
}

ACTOR Future<Void> expireBackupData(const char* name,
                                    std::string destinationContainer,
                                    Version endVersion,
                                    std::string endDatetime,
                                    Database db,
                                    bool force,
                                    Version restorableAfterVersion,
                                    std::string restorableAfterDatetime) {
	if (!endDatetime.empty()) {
		Version v = wait(timeKeeperVersionFromDatetime(endDatetime, db));
		endVersion = v;
	}

	if (!restorableAfterDatetime.empty()) {
		Version v = wait(timeKeeperVersionFromDatetime(restorableAfterDatetime, db));
		restorableAfterVersion = v;
	}

	if (endVersion == invalidVersion) {
		fprintf(stderr, "ERROR: No version or date/time is specified.\n");
		printHelpTeaser(name);
		throw backup_error();
		;
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
					if (p != lastProgress) {
						int spaces = lastProgress.size() - p.size();
						printf("\r%s%s", p.c_str(), (spaces > 0 ? std::string(spaces, ' ').c_str() : ""));
						lastProgress = p;
					}
				}
				when(wait(expire)) { break; }
			}
		}

		std::string p = progress.toString();
		int spaces = lastProgress.size() - p.size();
		printf("\r%s%s\n", p.c_str(), (spaces > 0 ? std::string(spaces, ' ').c_str() : ""));

		if (endVersion < 0)
			printf("All data before %" PRId64 " versions (%" PRId64
			       " days) prior to latest backup log has been deleted.\n",
			       -endVersion,
			       -endVersion / ((int64_t)24 * 3600 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
		else
			printf("All data before version %" PRId64 " has been deleted.\n", endVersion);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		if (e.code() == error_code_backup_cannot_expire)
			fprintf(stderr,
			        "ERROR: Requested expiration would be unsafe.  Backup would not meet minimum restorability.  Use "
			        "--force to delete data anyway.\n");
		else
			fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> deleteBackupContainer(const char* name, std::string destinationContainer) {
	try {
		state Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);
		state int numDeleted = 0;
		state Future<Void> done = c->deleteContainer(&numDeleted);

		state int lastUpdate = -1;
		printf("Deleting %s...\n", destinationContainer.c_str());

		loop {
			choose {
				when(wait(done)) { break; }
				when(wait(delay(5))) {
					if (numDeleted != lastUpdate) {
						printf("\r%d...", numDeleted);
						lastUpdate = numDeleted;
					}
				}
			}
		}
		printf("\r%d objects deleted\n", numDeleted);
		printf("The entire container has been deleted.\n");
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

ACTOR Future<Void> describeBackup(const char* name,
                                  std::string destinationContainer,
                                  bool deep,
                                  Optional<Database> cx,
                                  bool json) {
	try {
		Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer);
		state BackupDescription desc = wait(c->describeBackup(deep));
		if (cx.present())
			wait(desc.resolveVersionTimes(cx.get()));
		printf("%s\n", (json ? desc.toJSON() : desc.toString()).c_str());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

static void reportBackupQueryError(UID operationId, JsonBuilderObject& result, std::string errorMessage) {
	result["error"] = errorMessage;
	printf("%s\n", result.getJson().c_str());
	TraceEvent("BackupQueryFailure").detail("OperationId", operationId).detail("Reason", errorMessage);
}

// If restoreVersion is invalidVersion or latestVersion, use the maximum or minimum restorable version respectively for
// selected key ranges. If restoreTimestamp is specified, any specified restoreVersion will be overriden to the version
// resolved to that timestamp.
ACTOR Future<Void> queryBackup(const char* name,
                               std::string destinationContainer,
                               Standalone<VectorRef<KeyRangeRef>> keyRangesFilter,
                               Version restoreVersion,
                               std::string originalClusterFile,
                               std::string restoreTimestamp,
                               bool verbose) {
	state UID operationId = deterministicRandom()->randomUniqueID();
	state JsonBuilderObject result;
	state std::string errorMessage;
	result["key_ranges_filter"] = printable(keyRangesFilter);
	result["destination_container"] = destinationContainer;

	TraceEvent("BackupQueryStart")
	    .detail("OperationId", operationId)
	    .detail("DestinationContainer", destinationContainer)
	    .detail("KeyRangesFilter", printable(keyRangesFilter))
	    .detail("SpecifiedRestoreVersion", restoreVersion)
	    .detail("RestoreTimestamp", restoreTimestamp)
	    .detail("BackupClusterFile", originalClusterFile);

	// Resolve restoreTimestamp if given
	if (!restoreTimestamp.empty()) {
		if (originalClusterFile.empty()) {
			reportBackupQueryError(
			    operationId,
			    result,
			    format("an original cluster file must be given in order to resolve restore target timestamp '%s'",
			           restoreTimestamp.c_str()));
			return Void();
		}

		if (!fileExists(originalClusterFile)) {
			reportBackupQueryError(operationId,
			                       result,
			                       format("The specified original source database cluster file '%s' does not exist\n",
			                              originalClusterFile.c_str()));
			return Void();
		}

		Database origDb = Database::createDatabase(originalClusterFile, Database::API_VERSION_LATEST);
		Version v = wait(timeKeeperVersionFromDatetime(restoreTimestamp, origDb));
		result["restore_timestamp"] = restoreTimestamp;
		result["restore_timestamp_resolved_version"] = v;
		restoreVersion = v;
	}

	try {
		state Reference<IBackupContainer> bc = openBackupContainer(name, destinationContainer);
		if (restoreVersion == invalidVersion) {
			BackupDescription desc = wait(bc->describeBackup());
			if (desc.maxRestorableVersion.present()) {
				restoreVersion = desc.maxRestorableVersion.get();
				// Use continuous log end version for the maximum restorable version for the key ranges.
			} else if (keyRangesFilter.size() && desc.contiguousLogEnd.present()) {
				restoreVersion = desc.contiguousLogEnd.get();
			} else {
				reportBackupQueryError(
				    operationId,
				    result,
				    errorMessage = format("the backup for the specified key ranges is not restorable to any version"));
			}
		}

		if (restoreVersion < 0 && restoreVersion != latestVersion) {
			reportBackupQueryError(operationId,
			                       result,
			                       errorMessage =
			                           format("the specified restorable version %ld is not valid", restoreVersion));
			return Void();
		}
		Optional<RestorableFileSet> fileSet = wait(bc->getRestoreSet(restoreVersion, keyRangesFilter));
		if (fileSet.present()) {
			int64_t totalRangeFilesSize = 0, totalLogFilesSize = 0;
			result["restore_version"] = fileSet.get().targetVersion;
			JsonBuilderArray rangeFilesJson;
			JsonBuilderArray logFilesJson;
			for (const auto& rangeFile : fileSet.get().ranges) {
				JsonBuilderObject object;
				object["file_name"] = rangeFile.fileName;
				object["file_size"] = rangeFile.fileSize;
				object["version"] = rangeFile.version;
				object["key_range"] = fileSet.get().keyRanges.count(rangeFile.fileName) == 0
				                          ? "none"
				                          : fileSet.get().keyRanges.at(rangeFile.fileName).toString();
				rangeFilesJson.push_back(object);
				totalRangeFilesSize += rangeFile.fileSize;
			}
			for (const auto& log : fileSet.get().logs) {
				JsonBuilderObject object;
				object["file_name"] = log.fileName;
				object["file_size"] = log.fileSize;
				object["begin_version"] = log.beginVersion;
				object["end_version"] = log.endVersion;
				logFilesJson.push_back(object);
				totalLogFilesSize += log.fileSize;
			}

			result["total_range_files_size"] = totalRangeFilesSize;
			result["total_log_files_size"] = totalLogFilesSize;

			if (verbose) {
				result["ranges"] = rangeFilesJson;
				result["logs"] = logFilesJson;
			}

			TraceEvent("BackupQueryReceivedRestorableFilesSet")
			    .detail("DestinationContainer", destinationContainer)
			    .detail("KeyRangesFilter", printable(keyRangesFilter))
			    .detail("ActualRestoreVersion", fileSet.get().targetVersion)
			    .detail("NumRangeFiles", fileSet.get().ranges.size())
			    .detail("NumLogFiles", fileSet.get().logs.size())
			    .detail("RangeFilesBytes", totalRangeFilesSize)
			    .detail("LogFilesBytes", totalLogFilesSize);
		} else {
			reportBackupQueryError(operationId, result, "no restorable files set found for specified key ranges");
			return Void();
		}

	} catch (Error& e) {
		reportBackupQueryError(operationId, result, e.what());
		return Void();
	}

	printf("%s\n", result.getJson().c_str());
	return Void();
}

ACTOR Future<Void> listBackup(std::string baseUrl) {
	try {
		std::vector<std::string> containers = wait(IBackupContainer::listContainers(baseUrl));
		for (std::string container : containers) {
			printf("%s\n", container.c_str());
		}
	} catch (Error& e) {
		std::string msg = format("ERROR: %s", e.what());
		if (e.code() == error_code_backup_invalid_url && !IBackupContainer::lastOpenError.empty()) {
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
	if (!options.hasChanges()) {
		fprintf(stderr, "No changes were specified, nothing to do!\n");
		throw backup_error();
	}

	state KeyBackedTag tag = makeBackupTag(tagName);

	state Reference<IBackupContainer> bc;
	if (options.destURL.present()) {
		bc = openBackupContainer(exeBackup.toString().c_str(), options.destURL.get());
		try {
			wait(timeoutError(bc->create(), 30));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr,
			        "ERROR: Could not create backup container at '%s': %s\n",
			        options.destURL.get().c_str(),
			        e.what());
			throw backup_error();
		}
	}

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(db));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			state Optional<UidAndAbortedFlagT> uidFlag = wait(tag.get(db));

			if (!uidFlag.present()) {
				fprintf(stderr, "No backup exists on tag '%s'\n", tagName.c_str());
				throw backup_error();
			}

			if (uidFlag.get().second) {
				fprintf(stderr, "Cannot modify aborted backup on tag '%s'\n", tagName.c_str());
				throw backup_error();
			}

			state BackupConfig config(uidFlag.get().first);
			EBackupState s = wait(config.stateEnum().getOrThrow(tr, false, backup_invalid_info()));
			if (!FileBackupAgent::isRunnable(s)) {
				fprintf(stderr, "Backup on tag '%s' is not runnable.\n", tagName.c_str());
				throw backup_error();
			}

			if (options.verifyUID.present() && options.verifyUID.get() != uidFlag.get().first.toString()) {
				fprintf(stderr,
				        "UID verification failed, backup on tag '%s' is '%s' but '%s' was specified.\n",
				        tagName.c_str(),
				        uidFlag.get().first.toString().c_str(),
				        options.verifyUID.get().c_str());
				throw backup_error();
			}

			if (options.snapshotIntervalSeconds.present()) {
				config.snapshotIntervalSeconds().set(tr, options.snapshotIntervalSeconds.get());
			}

			if (options.activeSnapshotIntervalSeconds.present()) {
				Version begin = wait(config.snapshotBeginVersion().getOrThrow(tr, false, backup_error()));
				config.snapshotTargetEndVersion().set(tr,
				                                      begin + ((int64_t)options.activeSnapshotIntervalSeconds.get() *
				                                               CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
			}

			if (options.destURL.present()) {
				config.backupContainer().set(tr, bc);
			}

			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

static std::vector<std::vector<StringRef>> parseLine(std::string& line, bool& err, bool& partial) {
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
			} else
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
				buf.push_back(StringRef((uint8_t*)(line.data() + offset), i - offset));
				offset = i = line.find_first_not_of(' ', i);
				forcetoken = false;
			} else
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
				char* pEnd;
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

static void addKeyRange(std::string optionValue, Standalone<VectorRef<KeyRangeRef>>& keyRanges) {
	bool err = false, partial = false;
	int tokenArray = 0, tokenIndex = 0;

	auto parsed = parseLine(optionValue, err, partial);

	for (auto tokens : parsed) {
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
		switch (tokens.size()) {
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
			} catch (Error& e) {
				fprintf(stderr,
				        "ERROR: Invalid key range `%s %s' reported error %s\n",
				        tokens.at(0).toString().c_str(),
				        tokens.at(1).toString().c_str(),
				        e.what());
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

Version parseVersion(const char* str) {
	StringRef s((const uint8_t*)str, strlen(str));

	if (s.endsWith(LiteralStringRef("days")) || s.endsWith(LiteralStringRef("d"))) {
		float days;
		if (sscanf(str, "%f", &days) != 1) {
			fprintf(stderr, "Could not parse version: %s\n", str);
			flushAndExit(FDB_EXIT_ERROR);
		}
		return (double)CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 24 * 3600 * -days;
	}

	Version ver;
	if (sscanf(str, "%" SCNd64, &ver) != 1) {
		fprintf(stderr, "Could not parse version: %s\n", str);
		flushAndExit(FDB_EXIT_ERROR);
	}
	return ver;
}

#ifdef ALLOC_INSTRUMENTATION
extern uint8_t* g_extra_memory;
#endif

int main(int argc, char* argv[]) {
	platformInit();

	int status = FDB_EXIT_SUCCESS;

	std::string commandLine;
	for (int a = 0; a < argc; a++) {
		if (a)
			commandLine += ' ';
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

		ProgramExe programExe = getProgramType(argv[0]);
		BackupType backupType = BackupType::UNDEFINED;
		DBType dbType = DBType::UNDEFINED;

		std::unique_ptr<CSimpleOpt> args;

		switch (programExe) {
		case ProgramExe::BACKUP:
			// Display backup help, if no arguments
			if (argc < 2) {
				printBackupUsage(false);
				return FDB_EXIT_ERROR;
			} else {
				// Get the backup type
				backupType = getBackupType(argv[1]);

				// Create the appropriate simple opt
				switch (backupType) {
				case BackupType::START:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupStartOptions, SO_O_EXACT);
					break;
				case BackupType::STATUS:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupStatusOptions, SO_O_EXACT);
					break;
				case BackupType::ABORT:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupAbortOptions, SO_O_EXACT);
					break;
				case BackupType::CLEANUP:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupCleanupOptions, SO_O_EXACT);
					break;
				case BackupType::WAIT:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupWaitOptions, SO_O_EXACT);
					break;
				case BackupType::DISCONTINUE:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupDiscontinueOptions, SO_O_EXACT);
					break;
				case BackupType::PAUSE:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupPauseOptions, SO_O_EXACT);
					break;
				case BackupType::RESUME:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupPauseOptions, SO_O_EXACT);
					break;
				case BackupType::EXPIRE:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupExpireOptions, SO_O_EXACT);
					break;
				case BackupType::DELETE_BACKUP:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupDeleteOptions, SO_O_EXACT);
					break;
				case BackupType::DESCRIBE:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupDescribeOptions, SO_O_EXACT);
					break;
				case BackupType::DUMP:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupDumpOptions, SO_O_EXACT);
					break;
				case BackupType::LIST:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupListOptions, SO_O_EXACT);
					break;
				case BackupType::QUERY:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupQueryOptions, SO_O_EXACT);
					break;
				case BackupType::MODIFY:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgBackupModifyOptions, SO_O_EXACT);
					break;
				case BackupType::UNDEFINED:
				default:
					args = std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT);
					break;
				}
			}
			break;
		case ProgramExe::DB_BACKUP:
			// Display backup help, if no arguments
			if (argc < 2) {
				printDBBackupUsage(false);
				return FDB_EXIT_ERROR;
			} else {
				// Get the backup type
				dbType = getDBType(argv[1]);

				// Create the appropriate simple opt
				switch (dbType) {
				case DBType::START:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgDBStartOptions, SO_O_EXACT);
					break;
				case DBType::STATUS:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgDBStatusOptions, SO_O_EXACT);
					break;
				case DBType::SWITCH:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgDBSwitchOptions, SO_O_EXACT);
					break;
				case DBType::ABORT:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgDBAbortOptions, SO_O_EXACT);
					break;
				case DBType::PAUSE:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgDBPauseOptions, SO_O_EXACT);
					break;
				case DBType::RESUME:
					args = std::make_unique<CSimpleOpt>(argc - 1, &argv[1], g_rgDBPauseOptions, SO_O_EXACT);
					break;
				case DBType::UNDEFINED:
				default:
					args = std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT);
					break;
				}
			}
			break;
		case ProgramExe::UNDEFINED:
		default:
			fprintf(stderr, "FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
			fprintf(stderr, "ERROR: Unable to determine program type based on executable `%s'\n", argv[0]);
			return FDB_EXIT_ERROR;
			break;
		}

		std::string destinationContainer;
		bool describeDeep = false;
		bool describeTimestamps = false;
		int initialSnapshotIntervalSeconds =
		    0; // The initial snapshot has a desired duration of 0, meaning go as fast as possible.
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
		Standalone<VectorRef<KeyRangeRef>> backupKeysFilter;
		int maxErrors = 20;
		Version beginVersion = invalidVersion;
		Version restoreVersion = invalidVersion;
		std::string restoreTimestamp;
		bool waitForDone = false;
		bool stopWhenDone = true;
		bool usePartitionedLog = false; // Set to true to use new backup system
		bool incrementalBackupOnly = false;
		bool onlyAppyMutationLogs = false;
		bool inconsistentSnapshotOnly = false;
		bool forceAction = false;
		bool trace = false;
		bool quietDisplay = false;
		bool dryRun = false;
		std::string traceDir = "";
		std::string traceFormat = "";
		std::string traceLogGroup;
		uint64_t traceRollSize = TRACE_DEFAULT_ROLL_SIZE;
		uint64_t traceMaxLogsSize = TRACE_DEFAULT_MAX_LOGS_SIZE;
		ESOError lastError;
		bool partial = true;
		bool dstOnly = false;
		LocalityData localities;
		uint64_t memLimit = 8LL << 30;
		Optional<uint64_t> ti;
		BackupTLSConfig tlsConfig;
		Version dumpBegin = 0;
		Version dumpEnd = std::numeric_limits<Version>::max();
		std::string restoreClusterFileDest;
		std::string restoreClusterFileOrig;
		bool jsonOutput = false;
		bool deleteData = false;

		BackupModifyOptions modifyOptions;

		if (argc == 1) {
			printUsage(programExe, false);
			return FDB_EXIT_ERROR;
		}

#ifdef _WIN32
		// Windows needs a gentle nudge to format floats correctly
		//_set_output_format(_TWO_DIGIT_EXPONENT);
#endif

		while (args->Next()) {
			lastError = args->LastError();

			switch (lastError) {
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
			case OPT_BUILD_FLAGS:
				printBuildInformation();
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
				knobs.emplace_back("min_cleanup_seconds", args->OptionArg());
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
				processLocalityArg(*args, localities);
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
			case OPT_EXPIRE_DELETE_BEFORE_DAYS: {
				const char* a = args->OptionArg();
				long long ver = 0;
				if (!sscanf(a, "%lld", &ver)) {
					fprintf(stderr, "ERROR: Could not parse expiration version `%s'\n", a);
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}

				// Interpret the value as days worth of versions relative to now (negative)
				if (optId == OPT_EXPIRE_MIN_RESTORABLE_DAYS || optId == OPT_EXPIRE_DELETE_BEFORE_DAYS) {
					ver = -ver * 24 * 60 * 60 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
				}

				if (optId == OPT_EXPIRE_BEFORE_VERSION || optId == OPT_EXPIRE_DELETE_BEFORE_DAYS)
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
			case OPT_DSTONLY:
				dstOnly = true;
				break;
			case OPT_KNOB: {
				std::string syn = args->OptionSyntax();
				if (!StringRef(syn).startsWith(LiteralStringRef("--knob_"))) {
					fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", syn.c_str());
					return FDB_EXIT_ERROR;
				}
				syn = syn.substr(7);
				knobs.emplace_back(syn, args->OptionArg());
				break;
			}
			case OPT_BACKUPKEYS:
				try {
					addKeyRange(args->OptionArg(), backupKeys);
				} catch (Error&) {
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				break;
			case OPT_BACKUPKEYS_FILTER:
				try {
					addKeyRange(args->OptionArg(), backupKeysFilter);
				} catch (Error&) {
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				break;
			case OPT_DESTCONTAINER:
				destinationContainer = args->OptionArg();
				// If the url starts with '/' then prepend "file://" for backwards compatibility
				if (StringRef(destinationContainer).startsWith(LiteralStringRef("/")))
					destinationContainer = std::string("file://") + destinationContainer;
				modifyOptions.destURL = destinationContainer;
				break;
			case OPT_SNAPSHOTINTERVAL:
			case OPT_INITIAL_SNAPSHOT_INTERVAL:
			case OPT_MOD_ACTIVE_INTERVAL: {
				const char* a = args->OptionArg();
				int seconds;
				if (!sscanf(a, "%d", &seconds)) {
					fprintf(stderr, "ERROR: Could not parse snapshot interval `%s'\n", a);
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				if (optId == OPT_SNAPSHOTINTERVAL) {
					snapshotIntervalSeconds = seconds;
					modifyOptions.snapshotIntervalSeconds = seconds;
				} else if (optId == OPT_INITIAL_SNAPSHOT_INTERVAL) {
					initialSnapshotIntervalSeconds = seconds;
				} else if (optId == OPT_MOD_ACTIVE_INTERVAL) {
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
			case OPT_INCREMENTALONLY:
				incrementalBackupOnly = true;
				onlyAppyMutationLogs = true;
				break;
			case OPT_RESTORECONTAINER:
				restoreContainer = args->OptionArg();
				// If the url starts with '/' then prepend "file://" for backwards compatibility
				if (StringRef(restoreContainer).startsWith(LiteralStringRef("/")))
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
			case OPT_RESTORE_BEGIN_VERSION: {
				const char* a = args->OptionArg();
				long long ver = 0;
				if (!sscanf(a, "%lld", &ver)) {
					fprintf(stderr, "ERROR: Could not parse database beginVersion `%s'\n", a);
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				beginVersion = ver;
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
			case OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY: {
				inconsistentSnapshotOnly = true;
				break;
			}
#ifdef _WIN32
			case OPT_PARENTPID: {
				auto pid_str = args->OptionArg();
				int parent_pid = atoi(pid_str);
				auto pHandle = OpenProcess(SYNCHRONIZE, FALSE, parent_pid);
				if (!pHandle) {
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
				tlsConfig.blobCredentials.push_back(args->OptionArg());
				break;
#ifndef TLS_DISABLED
			case TLSConfig::OPT_TLS_PLUGIN:
				args->OptionArg();
				break;
			case TLSConfig::OPT_TLS_CERTIFICATES:
				tlsConfig.tlsCertPath = args->OptionArg();
				break;
			case TLSConfig::OPT_TLS_PASSWORD:
				tlsConfig.tlsPassword = args->OptionArg();
				break;
			case TLSConfig::OPT_TLS_CA_FILE:
				tlsConfig.tlsCAPath = args->OptionArg();
				break;
			case TLSConfig::OPT_TLS_KEY:
				tlsConfig.tlsKeyPath = args->OptionArg();
				break;
			case TLSConfig::OPT_TLS_VERIFY_PEERS:
				tlsConfig.tlsVerifyPeers = args->OptionArg();
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
		for (int argLoop = 0; argLoop < args->FileCount(); argLoop++) {
			switch (programExe) {
			// Add the backup key range
			case ProgramExe::BACKUP:
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
					} catch (Error&) {
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
				}
				break;

			case ProgramExe::DB_BACKUP:
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
					} catch (Error&) {
						printHelpTeaser(argv[0]);
						return FDB_EXIT_ERROR;
					}
				}
				break;

			case ProgramExe::UNDEFINED:
			default:
				return FDB_EXIT_ERROR;
			}
		}

		IKnobCollection::setGlobalKnobCollection(IKnobCollection::Type::CLIENT, Randomize::NO, IsSimulated::NO);
		auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
		for (const auto& [knobName, knobValueString] : knobs) {
			try {
				auto knobValue = g_knobs.parseKnobValue(knobName, knobValueString);
				g_knobs.setKnob(knobName, knobValue);
			} catch (Error& e) {
				if (e.code() == error_code_invalid_option_value) {
					fprintf(stderr,
					        "WARNING: Invalid value '%s' for knob option '%s'\n",
					        knobValueString.c_str(),
					        knobName.c_str());
					TraceEvent(SevWarnAlways, "InvalidKnobValue")
					    .detail("Knob", printable(knobName))
					    .detail("Value", printable(knobValueString));
				} else {
					fprintf(stderr, "ERROR: Failed to set knob option '%s': %s\n", knobName.c_str(), e.what());
					TraceEvent(SevError, "FailedToSetKnob")
					    .detail("Knob", printable(knobName))
					    .detail("Value", printable(knobValueString))
					    .error(e);
					throw;
				}
			}
		}

		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		g_knobs.initialize(Randomize::NO, IsSimulated::NO);

		if (trace) {
			if (!traceLogGroup.empty())
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
		if (!tlsConfig.setupTLS()) {
			return 1;
		}

		Error::init();
		std::set_new_handler(&platform::outOfMemory);
		setMemoryQuota(memLimit);

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
		} catch (Error& e) {
			fprintf(stderr, "ERROR: %s\n", e.what());
			return FDB_EXIT_ERROR;
		}

		TraceEvent("ProgramStart")
		    .setMaxEventLength(12000)
		    .detail("SourceVersion", getSourceVersion())
		    .detail("Version", FDB_VT_VERSION)
		    .detail("PackageName", FDB_VT_PACKAGE_NAME)
		    .detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
		    .setMaxFieldLength(10000)
		    .detail("CommandLine", commandLine)
		    .setMaxFieldLength(0)
		    .detail("MemoryLimit", memLimit)
		    .trackLatest("ProgramStart");

		// Ordinarily, this is done when the network is run. However, network thread should be set before TraceEvents
		// are logged. This thread will eventually run the network, so call it now.
		TraceEvent::setNetworkThread();

		// Sets up blob credentials, including one from the environment FDB_BLOB_CREDENTIALS.
		tlsConfig.setupBlobCredentials();

		// Opens a trace file if trace is set (and if a trace file isn't already open)
		// For most modes, initCluster() will open a trace file, but some fdbbackup operations do not require
		// a cluster so they should use this instead.
		auto initTraceFile = [&]() {
			if (trace)
				openTraceFile(NetworkAddress(), traceRollSize, traceMaxLogsSize, traceDir, "trace", traceLogGroup);
		};

		auto initCluster = [&](bool quiet = false) {
			auto resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(clusterFile);
			try {
				ccf = makeReference<ClusterConnectionFile>(resolvedClusterFile.first);
			} catch (Error& e) {
				if (!quiet)
					fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedClusterFile, e).c_str());
				return false;
			}

			try {
				db = Database::createDatabase(ccf, -1, true, localities);
			} catch (Error& e) {
				fprintf(stderr, "ERROR: %s\n", e.what());
				fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n", ccf->getFilename().c_str());
				return false;
			}

			return true;
		};

		if (sourceClusterFile.size()) {
			auto resolvedSourceClusterFile = ClusterConnectionFile::lookupClusterFileName(sourceClusterFile);
			try {
				sourceCcf = makeReference<ClusterConnectionFile>(resolvedSourceClusterFile.first);
			} catch (Error& e) {
				fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedSourceClusterFile, e).c_str());
				return FDB_EXIT_ERROR;
			}

			try {
				sourceDb = Database::createDatabase(sourceCcf, -1, true, localities);
			} catch (Error& e) {
				fprintf(stderr, "ERROR: %s\n", e.what());
				fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n", sourceCcf->getFilename().c_str());
				return FDB_EXIT_ERROR;
			}
		}

		switch (programExe) {
		case ProgramExe::BACKUP:
			switch (backupType) {
			case BackupType::START: {
				if (!initCluster())
					return FDB_EXIT_ERROR;
				// Test out the backup url to make sure it parses.  Doesn't test to make sure it's actually writeable.
				openBackupContainer(argv[0], destinationContainer);
				f = stopAfter(submitBackup(db,
				                           destinationContainer,
				                           initialSnapshotIntervalSeconds,
				                           snapshotIntervalSeconds,
				                           backupKeys,
				                           tagName,
				                           dryRun,
				                           waitForDone,
				                           stopWhenDone,
				                           usePartitionedLog,
				                           incrementalBackupOnly));
				break;
			}

			case BackupType::MODIFY: {
				if (!initCluster())
					return FDB_EXIT_ERROR;

				f = stopAfter(modifyBackup(db, tagName, modifyOptions));
				break;
			}

			case BackupType::STATUS:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(statusBackup(db, tagName, true, jsonOutput));
				break;

			case BackupType::ABORT:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(abortBackup(db, tagName));
				break;

			case BackupType::CLEANUP:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(cleanupMutations(db, deleteData));
				break;

			case BackupType::WAIT:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(waitBackup(db, tagName, stopWhenDone));
				break;

			case BackupType::DISCONTINUE:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(discontinueBackup(db, tagName, waitForDone));
				break;

			case BackupType::PAUSE:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(changeBackupResumed(db, true));
				break;

			case BackupType::RESUME:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(changeBackupResumed(db, false));
				break;

			case BackupType::EXPIRE:
				initTraceFile();
				// Must have a usable cluster if either expire DateTime options were used
				if (!expireDatetime.empty() || !expireRestorableAfterDatetime.empty()) {
					if (!initCluster())
						return FDB_EXIT_ERROR;
				}
				f = stopAfter(expireBackupData(argv[0],
				                               destinationContainer,
				                               expireVersion,
				                               expireDatetime,
				                               db,
				                               forceAction,
				                               expireRestorableAfterVersion,
				                               expireRestorableAfterDatetime));
				break;

			case BackupType::DELETE_BACKUP:
				initTraceFile();
				f = stopAfter(deleteBackupContainer(argv[0], destinationContainer));
				break;

			case BackupType::DESCRIBE:
				initTraceFile();
				// If timestamp lookups are desired, require a cluster file
				if (describeTimestamps && !initCluster())
					return FDB_EXIT_ERROR;

				// Only pass database optionDatabase Describe will lookup version timestamps if a cluster file was
				// given, but quietly skip them if not.
				f = stopAfter(describeBackup(argv[0],
				                             destinationContainer,
				                             describeDeep,
				                             describeTimestamps ? Optional<Database>(db) : Optional<Database>(),
				                             jsonOutput));
				break;

			case BackupType::LIST:
				initTraceFile();
				f = stopAfter(listBackup(baseUrl));
				break;

			case BackupType::QUERY:
				initTraceFile();
				f = stopAfter(queryBackup(argv[0],
				                          destinationContainer,
				                          backupKeysFilter,
				                          restoreVersion,
				                          restoreClusterFileOrig,
				                          restoreTimestamp,
				                          !quietDisplay));
				break;

			case BackupType::DUMP:
				initTraceFile();
				f = stopAfter(dumpBackupData(argv[0], destinationContainer, dumpBegin, dumpEnd));
				break;

			case BackupType::UNDEFINED:
			default:
				fprintf(stderr, "ERROR: Unsupported backup action %s\n", argv[1]);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;
			}

			break;
		case ProgramExe::DB_BACKUP:
			if (!initCluster())
				return FDB_EXIT_ERROR;
			switch (dbType) {
			case DBType::START:
				f = stopAfter(submitDBBackup(sourceDb, db, backupKeys, tagName));
				break;
			case DBType::STATUS:
				f = stopAfter(statusDBBackup(sourceDb, db, tagName, maxErrors));
				break;
			case DBType::SWITCH:
				f = stopAfter(switchDBBackup(sourceDb, db, backupKeys, tagName, forceAction));
				break;
			case DBType::ABORT:
				f = stopAfter(abortDBBackup(sourceDb, db, tagName, partial, dstOnly));
				break;
			case DBType::PAUSE:
				f = stopAfter(changeDBBackupResumed(sourceDb, db, true));
				break;
			case DBType::RESUME:
				f = stopAfter(changeDBBackupResumed(sourceDb, db, false));
				break;
			case DBType::UNDEFINED:
			default:
				fprintf(stderr, "ERROR: Unsupported DR action %s\n", argv[1]);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;
			}
			break;
		case ProgramExe::UNDEFINED:
		default:
			return FDB_EXIT_ERROR;
		}

		runNetwork();

		if (f.isValid() && f.isReady() && !f.isError() && !f.get().present()) {
			status = FDB_EXIT_ERROR;
		}

		if (fstatus.isValid() && fstatus.isReady() && !fstatus.isError() && fstatus.get().present()) {
			status = fstatus.get().get();
		}

#ifdef ALLOC_INSTRUMENTATION
		{
			cout << "Page Counts: " << FastAllocator<16>::pageCount << " " << FastAllocator<32>::pageCount << " "
			     << FastAllocator<64>::pageCount << " " << FastAllocator<128>::pageCount << " "
			     << FastAllocator<256>::pageCount << " " << FastAllocator<512>::pageCount << " "
			     << FastAllocator<1024>::pageCount << " " << FastAllocator<2048>::pageCount << " "
			     << FastAllocator<4096>::pageCount << " " << FastAllocator<8192>::pageCount << " "
			     << FastAllocator<16384>::pageCount << endl;

			vector<std::pair<std::string, const char*>> typeNames;
			for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
				std::string s;

#ifdef __linux__
				char* demangled = abi::__cxa_demangle(i->first, NULL, NULL, NULL);
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

				typeNames.emplace_back(s, i->first);
			}
			std::sort(typeNames.begin(), typeNames.end());
			for (int i = 0; i < typeNames.size(); i++) {
				const char* n = typeNames[i].second;
				auto& f = allocInstr[n];
				printf("%+d\t%+d\t%d\t%d\t%s\n",
				       f.allocCount,
				       -f.deallocCount,
				       f.allocCount - f.deallocCount,
				       f.maxAllocated,
				       typeNames[i].first.c_str());
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
