/*
 * backup.actor.cpp
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

#include "flow/ApiVersion.h"
#include "fmt/format.h"
#include "fdbclient/BackupTLSConfig.h"
#include "fdbbackup/Decode.h"
#include "fdbclient/JsonBuilder.h"
#include "flow/Arena.h"
#include "flow/ArgParseUtil.h"
#include "flow/Error.h"
#include "flow/SystemMonitor.h"
#include "flow/Trace.h"
#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/managed_shared_memory.hpp>

#include "flow/flow.h"
#include "flow/FastAlloc.h"
#include "flow/serialize.h"
#include "flow/IRandom.h"
#include "flow/genericactors.actor.h"
#include "flow/TLSConfig.actor.h"

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/Status.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbclient/S3BlobStore.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"

#include "flow/Platform.h"

#include <stdarg.h>
#include <stdio.h>
#include <cinttypes>
#include <algorithm> // std::transform
#include <string>
#include <iostream>
#include <ctime>

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

#include "fdbclient/versions.h"
#include "fdbclient/BuildFlags.h"

#include "SimpleOpt/SimpleOpt.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Type of program being executed
enum class ProgramExe { AGENT, BACKUP, RESTORE, FASTRESTORE_TOOL, DR_AGENT, DB_BACKUP, UNDEFINED };

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
	CLEANUP,
	TAGS,
};

enum class DBType { UNDEFINED = 0, START, STATUS, SWITCH, ABORT, PAUSE, RESUME };

// New fast restore reuses the type from legacy slow restore
enum class RestoreType { UNKNOWN, START, STATUS, ABORT, WAIT };

//
enum {
	// Backup constants
	OPT_DESTCONTAINER,
	OPT_SNAPSHOTINTERVAL,
	OPT_INITIAL_SNAPSHOT_INTERVAL,
	OPT_ERRORLIMIT,
	OPT_NOSTOPWHENDONE,
	OPT_EXPIRE_BEFORE_VERSION,
	OPT_EXPIRE_BEFORE_DATETIME,
	OPT_EXPIRE_DELETE_BEFORE_DAYS,
	OPT_EXPIRE_RESTORABLE_AFTER_VERSION,
	OPT_EXPIRE_RESTORABLE_AFTER_DATETIME,
	OPT_EXPIRE_MIN_RESTORABLE_DAYS,
	OPT_BASEURL,
	OPT_BLOB_CREDENTIALS,
	OPT_DESCRIBE_DEEP,
	OPT_DESCRIBE_TIMESTAMPS,
	OPT_DUMP_BEGIN,
	OPT_DUMP_END,
	OPT_JSON,
	OPT_DELETE_DATA,
	OPT_MIN_CLEANUP_SECONDS,
	OPT_USE_PARTITIONED_LOG,
	OPT_ENCRYPT_FILES,

	// Backup and Restore constants
	OPT_PROXY,
	OPT_TAGNAME,
	OPT_BACKUPKEYS,
	OPT_BACKUPKEYS_FILE,
	OPT_WAITFORDONE,
	OPT_BACKUPKEYS_FILTER,
	OPT_INCREMENTALONLY,
	OPT_ENCRYPTION_KEY_FILE,

	// Backup Modify
	OPT_MOD_ACTIVE_INTERVAL,
	OPT_MOD_VERIFY_UID,

	// Restore constants
	OPT_RESTORECONTAINER,
	OPT_RESTORE_VERSION,
	OPT_RESTORE_SNAPSHOT_VERSION,
	OPT_RESTORE_TIMESTAMP,
	OPT_PREFIX_ADD,
	OPT_PREFIX_REMOVE,
	OPT_RESTORE_CLUSTERFILE_DEST,
	OPT_RESTORE_CLUSTERFILE_ORIG,
	OPT_RESTORE_BEGIN_VERSION,
	OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY,
	// The two restore options below allow callers of fdbrestore to divide a normal restore into one which restores just
	// the system keyspace and another that restores just the user key space. This is unlike the backup command where
	// all keys (both system and user) will be backed up together
	OPT_RESTORE_USER_DATA,
	OPT_RESTORE_SYSTEM_DATA,

	// Shared constants
	OPT_CLUSTERFILE,
	OPT_QUIET,
	OPT_DRYRUN,
	OPT_FORCE,
	OPT_HELP,
	OPT_DEVHELP,
	OPT_VERSION,
	OPT_BUILD_FLAGS,
	OPT_PARENTPID,
	OPT_CRASHONERROR,
	OPT_NOBUFSTDOUT,
	OPT_BUFSTDOUTERR,
	OPT_TRACE,
	OPT_TRACE_DIR,
	OPT_KNOB,
	OPT_TRACE_LOG_GROUP,
	OPT_MEMLIMIT,
	OPT_VMEMLIMIT,
	OPT_LOCALITY,

	// DB constants
	OPT_SOURCE_CLUSTER,
	OPT_DEST_CLUSTER,
	OPT_CLEANUP,
	OPT_DSTONLY,

	OPT_TRACE_FORMAT,

	// blob granules backup/restore
	OPT_BLOB_MANIFEST_URL,
};

// Top level binary commands.
CSimpleOpt::SOption g_rgOptions[] = { { OPT_VERSION, "-v", SO_NONE },
	                                  { OPT_VERSION, "--version", SO_NONE },
	                                  { OPT_BUILD_FLAGS, "--build-flags", SO_NONE },
	                                  { OPT_HELP, "-?", SO_NONE },
	                                  { OPT_HELP, "-h", SO_NONE },
	                                  { OPT_HELP, "--help", SO_NONE },

	                                  SO_END_OF_OPTIONS };

CSimpleOpt::SOption g_rgAgentOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_VERSION, "--version", SO_NONE },
	{ OPT_VERSION, "-v", SO_NONE },
	{ OPT_BUILD_FLAGS, "--build-flags", SO_NONE },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_LOCALITY, "--locality-", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStartOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_WAITFORDONE, "-w", SO_NONE },
	{ OPT_WAITFORDONE, "--waitfordone", SO_NONE },
	{ OPT_NOSTOPWHENDONE, "-z", SO_NONE },
	{ OPT_NOSTOPWHENDONE, "--no-stop-when-done", SO_NONE },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	// Enable "-p" option after GA
	// { OPT_USE_PARTITIONED_LOG, "-p",                 SO_NONE },
	{ OPT_USE_PARTITIONED_LOG, "--partitioned-log-experimental", SO_NONE },
	{ OPT_SNAPSHOTINTERVAL, "-s", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "--snapshot-interval", SO_REQ_SEP },
	{ OPT_INITIAL_SNAPSHOT_INTERVAL, "--initial-snapshot-interval", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS_FILE, "--keys-file", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
	{ OPT_DRYRUN, "-n", SO_NONE },
	{ OPT_DRYRUN, "--dryrun", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_INCREMENTALONLY, "--incremental", SO_NONE },
	{ OPT_ENCRYPTION_KEY_FILE, "--encryption-key-file", SO_REQ_SEP },
	{ OPT_ENCRYPT_FILES, "--encrypt-files", SO_REQ_SEP },
	{ OPT_BLOB_MANIFEST_URL, "--blob-manifest-url", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
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
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_MOD_VERIFY_UID, "--verify-uid", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "-s", SO_REQ_SEP },
	{ OPT_SNAPSHOTINTERVAL, "--snapshot-interval", SO_REQ_SEP },
	{ OPT_MOD_ACTIVE_INTERVAL, "--active-snapshot-interval", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupStatusOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_ERRORLIMIT, "-e", SO_REQ_SEP },
	{ OPT_ERRORLIMIT, "--errorlimit", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_JSON, "--json", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupAbortOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupCleanupOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_DELETE_DATA, "--delete-data", SO_NONE },
	{ OPT_MIN_CLEANUP_SECONDS, "--min-cleanup-seconds", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDiscontinueOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_WAITFORDONE, "-w", SO_NONE },
	{ OPT_WAITFORDONE, "--waitfordone", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupWaitOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_NOSTOPWHENDONE, "-z", SO_NONE },
	{ OPT_NOSTOPWHENDONE, "--no-stop-when-done", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupPauseOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupExpireOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_FORCE, "-f", SO_NONE },
	{ OPT_FORCE, "--force", SO_NONE },
	{ OPT_EXPIRE_RESTORABLE_AFTER_VERSION, "--restorable-after-version", SO_REQ_SEP },
	{ OPT_EXPIRE_RESTORABLE_AFTER_DATETIME, "--restorable-after-timestamp", SO_REQ_SEP },
	{ OPT_EXPIRE_BEFORE_VERSION, "--expire-before-version", SO_REQ_SEP },
	{ OPT_EXPIRE_BEFORE_DATETIME, "--expire-before-timestamp", SO_REQ_SEP },
	{ OPT_EXPIRE_MIN_RESTORABLE_DAYS, "--min-restorable-days", SO_REQ_SEP },
	{ OPT_EXPIRE_DELETE_BEFORE_DAYS, "--delete-before-days", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDeleteOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDescribeOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_DESCRIBE_DEEP, "--deep", SO_NONE },
	{ OPT_DESCRIBE_TIMESTAMPS, "--version-timestamps", SO_NONE },
	{ OPT_JSON, "--json", SO_NONE },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupDumpOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_DUMP_BEGIN, "--begin", SO_REQ_SEP },
	{ OPT_DUMP_END, "--end", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupTagsOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_CLUSTERFILE, "-C", SO_REQ_SEP },
	{ OPT_CLUSTERFILE, "--cluster-file", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupListOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_BASEURL, "-b", SO_REQ_SEP },
	{ OPT_BASEURL, "--base-url", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgBackupQueryOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_RESTORE_TIMESTAMP, "--query-restore-timestamp", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "-d", SO_REQ_SEP },
	{ OPT_DESTCONTAINER, "--destcontainer", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "-qrv", SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "--query-restore-version", SO_REQ_SEP },
	{ OPT_RESTORE_SNAPSHOT_VERSION, "--query-restore-snapshot-version", SO_REQ_SEP },
	{ OPT_BACKUPKEYS_FILTER, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS_FILTER, "--keys", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_VERSION, "-v", SO_NONE },
	{ OPT_VERSION, "--version", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

// g_rgRestoreOptions is used by fdbrestore and fastrestore_tool
CSimpleOpt::SOption g_rgRestoreOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_RESTORE_CLUSTERFILE_DEST, "--dest-cluster-file", SO_REQ_SEP },
	{ OPT_RESTORE_CLUSTERFILE_ORIG, "--orig-cluster-file", SO_REQ_SEP },
	{ OPT_RESTORE_TIMESTAMP, "--timestamp", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_RESTORECONTAINER, "-r", SO_REQ_SEP },
	{ OPT_PROXY, "--proxy", SO_REQ_SEP },
	{ OPT_PREFIX_ADD, "--add-prefix", SO_REQ_SEP },
	{ OPT_PREFIX_REMOVE, "--remove-prefix", SO_REQ_SEP },
	{ OPT_TAGNAME, "-t", SO_REQ_SEP },
	{ OPT_TAGNAME, "--tagname", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "-k", SO_REQ_SEP },
	{ OPT_BACKUPKEYS_FILE, "--keys-file", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
	{ OPT_WAITFORDONE, "-w", SO_NONE },
	{ OPT_WAITFORDONE, "--waitfordone", SO_NONE },
	{ OPT_RESTORE_USER_DATA, "--user-data", SO_NONE },
	{ OPT_RESTORE_SYSTEM_DATA, "--system-metadata", SO_NONE },
	{ OPT_RESTORE_VERSION, "--version", SO_REQ_SEP },
	{ OPT_RESTORE_VERSION, "-v", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_DRYRUN, "-n", SO_NONE },
	{ OPT_DRYRUN, "--dryrun", SO_NONE },
	{ OPT_FORCE, "-f", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_BLOB_CREDENTIALS, "--blob-credentials", SO_REQ_SEP },
	{ OPT_INCREMENTALONLY, "--incremental", SO_NONE },
	{ OPT_RESTORE_BEGIN_VERSION, "--begin-version", SO_REQ_SEP },
	{ OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY, "--inconsistent-snapshot-only", SO_NONE },
	{ OPT_ENCRYPTION_KEY_FILE, "--encryption-key-file", SO_REQ_SEP },
	{ OPT_BLOB_MANIFEST_URL, "--blob-manifest-url", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

CSimpleOpt::SOption g_rgDBAgentOptions[] = {
#ifdef _WIN32
	{ OPT_PARENTPID, "--parentpid", SO_REQ_SEP },
#endif
	{ OPT_SOURCE_CLUSTER, "-s", SO_REQ_SEP },
	{ OPT_SOURCE_CLUSTER, "--source", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "-d", SO_REQ_SEP },
	{ OPT_DEST_CLUSTER, "--destination", SO_REQ_SEP },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	{ OPT_VERSION, "--version", SO_NONE },
	{ OPT_VERSION, "-v", SO_NONE },
	{ OPT_BUILD_FLAGS, "--build-flags", SO_NONE },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_LOCALITY, "--locality-", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	TLS_OPTION_FLAGS,
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
	{ OPT_BACKUPKEYS_FILE, "--keys-file", SO_REQ_SEP },
	{ OPT_BACKUPKEYS, "--keys", SO_REQ_SEP },
	{ OPT_TRACE, "--log", SO_NONE },
	{ OPT_TRACE_DIR, "--logdir", SO_REQ_SEP },
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
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
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
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
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_FORCE, "-f", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
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
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
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
	{ OPT_TRACE_FORMAT, "--trace-format", SO_REQ_SEP },
	{ OPT_TRACE_LOG_GROUP, "--loggroup", SO_REQ_SEP },
	{ OPT_QUIET, "-q", SO_NONE },
	{ OPT_QUIET, "--quiet", SO_NONE },
	{ OPT_CRASHONERROR, "--crash", SO_NONE },
	{ OPT_MEMLIMIT, "-m", SO_REQ_SEP },
	{ OPT_MEMLIMIT, "--memory", SO_REQ_SEP },
	{ OPT_VMEMLIMIT, "--memory-vsize", SO_REQ_SEP },
	{ OPT_HELP, "-?", SO_NONE },
	{ OPT_HELP, "-h", SO_NONE },
	{ OPT_HELP, "--help", SO_NONE },
	{ OPT_DEVHELP, "--dev-help", SO_NONE },
	{ OPT_KNOB, "--knob-", SO_REQ_SEP },
	TLS_OPTION_FLAGS,
	SO_END_OF_OPTIONS
};

const KeyRef exeAgent = "backup_agent"_sr;
const KeyRef exeBackup = "fdbbackup"_sr;
const KeyRef exeRestore = "fdbrestore"_sr;
const KeyRef exeFastRestoreTool = "fastrestore_tool"_sr; // must be lower case
const KeyRef exeDatabaseAgent = "dr_agent"_sr;
const KeyRef exeDatabaseBackup = "fdbdr"_sr;

extern const char* getSourceVersion();

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

static void printVersion() {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("source version %s\n", getSourceVersion());
	printf("protocol %llx\n", (long long)currentProtocolVersion().version());
}

static void printBuildInformation() {
	printf("%s", jsonBuildInformation().c_str());
}

const char* BlobCredentialInfo =
    "  BLOB CREDENTIALS\n"
    "     Blob account secret keys can optionally be omitted from blobstore:// URLs, in which case they will be\n"
    "     loaded, if possible, from 1 or more blob credentials definition files.\n\n"
    "     These files can be specified with the --blob-credentials argument described above or via the environment "
    "variable\n"
    "     FDB_BLOB_CREDENTIALS, whose value is a colon-separated list of files.  The command line takes priority over\n"
    "     over the environment but all files from both sources are used.\n\n"
    "     At connect time, the specified files are read in order and the first matching account specification "
    "(user@host)\n"
    "     will be used to obtain the secret key.\n\n"
    "     The JSON schema is:\n"
    "        { \"accounts\" : { \"user@host\" : { \"secret\" : \"SECRETKEY\" }, \"user2@host2\" : { \"secret\" : "
    "\"SECRET\" } } }\n";

static void printHelpTeaser(const char* name) {
	fprintf(stderr, "Try `%s --help' for more information.\n", name);
}

static void printAgentUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [OPTIONS]\n\n", exeAgent.toString().c_str());
	printf("  -C CONNFILE    The path of a file containing the connection string for the\n"
	       "                 FoundationDB cluster. The default is first the value of the\n"
	       "                 FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',\n"
	       "                 then `%s'.\n",
	       platform::getDefaultClusterFilePath().c_str());
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH  Specifies the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace-format FORMAT\n"
	       "                 Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                 Has no effect unless --log is specified.\n");
	printf("  -m SIZE, --memory SIZE\n"
	       "                 Memory limit. The default value is 8GiB. When specified\n"
	       "                 without a unit, MiB is assumed.\n");
	printf(TLS_HELP);
	printf("  --build-flags  Print build information and exit.\n");
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
	for (const auto& f : formats)
		printf("                     %s\n", f.c_str());
	printf("\n");
}

static void printBackupUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [TOP_LEVEL_OPTIONS] (start | status | abort | wait | discontinue | pause | resume | expire | "
	       "delete | describe | list | query | cleanup | tags) [ACTION_OPTIONS]\n\n",
	       exeBackup.toString().c_str());
	printf(" TOP LEVEL OPTIONS:\n");
	printf("  --build-flags  Print build information and exit.\n");
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
	printf("  -b, --base-url BASEURL\n"
	       "                 Base backup URL for list operations.  This looks like a Backup URL but without a backup "
	       "name.\n");
	printf("  --blob-credentials FILE\n"
	       "                 File containing blob credentials in JSON format.  Can be specified multiple times for "
	       "multiple files.  See below for more details.\n");
	printf("  --expire-before-timestamp DATETIME\n"
	       "                 Datetime cutoff for expire operations.  Requires a cluster file and will use "
	       "version/timestamp metadata\n"
	       "                 in the database to obtain a cutoff version very close to the timestamp given in %s.\n",
	       BackupAgentBase::timeFormat().c_str());
	printf("  --expire-before-version VERSION\n"
	       "                 Version cutoff for expire operations.  Deletes data files containing no data at or after "
	       "VERSION.\n");
	printf("  --delete-before-days NUM_DAYS\n"
	       "                 Another way to specify version cutoff for expire operations.  Deletes data files "
	       "containing no data at or after a\n"
	       "                 version approximately NUM_DAYS days worth of versions prior to the latest log version in "
	       "the backup.\n");
	printf("  --query-restore-snapshot-version VERSION\n"
	       "                 For query operations, set the snapshot version, inclusive, used to restore a backup.\n"
	       "                 Set -1 to use the latest valid snapshot,\n"
	       "                 Set -3 to use the oldest valid snapshot.\n");
	printf("  -qrv --query-restore-version VERSION\n"
	       "                 For query operations, set target version for restoring a backup. Set -1 for maximum\n"
	       "                 restorable version (default) and -3 for minimum restorable version.\n");
	printf(
	    "  --query-restore-timestamp DATETIME\n"
	    "                 For query operations, instead of a numeric version, use this to specify a timestamp in %s\n",
	    BackupAgentBase::timeFormat().c_str());
	printf(
	    "                 and it will be converted to a version from that time using metadata in the cluster file.\n");
	printf("  --restorable-after-timestamp DATETIME\n"
	       "                 For expire operations, set minimum acceptable restorability to the version equivalent of "
	       "DATETIME and later.\n");
	printf("  --restorable-after-version VERSION\n"
	       "                 For expire operations, set minimum acceptable restorability to the VERSION and later.\n");
	printf("  --min-restorable-days NUM-DAYS\n"
	       "                 For expire operations, set minimum acceptable restorability to approximately NUM_DAYS "
	       "days worth of versions\n"
	       "                 prior to the latest log version in the backup.\n");
	printf("  --version-timestamps\n");
	printf("                 For describe operations, lookup versions in the database to obtain timestamps.  A cluster "
	       "file is required.\n");
	printf(
	    "  -f, --force    For expire operations, force expiration even if minimum restorability would be violated.\n");
	printf("  -s, --snapshot-interval DURATION\n"
	       "                 For start or modify operations, specifies the backup's default target snapshot interval "
	       "as DURATION seconds.  Defaults to %d for start operations.\n",
	       CLIENT_KNOBS->BACKUP_DEFAULT_SNAPSHOT_INTERVAL_SEC);
	printf("  --active-snapshot-interval DURATION\n"
	       "                 For modify operations, sets the desired interval for the backup's currently active "
	       "snapshot, relative to the start of the snapshot.\n");
	printf("  --verify-uid UID\n"
	       "                 Specifies a UID to verify against the BackupUID of the running backup.  If provided, the "
	       "UID is verified in the same transaction\n"
	       "                 which sets the new backup parameters (if the UID matches).\n");
	printf("  -e ERRORLIMIT  The maximum number of errors printed by status (default is 10).\n");
	printf("  -k KEYS        List of key ranges to backup or to filter the backup in query operations.\n"
	       "                 If not specified, the entire database will be backed up or no filter will be applied.\n");
	printf("  --keys-file FILE\n"
	       "                 Same as -k option, except keys are specified in the input file.\n");
	printf("  --partitioned-log-experimental  Starts with new type of backup system using partitioned logs.\n");
	printf("  -n, --dryrun   For backup start or restore start, performs a trial run with no actual changes made.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH  Specifies the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace-format FORMAT\n"
	       "                 Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                 Has no effect unless --log is specified.\n");
	printf("  --max-cleanup-seconds SECONDS\n"
	       "                 Specifies the amount of time a backup or DR needs to be stale before cleanup will\n"
	       "                 remove mutations for it. By default this is set to one hour.\n");
	printf("  --delete-data\n"
	       "                 This flag will cause cleanup to remove mutations for the most stale backup or DR.\n");
	printf("  --incremental\n"
	       "                 Performs incremental backup without the base backup.\n"
	       "                 This option indicates to the backup agent that it will only need to record the log files, "
	       "and ignore the range files.\n");
	printf("  --encryption-key-file"
	       "                 The AES-256-GCM key in the provided file is used for encrypting backup files.\n");
	printf("  --encrypt-files 0/1"
	       "                 If passed, this argument will allow the user to override the database encryption state to "
	       "either enable (1) or disable (0) encryption at rest with snapshot backups. This option refers to block "
	       "level encryption of snapshot backups while --encryption-key-file (above) refers to file level encryption. "
	       "Generally, these two options should not be used together.\n");
	printf("  --blob-manifest-url URL\n"
	       "                 Perform blob manifest backup. Manifest files are stored to the destination URL.\n"
	       "                 Blob granules should be enabled first for manifest backup.\n");

	printf(TLS_HELP);
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

static void printRestoreUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [TOP_LEVEL_OPTIONS] (start | status | abort | wait) [OPTIONS]\n\n",
	       exeRestore.toString().c_str());

	printf(" TOP LEVEL OPTIONS:\n");
	printf("  --build-flags  Print build information and exit.\n");
	printf("  -v, --version  Print version information and exit.\n");
	printf("  -h, --help     Display this help and exit.\n");
	printf("\n");

	printf(" ACTION OPTIONS:\n");
	// printf("  FOLDERS        Paths to folders containing the backup files.\n");
	printf("  Options for all commands:\n\n");
	printf("  --dest-cluster-file CONNFILE\n");
	printf("                 The cluster file to restore data into.\n");
	printf("  -t, --tagname TAGNAME\n");
	printf("                 The restore tag to act on.  Default is 'default'\n");
	printf("\n");
	printf("  Options for start:\n\n");
	printf("  -r URL         The Backup URL for the restore to read from.\n");
	printBackupContainerInfo();
	printf("  -w, --waitfordone\n");
	printf("                 Wait for the restore to complete before exiting.  Prints progress updates.\n");
	printf("  -k KEYS        List of key ranges from the backup to restore.\n");
	printf("  --keys-file FILE\n"
	       "                 Same as -k option, except keys are specified in the input file.\n");
	printf("  --remove-prefix PREFIX\n");
	printf("                 Prefix to remove from the restored keys.\n");
	printf("  --add-prefix PREFIX\n");
	printf("                 Prefix to add to the restored keys\n");
	printf("  -n, --dryrun   Perform a trial run with no changes made.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH  Specifies the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace-format FORMAT\n"
	       "                 Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                 Has no effect unless --log is specified.\n");
	printf("  --incremental\n"
	       "                 Performs incremental restore without the base backup.\n"
	       "                 This tells the backup agent to only replay the log files from the backup source.\n"
	       "                 This also allows a restore to be performed into a non-empty destination database.\n");
	printf("  --begin-version\n"
	       "                 To be used in conjunction with incremental restore.\n"
	       "                 Indicates to the backup agent to only begin replaying log files from a certain version, "
	       "instead of the entire set.\n");
	printf("  --encryption-key-file"
	       "                 The AES-256-GCM key in the provided file is used for decrypting backup files.\n");
	printf("  --blob-manifest-url URL\n"
	       "                 Restore from blob granules. Manifest files are stored to the destination URL.\n");
	printf(TLS_HELP);
	printf("  -v DBVERSION   The version at which the database will be restored.\n");
	printf("  --timestamp    Instead of a numeric version, use this to specify a timestamp in %s\n",
	       BackupAgentBase::timeFormat().c_str());
	printf(
	    "                 and it will be converted to a version from that time using metadata in orig_cluster_file.\n");
	printf("  --orig-cluster-file CONNFILE\n");
	printf("                 The cluster file for the original database from which the backup was created.  The "
	       "original database\n");
	printf("                 is only needed to convert a --timestamp argument to a database version.\n");
	printf("  --user-data\n"
	       "                  Restore only the user keyspace. This option should NOT be used alongside "
	       "--system-metadata (below) and CANNOT be used alongside other specified key ranges.\n");
	printf(
	    "  --system-metadata\n"
	    "                 Restore only the relevant system keyspace. This option "
	    "should NOT be used alongside --user-data (above) and CANNOT be used alongside other specified key ranges.\n");

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

static void printFastRestoreUsage(bool devhelp) {
	printf(" NOTE: Fast restore aims to support the same fdbrestore option list.\n");
	printf("       But fast restore is still under development. The options may not be fully supported.\n");
	printf(" Supported options are: --dest-cluster-file, -r, --waitfordone, --logdir\n");
	printRestoreUsage(devhelp);
	return;
}

static void printDBAgentUsage(bool devhelp) {
	printf("FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
	printf("Usage: %s [OPTIONS]\n\n", exeDatabaseAgent.toString().c_str());
	printf("  -d, --destination CONNFILE\n"
	       "                 The path of a file containing the connection string for the\n"
	       "                 destination FoundationDB cluster.\n");
	printf("  -s, --source CONNFILE\n"
	       "                 The path of a file containing the connection string for the\n"
	       "                 source FoundationDB cluster.\n");
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH  Specifies the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace-format FORMAT\n"
	       "                 Select the format of the trace files. xml (the default) and json are supported.\n"
	       "                 Has no effect unless --log is specified.\n");
	printf("  -m, --memory SIZE\n"
	       "                 Memory limit. The default value is 8GiB. When specified\n"
	       "                 without a unit, MiB is assumed.\n");
	printf(TLS_HELP);
	printf("  --build-flags  Print build information and exit.\n");
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
	printf("Usage: %s [TOP_LEVEL_OPTIONS] (start | status | switch | abort | pause | resume) [OPTIONS]\n\n",
	       exeDatabaseBackup.toString().c_str());

	printf(" TOP LEVEL OPTIONS:\n");
	printf("  --build-flags  Print build information and exit.\n");
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
	printf("  --keys-file FILE\n"
	       "                 Same as -k option, except keys are specified in the input file.\n");
	printf("  --cleanup      Abort will attempt to stop mutation logging on the source cluster.\n");
	printf("  --dstonly      Abort will not make any changes on the source cluster.\n");
	printf(TLS_HELP);
	printf("  --log          Enables trace file logging for the CLI session.\n"
	       "  --logdir PATH  Specifies the output directory for trace files. If\n"
	       "                 unspecified, defaults to the current directory. Has\n"
	       "                 no effect unless --log is specified.\n");
	printf("  --loggroup LOG_GROUP\n"
	       "                 Sets the LogGroup field with the specified value for all\n"
	       "                 events in the trace output (defaults to `default').\n");
	printf("  --trace-format FORMAT\n"
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
	case ProgramExe::AGENT:
		printAgentUsage(devhelp);
		break;
	case ProgramExe::BACKUP:
		printBackupUsage(devhelp);
		break;
	case ProgramExe::RESTORE:
		printRestoreUsage(devhelp);
		break;
	case ProgramExe::FASTRESTORE_TOOL:
		printFastRestoreUsage(devhelp);
		break;
	case ProgramExe::DR_AGENT:
		printDBAgentUsage(devhelp);
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
	if (StringRef(programExe).endsWith(".debug"_sr))
		programExe = programExe.substr(0, programExe.size() - 6);

	// Check if backup agent
	if ((programExe.length() >= exeAgent.size()) &&
	    (programExe.compare(programExe.length() - exeAgent.size(), exeAgent.size(), (const char*)exeAgent.begin()) ==
	     0)) {
		enProgramExe = ProgramExe::AGENT;
	}

	// Check if backup
	else if ((programExe.length() >= exeBackup.size()) &&
	         (programExe.compare(
	              programExe.length() - exeBackup.size(), exeBackup.size(), (const char*)exeBackup.begin()) == 0)) {
		enProgramExe = ProgramExe::BACKUP;
	}

	// Check if restore
	else if ((programExe.length() >= exeRestore.size()) &&
	         (programExe.compare(
	              programExe.length() - exeRestore.size(), exeRestore.size(), (const char*)exeRestore.begin()) == 0)) {
		enProgramExe = ProgramExe::RESTORE;
	}

	// Check if restore
	else if ((programExe.length() >= exeFastRestoreTool.size()) &&
	         (programExe.compare(programExe.length() - exeFastRestoreTool.size(),
	                             exeFastRestoreTool.size(),
	                             (const char*)exeFastRestoreTool.begin()) == 0)) {
		enProgramExe = ProgramExe::FASTRESTORE_TOOL;
	}

	// Check if db agent
	else if ((programExe.length() >= exeDatabaseAgent.size()) &&
	         (programExe.compare(programExe.length() - exeDatabaseAgent.size(),
	                             exeDatabaseAgent.size(),
	                             (const char*)exeDatabaseAgent.begin()) == 0)) {
		enProgramExe = ProgramExe::DR_AGENT;
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
		values["tags"] = BackupType::TAGS;
	}

	auto i = values.find(backupType);
	if (i != values.end())
		enBackupType = i->second;

	return enBackupType;
}

RestoreType getRestoreType(std::string name) {
	if (name == "start")
		return RestoreType::START;
	if (name == "abort")
		return RestoreType::ABORT;
	if (name == "status")
		return RestoreType::STATUS;
	if (name == "wait")
		return RestoreType::WAIT;
	return RestoreType::UNKNOWN;
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

ACTOR Future<std::string> getLayerStatus(Reference<ReadYourWritesTransaction> tr,
                                         IPAddress localIP,
                                         std::string name,
                                         std::string id,
                                         ProgramExe exe,
                                         Database dest,
                                         Snapshot snapshot = Snapshot::False) {
	// This process will write a document that looks like this:
	// { backup : { $expires : {<subdoc>}, version: <version from approximately 30 seconds from now> }
	// so that the value under 'backup' will eventually expire to null and thus be ignored by
	// readers of status.  This is because if all agents die then they can no longer clean up old
	// status docs from other dead agents.

	state Version readVer = wait(tr->getReadVersion());

	state json_spirit::mValue layersRootValue; // Will contain stuff that goes into the doc at the layers status root
	JSONDoc layersRoot(layersRootValue); // Convenient mutator / accessor for the layers root
	JSONDoc op = layersRoot.subDoc(name); // Operator object for the $expires operation
	// Create the $expires key which is where the rest of the status output will go

	state JSONDoc layerRoot = op.subDoc("$expires");
	// Set the version argument in the $expires operator object.
	op.create("version") = readVer + 120 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND;

	layerRoot.create("instances_running.$sum") = 1;
	layerRoot.create("last_updated.$max") = now();

	state JSONDoc o = layerRoot.subDoc("instances." + id);

	o.create("version") = FDB_VT_VERSION;
	o.create("id") = id;
	o.create("last_updated") = now();
	o.create("memory_usage") = (int64_t)getMemoryUsage();
	o.create("resident_size") = (int64_t)getResidentMemoryUsage();
	o.create("main_thread_cpu_seconds") = getProcessorTimeThread();
	o.create("process_cpu_seconds") = getProcessorTimeProcess();
	o.create("configured_workers") = CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
	o.create("processID") = ::getpid();
	o.create("locality") = tr->getDatabase()->clientLocality.toJSON<json_spirit::mObject>();
	o.create("networkAddress") = localIP.toString();

	if (exe == ProgramExe::AGENT) {
		static S3BlobStoreEndpoint::Stats last_stats;
		static double last_ts = 0;
		S3BlobStoreEndpoint::Stats current_stats = S3BlobStoreEndpoint::s_stats;
		JSONDoc blobstats = o.create("blob_stats");
		blobstats.create("total") = current_stats.getJSON();
		S3BlobStoreEndpoint::Stats diff = current_stats - last_stats;
		json_spirit::mObject diffObj = diff.getJSON();
		if (last_ts > 0)
			diffObj["bytes_per_second"] = double(current_stats.bytes_sent - last_stats.bytes_sent) / (now() - last_ts);
		blobstats.create("recent") = diffObj;
		last_stats = current_stats;
		last_ts = now();

		JSONDoc totalBlobStats = layerRoot.subDoc("blob_recent_io");
		for (auto& p : diffObj)
			totalBlobStats.create(p.first + ".$sum") = p.second;

		state FileBackupAgent fba;
		state std::vector<KeyBackedTag> backupTags = wait(getAllBackupTags(tr, snapshot));
		state std::vector<Future<Optional<Version>>> tagLastRestorableVersions;
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

		wait(waitForAll(tagLastRestorableVersions) && waitForAll(tagStates) && waitForAll(tagContainers) &&
		     waitForAll(tagRangeBytes) && waitForAll(tagLogBytes) && success(fBackupPaused));

		JSONDoc tagsRoot = layerRoot.subDoc("tags.$latest");
		layerRoot.create("tags.timestamp") = now();
		layerRoot.create("total_workers.$sum") =
		    fBackupPaused.get().present() ? 0 : CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
		layerRoot.create("paused.$latest") = fBackupPaused.get().present();

		int j = 0;
		for (KeyBackedTag eachTag : backupTags) {
			EBackupState status = tagStates[j].get();
			const char* statusText = fba.getStateText(status);

			// The object for this backup tag inside this instance's subdocument
			JSONDoc tagRoot = tagsRoot.subDoc(eachTag.tagName);
			tagRoot.create("current_container") = tagContainers[j].get()->getURL();
			tagRoot.create("current_status") = statusText;
			if (tagLastRestorableVersions[j].get().present()) {
				Version last_restorable_version = tagLastRestorableVersions[j].get().get();
				double last_restorable_seconds_behind =
				    ((double)readVer - last_restorable_version) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
				tagRoot.create("last_restorable_version") = last_restorable_version;
				tagRoot.create("last_restorable_seconds_behind") = last_restorable_seconds_behind;
			}
			tagRoot.create("running_backup") =
			    (status == EBackupState::STATE_RUNNING_DIFFERENTIAL || status == EBackupState::STATE_RUNNING);
			tagRoot.create("running_backup_is_restorable") = (status == EBackupState::STATE_RUNNING_DIFFERENTIAL);
			tagRoot.create("range_bytes_written") = tagRangeBytes[j].get();
			tagRoot.create("mutation_log_bytes_written") = tagLogBytes[j].get();
			tagRoot.create("mutation_stream_id") = backupTagUids[j].toString();

			j++;
		}
	} else if (exe == ProgramExe::DR_AGENT) {
		state DatabaseBackupAgent dba;
		state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(dest));
		tr2->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr2->setOption(FDBTransactionOptions::LOCK_AWARE);
		state RangeResult tagNames = wait(tr2->getRange(dba.tagNames.range(), 10000, snapshot));
		state std::vector<Future<Optional<Key>>> backupVersion;
		state std::vector<Future<EBackupState>> backupStatus;
		state std::vector<Future<int64_t>> tagRangeBytesDR;
		state std::vector<Future<int64_t>> tagLogBytesDR;
		state Future<Optional<Value>> fDRPaused = tr->get(dba.taskBucket->getPauseKey(), snapshot);

		state std::vector<UID> drTagUids;
		for (int i = 0; i < tagNames.size(); i++) {
			backupVersion.push_back(tr2->get(tagNames[i].value.withPrefix(applyMutationsBeginRange.begin), snapshot));
			UID tagUID = BinaryReader::fromStringRef<UID>(tagNames[i].value, Unversioned());
			drTagUids.push_back(tagUID);
			backupStatus.push_back(dba.getStateValue(tr2, tagUID, snapshot));
			tagRangeBytesDR.push_back(dba.getRangeBytesWritten(tr2, tagUID, snapshot));
			tagLogBytesDR.push_back(dba.getLogBytesWritten(tr2, tagUID, snapshot));
		}

		wait(waitForAll(backupStatus) && waitForAll(backupVersion) && waitForAll(tagRangeBytesDR) &&
		     waitForAll(tagLogBytesDR) && success(fDRPaused));

		JSONDoc tagsRoot = layerRoot.subDoc("tags.$latest");
		layerRoot.create("tags.timestamp") = now();
		layerRoot.create("total_workers.$sum") = fDRPaused.get().present() ? 0 : CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT;
		layerRoot.create("paused.$latest") = fDRPaused.get().present();

		for (int i = 0; i < tagNames.size(); i++) {
			std::string tagName = dba.sourceTagNames.unpack(tagNames[i].key).getString(0).toString();

			auto status = backupStatus[i].get();

			JSONDoc tagRoot = tagsRoot.create(tagName);
			tagRoot.create("running_backup") =
			    (status == EBackupState::STATE_RUNNING_DIFFERENTIAL || status == EBackupState::STATE_RUNNING);
			tagRoot.create("running_backup_is_restorable") = (status == EBackupState::STATE_RUNNING_DIFFERENTIAL);
			tagRoot.create("range_bytes_written") = tagRangeBytesDR[i].get();
			tagRoot.create("mutation_log_bytes_written") = tagLogBytesDR[i].get();
			tagRoot.create("mutation_stream_id") = drTagUids[i].toString();

			if (backupVersion[i].get().present()) {
				double seconds_behind = ((double)readVer - BinaryReader::fromStringRef<Version>(
				                                               backupVersion[i].get().get(), Unversioned())) /
				                        CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
				tagRoot.create("seconds_behind") = seconds_behind;
				//TraceEvent("BackupMetrics").detail("SecondsBehind", seconds_behind);
			}

			tagRoot.create("backup_state") = BackupAgentBase::getStateText(status);
		}
	}

	std::string json = json_spirit::write_string(layersRootValue);
	return json;
}

// Check for unparsable or expired statuses and delete them.
// First checks the first doc in the key range, and if it is valid, alive and not "me" then
// returns.  Otherwise, checks the rest of the range as well.
ACTOR Future<Void> cleanupStatus(Reference<ReadYourWritesTransaction> tr,
                                 std::string rootKey,
                                 std::string name,
                                 std::string id,
                                 int limit = 1) {
	state RangeResult docs = wait(tr->getRange(KeyRangeRef(rootKey, strinc(rootKey)), limit, Snapshot::True));
	state bool readMore = false;
	state int i;
	for (i = 0; i < docs.size(); ++i) {
		json_spirit::mValue docValue;
		try {
			json_spirit::read_string(docs[i].value.toString(), docValue);
			JSONDoc doc(docValue);
			// Update the reference version for $expires
			JSONDoc::expires_reference_version = tr->getReadVersion().get();
			// Evaluate the operators in the document, which will reduce to nothing if it is expired.
			doc.cleanOps();
			if (!doc.has(name + ".last_updated"))
				throw Error();

			// Alive and valid.
			// If limit == 1 and id is present then read more
			if (limit == 1 && doc.has(name + ".instances." + id))
				readMore = true;
		} catch (Error& e) {
			// If doc can't be parsed or isn't alive, delete it.
			TraceEvent(SevWarn, "RemovedDeadBackupLayerStatus").errorUnsuppressed(e).detail("Key", docs[i].key);
			tr->clear(docs[i].key);
			// If limit is 1 then read more.
			if (limit == 1)
				readMore = true;
		}
		if (readMore) {
			limit = 10000;
			RangeResult docs2 = wait(tr->getRange(KeyRangeRef(rootKey, strinc(rootKey)), limit, Snapshot::True));
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
			state RangeResult kvPairs =
			    wait(tr.getRange(KeyRangeRef(rootKey, strinc(rootKey)), GetRangeLimits::ROW_LIMIT_UNLIMITED));
			state json_spirit::mObject statusDoc;
			state JSONDoc modifier(statusDoc);
			for (auto& kv : kvPairs) {
				state json_spirit::mValue docValue;
				json_spirit::read_string(kv.value.toString(), docValue);
				wait(yield());
				modifier.absorb(docValue);
				wait(yield());
			}
			JSONDoc::expires_reference_version = (uint64_t)tr.getReadVersion().get();
			modifier.cleanOps();
			return statusDoc;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Read layer status for this layer and get the total count of agent processes (instances) then adjust the poll delay
// based on that and BACKUP_AGGREGATE_POLL_RATE
ACTOR Future<Void> updateAgentPollRate(Database src,
                                       std::string rootKey,
                                       std::string name,
                                       std::shared_ptr<double> pollDelay) {
	loop {
		try {
			json_spirit::mObject status = wait(getLayerStatus(src, rootKey));
			int64_t processes = 0;
			// If instances count is present and greater than 0 then update pollDelay
			if (JSONDoc(status).tryGet<int64_t>(name + ".instances_running", processes) && processes > 0) {
				// The aggregate poll rate is the target poll rate for all agent processes in the cluster
				// The poll rate (polls/sec) for a single processes is aggregate poll rate / processes, and pollDelay is
				// the inverse of that
				*pollDelay = (double)processes / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "BackupAgentPollRateUpdateError").error(e);
		}
		wait(delay(CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE_UPDATE_INTERVAL));
	}
}

ACTOR Future<Void> statusUpdateActor(Database statusUpdateDest,
                                     std::string name,
                                     ProgramExe exe,
                                     std::shared_ptr<double> pollDelay,
                                     Database taskDest = Database(),
                                     std::string id = nondeterministicRandom()->randomUniqueID().toString()) {
	state std::string metaKey = layerStatusMetaPrefixRange.begin.toString() + "json/" + name;
	state std::string rootKey = backupStatusPrefixRange.begin.toString() + name + "/json";
	state std::string instanceKey = rootKey + "/" + "agent-" + id;
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(statusUpdateDest));
	state Future<Void> pollRateUpdater;

	// In order to report a useful networkAddress to the cluster's layer status JSON object, determine which local
	// network interface IP will be used to talk to the cluster.  This is a blocking call, so it is only done once,
	// and in a retry loop because if we can't connect to the cluster we can't do any work anyway.
	state IPAddress localIP;

	loop {
		try {
			localIP = statusUpdateDest->getConnectionRecord()->getConnectionString().determineLocalSourceIP();
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "AgentCouldNotDetermineLocalIP").error(e);
			wait(delay(1.0));
		}
	}

	// Register the existence of this layer in the meta key space
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->set(metaKey, rootKey);
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	// Write status periodically
	loop {
		tr->reset();
		try {
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					state Future<std::string> futureStatusDoc =
					    getLayerStatus(tr, localIP, name, id, exe, taskDest, Snapshot::True);
					wait(cleanupStatus(tr, rootKey, name, id));
					std::string statusdoc = wait(futureStatusDoc);
					tr->set(instanceKey, statusdoc);
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}

			wait(delay(CLIENT_KNOBS->BACKUP_STATUS_DELAY *
			           ((1.0 - CLIENT_KNOBS->BACKUP_STATUS_JITTER) +
			            2 * deterministicRandom()->random01() * CLIENT_KNOBS->BACKUP_STATUS_JITTER)));

			// Now that status was written at least once by this process (and hopefully others), start the poll rate
			// control updater if it wasn't started yet
			if (!pollRateUpdater.isValid())
				pollRateUpdater = updateAgentPollRate(statusUpdateDest, rootKey, name, pollDelay);
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "UnableToWriteStatus").error(e);
			wait(delay(10.0));
		}
	}
}

ACTOR Future<Void> runDBAgent(Database src, Database dest) {
	state std::shared_ptr<double> pollDelay = std::make_shared<double>(1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE);
	std::string id = nondeterministicRandom()->randomUniqueID().toString();
	state Future<Void> status = statusUpdateActor(src, "dr_backup", ProgramExe::DR_AGENT, pollDelay, dest, id);
	state Future<Void> status_other =
	    statusUpdateActor(dest, "dr_backup_dest", ProgramExe::DR_AGENT, pollDelay, dest, id);

	state DatabaseBackupAgent backupAgent(src);

	loop {
		try {
			wait(backupAgent.run(dest, pollDelay, CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT));
			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				throw;

			TraceEvent(SevError, "DA_runAgent").error(e);
			fprintf(stderr, "ERROR: DR agent encountered fatal error `%s'\n", e.what());

			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
		}
	}

	return Void();
}

ACTOR Future<Void> runAgent(Database db) {
	state std::shared_ptr<double> pollDelay = std::make_shared<double>(1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE);
	state Future<Void> status = statusUpdateActor(db, "backup", ProgramExe::AGENT, pollDelay);

	state FileBackupAgent backupAgent;

	loop {
		try {
			wait(backupAgent.run(db, pollDelay, CLIENT_KNOBS->BACKUP_TASKS_PER_AGENT));
			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				throw;

			TraceEvent(SevError, "BA_runAgent").error(e);
			fprintf(stderr, "ERROR: backup agent encountered fatal error `%s'\n", e.what());

			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
		}
	}

	return Void();
}

ACTOR Future<Void> submitDBBackup(Database src,
                                  Database dest,
                                  Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                  std::string tagName) {
	try {
		state DatabaseBackupAgent backupAgent(src);
		ASSERT(!backupRanges.empty());

		wait(backupAgent.submitBackup(
		    dest, KeyRef(tagName), backupRanges, StopWhenDone::False, StringRef(), StringRef(), LockDB::True));

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
                                Optional<std::string> proxy,
                                int initialSnapshotIntervalSeconds,
                                int snapshotIntervalSeconds,
                                Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                bool encryptionEnabled,
                                std::string tagName,
                                bool dryRun,
                                WaitForComplete waitForCompletion,
                                StopWhenDone stopWhenDone,
                                UsePartitionedLog usePartitionedLog,
                                IncrementalBackupOnly incrementalBackupOnly,
                                Optional<std::string> blobManifestUrl) {
	try {
		state FileBackupAgent backupAgent;
		ASSERT(!backupRanges.empty());

		if (dryRun) {
			state KeyBackedTag tag = makeBackupTag(tagName);
			Optional<UidAndAbortedFlagT> uidFlag = wait(tag.get(db.getReference()));

			if (uidFlag.present()) {
				BackupConfig config(uidFlag.get().first);
				EBackupState backupStatus = wait(config.stateEnum().getOrThrow(db.getReference()));

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
			                              proxy,
			                              initialSnapshotIntervalSeconds,
			                              snapshotIntervalSeconds,
			                              tagName,
			                              backupRanges,
			                              encryptionEnabled,
			                              stopWhenDone,
			                              usePartitionedLog,
			                              incrementalBackupOnly,
			                              {},
			                              blobManifestUrl));

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
                                  ForceAction forceAction) {
	try {
		state DatabaseBackupAgent backupAgent(src);
		ASSERT(!backupRanges.empty());

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

ACTOR Future<Void> statusBackup(Database db, std::string tagName, ShowErrors showErrors, bool json) {
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

ACTOR Future<Void> abortDBBackup(Database src,
                                 Database dest,
                                 std::string tagName,
                                 PartialBackup partial,
                                 DstOnly dstOnly) {
	try {
		state DatabaseBackupAgent backupAgent(src);

		wait(backupAgent.abortBackup(dest, Key(tagName), partial, AbortOldBackup::False, dstOnly));
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

ACTOR Future<Void> cleanupMutations(Database db, DeleteData deleteData) {
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

ACTOR Future<Void> waitBackup(Database db, std::string tagName, StopWhenDone stopWhenDone) {
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

ACTOR Future<Void> discontinueBackup(Database db, std::string tagName, WaitForComplete waitForCompletion) {
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

Reference<IBackupContainer> openBackupContainer(const char* name,
                                                const std::string& destinationContainer,
                                                const Optional<std::string>& proxy,
                                                const Optional<std::string>& encryptionKeyFile) {
	// Error, if no dest container was specified
	if (destinationContainer.empty()) {
		fprintf(stderr, "ERROR: No backup destination was specified.\n");
		printHelpTeaser(name);
		throw backup_error();
	}

	Reference<IBackupContainer> c;
	try {
		c = IBackupContainer::openContainer(destinationContainer, proxy, encryptionKeyFile);
	} catch (Error& e) {
		std::string msg = format("ERROR: '%s' on URL '%s'", e.what(), destinationContainer.c_str());
		if (e.code() == error_code_backup_invalid_url && !IBackupContainer::lastOpenError.empty()) {
			msg += format(": %s", IBackupContainer::lastOpenError.c_str());
		}
		fprintf(stderr, "%s\n", msg.c_str());
		printHelpTeaser(name);
		throw;
	}

	return c;
}

// Submit the restore request to the database if "performRestore" is true. Otherwise,
// check if the restore can be performed.
ACTOR Future<Void> runRestore(Database db,
                              std::string originalClusterFile,
                              std::string tagName,
                              std::string container,
                              Optional<std::string> proxy,
                              Standalone<VectorRef<KeyRangeRef>> ranges,
                              Version beginVersion,
                              Version targetVersion,
                              std::string targetTimestamp,
                              bool performRestore,
                              Verbose verbose,
                              WaitForComplete waitForDone,
                              std::string addPrefix,
                              std::string removePrefix,
                              OnlyApplyMutationLogs onlyApplyMutationLogs,
                              InconsistentSnapshotOnly inconsistentSnapshotOnly,
                              Optional<std::string> encryptionKeyFile,
                              Optional<std::string> blobManifestUrl) {
	ASSERT(!ranges.empty());

	if (targetVersion != invalidVersion && !targetTimestamp.empty()) {
		fprintf(stderr, "Restore target version and target timestamp cannot both be specified\n");
		throw restore_error();
	}

	state Optional<Database> origDb;

	// Resolve targetTimestamp if given
	if (!targetTimestamp.empty()) {
		if (originalClusterFile.empty()) {
			fprintf(stderr,
			        "An original cluster file must be given in order to resolve restore target timestamp '%s'\n",
			        targetTimestamp.c_str());
			throw restore_error();
		}

		if (!fileExists(originalClusterFile)) {
			fprintf(
			    stderr, "Original source database cluster file '%s' does not exist.\n", originalClusterFile.c_str());
			throw restore_error();
		}

		origDb = Database::createDatabase(originalClusterFile, ApiVersion::LATEST_VERSION);
		Version v = wait(timeKeeperVersionFromDatetime(targetTimestamp, origDb.get()));
		fmt::print("Timestamp '{0}' resolves to version {1}\n", targetTimestamp, v);
		targetVersion = v;
	}

	try {
		state FileBackupAgent backupAgent;

		state Reference<IBackupContainer> bc =
		    openBackupContainer(exeRestore.toString().c_str(), container, proxy, encryptionKeyFile);

		// If targetVersion is unset then use the maximum restorable version from the backup description
		if (targetVersion == invalidVersion) {
			if (verbose)
				printf(
				    "No restore target version given, will use maximum restorable version from backup description.\n");

			BackupDescription desc = wait(bc->describeBackup());
			if (blobManifestUrl.present()) {
				onlyApplyMutationLogs = OnlyApplyMutationLogs::True;
			}

			if (onlyApplyMutationLogs && desc.contiguousLogEnd.present()) {
				targetVersion = desc.contiguousLogEnd.get() - 1;
			} else if (desc.maxRestorableVersion.present()) {
				targetVersion = desc.maxRestorableVersion.get();
			} else {
				fprintf(stderr, "The specified backup is not restorable to any version.\n");
				throw restore_error();
			}

			if (verbose) {
				fmt::print("Using target restore version {}\n", targetVersion);
			}
		}

		if (performRestore) {
			Version restoredVersion = wait(backupAgent.restore(db,
			                                                   origDb,
			                                                   KeyRef(tagName),
			                                                   KeyRef(container),
			                                                   proxy,
			                                                   ranges,
			                                                   waitForDone,
			                                                   targetVersion,
			                                                   verbose,
			                                                   KeyRef(addPrefix),
			                                                   KeyRef(removePrefix),
			                                                   LockDB::True,
			                                                   UnlockDB::True,
			                                                   onlyApplyMutationLogs,
			                                                   inconsistentSnapshotOnly,
			                                                   beginVersion,
			                                                   encryptionKeyFile,
			                                                   blobManifestUrl));

			if (waitForDone && verbose) {
				// If restore is now complete then report version restored
				fmt::print("Restored to version {}\n", restoredVersion);
			}
		} else {
			state Optional<RestorableFileSet> rset = wait(bc->getRestoreSet(targetVersion, ranges));

			if (!rset.present()) {
				fmt::print(stderr,
				           "Insufficient data to restore to version {}.  Describe backup for more information.\n",
				           targetVersion);
				throw restore_invalid_version();
			}

			fmt::print("Backup can be used to restore to version {}\n", targetVersion);
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		fprintf(stderr, "ERROR: %s\n", e.what());
		throw;
	}

	return Void();
}

// Fast restore agent that kicks off the restore: send restore requests to restore workers.
ACTOR Future<Void> runFastRestoreTool(Database db,
                                      std::string tagName,
                                      std::string container,
                                      Optional<std::string> proxy,
                                      Standalone<VectorRef<KeyRangeRef>> ranges,
                                      Version dbVersion,
                                      bool performRestore,
                                      Verbose verbose,
                                      WaitForComplete waitForDone) {
	try {
		state FileBackupAgent backupAgent;
		state Version restoreVersion = invalidVersion;

		if (ranges.size() > 1) {
			fprintf(stdout, "[WARNING] Currently only a single restore range is tested!\n");
		}

		if (ranges.size() == 0) {
			ranges.push_back(ranges.arena(), normalKeys);
		}

		printf("[INFO] runFastRestoreTool: restore_ranges:%d first range:%s\n",
		       ranges.size(),
		       ranges.front().toString().c_str());
		TraceEvent ev("FastRestoreTool");
		ev.detail("RestoreRanges", ranges.size());
		for (int i = 0; i < ranges.size(); ++i) {
			ev.detail(format("Range%d", i), ranges[i]);
		}

		if (performRestore) {
			if (dbVersion == invalidVersion) {
				TraceEvent("FastRestoreTool").detail("TargetRestoreVersion", "Largest restorable version");
				BackupDescription desc = wait(IBackupContainer::openContainer(container, proxy, {})->describeBackup());
				if (!desc.maxRestorableVersion.present()) {
					fprintf(stderr, "The specified backup is not restorable to any version.\n");
					throw restore_error();
				}

				dbVersion = desc.maxRestorableVersion.get();
				TraceEvent("FastRestoreTool").detail("TargetRestoreVersion", dbVersion);
			}
			state UID randomUID = deterministicRandom()->randomUniqueID();
			TraceEvent("FastRestoreTool")
			    .detail("SubmitRestoreRequests", ranges.size())
			    .detail("RestoreUID", randomUID);
			wait(backupAgent.submitParallelRestore(db,
			                                       KeyRef(tagName),
			                                       ranges,
			                                       KeyRef(container),
			                                       proxy,
			                                       dbVersion,
			                                       LockDB::True,
			                                       randomUID,
			                                       ""_sr,
			                                       ""_sr));
			// TODO: Support addPrefix and removePrefix
			if (waitForDone) {
				// Wait for parallel restore to finish and unlock DB after that
				TraceEvent("FastRestoreTool").detail("BackupAndParallelRestore", "WaitForRestoreToFinish");
				wait(backupAgent.parallelRestoreFinish(db, randomUID));
				TraceEvent("FastRestoreTool").detail("BackupAndParallelRestore", "RestoreFinished");
			} else {
				TraceEvent("FastRestoreTool")
				    .detail("RestoreUID", randomUID)
				    .detail("OperationGuide", "Manually unlock DB when restore finishes");
				printf("WARNING: DB will be in locked state after restore. Need UID:%s to unlock DB\n",
				       randomUID.toString().c_str());
			}

			restoreVersion = dbVersion;
		} else {
			state Reference<IBackupContainer> bc = IBackupContainer::openContainer(container, proxy, {});
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
				fmt::print(stderr, "Insufficient data to restore to version {}\n", restoreVersion);
				throw restore_invalid_version();
			}

			// Display the restore information, if requested
			if (verbose) {
				fmt::print("[DRY RUN] Restoring backup to version: {}\n", restoreVersion);
				fmt::print("{}\n", description.toString());
			}
		}

		if (waitForDone && verbose) {
			// If restore completed then report version restored
			fmt::print("Restored to version {0}{1}\n", restoreVersion, (performRestore) ? "" : " (DRY RUN)");
		}
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
                                  Optional<std::string> proxy,
                                  Version beginVersion,
                                  Version endVersion) {
	state Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer, proxy, {});

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

	fmt::print("Scanning version range {0} to {1}\n", beginVersion, endVersion);
	BackupFileList files = wait(c->dumpFileList(beginVersion, endVersion));
	files.toStream(stdout);

	return Void();
}

ACTOR Future<Void> expireBackupData(const char* name,
                                    std::string destinationContainer,
                                    Optional<std::string> proxy,
                                    Version endVersion,
                                    std::string endDatetime,
                                    Database db,
                                    bool force,
                                    Version restorableAfterVersion,
                                    std::string restorableAfterDatetime,
                                    Optional<std::string> encryptionKeyFile) {
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
		Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer, proxy, encryptionKeyFile);

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
				when(wait(expire)) {
					break;
				}
			}
		}

		std::string p = progress.toString();
		int spaces = lastProgress.size() - p.size();
		printf("\r%s%s\n", p.c_str(), (spaces > 0 ? std::string(spaces, ' ').c_str() : ""));

		if (endVersion < 0)
			fmt::print("All data before {0} versions ({1}"
			           " days) prior to latest backup log has been deleted.\n",
			           -endVersion,
			           -endVersion / ((int64_t)24 * 3600 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
		else
			fmt::print("All data before version {} has been deleted.\n", endVersion);
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

ACTOR Future<Void> deleteBackupContainer(const char* name,
                                         std::string destinationContainer,
                                         Optional<std::string> proxy) {
	try {
		state Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer, proxy, {});
		state int numDeleted = 0;
		state Future<Void> done = c->deleteContainer(&numDeleted);

		state int lastUpdate = -1;
		printf("Deleting %s...\n", destinationContainer.c_str());

		loop {
			choose {
				when(wait(done)) {
					break;
				}
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
                                  Optional<std::string> proxy,
                                  bool deep,
                                  Optional<Database> cx,
                                  bool json,
                                  Optional<std::string> encryptionKeyFile) {
	try {
		Reference<IBackupContainer> c = openBackupContainer(name, destinationContainer, proxy, encryptionKeyFile);
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

std::pair<Version, Version> getMaxMinRestorableVersions(const BackupDescription& desc, bool mayOnlyApplyMutationLog) {
	Version maxRestorableVersion = invalidVersion;
	Version minRestorableVersion = invalidVersion;
	if (desc.maxRestorableVersion.present()) {
		maxRestorableVersion = desc.maxRestorableVersion.get();
	} else if (mayOnlyApplyMutationLog && desc.contiguousLogEnd.present()) {
		maxRestorableVersion = desc.contiguousLogEnd.get();
	}

	if (desc.minRestorableVersion.present()) {
		minRestorableVersion = desc.minRestorableVersion.get();
	} else if (mayOnlyApplyMutationLog && desc.minLogBegin.present()) {
		minRestorableVersion = desc.minLogBegin.get();
	}

	return std::make_pair(maxRestorableVersion, minRestorableVersion);
}

// If restoreVersion is invalidVersion or latestVersion, use the maximum or minimum restorable version respectively for
// selected key ranges. If restoreTimestamp is specified, any specified restoreVersion will be overridden to the version
// resolved to that timestamp.
ACTOR Future<Void> queryBackup(const char* name,
                               std::string destinationContainer,
                               Optional<std::string> proxy,
                               Standalone<VectorRef<KeyRangeRef>> keyRangesFilter,
                               Version restoreVersion,
                               Version snapshotVersion,
                               std::string originalClusterFile,
                               std::string restoreTimestamp,
                               Verbose verbose,
                               Optional<Database> cx) {
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
	    .detail("SpecifiedSnapshotVersion", snapshotVersion)
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

		Database origDb = Database::createDatabase(originalClusterFile, ApiVersion::LATEST_VERSION);
		Version v = wait(timeKeeperVersionFromDatetime(restoreTimestamp, origDb));
		result["restore_timestamp"] = restoreTimestamp;
		result["restore_timestamp_resolved_version"] = v;
		restoreVersion = v;
	}

	state int64_t totalRangeFilesSize = 0;
	state int64_t totalLogFilesSize = 0;
	state JsonBuilderArray rangeFilesJson;
	state JsonBuilderArray logFilesJson;
	try {
		state Reference<IBackupContainer> bc = openBackupContainer(name, destinationContainer, proxy, {});
		BackupDescription desc = wait(bc->describeBackup());
		// Use continuous log end version for the maximum restorable version for the key ranges when a restorable
		// version doesn't exist.
		auto [maxRestorableVersion, minRestorableVersion] = getMaxMinRestorableVersions(desc, !keyRangesFilter.empty());
		if (restoreVersion == invalidVersion) {
			restoreVersion = maxRestorableVersion;
		}
		if (restoreVersion == earliestVersion) {
			restoreVersion = minRestorableVersion;
		}
		if (snapshotVersion == earliestVersion) {
			snapshotVersion = minRestorableVersion;
		}
		TraceEvent("BackupQueryResolveRestoreVersion")
		    .detail("RestoreVersion", restoreVersion)
		    .detail("SnapshotVersion", snapshotVersion);
		if (restoreVersion < 0) {
			reportBackupQueryError(operationId,
			                       result,
			                       errorMessage =
			                           format("the specified restorable version %lld is not valid", restoreVersion));
			return Void();
		}

		state Optional<RestorableFileSet> fileSet;
		if (snapshotVersion != invalidVersion) {
			// When a snapshot version is specified, we will first get a restore set using the latest snapshot file to
			// restore to the snapshot version. After snapshot version, we will only use mutation logs to restore.
			wait(store(fileSet, bc->getRestoreSet(snapshotVersion, keyRangesFilter)));
			if (fileSet.present()) {
				result["snapshot_version"] = fileSet.get().targetVersion;
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

				snapshotVersion = fileSet.get().targetVersion;

				TraceEvent("BackupQueryReceivedRestorableFilesSetFromSnapshot")
				    .detail("SnapshotVersion", snapshotVersion)
				    .detail("DestinationContainer", destinationContainer)
				    .detail("KeyRangesFilter", printable(keyRangesFilter))
				    .detail("ActualRestoreVersion", fileSet.get().targetVersion)
				    .detail("NumRangeFiles", fileSet.get().ranges.size())
				    .detail("NumLogFiles", fileSet.get().logs.size())
				    .detail("RangeFilesBytes", totalRangeFilesSize)
				    .detail("LogFilesBytes", totalLogFilesSize);
			} else {
				reportBackupQueryError(
				    operationId,
				    result,
				    format("no restorable files set found for specified key ranges from snapshotVersion %lld",
				           snapshotVersion));
				return Void();
			}

			// We only need to know all the mutation logs from `snapshotVersion` to `restoreVersion`.
			wait(store(fileSet, bc->getRestoreSet(restoreVersion, keyRangesFilter, /*logOnly=*/true, snapshotVersion)));
		} else {
			// When a snapshot version is not specified, we use the latest snapshot to restore to the `restoreVersion`.
			wait(store(fileSet, bc->getRestoreSet(restoreVersion, keyRangesFilter)));
		}

		if (fileSet.present()) {
			result["restore_version"] = fileSet.get().targetVersion;
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

			TraceEvent("BackupQueryReceivedRestorableFilesSet")
			    .detail("DestinationContainer", destinationContainer)
			    .detail("KeyRangesFilter", printable(keyRangesFilter))
			    .detail("ActualRestoreVersion", fileSet.get().targetVersion)
			    .detail("NumRangeFiles", fileSet.get().ranges.size())
			    .detail("NumLogFiles", fileSet.get().logs.size())
			    .detail("RangeFilesBytes", totalRangeFilesSize)
			    .detail("LogFilesBytes", totalLogFilesSize);
		} else if (snapshotVersion == invalidVersion) {
			reportBackupQueryError(operationId, result, "no restorable files set found for specified key ranges");
			return Void();
		}

	} catch (Error& e) {
		reportBackupQueryError(operationId, result, e.what());
		return Void();
	}

	result["total_range_files_size"] = totalRangeFilesSize;
	result["total_log_files_size"] = totalLogFilesSize;

	if (verbose) {
		result["ranges"] = rangeFilesJson;
		result["logs"] = logFilesJson;
	}

	printf("%s\n", result.getJson().c_str());
	return Void();
}

ACTOR Future<Void> listBackup(std::string baseUrl, Optional<std::string> proxy) {
	try {
		std::vector<std::string> containers = wait(IBackupContainer::listContainers(baseUrl, proxy));
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

ACTOR Future<Void> listBackupTags(Database cx) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			std::vector<KeyBackedTag> tags = wait(getAllBackupTags(tr));
			for (const auto& tag : tags) {
				printf("%s\n", tag.tagName.c_str());
			}
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

struct BackupModifyOptions {
	Optional<std::string> verifyUID;
	Optional<std::string> destURL;
	Optional<std::string> proxy;
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
		bc = openBackupContainer(exeBackup.toString().c_str(), options.destURL.get(), options.proxy, {});
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

			state Optional<UidAndAbortedFlagT> uidFlag = wait(tag.get(db.getReference()));

			if (!uidFlag.present()) {
				fprintf(stderr, "No backup exists on tag '%s'\n", tagName.c_str());
				throw backup_error();
			}

			if (uidFlag.get().second) {
				fprintf(stderr, "Cannot modify aborted backup on tag '%s'\n", tagName.c_str());
				throw backup_error();
			}

			state BackupConfig config(uidFlag.get().first);
			EBackupState s = wait(config.stateEnum().getOrThrow(tr, Snapshot::False, backup_invalid_info()));
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
				Version begin = wait(config.snapshotBeginVersion().getOrThrow(tr, Snapshot::False, backup_error()));
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
	[[maybe_unused]] int tokenArray = 0;

	auto parsed = parseLine(optionValue, err, partial);

	for (auto tokens : parsed) {
		tokenArray++;

		/*
		int tokenIndex = 0;
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
			fmt::print(stderr, "ERROR: Invalid key range identified with {} keys", tokens.size());
			throw invalid_option_value();
			break;
		}
	}

	return;
}

Version parseVersion(const char* str) {
	StringRef s((const uint8_t*)str, strlen(str));

	if (s.endsWith("days"_sr) || s.endsWith("d"_sr)) {
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

// Creates a connection to a cluster. Optionally prints an error if the connection fails.
Optional<Database> connectToCluster(std::string const& clusterFile,
                                    LocalityData const& localities,
                                    bool quiet = false) {
	auto resolvedClusterFile = ClusterConnectionFile::lookupClusterFileName(clusterFile);
	Reference<ClusterConnectionFile> ccf;

	Optional<Database> db;

	try {
		ccf = makeReference<ClusterConnectionFile>(resolvedClusterFile.first);
	} catch (Error& e) {
		if (!quiet)
			fprintf(stderr, "%s\n", ClusterConnectionFile::getErrorString(resolvedClusterFile, e).c_str());
		return db;
	}

	try {
		db = Database::createDatabase(ccf, ApiVersion::LATEST_VERSION, IsInternal::True, localities);
	} catch (Error& e) {
		if (!quiet) {
			fprintf(stderr, "ERROR: %s\n", e.what());
			fprintf(stderr, "ERROR: Unable to connect to cluster from `%s'\n", ccf->getLocation().c_str());
		}
		return db;
	}

	return db;
};

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
		RestoreType restoreType = RestoreType::UNKNOWN;
		DBType dbType = DBType::UNDEFINED;

		std::unique_ptr<CSimpleOpt> args;

		switch (programExe) {
		case ProgramExe::AGENT:
			args = std::make_unique<CSimpleOpt>(argc, argv, g_rgAgentOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
			break;
		case ProgramExe::DR_AGENT:
			args = std::make_unique<CSimpleOpt>(argc, argv, g_rgDBAgentOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
			break;
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
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupStartOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::STATUS:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupStatusOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::ABORT:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupAbortOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::CLEANUP:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupCleanupOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::WAIT:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupWaitOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::DISCONTINUE:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupDiscontinueOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::PAUSE:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupPauseOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::RESUME:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupPauseOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::EXPIRE:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupExpireOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::DELETE_BACKUP:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupDeleteOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::DESCRIBE:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupDescribeOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::DUMP:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupDumpOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::LIST:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupListOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::QUERY:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupQueryOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::MODIFY:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupModifyOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::TAGS:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgBackupTagsOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case BackupType::UNDEFINED:
				default:
					args =
					    std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
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
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgDBStartOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case DBType::STATUS:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgDBStatusOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case DBType::SWITCH:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgDBSwitchOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case DBType::ABORT:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgDBAbortOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case DBType::PAUSE:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgDBPauseOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case DBType::RESUME:
					args = std::make_unique<CSimpleOpt>(
					    argc - 1, &argv[1], g_rgDBPauseOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				case DBType::UNDEFINED:
				default:
					args =
					    std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
					break;
				}
			}
			break;
		case ProgramExe::RESTORE:
			if (argc < 2) {
				printRestoreUsage(false);
				return FDB_EXIT_ERROR;
			}
			// Get the restore operation type
			restoreType = getRestoreType(argv[1]);
			if (restoreType == RestoreType::UNKNOWN) {
				args = std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
			} else {
				args = std::make_unique<CSimpleOpt>(
				    argc - 1, argv + 1, g_rgRestoreOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
			}
			break;
		case ProgramExe::FASTRESTORE_TOOL:
			if (argc < 2) {
				printFastRestoreUsage(false);
				return FDB_EXIT_ERROR;
			}
			// Get the restore operation type
			restoreType = getRestoreType(argv[1]);
			if (restoreType == RestoreType::UNKNOWN) {
				args = std::make_unique<CSimpleOpt>(argc, argv, g_rgOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
			} else {
				args = std::make_unique<CSimpleOpt>(
				    argc - 1, argv + 1, g_rgRestoreOptions, SO_O_EXACT | SO_O_HYPHEN_TO_UNDERSCORE);
			}
			break;
		case ProgramExe::UNDEFINED:
		default:
			fprintf(stderr, "FoundationDB " FDB_VT_PACKAGE_NAME " (v" FDB_VT_VERSION ")\n");
			fprintf(stderr, "ERROR: Unable to determine program type based on executable `%s'\n", argv[0]);
			return FDB_EXIT_ERROR;
			break;
		}

		Optional<std::string> proxy;
		std::string p;
		if (platform::getEnvironmentVar("HTTP_PROXY", p) || platform::getEnvironmentVar("HTTPS_PROXY", p)) {
			proxy = p;
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
		Version snapshotVersion = invalidVersion;
		std::string restoreTimestamp;
		WaitForComplete waitForDone{ false };
		StopWhenDone stopWhenDone{ true };
		UsePartitionedLog usePartitionedLog{ false }; // Set to true to use new backup system
		IncrementalBackupOnly incrementalBackupOnly{ false };
		OnlyApplyMutationLogs onlyApplyMutationLogs{ false };
		InconsistentSnapshotOnly inconsistentSnapshotOnly{ false };
		ForceAction forceAction{ false };
		bool trace = false;
		bool quietDisplay = false;
		bool dryRun = false;
		bool restoreSystemKeys = false;
		bool restoreUserKeys = false;
		bool encryptionEnabled = true;
		bool encryptSnapshotFilesPresent = false;
		std::string traceDir = "";
		std::string traceFormat = "";
		std::string traceLogGroup;
		uint64_t traceRollSize = TRACE_DEFAULT_ROLL_SIZE;
		uint64_t traceMaxLogsSize = TRACE_DEFAULT_MAX_LOGS_SIZE;
		ESOError lastError;
		PartialBackup partial{ true };
		DstOnly dstOnly{ false };
		LocalityData localities;
		uint64_t memLimit = 8LL << 30;
		uint64_t virtualMemLimit = 0; // unlimited
		Optional<uint64_t> ti;
		BackupTLSConfig tlsConfig;
		Version dumpBegin = 0;
		Version dumpEnd = std::numeric_limits<Version>::max();
		std::string restoreClusterFileDest;
		std::string restoreClusterFileOrig;
		bool jsonOutput = false;
		DeleteData deleteData{ false };
		Optional<std::string> encryptionKeyFile;
		Optional<std::string> blobManifestUrl;

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
				deleteData.set(true);
				break;
			case OPT_MIN_CLEANUP_SECONDS:
				knobs.emplace_back("min_cleanup_seconds", args->OptionArg());
				break;
			case OPT_FORCE:
				forceAction.set(true);
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
				Optional<std::string> localityKey = extractPrefixedArgument("--locality", args->OptionSyntax());
				if (!localityKey.present()) {
					fprintf(stderr, "ERROR: unable to parse locality key '%s'\n", args->OptionSyntax());
					return FDB_EXIT_ERROR;
				}
				Standalone<StringRef> key = StringRef(localityKey.get());
				std::transform(key.begin(), key.end(), mutateString(key), ::tolower);
				localities.set(key, Standalone<StringRef>(std::string(args->OptionArg())));
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
			case OPT_ENCRYPT_FILES: {
				const char* a = args->OptionArg();
				int encryptFiles;
				if (!sscanf(a, "%d", &encryptFiles)) {
					fprintf(stderr, "ERROR: Could not parse encrypt-files `%s'\n", a);
					return FDB_EXIT_ERROR;
				}
				if (encryptFiles != 0 && encryptFiles != 1) {
					fprintf(stderr, "ERROR: encrypt-files must be either 0 or 1\n");
					return FDB_EXIT_ERROR;
				}
				encryptSnapshotFilesPresent = true;
				if (encryptFiles == 0) {
					encryptionEnabled = false;
				} else {
					encryptionEnabled = true;
				}
				break;
			}
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
				partial.set(false);
				break;
			case OPT_DSTONLY:
				dstOnly.set(true);
				break;
			case OPT_KNOB: {
				Optional<std::string> knobName = extractPrefixedArgument("--knob", args->OptionSyntax());
				if (!knobName.present()) {
					fprintf(stderr, "ERROR: unable to parse knob option '%s'\n", args->OptionSyntax());
					return FDB_EXIT_ERROR;
				}
				knobs.emplace_back(knobName.get(), args->OptionArg());
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
			case OPT_BACKUPKEYS_FILE:
				try {
					std::string line = readFileBytes(args->OptionArg(), 64 * 1024 * 1024);
					addKeyRange(line, backupKeys);
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
			case OPT_PROXY:
				proxy = args->OptionArg();
				if (!Hostname::isHostname(proxy.get()) && !NetworkAddress::parseOptional(proxy.get()).present()) {
					fprintf(stderr, "ERROR: Proxy format should be either IP:port or host:port\n");
					return FDB_EXIT_ERROR;
				}
				modifyOptions.proxy = proxy;
				break;
			case OPT_DESTCONTAINER:
				destinationContainer = args->OptionArg();
				// If the url starts with '/' then prepend "file://" for backwards compatibility
				if (StringRef(destinationContainer).startsWith("/"_sr))
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
				waitForDone.set(true);
				break;
			case OPT_NOSTOPWHENDONE:
				stopWhenDone.set(false);
				break;
			case OPT_USE_PARTITIONED_LOG:
				usePartitionedLog.set(true);
				break;
			case OPT_INCREMENTALONLY:
				incrementalBackupOnly.set(true);
				onlyApplyMutationLogs.set(true);
				break;
			case OPT_ENCRYPTION_KEY_FILE:
				encryptionKeyFile = args->OptionArg();
				break;
			case OPT_RESTORECONTAINER:
				restoreContainer = args->OptionArg();
				// If the url starts with '/' then prepend "file://" for backwards compatibility
				if (StringRef(restoreContainer).startsWith("/"_sr))
					restoreContainer = std::string("file://") + restoreContainer;
				break;
			case OPT_DESCRIBE_DEEP:
				describeDeep = true;
				break;
			case OPT_DESCRIBE_TIMESTAMPS:
				describeTimestamps = true;
				break;
			case OPT_PREFIX_ADD: {
				bool err = false;
				addPrefix = decode_hex_string(args->OptionArg(), err);
				if (err) {
					fprintf(stderr, "ERROR: Could not parse add prefix\n");
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				break;
			}
			case OPT_PREFIX_REMOVE: {
				bool err = false;
				removePrefix = decode_hex_string(args->OptionArg(), err);
				if (err) {
					fprintf(stderr, "ERROR: Could not parse remove prefix\n");
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				break;
			}
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
			case OPT_RESTORE_SNAPSHOT_VERSION: {
				const char* a = args->OptionArg();
				long long ver = 0;
				if (!sscanf(a, "%lld", &ver)) {
					fprintf(stderr, "ERROR: Could not parse database version `%s'\n", a);
					printHelpTeaser(argv[0]);
					return FDB_EXIT_ERROR;
				}
				snapshotVersion = ver;
				break;
			}
			case OPT_RESTORE_USER_DATA: {
				restoreUserKeys = true;
				break;
			}
			case OPT_RESTORE_SYSTEM_DATA: {
				restoreSystemKeys = true;
				break;
			}
			case OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY: {
				inconsistentSnapshotOnly.set(true);
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
			case OPT_VMEMLIMIT:
				ti = parse_with_suffix(args->OptionArg(), "MiB");
				if (!ti.present()) {
					fprintf(stderr, "ERROR: Could not parse virtual memory limit from `%s'\n", args->OptionArg());
					printHelpTeaser(argv[0]);
					flushAndExit(FDB_EXIT_ERROR);
				}
				virtualMemLimit = ti.get();
				break;
			case OPT_BLOB_CREDENTIALS:
				tlsConfig.blobCredentials.push_back(args->OptionArg());
				break;
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
			case OPT_DUMP_BEGIN:
				dumpBegin = parseVersion(args->OptionArg());
				break;
			case OPT_DUMP_END:
				dumpEnd = parseVersion(args->OptionArg());
				break;
			case OPT_JSON:
				jsonOutput = true;
				break;
			case OPT_BLOB_MANIFEST_URL: {
				blobManifestUrl = args->OptionArg();
				break;
			}
			}
		}

		if (encryptionKeyFile.present() && encryptSnapshotFilesPresent) {
			fprintf(stderr, "WARNING: Use of --encrypt-files and --encryption-key-file together is discouraged\n");
		}

		// Process the extra arguments
		for (int argLoop = 0; argLoop < args->FileCount(); argLoop++) {
			switch (programExe) {
			case ProgramExe::AGENT:
				fprintf(stderr, "ERROR: Backup Agent does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

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

			case ProgramExe::RESTORE:
				fprintf(stderr, "ERROR: FDB Restore does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case ProgramExe::FASTRESTORE_TOOL:
				fprintf(
				    stderr, "ERROR: FDB Fast Restore Tool does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;

			case ProgramExe::DR_AGENT:
				fprintf(stderr, "ERROR: DR Agent does not support argument value `%s'\n", args->File(argLoop));
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
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

		if (restoreSystemKeys && restoreUserKeys) {
			fprintf(stderr, "ERROR: Please only specify one of --user-data or --system-metadata, not both\n");
			return FDB_EXIT_ERROR;
		}

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
		setMemoryQuota(virtualMemLimit);

		Database db;
		Database sourceDb;
		FileBackupAgent ba;
		Key tag;
		Future<Optional<Void>> f;
		Future<Optional<int>> fstatus;
		Reference<IBackupContainer> c;

		try {
			setupNetwork(0, UseMetrics::True);
		} catch (Error& e) {
			fprintf(stderr, "ERROR: %s\n", e.what());
			return FDB_EXIT_ERROR;
		}

		Future<Void> memoryUsageMonitor = startMemoryUsageMonitor(memLimit);

		IKnobCollection::setupKnobs(knobs);
		// Reinitialize knobs in order to update knobs that are dependent on explicitly set knobs
		IKnobCollection::getMutableGlobalKnobCollection().initialize(Randomize::False, IsSimulated::False);

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
		    .detail("Proxy", proxy.orDefault(""))
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
				openTraceFile({}, traceRollSize, traceMaxLogsSize, traceDir, "trace", traceLogGroup);
		};

		auto initCluster = [&](bool quiet = false) {
			Optional<Database> result = connectToCluster(clusterFile, localities, quiet);
			if (result.present()) {
				db = result.get();
				// Make sure we are setting a transaction timeout and retry limit to prevent cases
				// where the fdbbackup command hangs infinitely. 60 seconds should be more than
				// enough for all cases to finish and 5 retries should also be good enough for
				// most cases.
				int64_t timeout = 60000;
				db->setOption(FDBDatabaseOptions::TRANSACTION_TIMEOUT,
				              Optional<StringRef>(StringRef((const uint8_t*)&timeout, sizeof(timeout))));
				int64_t retryLimit = 5;
				db->setOption(FDBDatabaseOptions::TRANSACTION_RETRY_LIMIT,
				              Optional<StringRef>(StringRef((const uint8_t*)&retryLimit, sizeof(retryLimit))));
			}

			return result.present();
		};

		auto initSourceCluster = [&](bool required, bool quiet = false) {
			if (!sourceClusterFile.size() && required) {
				if (!quiet) {
					fprintf(stderr, "ERROR: source cluster file is required\n");
				}
				return false;
			}

			Optional<Database> result = connectToCluster(sourceClusterFile, localities, quiet);
			if (result.present()) {
				sourceDb = result.get();
				// Make sure we are setting a transaction timeout and retry limit to prevent cases
				// where the fdbbackup command hangs infinitely. 60 seconds should be more than
				// enough for all cases to finish and 5 retries should also be good enough for
				// most cases.
				int64_t timeout = 60000;
				sourceDb->setOption(FDBDatabaseOptions::TRANSACTION_TIMEOUT,
				                    Optional<StringRef>(StringRef((const uint8_t*)&timeout, sizeof(timeout))));
				int64_t retryLimit = 5;
				sourceDb->setOption(FDBDatabaseOptions::TRANSACTION_RETRY_LIMIT,
				                    Optional<StringRef>(StringRef((const uint8_t*)&retryLimit, sizeof(retryLimit))));
			}

			return result.present();
		};

		// The fastrestore tool does not yet support multiple ranges and is incompatible with tenants
		// or other features that back up data in the system keys
		if (!restoreSystemKeys && !restoreUserKeys && backupKeys.empty() &&
		    programExe != ProgramExe::FASTRESTORE_TOOL) {
			addDefaultBackupRanges(backupKeys);
		}

		if ((restoreSystemKeys || restoreUserKeys) && programExe == ProgramExe::FASTRESTORE_TOOL) {
			fprintf(stderr, "ERROR: Options: --user-data and --system-metadata are not supported with fastrestore\n");
			return FDB_EXIT_ERROR;
		}

		if ((restoreUserKeys || restoreSystemKeys) && !backupKeys.empty()) {
			fprintf(stderr,
			        "ERROR: Cannot specify additional ranges when using --user-data or --system-metadata "
			        "options\n");
			return FDB_EXIT_ERROR;
		}
		if (restoreUserKeys) {
			backupKeys.push_back_deep(backupKeys.arena(), normalKeys);
		} else if (restoreSystemKeys) {
			for (const auto& r : getSystemBackupRanges()) {
				backupKeys.push_back_deep(backupKeys.arena(), r);
			}
		}

		switch (programExe) {
		case ProgramExe::AGENT:
			if (!initCluster())
				return FDB_EXIT_ERROR;
			fileBackupAgentProxy = proxy;
			f = stopAfter(runAgent(db));
			break;
		case ProgramExe::BACKUP:
			switch (backupType) {
			case BackupType::START: {
				if (!initCluster())
					return FDB_EXIT_ERROR;
				// Test out the backup url to make sure it parses.  Doesn't test to make sure it's actually writeable.
				openBackupContainer(argv[0], destinationContainer, proxy, encryptionKeyFile);
				f = stopAfter(submitBackup(db,
				                           destinationContainer,
				                           proxy,
				                           initialSnapshotIntervalSeconds,
				                           snapshotIntervalSeconds,
				                           backupKeys,
				                           encryptionEnabled,
				                           tagName,
				                           dryRun,
				                           waitForDone,
				                           stopWhenDone,
				                           usePartitionedLog,
				                           incrementalBackupOnly,
				                           blobManifestUrl));
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
				f = stopAfter(statusBackup(db, tagName, ShowErrors::True, jsonOutput));
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
				                               proxy,
				                               expireVersion,
				                               expireDatetime,
				                               db,
				                               forceAction,
				                               expireRestorableAfterVersion,
				                               expireRestorableAfterDatetime,
				                               encryptionKeyFile));
				break;

			case BackupType::DELETE_BACKUP:
				initTraceFile();
				f = stopAfter(deleteBackupContainer(argv[0], destinationContainer, proxy));
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
				                             proxy,
				                             describeDeep,
				                             describeTimestamps ? Optional<Database>(db) : Optional<Database>(),
				                             jsonOutput,
				                             encryptionKeyFile));
				break;

			case BackupType::LIST:
				initTraceFile();
				f = stopAfter(listBackup(baseUrl, proxy));
				break;

			case BackupType::TAGS:
				if (!initCluster())
					return FDB_EXIT_ERROR;
				f = stopAfter(listBackupTags(db));
				break;

			case BackupType::QUERY:
				initTraceFile();
				f = stopAfter(queryBackup(argv[0],
				                          destinationContainer,
				                          proxy,
				                          backupKeysFilter,
				                          restoreVersion,
				                          snapshotVersion,
				                          restoreClusterFileOrig,
				                          restoreTimestamp,
				                          Verbose{ !quietDisplay },
				                          db));
				break;

			case BackupType::DUMP:
				initTraceFile();
				f = stopAfter(dumpBackupData(argv[0], destinationContainer, proxy, dumpBegin, dumpEnd));
				break;

			case BackupType::UNDEFINED:
			default:
				fprintf(stderr, "ERROR: Unsupported backup action %s\n", argv[1]);
				printHelpTeaser(argv[0]);
				return FDB_EXIT_ERROR;
				break;
			}

			break;
		case ProgramExe::RESTORE:
			if (dryRun) {
				if (restoreType != RestoreType::START) {
					fprintf(stderr, "Restore dry run only works for 'start' command\n");
					return FDB_EXIT_ERROR;
				}

				// Must explicitly call trace file options handling if not calling Database::createDatabase()
				initTraceFile();
			} else {
				if (restoreClusterFileDest.empty()) {
					fprintf(stderr, "Restore destination cluster file must be specified explicitly.\n");
					return FDB_EXIT_ERROR;
				}

				if (!fileExists(restoreClusterFileDest)) {
					fprintf(stderr,
					        "Restore destination cluster file '%s' does not exist.\n",
					        restoreClusterFileDest.c_str());
					return FDB_EXIT_ERROR;
				}

				try {
					db = Database::createDatabase(restoreClusterFileDest, ApiVersion::LATEST_VERSION);
				} catch (Error& e) {
					fprintf(stderr,
					        "Restore destination cluster file '%s' invalid: %s\n",
					        restoreClusterFileDest.c_str(),
					        e.what());
					return FDB_EXIT_ERROR;
				}
			}

			switch (restoreType) {
			case RestoreType::START:
				f = stopAfter(runRestore(db,
				                         restoreClusterFileOrig,
				                         tagName,
				                         restoreContainer,
				                         proxy,
				                         backupKeys,
				                         beginVersion,
				                         restoreVersion,
				                         restoreTimestamp,
				                         !dryRun,
				                         Verbose{ !quietDisplay },
				                         waitForDone,
				                         addPrefix,
				                         removePrefix,
				                         onlyApplyMutationLogs,
				                         inconsistentSnapshotOnly,
				                         encryptionKeyFile,
				                         blobManifestUrl));

				break;
			case RestoreType::WAIT:
				f = stopAfter(success(ba.waitRestore(db, KeyRef(tagName), Verbose::True)));
				break;
			case RestoreType::ABORT:
				f = stopAfter(
				    map(ba.abortRestore(db, KeyRef(tagName)), [tagName](FileBackupAgent::ERestoreState s) -> Void {
					    printf("RESTORE_ABORT Tag: %s  State: %s\n",
					           tagName.c_str(),
					           FileBackupAgent::restoreStateText(s).toString().c_str());
					    return Void();
				    }));
				break;
			case RestoreType::STATUS:
				// If no tag is specifically provided then print all tag status, don't just use "default"
				if (tagProvided)
					tag = tagName;
				f = stopAfter(map(ba.restoreStatus(db, KeyRef(tag)), [](std::string s) -> Void {
					printf("%s\n", s.c_str());
					return Void();
				}));
				break;
			default:
				throw restore_error();
			}
			break;
		case ProgramExe::FASTRESTORE_TOOL:
			// Support --dest-cluster-file option as fdbrestore does
			if (dryRun) {
				if (restoreType != RestoreType::START) {
					fprintf(stderr, "Restore dry run only works for 'start' command\n");
					return FDB_EXIT_ERROR;
				}

				// Must explicitly call trace file options handling if not calling Database::createDatabase()
				initTraceFile();
			} else {
				if (restoreClusterFileDest.empty()) {
					fprintf(stderr, "Restore destination cluster file must be specified explicitly.\n");
					return FDB_EXIT_ERROR;
				}

				if (!fileExists(restoreClusterFileDest)) {
					fprintf(stderr,
					        "Restore destination cluster file '%s' does not exist.\n",
					        restoreClusterFileDest.c_str());
					return FDB_EXIT_ERROR;
				}

				try {
					db = Database::createDatabase(restoreClusterFileDest, ApiVersion::LATEST_VERSION);
				} catch (Error& e) {
					fprintf(stderr,
					        "Restore destination cluster file '%s' invalid: %s\n",
					        restoreClusterFileDest.c_str(),
					        e.what());
					return FDB_EXIT_ERROR;
				}
			}
			// TODO: We have not implemented the code commented out in this case
			switch (restoreType) {
			case RestoreType::START:
				f = stopAfter(runFastRestoreTool(db,
				                                 tagName,
				                                 restoreContainer,
				                                 proxy,
				                                 backupKeys,
				                                 restoreVersion,
				                                 !dryRun,
				                                 Verbose{ !quietDisplay },
				                                 waitForDone));
				break;
			case RestoreType::WAIT:
				printf("[TODO][ERROR] FastRestore does not support RESTORE_WAIT yet!\n");
				throw restore_error();
				//					f = stopAfter( success(ba.waitRestore(db, KeyRef(tagName), true)) );
				break;
			case RestoreType::ABORT:
				printf("[TODO][ERROR] FastRestore does not support RESTORE_ABORT yet!\n");
				throw restore_error();
				//					f = stopAfter( map(ba.abortRestore(db, KeyRef(tagName)),
				//[tagName](FileBackupAgent::ERestoreState s) -> Void { 						printf("Tag: %s  State:
				//%s\n", tagName.c_str(),
				// FileBackupAgent::restoreStateText(s).toString().c_str()); 						return Void();
				//					}) );
				break;
			case RestoreType::STATUS:
				printf("[TODO][ERROR] FastRestore does not support RESTORE_STATUS yet!\n");
				throw restore_error();
				// If no tag is specifically provided then print all tag status, don't just use "default"
				if (tagProvided)
					tag = tagName;
				//					f = stopAfter( map(ba.restoreStatus(db, KeyRef(tag)), [](std::string s) -> Void {
				//						printf("%s\n", s.c_str());
				//						return Void();
				//					}) );
				break;
			default:
				throw restore_error();
			}
			break;
		case ProgramExe::DR_AGENT:
			if (!initCluster() || !initSourceCluster(true)) {
				return FDB_EXIT_ERROR;
			}
			f = stopAfter(runDBAgent(sourceDb, db));
			break;
		case ProgramExe::DB_BACKUP:
			if (!initCluster() || !initSourceCluster(dbType != DBType::ABORT || !dstOnly)) {
				return FDB_EXIT_ERROR;
			}
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
			std::cout << "Page Counts: " << FastAllocator<16>::pageCount << " " << FastAllocator<32>::pageCount << " "
			          << FastAllocator<64>::pageCount << " " << FastAllocator<128>::pageCount << " "
			          << FastAllocator<256>::pageCount << " " << FastAllocator<512>::pageCount << " "
			          << FastAllocator<1024>::pageCount << " " << FastAllocator<2048>::pageCount << " "
			          << FastAllocator<4096>::pageCount << " " << FastAllocator<8192>::pageCount << " "
			          << FastAllocator<16384>::pageCount << std::endl;

			std::vector<std::pair<std::string, const char*>> typeNames;
			for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
				std::string s;

#ifdef __linux__
				char* demangled = abi::__cxa_demangle(i->first, NULL, NULL, NULL);
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
