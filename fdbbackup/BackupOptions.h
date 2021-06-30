/*
 * BackupOptions.h
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

#pragma once

enum {
	// Backup options
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

	// Backup and Restore options
	OPT_TAGNAME,
	OPT_BACKUPKEYS,
	OPT_WAITFORDONE,
	OPT_BACKUPKEYS_FILTER,
	OPT_INCREMENTALONLY,

	// Backup Modify options
	OPT_MOD_ACTIVE_INTERVAL,
	OPT_MOD_VERIFY_UID,

	// Restore options
	OPT_RESTORECONTAINER,
	OPT_RESTORE_VERSION,
	OPT_RESTORE_TIMESTAMP,
	OPT_PREFIX_ADD,
	OPT_PREFIX_REMOVE,
	OPT_RESTORE_CLUSTERFILE_DEST,
	OPT_RESTORE_CLUSTERFILE_ORIG,
	OPT_RESTORE_BEGIN_VERSION,
	OPT_RESTORE_INCONSISTENT_SNAPSHOT_ONLY,

	// Shared options
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
	OPT_LOCALITY,
	OPT_TRACE_FORMAT,

	// DB options
	OPT_SOURCE_CLUSTER,
	OPT_DEST_CLUSTER,
	OPT_CLEANUP,
	OPT_DSTONLY,
};

// Top level binary commands.
CSimpleOpt::SOption g_rgOptions[] = { { OPT_VERSION, "-v", SO_NONE },
	                                  { OPT_VERSION, "--version", SO_NONE },
	                                  { OPT_BUILD_FLAGS, "--build_flags", SO_NONE },
	                                  { OPT_HELP, "-?", SO_NONE },
	                                  { OPT_HELP, "-h", SO_NONE },
	                                  { OPT_HELP, "--help", SO_NONE },

	                                  SO_END_OF_OPTIONS };
