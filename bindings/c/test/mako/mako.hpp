/*
 * mako.hpp
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

#ifndef MAKO_HPP
#define MAKO_HPP

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 730
#endif

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <list>
#include <map>
#include <vector>
#include <string_view>
#include <fdb_api.hpp>
#include <pthread.h>
#include <sys/types.h>
#include <stdbool.h>
#include "limit.hpp"

namespace mako {

constexpr const int MODE_INVALID = -1;
constexpr const int MODE_CLEAN = 0;
constexpr const int MODE_BUILD = 1;
constexpr const int MODE_RUN = 2;
constexpr const int MODE_REPORT = 3;

/* for long arguments */
enum ArgKind {
	ARG_KEYLEN,
	ARG_VALLEN,
	ARG_ACTIVE_TENANTS,
	ARG_TOTAL_TENANTS,
	ARG_TENANT_BATCH_SIZE,
	ARG_TPS,
	ARG_ASYNC,
	ARG_COMMITGET,
	ARG_SAMPLING,
	ARG_VERSION,
	ARG_KNOBS,
	ARG_FLATBUFFERS,
	ARG_LOGGROUP,
	ARG_PREFIXPADDING,
	ARG_TRACE,
	ARG_TRACEPATH,
	ARG_TRACEFORMAT,
	ARG_TPSMAX,
	ARG_TPSMIN,
	ARG_TPSINTERVAL,
	ARG_TPSCHANGE,
	ARG_TXNTRACE,
	ARG_TXNTAGGING,
	ARG_TXNTAGGINGPREFIX,
	ARG_STREAMING_MODE,
	ARG_DISABLE_RYW,
	ARG_CLIENT_THREADS_PER_VERSION,
	ARG_DISABLE_CLIENT_BYPASS,
	ARG_JSON_REPORT,
	ARG_BG_FILE_PATH, // if blob granule files are stored locally, mako will read and materialize them if this is set
	ARG_EXPORT_PATH,
	ARG_DISTRIBUTED_TRACER_CLIENT,
	ARG_TLS_CERTIFICATE_FILE,
	ARG_TLS_KEY_FILE,
	ARG_TLS_CA_FILE,
	ARG_AUTHORIZATION_TOKEN_FILE,
	ARG_TRANSACTION_TIMEOUT_TX,
	ARG_TRANSACTION_TIMEOUT_DB,
};

constexpr const int OP_COUNT = 0;
constexpr const int OP_RANGE = 1;
constexpr const int OP_REVERSE = 2;

/* transaction specification */
enum OpKind {
	OP_GETREADVERSION,
	OP_GET,
	OP_GETRANGE,
	OP_SGET,
	OP_SGETRANGE,
	OP_UPDATE,
	OP_INSERT,
	OP_INSERTRANGE,
	OP_OVERWRITE,
	OP_CLEAR,
	OP_SETCLEAR,
	OP_CLEARRANGE,
	OP_SETCLEARRANGE,
	OP_COMMIT,
	OP_TRANSACTION, /* pseudo-operation - time it takes to run one iteration of ops sequence */
	OP_READ_BG,
	MAX_OP /* must be the last item */
};

enum TPSChangeTypes { TPS_SIN, TPS_SQUARE, TPS_PULSE };

enum DistributedTracerClient { DISABLED, NETWORK_LOSSY, LOG_FILE };

/* we set WorkloadSpec and Arguments only once in the master process,
 * and won't be touched by child processes.
 */

struct WorkloadSpec {
	/* for each operation, it stores "count", "range" and "reverse" */
	int ops[MAX_OP][3];
};

constexpr const int LOGGROUP_MAX = 256;
constexpr const int KNOB_MAX = 256;
constexpr const int TAGPREFIXLENGTH_MAX = 8;
constexpr const int NUM_CLUSTERS_MAX = 3;
constexpr const int NUM_DATABASES_MAX = 10;
constexpr const std::string_view KEY_PREFIX{ "mako" };
constexpr const std::string_view TEMP_DATA_STORE{ "/tmp/makoTemp" };
constexpr const int MAX_REPORT_FILES = 200;

/* benchmark parameters */
struct Arguments {
	Arguments();
	int validate();
	bool isAnyTimeoutEnabled() const;

	int api_version;
	int json;
	int num_processes;
	int num_threads;
	int async_xacts;
	int mode;
	int rows; /* is 2 billion enough? */
	double load_factor;
	int row_digits;
	int seconds;
	int iteration;
	int tpsmax;
	int tpsmin;
	int tpsinterval;
	int tpschange;
	int sampling;
	int key_length;
	int value_length;
	int active_tenants;
	int total_tenants;
	int tenant_batch_size;
	int zipf;
	int commit_get;
	int verbose;
	WorkloadSpec txnspec;
	char cluster_files[NUM_CLUSTERS_MAX][PATH_MAX];
	int num_fdb_clusters;
	int num_databases;
	char log_group[LOGGROUP_MAX];
	int prefixpadding;
	int trace;
	char tracepath[PATH_MAX];
	int traceformat; /* 0 - XML, 1 - JSON */
	char knobs[KNOB_MAX];
	uint8_t flatbuffers;
	int txntrace;
	int txntagging;
	char txntagging_prefix[TAGPREFIXLENGTH_MAX];
	FDBStreamingMode streaming_mode;
	int64_t client_threads_per_version;
	bool disable_client_bypass;
	int disable_ryw;
	char json_output_path[PATH_MAX];
	bool bg_materialize_files;
	char bg_file_path[PATH_MAX];
	char stats_export_path[PATH_MAX];
	char report_files[MAX_REPORT_FILES][PATH_MAX];
	int num_report_files;
	int distributed_tracer_client;
	std::optional<std::string> tls_certificate_file;
	std::optional<std::string> tls_key_file;
	std::optional<std::string> tls_ca_file;
	std::map<std::string, std::string> authorization_tokens; // maps tenant name to token string
	int transaction_timeout_db;
	int transaction_timeout_tx;
};

// helper functions
inline void setTransactionTimeoutIfEnabled(const Arguments& args, fdb::Transaction& tx) {
	if (args.transaction_timeout_tx > 0) {
		tx.setOption(FDB_TR_OPTION_TIMEOUT, args.transaction_timeout_tx);
	}
}

} // namespace mako

#endif /* MAKO_HPP */
