#ifndef MAKO_H
#define MAKO_H
#pragma once

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 720
#endif

#include <foundationdb/fdb_c.h>
#include <pthread.h>
#include <sys/types.h>
#include <stdbool.h>
#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <sys/syslimits.h>
#else
#include <limits.h>
#endif

#define VERBOSE_NONE 0
#define VERBOSE_DEFAULT 1
#define VERBOSE_ANNOYING 2
#define VERBOSE_DEBUG 3

#define MODE_INVALID -1
#define MODE_CLEAN 0
#define MODE_BUILD 1
#define MODE_RUN 2

#define FDB_SUCCESS 0
#define FDB_ERROR_RETRY -1
#define FDB_ERROR_ABORT -2
#define FDB_ERROR_CONFLICT -3

#define LAT_BLOCK_SIZE 511 /* size of each block to get detailed latency for each operation */

/* transaction specification */
enum Operations {
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
	OP_TRANSACTION, /* pseudo-operation - cumulative time for the operation + commit */
	OP_READ_BG,
	MAX_OP /* must be the last item */
};

#define OP_COUNT 0
#define OP_RANGE 1
#define OP_REVERSE 2

/* for long arguments */
enum Arguments {
	ARG_KEYLEN,
	ARG_VALLEN,
	ARG_TPS,
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
	ARG_JSON_REPORT,
	ARG_BG_FILE_PATH // if blob granule files are stored locally, mako will read and materialize them if this is set
};

enum TPSChangeTypes { TPS_SIN, TPS_SQUARE, TPS_PULSE };

#define KEYPREFIX "mako"
#define KEYPREFIXLEN 4

#define TEMP_DATA_STORE "/tmp/makoTemp"

/* we set mako_txnspec_t and mako_args_t only once in the master process,
 * and won't be touched by child processes.
 */

typedef struct {
	/* for each operation, it stores "count", "range" and "reverse" */
	int ops[MAX_OP][3];
} mako_txnspec_t;

#define LOGGROUP_MAX 256
#define KNOB_MAX 256
#define TAGPREFIXLENGTH_MAX 8
#define NUM_CLUSTERS_MAX 3
#define NUM_DATABASES_MAX 10
#define MAX_BG_IDS 1000

/* benchmark parameters */
typedef struct {
	int api_version;
	int json;
	int num_processes;
	int num_threads;
	int mode;
	int rows; /* is 2 billion enough? */
	int seconds;
	int iteration;
	int tpsmax;
	int tpsmin;
	int tpsinterval;
	int tpschange;
	int sampling;
	int key_length;
	int value_length;
	int zipf;
	int commit_get;
	int verbose;
	mako_txnspec_t txnspec;
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
	int client_threads_per_version;
	int disable_ryw;
	char json_output_path[PATH_MAX];
	bool bg_materialize_files;
	char bg_file_path[PATH_MAX];
} mako_args_t;

/* shared memory */
#define SIGNAL_RED 0
#define SIGNAL_GREEN 1
#define SIGNAL_OFF 2

typedef struct {
	int signal;
	int readycount;
	double throttle_factor;
	int stopcount;
} mako_shmhdr_t;

/* memory block allocated to each operation when collecting detailed latency */
typedef struct {
	uint64_t data[LAT_BLOCK_SIZE];
	void* next_block;
} lat_block_t;

typedef struct {
	uint64_t xacts;
	uint64_t conflicts;
	uint64_t ops[MAX_OP];
	uint64_t errors[MAX_OP];
	uint64_t latency_samples[MAX_OP];
	uint64_t latency_us_total[MAX_OP];
	uint64_t latency_us_min[MAX_OP];
	uint64_t latency_us_max[MAX_OP];
} mako_stats_t;

/* per-process information */
typedef struct {
	int worker_id;
	pid_t parent_id;
	mako_args_t* args;
	mako_shmhdr_t* shm;
	FDBDatabase* databases[NUM_DATABASES_MAX];
} process_info_t;

/* args for threads */
typedef struct {
	int thread_id;
	int database_index; // index of the database to do work to
	int elem_size[MAX_OP]; /* stores the multiple of LAT_BLOCK_SIZE to check the memory allocation of each operation */
	bool is_memory_allocated[MAX_OP]; /* flag specified for each operation, whether the memory was allocated to that
	                                     specific operation */
	lat_block_t* block[MAX_OP];
	process_info_t* process;
} thread_args_t;

/* process type */
typedef enum { proc_master = 0, proc_worker, proc_stats } proc_type_t;

#endif /* MAKO_H */
