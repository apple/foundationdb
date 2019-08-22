#ifndef MAKO_H
#define MAKO_H
#pragma once

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 620
#endif

#include <foundationdb/fdb_c.h>
#include <pthread.h>
#include <sys/types.h>
#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <sys/syslimits.h>
#else
#include <limits.h>
#endif

#define DEFAULT_RETRY_COUNT 3

#define VERBOSE_NONE 0
#define VERBOSE_DEFAULT 1
#define VERBOSE_ANNOYING 2
#define VERBOSE_DEBUG 3

#define MODE_INVALID -1
#define MODE_CLEAN 0
#define MODE_BUILD 1
#define MODE_RUN 2

/* we set mako_txn_t and mako_args_t only once in the master process,
 * and won't be touched by child processes.
 */

/* transaction specification */
#define OP_GETREADVERSION 0
#define OP_GET 1
#define OP_GETRANGE 2
#define OP_SGET 3
#define OP_SGETRANGE 4
#define OP_UPDATE 5
#define OP_INSERT 6
#define OP_INSERTRANGE 7
#define OP_CLEAR 8
#define OP_SETCLEAR 9
#define OP_CLEARRANGE 10
#define OP_SETCLEARRANGE 11
#define OP_COMMIT 12
#define MAX_OP 13 /* update this when adding a new operation */

#define OP_COUNT 0
#define OP_RANGE 1
#define OP_REVERSE 2

/* for arguments */
#define ARG_KEYLEN 1
#define ARG_VALLEN 2
#define ARG_TPS 3
#define ARG_COMMITGET 4
#define ARG_SAMPLING 5
#define ARG_VERSION 6
#define ARG_KNOBS 7
#define ARG_FLATBUFFERS 8
#define ARG_TRACE 9
#define ARG_TRACEPATH 10

#define KEYPREFIX "mako"
#define KEYPREFIXLEN 4

typedef struct {
  /* for each operation, it stores "count", "range" and "reverse" */
  int ops[MAX_OP][3];
} mako_txnspec_t;

#define KNOB_MAX 256

/* benchmark parameters */
typedef struct {
  int json;
  int num_processes;
  int num_threads;
  int mode;
  int rows; /* is 2 billion enough? */
  int seconds;
  int iteration;
  int tps;
  int sampling;
  int key_length;
  int value_length;
  int zipf;
  int commit_get;
  int verbose;
  mako_txnspec_t txnspec;
  char cluster_file[PATH_MAX];
  int trace;
  char tracepath[PATH_MAX];
  char knobs[KNOB_MAX];
  uint8_t flatbuffers;
} mako_args_t;

/* shared memory */
#define SIGNAL_RED 0
#define SIGNAL_GREEN 1
#define SIGNAL_OFF 2

typedef struct {
  int signal;
  int readycount;
} mako_shmhdr_t;

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
  FDBDatabase *database;
  mako_args_t *args;
  mako_shmhdr_t *shm;
} process_info_t;

/* args for threads */
typedef struct {
  int thread_id;
  process_info_t *process;
} thread_args_t;

/* process type */
typedef enum { proc_master = 0, proc_worker, proc_stats } proc_type_t;

#endif /* MAKO_H */
