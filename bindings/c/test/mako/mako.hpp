#ifndef MAKO_HPP
#define MAKO_HPP

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 710
#endif

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <list>
#include <vector>
#include <string_view>
#include <fdb.hpp>
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
#include "operations.hpp"
#include "shm.hpp"
#include "stats.hpp"
#include "time.hpp"

namespace mako {

constexpr const int VERBOSE_NONE = 0;
constexpr const int VERBOSE_DEFAULT = 1;
constexpr const int VERBOSE_ANNOYING = 2;
constexpr const int VERBOSE_DEBUG = 3;

constexpr const int MODE_INVALID = -1;
constexpr const int MODE_CLEAN = 0;
constexpr const int MODE_BUILD = 1;
constexpr const int MODE_RUN = 2;

/* for long arguments */
enum ArgKind {
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
constexpr const int MAX_BG_IDS = 1000;

/* benchmark parameters */
struct Arguments {
	int api_version;
	int json;
	int num_processes;
	int num_threads;
	int mode;
	int rows; /* is 2 billion enough? */
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
	int disable_ryw;
	char json_output_path[PATH_MAX];
	bool bg_materialize_files;
	char bg_file_path[PATH_MAX];
};

/* shared memory */
constexpr const int SIGNAL_RED = 0;
constexpr const int SIGNAL_GREEN = 1;
constexpr const int SIGNAL_OFF = 2;

/* args for threads */
struct alignas(64) ThreadArgs {
	int worker_id;
	int thread_id;
	pid_t parent_id;
	LatencySampleBinArray sample_bins;
	Arguments const* args;
	shared_memory::Access shm;
	fdb::Database database; // database to work with
};

/* process type */
typedef enum { PROC_MASTER = 0, PROC_WORKER, PROC_STATS } ProcKind;

} // namespace mako

#endif /* MAKO_HPP */
