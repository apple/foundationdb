#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__FreeBSD__)
#include <sys/stat.h>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC_FAST
#elif defined(__APPLE__)
#include <sys/syslimits.h>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#else
#include <limits.h>
#endif

#include "fdbclient/zipf.h"
#include "mako.h"
#include "utils.h"

/* global variables */
FILE* printme; /* descriptor used for default messages */
FILE* annoyme; /* descriptor used for annoying messages */
FILE* debugme; /* descriptor used for debug messages */

#define check_fdb_error(_e)                                                                                            \
	do {                                                                                                               \
		if (_e) {                                                                                                      \
			fprintf(stderr, "ERROR: Failed at %s:%d (%s)\n", __FILE__, __LINE__, fdb_get_error(_e));                   \
			goto failExit;                                                                                             \
		}                                                                                                              \
	} while (0)

#define fdb_block_wait(_f)                                                                                             \
	do {                                                                                                               \
		if ((fdb_future_block_until_ready(_f)) != 0) {                                                                 \
			fprintf(stderr, "ERROR: fdb_future_block_until_ready failed at %s:%d\n", __FILE__, __LINE__);              \
			goto failExit;                                                                                             \
		}                                                                                                              \
	} while (0)

#define fdb_wait_and_handle_error(_func, _f, _t)                                                                       \
	do {                                                                                                               \
		int err = wait_future(_f);                                                                                     \
		if (err) {                                                                                                     \
			int err2;                                                                                                  \
			if ((err != 1020 /* not_committed */) && (err != 1021 /* commit_unknown_result */) &&                      \
			    (err != 1213 /* tag_throttled */)) {                                                                   \
				fprintf(stderr, "ERROR: Error %s (%d) occured at %s\n", #_func, err, fdb_get_error(err));              \
			} else {                                                                                                   \
				fprintf(annoyme, "ERROR: Error %s (%d) occured at %s\n", #_func, err, fdb_get_error(err));             \
			}                                                                                                          \
			fdb_future_destroy(_f);                                                                                    \
			_f = fdb_transaction_on_error(_t, err);                                                                    \
			/* this will return the original error for non-retryable errors */                                         \
			err2 = wait_future(_f);                                                                                    \
			fdb_future_destroy(_f);                                                                                    \
			if (err2) {                                                                                                \
				/* unretryable error */                                                                                \
				fprintf(stderr, "ERROR: fdb_transaction_on_error returned %d at %s:%d\n", err2, __FILE__, __LINE__);   \
				fdb_transaction_reset(_t);                                                                             \
				/* TODO: if we add a retry limit in the future,                                                        \
				 *       handle the conflict stats properly.                                                           \
				 */                                                                                                    \
				return FDB_ERROR_ABORT;                                                                                \
			}                                                                                                          \
			if (err == 1020 /* not_committed */) {                                                                     \
				return FDB_ERROR_CONFLICT;                                                                             \
			}                                                                                                          \
			return FDB_ERROR_RETRY;                                                                                    \
		}                                                                                                              \
	} while (0)

#define fdb_handle_result_error(_func, err, _t)                                                                        \
	do {                                                                                                               \
		if (err) {                                                                                                     \
			int err2;                                                                                                  \
			FDBFuture* fErr;                                                                                           \
			if ((err != 1020 /* not_committed */) && (err != 1021 /* commit_unknown_result */) &&                      \
			    (err != 1213 /* tag_throttled */)) {                                                                   \
				fprintf(stderr, "ERROR: Error %s (%d) occured at %s\n", #_func, err, fdb_get_error(err));              \
			} else {                                                                                                   \
				fprintf(annoyme, "ERROR: Error %s (%d) occured at %s\n", #_func, err, fdb_get_error(err));             \
			}                                                                                                          \
			fErr = fdb_transaction_on_error(_t, err);                                                                  \
			/* this will return the original error for non-retryable errors */                                         \
			err2 = wait_future(fErr);                                                                                  \
			fdb_future_destroy(fErr);                                                                                  \
			if (err2) {                                                                                                \
				/* unretryable error */                                                                                \
				fprintf(stderr, "ERROR: fdb_transaction_on_error returned %d at %s:%d\n", err2, __FILE__, __LINE__);   \
				fdb_transaction_reset(_t);                                                                             \
				/* TODO: if we add a retry limit in the future,                                                        \
				 *       handle the conflict stats properly.                                                           \
				 */                                                                                                    \
				return FDB_ERROR_ABORT;                                                                                \
			}                                                                                                          \
			if (err == 1020 /* not_committed */) {                                                                     \
				return FDB_ERROR_CONFLICT;                                                                             \
			}                                                                                                          \
			return FDB_ERROR_RETRY;                                                                                    \
		}                                                                                                              \
	} while (0)

fdb_error_t wait_future(FDBFuture* f) {
	fdb_error_t err;

	err = fdb_future_block_until_ready(f);
	if (err) {
		return err; /* error from fdb_future_block_until_ready() */
	}
	return fdb_future_get_error(f);
}

int commit_transaction(FDBTransaction* transaction) {
	FDBFuture* f;

	f = fdb_transaction_commit(transaction);
	fdb_wait_and_handle_error(commit_transaction, f, transaction);
	fdb_future_destroy(f);

	return FDB_SUCCESS;
}

void update_op_lat_stats(struct timespec* start,
                         struct timespec* end,
                         int op,
                         mako_stats_t* stats,
                         lat_block_t* block[],
                         int* elem_size,
                         bool* is_memory_allocated) {
	uint64_t latencyus;

	latencyus = (((uint64_t)end->tv_sec * 1000000000 + end->tv_nsec) -
	             ((uint64_t)start->tv_sec * 1000000000 + start->tv_nsec)) /
	            1000;
	stats->latency_samples[op]++;
	stats->latency_us_total[op] += latencyus;
	if (latencyus < stats->latency_us_min[op]) {
		stats->latency_us_min[op] = latencyus;
	}
	if (latencyus > stats->latency_us_max[op]) {
		stats->latency_us_max[op] = latencyus;
	}
	if (!is_memory_allocated[op])
		return;
	if (elem_size[op] < stats->latency_samples[op]) {
		elem_size[op] = elem_size[op] + LAT_BLOCK_SIZE;
		lat_block_t* temp_block = (lat_block_t*)malloc(sizeof(lat_block_t));
		if (temp_block == NULL) {
			is_memory_allocated[op] = false;
			return;
		}
		temp_block->next_block = NULL;
		block[op]->next_block = (lat_block_t*)temp_block;
		block[op] = temp_block;
	}
	block[op]->data[(stats->latency_samples[op] - 1) % LAT_BLOCK_SIZE] = latencyus;
}

/* FDB network thread */
void* fdb_network_thread(void* args) {
	fdb_error_t err;

	fprintf(debugme, "DEBUG: fdb_network_thread started\n");

	err = fdb_run_network();
	if (err) {
		fprintf(stderr, "ERROR: fdb_run_network: %s\n", fdb_get_error(err));
	}

	return 0;
}

int genprefix(char* str, char* prefix, int prefixlen, int prefixpadding, int rows, int len) {
	const int rowdigit = digits(rows);
	const int paddinglen = len - (prefixlen + rowdigit) - 1;
	int offset = 0;
	if (prefixpadding) {
		memset(str, 'x', paddinglen);
		offset += paddinglen;
	}
	memcpy(str + offset, prefix, prefixlen);
	str[len - 1] = '\0';
	return offset + prefixlen;
}

/* cleanup database */
int cleanup(FDBTransaction* transaction, mako_args_t* args) {
	struct timespec timer_start, timer_end;
	char* prefixstr = (char*)malloc(sizeof(char) * args->key_length + 1);
	if (!prefixstr)
		return -1;
	char* beginstr = (char*)malloc(sizeof(char) * args->key_length + 1);
	if (!beginstr) {
		free(prefixstr);
		return -1;
	}
	char* endstr = (char*)malloc(sizeof(char) * args->key_length + 1);
	if (!endstr) {
		free(prefixstr);
		free(beginstr);
		return -1;
	}

	int len = genprefix(prefixstr, KEYPREFIX, KEYPREFIXLEN, args->prefixpadding, args->rows, args->key_length + 1);
	snprintf(beginstr, len + 2, "%s%c", prefixstr, 0x00);
	snprintf(endstr, len + 2, "%s%c", prefixstr, 0xff);
	free(prefixstr);
	len += 1;

retryTxn:
	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_start);

	fdb_transaction_clear_range(transaction, (uint8_t*)beginstr, len + 1, (uint8_t*)endstr, len + 1);
	switch (commit_transaction(transaction)) {
	case (FDB_SUCCESS):
		break;
	case (FDB_ERROR_RETRY):
		fdb_transaction_reset(transaction);
		goto retryTxn;
	default:
		goto failExit;
	}

	fdb_transaction_reset(transaction);
	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_end);
	fprintf(printme,
	        "INFO: Clear range: %6.3f sec\n",
	        ((timer_end.tv_sec - timer_start.tv_sec) * 1000000000.0 + timer_end.tv_nsec - timer_start.tv_nsec) /
	            1000000000);

	free(beginstr);
	free(endstr);

	return 0;

failExit:
	free(beginstr);
	free(endstr);

	fprintf(stderr, "ERROR: FDB failure in cleanup()\n");
	return -1;
}

/* populate database */
int populate(FDBTransaction* transaction,
             mako_args_t* args,
             int worker_id,
             int thread_id,
             int thread_tps,
             mako_stats_t* stats,
             lat_block_t* block[],
             int* elem_size,
             bool* is_memory_allocated) {
	int i;
	struct timespec timer_start, timer_end;
	struct timespec timer_prev, timer_now; /* for throttling */
	struct timespec timer_per_xact_start, timer_per_xact_end;
	struct timespec timer_start_commit;
	char* keystr;
	char* valstr;

	int begin = insert_begin(args->rows, worker_id, thread_id, args->num_processes, args->num_threads);
	int end = insert_end(args->rows, worker_id, thread_id, args->num_processes, args->num_threads);
	int xacts = 0;
	int tracetimer = 0;

	keystr = (char*)malloc(sizeof(char) * args->key_length + 1);
	if (!keystr)
		return -1;
	valstr = (char*)malloc(sizeof(char) * args->value_length + 1);
	if (!valstr) {
		free(keystr);
		return -1;
	}

	clock_gettime(CLOCK_MONOTONIC, &timer_start);
	timer_prev.tv_sec = timer_start.tv_sec;
	timer_prev.tv_nsec = timer_start.tv_nsec;
	timer_per_xact_start.tv_sec = timer_start.tv_sec;
	timer_per_xact_start.tv_nsec = timer_start.tv_nsec;

	for (i = begin; i <= end; i++) {

		/* sequential keys */
		genkey(keystr, KEYPREFIX, KEYPREFIXLEN, args->prefixpadding, i, args->rows, args->key_length + 1);
		/* random values */
		randstr(valstr, args->value_length + 1);

		if (((thread_tps > 0) && (xacts >= thread_tps)) /* throttle */ || (args->txntrace) /* txn tracing */) {

		throttle:
			clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
			if ((timer_now.tv_sec > timer_prev.tv_sec + 1) ||
			    ((timer_now.tv_sec == timer_prev.tv_sec + 1) && (timer_now.tv_nsec > timer_prev.tv_nsec))) {
				/* more than 1 second passed, no need to throttle */
				xacts = 0;
				timer_prev.tv_sec = timer_now.tv_sec;
				timer_prev.tv_nsec = timer_now.tv_nsec;

				/* enable transaction tracing */
				if (args->txntrace) {
					tracetimer++;
					if (tracetimer == args->txntrace) {
						fdb_error_t err;
						tracetimer = 0;
						fprintf(debugme, "DEBUG: txn tracing %s\n", keystr);
						err = fdb_transaction_set_option(
						    transaction, FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, (uint8_t*)keystr, strlen(keystr));
						if (err) {
							fprintf(
							    stderr,
							    "ERROR: fdb_transaction_set_option(FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER): %s\n",
							    fdb_get_error(err));
						}
						err = fdb_transaction_set_option(transaction, FDB_TR_OPTION_LOG_TRANSACTION, (uint8_t*)NULL, 0);
						if (err) {
							fprintf(stderr,
							        "ERROR: fdb_transaction_set_option(FDB_TR_OPTION_LOG_TRANSACTION): %s\n",
							        fdb_get_error(err));
						}
					}
				}
			} else {
				if (thread_tps > 0) {
					/* 1 second not passed, throttle */
					usleep(1000); /* sleep for 1ms */
					goto throttle;
				}
			}
		} /* throttle or txntrace */

		/* insert (SET) */
		fdb_transaction_set(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)valstr, strlen(valstr));
		stats->ops[OP_INSERT]++;

		/* commit every 100 inserts (default) */
		if (i % args->txnspec.ops[OP_INSERT][OP_COUNT] == 0) {
		retryTxn:
			if (stats->xacts % args->sampling == 0) {
				clock_gettime(CLOCK_MONOTONIC, &timer_start_commit);
			}

			switch (commit_transaction(transaction)) {
			case (FDB_SUCCESS):
				break;
			case (FDB_ERROR_RETRY):
				goto retryTxn;
			default:
				goto failExit;
			}

			/* xact latency stats */
			if (stats->xacts % args->sampling == 0) {
				clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
				update_op_lat_stats(
				    &timer_start_commit, &timer_per_xact_end, OP_COMMIT, stats, block, elem_size, is_memory_allocated);
				update_op_lat_stats(&timer_per_xact_start,
				                    &timer_per_xact_end,
				                    OP_TRANSACTION,
				                    stats,
				                    block,
				                    elem_size,
				                    is_memory_allocated);
			}

			stats->ops[OP_COMMIT]++;
			stats->ops[OP_TRANSACTION]++;
			clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_start);

			fdb_transaction_reset(transaction);
			stats->xacts++;
			xacts++; /* for throttling */
		}
	}
	time_t start_time_sec, current_time_sec;
	time(&start_time_sec);
	int is_committed = false;
	// will hit FDB_ERROR_RETRY if running mako with multi-version client
	while (!is_committed) {
		if (stats->xacts % args->sampling == 0) {
			clock_gettime(CLOCK_MONOTONIC, &timer_start_commit);
		}
		int rc;
		if ((rc = commit_transaction(transaction) != FDB_SUCCESS)) {
			if (rc == FDB_ERROR_RETRY) {
				time(&current_time_sec);
				if (difftime(current_time_sec, start_time_sec) > 5) {
					goto failExit;
				} else {
					continue;
				}
			} else {
				goto failExit;
			}
		}
		is_committed = true;
		/* xact latency stats */
		if (stats->xacts % args->sampling == 0) {
			clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
			update_op_lat_stats(
			    &timer_start_commit, &timer_per_xact_end, OP_COMMIT, stats, block, elem_size, is_memory_allocated);
			update_op_lat_stats(&timer_per_xact_start,
			                    &timer_per_xact_end,
			                    OP_TRANSACTION,
			                    stats,
			                    block,
			                    elem_size,
			                    is_memory_allocated);
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &timer_end);
	stats->xacts++;

	fprintf(debugme,
	        "DEBUG: Populated %d rows (%d-%d): %6.3f sec\n",
	        end - begin,
	        begin,
	        end,
	        ((timer_end.tv_sec - timer_start.tv_sec) * 1000000000.0 + timer_end.tv_nsec - timer_start.tv_nsec) /
	            1000000000);

	free(keystr);
	free(valstr);
	return 0;

failExit:
	if (keystr)
		free(keystr);
	if (valstr)
		free(valstr);
	fprintf(stderr, "ERROR: FDB failure in populate()\n");
	return -1;
}

int64_t run_op_getreadversion(FDBTransaction* transaction, int64_t* rv) {
	FDBFuture* f;
	fdb_error_t err;

	*rv = 0;

	f = fdb_transaction_get_read_version(transaction);
	fdb_wait_and_handle_error(fdb_transaction_get_read_version, f, transaction);

#if FDB_API_VERSION < 620
	err = fdb_future_get_version(f, rv);
#else
	err = fdb_future_get_int64(f, rv);
#endif

	fdb_future_destroy(f);
	if (err) {
#if FDB_API_VERSION < 620
		fprintf(stderr, "ERROR: fdb_future_get_version: %s\n", fdb_get_error(err));
#else
		fprintf(stderr, "ERROR: fdb_future_get_int64: %s\n", fdb_get_error(err));
#endif
		return FDB_ERROR_RETRY;
	}

	/* fail if rv not properly set */
	if (!*rv) {
		return FDB_ERROR_RETRY;
	}
	return FDB_SUCCESS;
}

int run_op_get(FDBTransaction* transaction, char* keystr, char* valstr, int snapshot) {
	FDBFuture* f;
	int out_present;
	char* val;
	int vallen;
	fdb_error_t err;

	f = fdb_transaction_get(transaction, (uint8_t*)keystr, strlen(keystr), snapshot);
	fdb_wait_and_handle_error(fdb_transaction_get, f, transaction);

	err = fdb_future_get_value(f, &out_present, (const uint8_t**)&val, &vallen);
	fdb_future_destroy(f);
	if (err || !out_present) {
		/* error or value not present */
		return FDB_ERROR_RETRY;
	}
	strncpy(valstr, val, vallen);
	valstr[vallen] = '\0';
	return FDB_SUCCESS;
}

int run_op_getrange(FDBTransaction* transaction,
                    char* keystr,
                    char* keystr2,
                    char* valstr,
                    int snapshot,
                    int reverse,
                    FDBStreamingMode streaming_mode) {
	FDBFuture* f;
	fdb_error_t err;
	FDBKeyValue const* out_kv;
	int out_count;
	int out_more;

	f = fdb_transaction_get_range(transaction,
	                              FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)keystr, strlen(keystr)),
	                              FDB_KEYSEL_LAST_LESS_OR_EQUAL((uint8_t*)keystr2, strlen(keystr2)) + 1,
	                              0 /* limit */,
	                              0 /* target_bytes */,
	                              streaming_mode /* FDBStreamingMode */,
	                              0 /* iteration */,
	                              snapshot,
	                              reverse /* reverse */);
	fdb_wait_and_handle_error(fdb_transaction_get_range, f, transaction);

	err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
	if (err) {
		fprintf(stderr, "ERROR: fdb_future_get_keyvalue_array: %s\n", fdb_get_error(err));
		fdb_future_destroy(f);
		return FDB_ERROR_RETRY;
	}
	fdb_future_destroy(f);
	return FDB_SUCCESS;
}

/* Update -- GET and SET the same key */
int run_op_update(FDBTransaction* transaction, char* keystr, char* valstr) {
	FDBFuture* f;
	int out_present;
	char* val;
	int vallen;
	fdb_error_t err;

	/* GET first */
	f = fdb_transaction_get(transaction, (uint8_t*)keystr, strlen(keystr), 0);
	fdb_wait_and_handle_error(fdb_transaction_get, f, transaction);

	err = fdb_future_get_value(f, &out_present, (const uint8_t**)&val, &vallen);
	fdb_future_destroy(f);
	if (err || !out_present) {
		/* error or value not present */
		return FDB_ERROR_RETRY;
	}

	/* Update Value (SET) */
	fdb_transaction_set(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)valstr, strlen(valstr));
	return FDB_SUCCESS;
}

int run_op_insert(FDBTransaction* transaction, char* keystr, char* valstr) {
	fdb_transaction_set(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)valstr, strlen(valstr));
	return FDB_SUCCESS;
}

int run_op_clear(FDBTransaction* transaction, char* keystr) {
	fdb_transaction_clear(transaction, (uint8_t*)keystr, strlen(keystr));
	return FDB_SUCCESS;
}

int run_op_clearrange(FDBTransaction* transaction, char* keystr, char* keystr2) {
	fdb_transaction_clear_range(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)keystr2, strlen(keystr2));
	return FDB_SUCCESS;
}

// TODO: could always abstract this into something more generically usable by something other than mako.
// But outside of testing there are likely few use cases for local granules
typedef struct {
	char* bgFilePath;
	int nextId;
	uint8_t** data_by_id;
} BGLocalFileContext;

int64_t granule_start_load(const char* filename,
                           int filenameLength,
                           int64_t offset,
                           int64_t length,
                           int64_t fullFileLength,
                           void* userContext) {
	FILE* fp;
	char full_fname[PATH_MAX];
	int loadId;
	uint8_t* data;
	size_t readSize;

	BGLocalFileContext* context = (BGLocalFileContext*)userContext;

	loadId = context->nextId;
	if (context->data_by_id[loadId] != 0) {
		fprintf(stderr, "ERROR: too many granule file loads at once: %d\n", MAX_BG_IDS);
		return -1;
	}
	context->nextId = (context->nextId + 1) % MAX_BG_IDS;

	int ret = snprintf(full_fname, PATH_MAX, "%s%s", context->bgFilePath, filename);
	if (ret < 0 || ret >= PATH_MAX) {
		fprintf(stderr, "ERROR: BG filename too long: %s%s\n", context->bgFilePath, filename);
		return -1;
	}

	fp = fopen(full_fname, "r");
	if (!fp) {
		fprintf(stderr, "ERROR: BG could not open file: %s\n", full_fname);
		return -1;
	}

	// don't seek if offset == 0
	if (offset && fseek(fp, offset, SEEK_SET)) {
		// if fseek was non-zero, it failed
		fprintf(stderr, "ERROR: BG could not seek to %" PRId64 " in file %s\n", offset, full_fname);
		fclose(fp);
		return -1;
	}

	data = (uint8_t*)malloc(length);
	readSize = fread(data, sizeof(uint8_t), length, fp);
	fclose(fp);

	if (readSize != length) {
		fprintf(stderr, "ERROR: BG could not read %" PRId64 " bytes from file: %s\n", length, full_fname);
		return -1;
	}

	context->data_by_id[loadId] = data;
	return loadId;
}

uint8_t* granule_get_load(int64_t loadId, void* userContext) {
	BGLocalFileContext* context = (BGLocalFileContext*)userContext;
	if (context->data_by_id[loadId] == 0) {
		fprintf(stderr, "ERROR: BG loadId invalid for get_load: %" PRId64 "\n", loadId);
		return 0;
	}
	return context->data_by_id[loadId];
}

void granule_free_load(int64_t loadId, void* userContext) {
	BGLocalFileContext* context = (BGLocalFileContext*)userContext;
	if (context->data_by_id[loadId] == 0) {
		fprintf(stderr, "ERROR: BG loadId invalid for free_load: %" PRId64 "\n", loadId);
	}
	free(context->data_by_id[loadId]);
	context->data_by_id[loadId] = 0;
}

int run_op_read_blob_granules(FDBTransaction* transaction,
                              char* keystr,
                              char* keystr2,
                              bool doMaterialize,
                              char* bgFilePath) {
	FDBResult* r;
	fdb_error_t err;
	FDBKeyValue const* out_kv;
	int out_count;
	int out_more;

	err = fdb_transaction_set_option(transaction, FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, (uint8_t*)NULL, 0);
	if (err) {
		fprintf(stderr, "ERROR: FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE: %s\n", fdb_get_error(err));
		return FDB_ERROR_RETRY;
	}

	// Allocate a separate context per call to avoid multiple threads accessing
	BGLocalFileContext fileContext;
	fileContext.bgFilePath = bgFilePath;
	fileContext.nextId = 0;
	fileContext.data_by_id = (uint8_t**)malloc(MAX_BG_IDS * sizeof(uint8_t*));
	memset(fileContext.data_by_id, 0, MAX_BG_IDS * sizeof(uint8_t*));

	FDBReadBlobGranuleContext granuleContext;
	granuleContext.userContext = &fileContext;
	granuleContext.start_load_f = &granule_start_load;
	granuleContext.get_load_f = &granule_get_load;
	granuleContext.free_load_f = &granule_free_load;
	granuleContext.debugNoMaterialize = !doMaterialize;
	granuleContext.granuleParallelism = 2; // TODO make knob or setting for changing this?

	r = fdb_transaction_read_blob_granules(transaction,
	                                       (uint8_t*)keystr,
	                                       strlen(keystr),
	                                       (uint8_t*)keystr2,
	                                       strlen(keystr2),
	                                       0 /* beginVersion*/,
	                                       -2, /* endVersion. -2 (latestVersion) is use txn read version */
	                                       granuleContext);

	free(fileContext.data_by_id);

	err = fdb_result_get_keyvalue_array(r, &out_kv, &out_count, &out_more);

	if (err) {
		if (err != 2037 /* blob_granule_not_materialized */) {
			fdb_handle_result_error(fdb_transaction_read_blob_granules, err, transaction);
		} else {
			fdb_result_destroy(r);
			return FDB_SUCCESS;
		}
	}

	fdb_result_destroy(r);

	return FDB_SUCCESS;
}

/* run one transaction */
int run_one_transaction(FDBTransaction* transaction,
                        mako_args_t* args,
                        mako_stats_t* stats,
                        char* keystr,
                        char* keystr2,
                        char* valstr,
                        lat_block_t* block[],
                        int* elem_size,
                        bool* is_memory_allocated) {
	int i;
	int count;
	int rc;
	struct timespec timer_start, timer_end;
	struct timespec timer_per_xact_start, timer_per_xact_end;
	struct timespec timer_start_commit;
	int docommit = 0;
	int keynum;
	int keyend;
	int64_t readversion;
	int randstrlen;
	int rangei;

#if 0 /* this call conflicts with debug transaction */
  /* make sure that the transaction object is clean */
  fdb_transaction_reset(transaction);
#endif

	clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_start);

retryTxn:
	for (i = 0; i < MAX_OP; i++) {

		if ((args->txnspec.ops[i][OP_COUNT] > 0) && (i != OP_TRANSACTION) && (i != OP_COMMIT)) {
			for (count = 0; count < args->txnspec.ops[i][OP_COUNT]; count++) {

				/* note: for simplicity, always generate a new key(s) even when retrying */

				/* pick a random key(s) */
				if (args->zipf) {
					keynum = zipfian_next();
				} else {
					keynum = urand(0, args->rows - 1);
				}
				genkey(keystr, KEYPREFIX, KEYPREFIXLEN, args->prefixpadding, keynum, args->rows, args->key_length + 1);

				/* range */
				if (args->txnspec.ops[i][OP_RANGE] > 0) {
					keyend = keynum + args->txnspec.ops[i][OP_RANGE] - 1; /* inclusive */
					if (keyend > args->rows - 1) {
						keyend = args->rows - 1;
					}
					genkey(keystr2,
					       KEYPREFIX,
					       KEYPREFIXLEN,
					       args->prefixpadding,
					       keyend,
					       args->rows,
					       args->key_length + 1);
				}

				if (stats->xacts % args->sampling == 0) {
					/* per op latency */
					clock_gettime(CLOCK_MONOTONIC, &timer_start);
				}

				switch (i) {
				case OP_GETREADVERSION:
					rc = run_op_getreadversion(transaction, &readversion);
					break;
				case OP_GET:
					rc = run_op_get(transaction, keystr, valstr, 0);
					break;
				case OP_GETRANGE:
					rc = run_op_getrange(transaction,
					                     keystr,
					                     keystr2,
					                     valstr,
					                     0,
					                     args->txnspec.ops[i][OP_REVERSE],
					                     args->streaming_mode);
					break;
				case OP_SGET:
					rc = run_op_get(transaction, keystr, valstr, 1);
					break;
				case OP_SGETRANGE:
					rc = run_op_getrange(transaction,
					                     keystr,
					                     keystr2,
					                     valstr,
					                     1,
					                     args->txnspec.ops[i][OP_REVERSE],
					                     args->streaming_mode);
					break;
				case OP_UPDATE:
					randstr(valstr, args->value_length + 1);
					rc = run_op_update(transaction, keystr, valstr);
					docommit = 1;
					break;
				case OP_INSERT:
					randstr(keystr + KEYPREFIXLEN, args->key_length - KEYPREFIXLEN + 1); /* make it (almost) unique */
					randstr(valstr, args->value_length + 1);
					rc = run_op_insert(transaction, keystr, valstr);
					docommit = 1;
					break;
				case OP_INSERTRANGE:
					randstrlen = args->key_length - KEYPREFIXLEN - digits(args->txnspec.ops[i][OP_RANGE]);
					randstr(keystr + KEYPREFIXLEN, randstrlen + 1); /* make it (almost) unique */
					randstr(valstr, args->value_length + 1);
					for (rangei = 0; rangei < args->txnspec.ops[i][OP_RANGE]; rangei++) {
						sprintf(keystr + KEYPREFIXLEN + randstrlen,
						        "%0.*d",
						        digits(args->txnspec.ops[i][OP_RANGE]),
						        rangei);
						rc = run_op_insert(transaction, keystr, valstr);
						if (rc != FDB_SUCCESS)
							break;
					}
					docommit = 1;
					break;
				case OP_OVERWRITE:
					rc = run_op_insert(transaction, keystr, valstr);
					docommit = 1;
					break;
				case OP_CLEAR:
					rc = run_op_clear(transaction, keystr);
					docommit = 1;
					break;
				case OP_SETCLEAR:
					randstr(keystr + KEYPREFIXLEN, args->key_length - KEYPREFIXLEN + 1); /* make it (almost) unique */
					randstr(valstr, args->value_length + 1);
					rc = run_op_insert(transaction, keystr, valstr);
					if (rc == FDB_SUCCESS) {
						/* commit insert so mutation goes to storage */
						/* to measure commit latency */
						if (stats->xacts % args->sampling == 0) {
							clock_gettime(CLOCK_MONOTONIC, &timer_start_commit);
						}
						rc = commit_transaction(transaction);
						if (rc == FDB_SUCCESS) {
							stats->ops[OP_COMMIT]++;
							stats->ops[OP_TRANSACTION]++;
							if (stats->xacts % args->sampling == 0) {
								clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
								update_op_lat_stats(&timer_start_commit,
								                    &timer_per_xact_end,
								                    OP_COMMIT,
								                    stats,
								                    block,
								                    elem_size,
								                    is_memory_allocated);
								update_op_lat_stats(&timer_per_xact_start,
								                    &timer_per_xact_end,
								                    OP_TRANSACTION,
								                    stats,
								                    block,
								                    elem_size,
								                    is_memory_allocated);
							}
						} else {
							/* error */
							if (rc == FDB_ERROR_CONFLICT) {
								stats->conflicts++;
							} else {
								stats->errors[OP_COMMIT]++;
							}
							if (rc == FDB_ERROR_ABORT) {
								/* make sure to reset transaction */
								fdb_transaction_reset(transaction);
								return rc; /* abort */
							}
							goto retryTxn;
						}
						fdb_transaction_reset(transaction);
						rc = run_op_clear(transaction, keystr);
					}
					docommit = 1;
					break;
				case OP_CLEARRANGE:
					rc = run_op_clearrange(transaction, keystr, keystr2);
					docommit = 1;
					break;
				case OP_SETCLEARRANGE:
					randstrlen = args->key_length - KEYPREFIXLEN - digits(args->txnspec.ops[i][OP_RANGE]);
					randstr(keystr + KEYPREFIXLEN, randstrlen + 1); /* make it (almost) unique */
					randstr(valstr, args->value_length + 1);
					for (rangei = 0; rangei < args->txnspec.ops[i][OP_RANGE]; rangei++) {
						sprintf(keystr + KEYPREFIXLEN + randstrlen,
						        "%0.*d",
						        digits(args->txnspec.ops[i][OP_RANGE]),
						        rangei);
						if (rangei == 0) {
							strcpy(keystr2, keystr);
							keystr2[strlen(keystr)] = '\0';
						}
						rc = run_op_insert(transaction, keystr, valstr);
						/* rollback not necessary, move on */
						if (rc == FDB_ERROR_RETRY) {
							goto retryTxn;
						} else if (rc == FDB_ERROR_ABORT) {
							/* make sure to reset transaction */
							fdb_transaction_reset(transaction);
							return rc; /* abort */
						}
					}
					/* commit insert so mutation goes to storage */
					if (stats->xacts % args->sampling == 0) {
						clock_gettime(CLOCK_MONOTONIC, &timer_start_commit);
					}
					rc = commit_transaction(transaction);
					if (rc == FDB_SUCCESS) {
						stats->ops[OP_COMMIT]++;
						stats->ops[OP_TRANSACTION]++;
						if (stats->xacts % args->sampling == 0) {
							clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
							update_op_lat_stats(&timer_start_commit,
							                    &timer_per_xact_end,
							                    OP_COMMIT,
							                    stats,
							                    block,
							                    elem_size,
							                    is_memory_allocated);
							update_op_lat_stats(&timer_per_xact_start,
							                    &timer_per_xact_end,
							                    OP_TRANSACTION,
							                    stats,
							                    block,
							                    elem_size,
							                    is_memory_allocated);
						}
					} else {
						/* error */
						if (rc == FDB_ERROR_CONFLICT) {
							stats->conflicts++;
						} else {
							stats->errors[OP_TRANSACTION]++;
						}
						if (rc == FDB_ERROR_ABORT) {
							/* make sure to reset transaction */
							fdb_transaction_reset(transaction);
							return rc; /* abort */
						}
						goto retryTxn;
					}
					fdb_transaction_reset(transaction);
					rc = run_op_clearrange(transaction, keystr2, keystr);
					docommit = 1;
					break;
				case OP_READ_BG:
					rc = run_op_read_blob_granules(
					    transaction, keystr, keystr2, args->bg_materialize_files, args->bg_file_path);
					break;
				default:
					fprintf(stderr, "ERROR: Unknown Operation %d\n", i);
					break;
				}

				if (stats->xacts % args->sampling == 0) {
					clock_gettime(CLOCK_MONOTONIC, &timer_end);
					if (rc == FDB_SUCCESS) {
						/* per op latency, record successful transactions */
						update_op_lat_stats(&timer_start, &timer_end, i, stats, block, elem_size, is_memory_allocated);
					}
				}

				/* check rc and update stats */
				if (rc == FDB_SUCCESS) {
					stats->ops[i]++;
				} else {
					/* error */
					if (rc == FDB_ERROR_CONFLICT) {
						stats->conflicts++;
					} else {
						stats->errors[OP_TRANSACTION]++;
					}
					if (rc == FDB_ERROR_ABORT) {
						/* make sure to reset transaction */
						fdb_transaction_reset(transaction);
						return rc; /* abort */
					}
					goto retryTxn;
				}
			}
		}
	}

	/* commit only successful transaction */
	if (docommit | args->commit_get) {
		if (stats->xacts % args->sampling == 0) {
			clock_gettime(CLOCK_MONOTONIC, &timer_start_commit);
		}
		rc = commit_transaction(transaction);
		if (rc == FDB_SUCCESS) {
			/* success */
			stats->ops[OP_COMMIT]++;
			if (stats->xacts % args->sampling == 0) {
				clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
				update_op_lat_stats(
				    &timer_start_commit, &timer_per_xact_end, OP_COMMIT, stats, block, elem_size, is_memory_allocated);
			}
		} else {
			/* error */
			if (rc == FDB_ERROR_CONFLICT) {
				stats->conflicts++;
			} else {
				stats->errors[OP_TRANSACTION]++;
			}
			if (rc == FDB_ERROR_ABORT) {
				/* make sure to reset transaction */
				fdb_transaction_reset(transaction);
				return rc; /* abort */
			}
			goto retryTxn;
		}
	}

	stats->ops[OP_TRANSACTION]++;
	if (stats->xacts % args->sampling == 0) {
		clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
		update_op_lat_stats(
		    &timer_per_xact_start, &timer_per_xact_end, OP_TRANSACTION, stats, block, elem_size, is_memory_allocated);
	}

	stats->xacts++;

	/* make sure to reset transaction */
	fdb_transaction_reset(transaction);
	return 0;
}

int run_workload(FDBTransaction* transaction,
                 mako_args_t* args,
                 int thread_tps,
                 volatile double* throttle_factor,
                 int thread_iters,
                 volatile int* signal,
                 mako_stats_t* stats,
                 int dotrace,
                 int dotagging,
                 lat_block_t* block[],
                 int* elem_size,
                 bool* is_memory_allocated) {
	int xacts = 0;
	int64_t total_xacts = 0;
	int rc = 0;
	struct timespec timer_prev, timer_now;
	char* keystr;
	char* keystr2;
	char* valstr;
	int current_tps;
	char* traceid;
	int tracetimer = 0;
	char* tagstr;

	if (thread_tps < 0)
		return 0;

	if (dotrace) {
		traceid = (char*)malloc(32);
	}

	if (dotagging) {
		tagstr = (char*)calloc(16, 1);
		memcpy(tagstr, KEYPREFIX, KEYPREFIXLEN);
		memcpy(tagstr + KEYPREFIXLEN, args->txntagging_prefix, TAGPREFIXLENGTH_MAX);
	}

	current_tps = (int)((double)thread_tps * *throttle_factor);

	keystr = (char*)malloc(sizeof(char) * args->key_length + 1);
	if (!keystr)
		return -1;
	keystr2 = (char*)malloc(sizeof(char) * args->key_length + 1);
	if (!keystr2) {
		free(keystr);
		return -1;
	}
	valstr = (char*)malloc(sizeof(char) * args->value_length + 1);
	if (!valstr) {
		free(keystr);
		free(keystr2);
		return -1;
	}

	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_prev);

	/* main transaction loop */
	while (1) {

		if (((thread_tps > 0) && (xacts >= current_tps)) /* throttle on */ || dotrace /* transaction tracing on */) {

			clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
			if ((timer_now.tv_sec > timer_prev.tv_sec + 1) ||
			    ((timer_now.tv_sec == timer_prev.tv_sec + 1) && (timer_now.tv_nsec > timer_prev.tv_nsec))) {
				/* more than 1 second passed, no need to throttle */
				xacts = 0;
				timer_prev.tv_sec = timer_now.tv_sec;
				timer_prev.tv_nsec = timer_now.tv_nsec;

				/* update throttle rate */
				if (thread_tps > 0) {
					current_tps = (int)((double)thread_tps * *throttle_factor);
				}

				/* enable transaction trace */
				if (dotrace) {
					tracetimer++;
					if (tracetimer == dotrace) {
						fdb_error_t err;
						tracetimer = 0;
						snprintf(traceid, 32, "makotrace%019" PRId64, total_xacts);
						fprintf(debugme, "DEBUG: txn tracing %s\n", traceid);
						err = fdb_transaction_set_option(transaction,
						                                 FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER,
						                                 (uint8_t*)traceid,
						                                 strlen(traceid));
						if (err) {
							fprintf(
							    stderr, "ERROR: FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER: %s\n", fdb_get_error(err));
						}
						err = fdb_transaction_set_option(transaction, FDB_TR_OPTION_LOG_TRANSACTION, (uint8_t*)NULL, 0);
						if (err) {
							fprintf(stderr, "ERROR: FDB_TR_OPTION_LOG_TRANSACTION: %s\n", fdb_get_error(err));
						}
					}
				}

			} else {
				if (thread_tps > 0) {
					/* 1 second not passed, throttle */
					usleep(1000);
					continue;
				}
			}
		} /* throttle or txntrace */

		/* enable transaction tagging */
		if (dotagging > 0) {
			sprintf(tagstr + KEYPREFIXLEN + TAGPREFIXLENGTH_MAX, "%03d", urand(0, args->txntagging - 1));
			fdb_error_t err =
			    fdb_transaction_set_option(transaction, FDB_TR_OPTION_AUTO_THROTTLE_TAG, (uint8_t*)tagstr, 16);
			if (err) {
				fprintf(stderr, "ERROR: FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER: %s\n", fdb_get_error(err));
			}
		}

		rc = run_one_transaction(
		    transaction, args, stats, keystr, keystr2, valstr, block, elem_size, is_memory_allocated);
		if (rc) {
			/* FIXME: run_one_transaction should return something meaningful */
			fprintf(annoyme, "ERROR: run_one_transaction failed (%d)\n", rc);
		}

		if (thread_iters > 0) {
			if (thread_iters == xacts) {
				/* xact limit reached */
				break;
			}
		} else if (*signal == SIGNAL_RED) {
			/* signal turned red, target duration reached */
			break;
		}
		xacts++;
		total_xacts++;
	}
	free(keystr);
	free(keystr2);
	free(valstr);
	if (dotrace) {
		free(traceid);
	}
	if (dotagging) {
		free(tagstr);
	}

	return rc;
}

void get_stats_file_name(char filename[], int worker_id, int thread_id, int op) {
	char str1[256];
	sprintf(str1, "/%d_%d_", worker_id + 1, thread_id + 1);
	strcat(filename, str1);
	switch (op) {
	case OP_GETREADVERSION:
		strcat(filename, "GRV");
		break;
	case OP_GET:
		strcat(filename, "GET");
		break;
	case OP_GETRANGE:
		strcat(filename, "GETRANGE");
		break;
	case OP_SGET:
		strcat(filename, "SGET");
		break;
	case OP_SGETRANGE:
		strcat(filename, "SGETRANGE");
		break;
	case OP_UPDATE:
		strcat(filename, "UPDATE");
		break;
	case OP_INSERT:
		strcat(filename, "INSERT");
		break;
	case OP_INSERTRANGE:
		strcat(filename, "INSERTRANGE");
		break;
	case OP_OVERWRITE:
		strcat(filename, "OVERWRITE");
		break;
	case OP_CLEAR:
		strcat(filename, "CLEAR");
		break;
	case OP_SETCLEAR:
		strcat(filename, "SETCLEAR");
		break;
	case OP_CLEARRANGE:
		strcat(filename, "CLEARRANGE");
		break;
	case OP_SETCLEARRANGE:
		strcat(filename, "SETCLRRANGE");
		break;
	case OP_COMMIT:
		strcat(filename, "COMMIT");
		break;
	case OP_TRANSACTION:
		strcat(filename, "TRANSACTION");
		break;
	case OP_READ_BG:
		strcat(filename, "READBLOBGRANULES");
		break;
	}
}

/* mako worker thread */
void* worker_thread(void* thread_args) {
	int worker_id = ((thread_args_t*)thread_args)->process->worker_id;
	int thread_id = ((thread_args_t*)thread_args)->thread_id;
	mako_args_t* args = ((thread_args_t*)thread_args)->process->args;
	size_t database_index = ((thread_args_t*)thread_args)->database_index;
	FDBDatabase* database = ((thread_args_t*)thread_args)->process->databases[database_index];
	fdb_error_t err;
	int rc;
	FDBTransaction* transaction;
	int thread_tps = 0;
	int thread_iters = 0;
	int op;
	int i, size;
	int dotrace = (worker_id == 0 && thread_id == 0 && args->txntrace) ? args->txntrace : 0;
	int dotagging = args->txntagging;
	volatile int* signal = &((thread_args_t*)thread_args)->process->shm->signal;
	volatile double* throttle_factor = &((thread_args_t*)thread_args)->process->shm->throttle_factor;
	volatile int* readycount = &((thread_args_t*)thread_args)->process->shm->readycount;
	volatile int* stopcount = &((thread_args_t*)thread_args)->process->shm->stopcount;
	mako_stats_t* stats = (void*)((thread_args_t*)thread_args)->process->shm + sizeof(mako_shmhdr_t) /* skip header */
	                      + (sizeof(mako_stats_t) * (worker_id * args->num_threads + thread_id));

	lat_block_t* block[MAX_OP];
	for (int i = 0; i < MAX_OP; i++) {
		block[i] = ((thread_args_t*)thread_args)->block[i];
	}
	int* elem_size = &((thread_args_t*)thread_args)->elem_size[0];
	pid_t* parent_id = &((thread_args_t*)thread_args)->process->parent_id;
	bool* is_memory_allocated = &((thread_args_t*)thread_args)->is_memory_allocated[0];

	/* init latency */
	for (op = 0; op < MAX_OP; op++) {
		stats->latency_us_min[op] = 0xFFFFFFFFFFFFFFFF; /* uint64_t */
		stats->latency_us_max[op] = 0;
		stats->latency_us_total[op] = 0;
		stats->latency_samples[op] = 0;
	}

	fprintf(debugme,
	        "DEBUG: worker_id:%d (%d) thread_id:%d (%d) database_index:%lu (tid:%" PRIu64 ")\n",
	        worker_id,
	        args->num_processes,
	        thread_id,
	        args->num_threads,
	        database_index,
	        (uint64_t)pthread_self());

	if (args->tpsmax) {
		thread_tps = compute_thread_tps(args->tpsmax, worker_id, thread_id, args->num_processes, args->num_threads);
	}

	if (args->iteration) {
		thread_iters =
		    compute_thread_iters(args->iteration, worker_id, thread_id, args->num_processes, args->num_threads);
	}

	/* create my own transaction object */
	err = fdb_database_create_transaction(database, &transaction);
	check_fdb_error(err);

	/* i'm ready */
	__sync_fetch_and_add(readycount, 1);
	while (*signal == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	/* clean */
	if (args->mode == MODE_CLEAN) {
		rc = cleanup(transaction, args);
		if (rc < 0) {
			fprintf(stderr, "ERROR: cleanup failed\n");
		}
	}

	/* build/popualte */
	else if (args->mode == MODE_BUILD) {
		rc =
		    populate(transaction, args, worker_id, thread_id, thread_tps, stats, block, elem_size, is_memory_allocated);
		if (rc < 0) {
			fprintf(stderr, "ERROR: populate failed\n");
		}
	}

	/* run the workload */
	else if (args->mode == MODE_RUN) {
		rc = run_workload(transaction,
		                  args,
		                  thread_tps,
		                  throttle_factor,
		                  thread_iters,
		                  signal,
		                  stats,
		                  dotrace,
		                  dotagging,
		                  block,
		                  elem_size,
		                  is_memory_allocated);
		if (rc < 0) {
			fprintf(stderr, "ERROR: run_workload failed\n");
		}
	}

	if (args->mode == MODE_BUILD || args->mode == MODE_RUN) {
		char str2[1000];
		sprintf(str2, "%s%d", TEMP_DATA_STORE, *parent_id);
		rc = mkdir(str2, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
		if (rc < 0 && errno != EEXIST) {
			int ec = errno;
			fprintf(stderr, "Failed to make directory: %s because %s\n", str2, strerror(ec));
			goto failExit;
		}
		for (op = 0; op < MAX_OP; op++) {
			if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_COMMIT || op == OP_TRANSACTION) {
				FILE* fp;
				char file_name[NAME_MAX] = { '\0' };
				strcat(file_name, str2);
				get_stats_file_name(file_name, worker_id, thread_id, op);
				fp = fopen(file_name, "w");
				if (!fp) {
					int ec = errno;
					fprintf(stderr, "Failed to open file: %s because %s\n", file_name, strerror(ec));
					goto failExit;
				}
				lat_block_t* temp_block = ((thread_args_t*)thread_args)->block[op];
				if (is_memory_allocated[op]) {
					size = stats->latency_samples[op] / LAT_BLOCK_SIZE;
					for (i = 0; i < size && temp_block != NULL; i++) {
						fwrite(&temp_block->data, sizeof(uint64_t) * LAT_BLOCK_SIZE, 1, fp);
						temp_block = temp_block->next_block;
					}
					size = stats->latency_samples[op] % LAT_BLOCK_SIZE;
					if (size != 0)
						fwrite(&temp_block->data, sizeof(uint64_t) * size, 1, fp);
				} else {
					while (temp_block) {
						fwrite(&temp_block->data, sizeof(uint64_t) * LAT_BLOCK_SIZE, 1, fp);
						temp_block = temp_block->next_block;
					}
				}
				fclose(fp);
			}
		}
	}

	/* fall through */
failExit:
	__sync_fetch_and_add(stopcount, 1);
	for (op = 0; op < MAX_OP; op++) {
		lat_block_t* curr = ((thread_args_t*)thread_args)->block[op];
		lat_block_t* prev = NULL;
		size = elem_size[op] / LAT_BLOCK_SIZE;
		while (size--) {
			prev = curr;
			curr = curr->next_block;
			free(prev);
		}
	}
	fdb_transaction_destroy(transaction);
	pthread_exit(0);
}

/* mako worker process */
int worker_process_main(mako_args_t* args, int worker_id, mako_shmhdr_t* shm, pid_t* pid_main) {
	int i;
	pthread_t network_thread; /* handle for thread which invoked fdb_run_network() */
	pthread_t* worker_threads = NULL;
#if FDB_API_VERSION < 610
	FDBCluster* cluster;
#endif
	process_info_t process;
	thread_args_t* thread_args = NULL;
	int rc;
	fdb_error_t err;

	process.worker_id = worker_id;
	process.parent_id = *pid_main;
	process.args = args;
	process.shm = (mako_shmhdr_t*)shm;

	fprintf(debugme, "DEBUG: worker %d started\n", worker_id);

	/* Everything starts from here */

	err = fdb_select_api_version(args->api_version);
	if (err) {
		fprintf(stderr, "ERROR: Failed at %s:%d (%s)\n", __FILE__, __LINE__, fdb_get_error(err));
		return -1;
	}

	/* enable flatbuffers if specified */
	if (args->flatbuffers) {
#ifdef FDB_NET_OPTION_USE_FLATBUFFERS
		fprintf(debugme, "DEBUG: Using flatbuffers\n");
		err = fdb_network_set_option(FDB_NET_OPTION_USE_FLATBUFFERS, (uint8_t*)&args->flatbuffers, sizeof(uint8_t));
		if (err) {
			fprintf(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_USE_FLATBUFFERS): %s\n", fdb_get_error(err));
		}
#else
		fprintf(printme, "INFO: flatbuffers is not supported in FDB API version %d\n", FDB_API_VERSION);
#endif
	}

	/* Set client Log group */
	if (strlen(args->log_group) != 0) {
		err =
		    fdb_network_set_option(FDB_NET_OPTION_TRACE_LOG_GROUP, (uint8_t*)args->log_group, strlen(args->log_group));
		if (err) {
			fprintf(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_TRACE_LOG_GROUP): %s\n", fdb_get_error(err));
		}
	}

	/* enable tracing if specified */
	if (args->trace) {
		fprintf(debugme,
		        "DEBUG: Enable Tracing in %s (%s)\n",
		        (args->traceformat == 0) ? "XML" : "JSON",
		        (args->tracepath[0] == '\0') ? "current directory" : args->tracepath);
		err = fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE, (uint8_t*)args->tracepath, strlen(args->tracepath));
		if (err) {
			fprintf(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE): %s\n", fdb_get_error(err));
		}
		if (args->traceformat == 1) {
			err = fdb_network_set_option(FDB_NET_OPTION_TRACE_FORMAT, (uint8_t*)"json", 4);
			if (err) {
				fprintf(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_TRACE_FORMAT): %s\n", fdb_get_error(err));
			}
		}
	}

	/* enable knobs if specified */
	if (args->knobs[0] != '\0') {
		char delim[] = ", ";
		char* knob = strtok(args->knobs, delim);
		while (knob != NULL) {
			fprintf(debugme, "DEBUG: Setting client knobs: %s\n", knob);
			err = fdb_network_set_option(FDB_NET_OPTION_KNOB, (uint8_t*)knob, strlen(knob));
			if (err) {
				fprintf(stderr, "ERROR: fdb_network_set_option: %s\n", fdb_get_error(err));
			}
			knob = strtok(NULL, delim);
		}
	}

	if (args->client_threads_per_version > 0) {
		err = fdb_network_set_option(
		    FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, (uint8_t*)&args->client_threads_per_version, sizeof(int64_t));
		if (err) {
			fprintf(stderr,
			        "ERROR: fdb_network_set_option (FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION) (%d): %s\n",
			        args->client_threads_per_version,
			        fdb_get_error(err));
			// let's exit here since we do not want to confuse users
			// that mako is running with multi-threaded client enabled
			return -1;
		}
	}

	/* Network thread must be setup before doing anything */
	fprintf(debugme, "DEBUG: fdb_setup_network\n");
	err = fdb_setup_network();
	if (err) {
		fprintf(stderr, "ERROR: Failed at %s:%d (%s)\n", __FILE__, __LINE__, fdb_get_error(err));
		return -1;
	}

	/* Each worker process will have its own network thread */
	fprintf(debugme, "DEBUG: creating network thread\n");
	rc = pthread_create(&network_thread, NULL, fdb_network_thread, (void*)args);
	if (rc != 0) {
		fprintf(stderr, "ERROR: Cannot create a network thread\n");
		return -1;
	}

	/*** let's party! ***/

	/* set up cluster and datbase for worker threads */

#if FDB_API_VERSION < 610
	/* cluster */
	f = fdb_create_cluster(args->cluster_file);
	fdb_block_wait(f);
	err = fdb_future_get_cluster(f, &cluster);
	check_fdb_error(err);
	fdb_future_destroy(f);

	/* database */
	/* big mystery -- do we ever have a database named other than "DB"? */
	f = fdb_cluster_create_database(cluster, (uint8_t*)"DB", 2);
	fdb_block_wait(f);
	err = fdb_future_get_database(f, &process.database);
	check_fdb_error(err);
	fdb_future_destroy(f);

#else /* >= 610 */
	for (size_t i = 0; i < args->num_databases; i++) {
		size_t cluster_index = args->num_fdb_clusters <= 1 ? 0 : i % args->num_fdb_clusters;
		fdb_create_database(args->cluster_files[cluster_index], &process.databases[i]);
		fprintf(debugme, "DEBUG: creating database at cluster %s\n", args->cluster_files[cluster_index]);
		if (args->disable_ryw) {
			fdb_database_set_option(process.databases[i], FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE, (uint8_t*)NULL, 0);
		}
	}
#endif

	fprintf(debugme, "DEBUG: creating %d worker threads\n", args->num_threads);
	worker_threads = (pthread_t*)calloc(sizeof(pthread_t), args->num_threads);
	if (!worker_threads) {
		fprintf(stderr, "ERROR: cannot allocate worker_threads\n");
		goto failExit;
	}

	/* spawn worker threads */
	thread_args = (thread_args_t*)calloc(sizeof(thread_args_t), args->num_threads);
	if (!thread_args) {
		fprintf(stderr, "ERROR: cannot allocate thread_args\n");
		goto failExit;
	}

	for (i = 0; i < args->num_threads; i++) {
		thread_args[i].thread_id = i;
		thread_args[i].database_index = i % args->num_databases;

		for (int op = 0; op < MAX_OP; op++) {
			if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
				thread_args[i].block[op] = (lat_block_t*)malloc(sizeof(lat_block_t));
				if (thread_args[i].block[op] == NULL) {
					thread_args[i].is_memory_allocated[op] = false;
					thread_args[i].elem_size[op] = 0;
				} else {
					thread_args[i].is_memory_allocated[op] = true;
					thread_args[i].block[op]->next_block = NULL;
					thread_args[i].elem_size[op] = LAT_BLOCK_SIZE;
				}
			}
		}
		thread_args[i].process = &process;
		rc = pthread_create(&worker_threads[i], NULL, worker_thread, (void*)&thread_args[i]);
		if (rc != 0) {
			fprintf(stderr, "ERROR: cannot create a new worker thread %d\n", i);
			/* ignore this thread? */
		}
	}

	/*** party is over ***/

	/* wait for everyone to finish */
	for (i = 0; i < args->num_threads; i++) {
		fprintf(debugme, "DEBUG: worker_thread %d joining\n", i);
		rc = pthread_join(worker_threads[i], NULL);
		if (rc != 0) {
			fprintf(stderr, "ERROR: threads %d failed to join\n", i);
		}
	}

failExit:
	if (worker_threads)
		free(worker_threads);
	if (thread_args)
		free(thread_args);

	/* clean up database and cluster */
	for (size_t i = 0; i < args->num_databases; i++) {
		fdb_database_destroy(process.databases[i]);
	}

#if FDB_API_VERSION < 610
	fdb_cluster_destroy(cluster);
#endif

	/* stop the network thread */
	fprintf(debugme, "DEBUG: fdb_stop_network\n");
	err = fdb_stop_network();
	check_fdb_error(err);

	/* wait for the network thread to join */
	fprintf(debugme, "DEBUG: network_thread joining\n");
	rc = pthread_join(network_thread, NULL);
	if (rc != 0) {
		fprintf(stderr, "ERROR: network thread failed to join\n");
	}

	return 0;
}

/* initialize the parameters with default values */
int init_args(mako_args_t* args) {
	int i;
	if (!args)
		return -1;
	memset(args, 0, sizeof(mako_args_t)); /* zero-out everything */
	args->num_fdb_clusters = 0;
	args->num_databases = 1;
	args->api_version = fdb_get_max_api_version();
	args->json = 0;
	args->num_processes = 1;
	args->num_threads = 1;
	args->mode = MODE_INVALID;
	args->rows = 100000;
	args->seconds = 30;
	args->iteration = 0;
	args->tpsmax = 0;
	args->tpsmin = -1;
	args->tpsinterval = 10;
	args->tpschange = TPS_SIN;
	args->sampling = 1000;
	args->key_length = 32;
	args->value_length = 16;
	args->zipf = 0;
	args->commit_get = 0;
	args->verbose = 1;
	args->flatbuffers = 0; /* internal */
	args->knobs[0] = '\0';
	args->log_group[0] = '\0';
	args->prefixpadding = 0;
	args->trace = 0;
	args->tracepath[0] = '\0';
	args->traceformat = 0; /* default to client's default (XML) */
	args->streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
	args->txntrace = 0;
	args->txntagging = 0;
	memset(args->txntagging_prefix, 0, TAGPREFIXLENGTH_MAX);
	for (i = 0; i < MAX_OP; i++) {
		args->txnspec.ops[i][OP_COUNT] = 0;
	}
	args->client_threads_per_version = 0;
	args->disable_ryw = 0;
	args->json_output_path[0] = '\0';
	args->bg_materialize_files = false;
	args->bg_file_path[0] = '\0';
	return 0;
}

/* parse transaction specification */
int parse_transaction(mako_args_t* args, char* optarg) {
	char* ptr = optarg;
	int op = 0;
	int rangeop = 0;
	int num;
	int error = 0;

	for (op = 0; op < MAX_OP; op++) {
		args->txnspec.ops[op][OP_COUNT] = 0;
		args->txnspec.ops[op][OP_RANGE] = 0;
	}

	op = 0;
	while (*ptr) {
// Clang gives false positive array bounds warning, which must be ignored:
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Warray-bounds"
		if (strncmp(ptr, "grv", 3) == 0) {
			op = OP_GETREADVERSION;
			ptr += 3;
		} else if (strncmp(ptr, "gr", 2) == 0) {
			op = OP_GETRANGE;
			rangeop = 1;
			ptr += 2;
		} else if (strncmp(ptr, "g", 1) == 0) {
			op = OP_GET;
			ptr++;
		} else if (strncmp(ptr, "sgr", 3) == 0) {
			op = OP_SGETRANGE;
			rangeop = 1;
			ptr += 3;
		} else if (strncmp(ptr, "sg", 2) == 0) {
			op = OP_SGET;
			ptr += 2;
		} else if (strncmp(ptr, "u", 1) == 0) {
			op = OP_UPDATE;
			ptr++;
		} else if (strncmp(ptr, "ir", 2) == 0) {
			op = OP_INSERTRANGE;
			rangeop = 1;
			ptr += 2;
		} else if (strncmp(ptr, "i", 1) == 0) {
			op = OP_INSERT;
			ptr++;
		} else if (strncmp(ptr, "o", 1) == 0) {
			op = OP_OVERWRITE;
			ptr++;
		} else if (strncmp(ptr, "cr", 2) == 0) {
			op = OP_CLEARRANGE;
			rangeop = 1;
			ptr += 2;
		} else if (strncmp(ptr, "c", 1) == 0) {
			op = OP_CLEAR;
			ptr++;
		} else if (strncmp(ptr, "scr", 3) == 0) {
			op = OP_SETCLEARRANGE;
			rangeop = 1;
			ptr += 3;
		} else if (strncmp(ptr, "sc", 2) == 0) {
			op = OP_SETCLEAR;
			ptr += 2;
		} else if (strncmp(ptr, "bg", 2) == 0) {
			op = OP_READ_BG;
			rangeop = 1;
			ptr += 2;
		} else {
			fprintf(debugme, "Error: Invalid transaction spec: %s\n", ptr);
			error = 1;
			break;
		}
#pragma clang diagnostic pop

		/* count */
		num = 0;
		if ((*ptr < '0') || (*ptr > '9')) {
			num = 1; /* if omitted, set it to 1 */
		} else {
			while ((*ptr >= '0') && (*ptr <= '9')) {
				num = num * 10 + *ptr - '0';
				ptr++;
			}
		}
		/* set count */
		args->txnspec.ops[op][OP_COUNT] = num;

		if (rangeop) {
			if (*ptr != ':') {
				error = 1;
				break;
			} else {
				ptr++; /* skip ':' */
				/* check negative '-' sign */
				if (*ptr == '-') {
					args->txnspec.ops[op][OP_REVERSE] = 1;
					ptr++;
				} else {
					args->txnspec.ops[op][OP_REVERSE] = 0;
				}
				num = 0;
				if ((*ptr < '0') || (*ptr > '9')) {
					error = 1;
					break;
				}
				while ((*ptr >= '0') && (*ptr <= '9')) {
					num = num * 10 + *ptr - '0';
					ptr++;
				}
				/* set range */
				args->txnspec.ops[op][OP_RANGE] = num;
			}
		}
		rangeop = 0;
	}

	if (error) {
		fprintf(stderr, "ERROR: invalid transaction specification %s\n", optarg);
		return -1;
	}

	if (args->verbose == VERBOSE_DEBUG) {
		for (op = 0; op < MAX_OP; op++) {
			fprintf(debugme, "DEBUG: OP: %d: %d: %d\n", op, args->txnspec.ops[op][0], args->txnspec.ops[op][1]);
		}
	}

	return 0;
}

void usage() {
	printf("Usage:\n");
	printf("%-24s %s\n", "-h, --help", "Print this message");
	printf("%-24s %s\n", "    --version", "Print FDB version");
	printf("%-24s %s\n", "-v, --verbose", "Specify verbosity");
	printf("%-24s %s\n", "-a, --api_version=API_VERSION", "Specify API_VERSION to use");
	printf("%-24s %s\n", "-c, --cluster=FILE", "Specify FDB cluster file");
	printf("%-24s %s\n", "-d, --num_databases=NUM_DATABASES", "Specify number of databases");
	printf("%-24s %s\n", "-p, --procs=PROCS", "Specify number of worker processes");
	printf("%-24s %s\n", "-t, --threads=THREADS", "Specify number of worker threads");
	printf("%-24s %s\n", "-r, --rows=ROWS", "Specify number of records");
	printf("%-24s %s\n", "-s, --seconds=SECONDS", "Specify the test duration in seconds\n");
	printf("%-24s %s\n", "", "This option cannot be specified with --iteration.");
	printf("%-24s %s\n", "-i, --iteration=ITERS", "Specify the number of iterations.\n");
	printf("%-24s %s\n", "", "This option cannot be specified with --seconds.");
	printf("%-24s %s\n", "    --keylen=LENGTH", "Specify the key lengths");
	printf("%-24s %s\n", "    --vallen=LENGTH", "Specify the value lengths");
	printf("%-24s %s\n", "-x, --transaction=SPEC", "Transaction specification");
	printf("%-24s %s\n", "    --tps|--tpsmax=TPS", "Specify the target max TPS");
	printf("%-24s %s\n", "    --tpsmin=TPS", "Specify the target min TPS");
	printf("%-24s %s\n", "    --tpsinterval=SEC", "Specify the TPS change interval (Default: 10 seconds)");
	printf("%-24s %s\n", "    --tpschange=<sin|square|pulse>", "Specify the TPS change type (Default: sin)");
	printf("%-24s %s\n", "    --sampling=RATE", "Specify the sampling rate for latency stats");
	printf("%-24s %s\n", "-m, --mode=MODE", "Specify the mode (build, run, clean)");
	printf("%-24s %s\n", "-z, --zipf", "Use zipfian distribution instead of uniform distribution");
	printf("%-24s %s\n", "    --commitget", "Commit GETs");
	printf("%-24s %s\n", "    --loggroup=LOGGROUP", "Set client log group");
	printf("%-24s %s\n", "    --prefix_padding", "Pad key by prefixing data (Default: postfix padding)");
	printf("%-24s %s\n", "    --trace", "Enable tracing");
	printf("%-24s %s\n", "    --tracepath=PATH", "Set trace file path");
	printf("%-24s %s\n", "    --trace_format <xml|json>", "Set trace format (Default: json)");
	printf("%-24s %s\n", "    --txntrace=sec", "Specify transaction tracing interval (Default: 0)");
	printf(
	    "%-24s %s\n", "    --txntagging", "Specify the number of different transaction tag (Default: 0, max = 1000)");
	printf("%-24s %s\n",
	       "    --txntagging_prefix",
	       "Specify the prefix of transaction tag - mako${txntagging_prefix} (Default: '')");
	printf("%-24s %s\n", "    --knobs=KNOBS", "Set client knobs");
	printf("%-24s %s\n", "    --flatbuffers", "Use flatbuffers");
	printf("%-24s %s\n", "    --streaming", "Streaming mode: all (default), iterator, small, medium, large, serial");
	printf("%-24s %s\n", "    --disable_ryw", "Disable snapshot read-your-writes");
	printf("%-24s %s\n", "    --json_report=PATH", "Output stats to the specified json file (Default: mako.json)");
	printf("%-24s %s\n",
	       "    --bg_file_path=PATH",
	       "Read blob granule files from the local filesystem at PATH and materialize the results.");
}

/* parse benchmark paramters */
int parse_args(int argc, char* argv[], mako_args_t* args) {
	int rc;
	int c;
	int idx;
	while (1) {
		const char* short_options = "a:c:d:p:t:r:s:i:x:v:m:hz";
		static struct option long_options[] = {
			/* name, has_arg, flag, val */
			{ "api_version", required_argument, NULL, 'a' },
			{ "cluster", required_argument, NULL, 'c' },
			{ "num_databases", optional_argument, NULL, 'd' },
			{ "procs", required_argument, NULL, 'p' },
			{ "threads", required_argument, NULL, 't' },
			{ "rows", required_argument, NULL, 'r' },
			{ "seconds", required_argument, NULL, 's' },
			{ "iteration", required_argument, NULL, 'i' },
			{ "keylen", required_argument, NULL, ARG_KEYLEN },
			{ "vallen", required_argument, NULL, ARG_VALLEN },
			{ "transaction", required_argument, NULL, 'x' },
			{ "tps", required_argument, NULL, ARG_TPS },
			{ "tpsmax", required_argument, NULL, ARG_TPSMAX },
			{ "tpsmin", required_argument, NULL, ARG_TPSMIN },
			{ "tpsinterval", required_argument, NULL, ARG_TPSINTERVAL },
			{ "tpschange", required_argument, NULL, ARG_TPSCHANGE },
			{ "sampling", required_argument, NULL, ARG_SAMPLING },
			{ "verbose", required_argument, NULL, 'v' },
			{ "mode", required_argument, NULL, 'm' },
			{ "knobs", required_argument, NULL, ARG_KNOBS },
			{ "loggroup", required_argument, NULL, ARG_LOGGROUP },
			{ "tracepath", required_argument, NULL, ARG_TRACEPATH },
			{ "trace_format", required_argument, NULL, ARG_TRACEFORMAT },
			{ "streaming", required_argument, NULL, ARG_STREAMING_MODE },
			{ "txntrace", required_argument, NULL, ARG_TXNTRACE },
			/* no args */
			{ "help", no_argument, NULL, 'h' },
			{ "zipf", no_argument, NULL, 'z' },
			{ "commitget", no_argument, NULL, ARG_COMMITGET },
			{ "flatbuffers", no_argument, NULL, ARG_FLATBUFFERS },
			{ "prefix_padding", no_argument, NULL, ARG_PREFIXPADDING },
			{ "trace", no_argument, NULL, ARG_TRACE },
			{ "txntagging", required_argument, NULL, ARG_TXNTAGGING },
			{ "txntagging_prefix", required_argument, NULL, ARG_TXNTAGGINGPREFIX },
			{ "version", no_argument, NULL, ARG_VERSION },
			{ "client_threads_per_version", required_argument, NULL, ARG_CLIENT_THREADS_PER_VERSION },
			{ "disable_ryw", no_argument, NULL, ARG_DISABLE_RYW },
			{ "json_report", optional_argument, NULL, ARG_JSON_REPORT },
			{ "bg_file_path", required_argument, NULL, ARG_BG_FILE_PATH },
			{ NULL, 0, NULL, 0 }
		};
		idx = 0;
		c = getopt_long(argc, argv, short_options, long_options, &idx);
		if (c < 0) {
			break;
		}
		switch (c) {
		case '?':
		case 'h':
			usage();
			return -1;
		case 'a':
			args->api_version = atoi(optarg);
			break;
		case 'c': {
			const char delim[] = ",";
			char* cluster_file = strtok(optarg, delim);
			while (cluster_file != NULL) {
				strcpy(args->cluster_files[args->num_fdb_clusters++], cluster_file);
				cluster_file = strtok(NULL, delim);
			}
			break;
		}
		case 'd':
			args->num_databases = atoi(optarg);
			break;
		case 'p':
			args->num_processes = atoi(optarg);
			break;
		case 't':
			args->num_threads = atoi(optarg);
			break;
		case 'r':
			args->rows = atoi(optarg);
			break;
		case 's':
			args->seconds = atoi(optarg);
			break;
		case 'i':
			args->iteration = atoi(optarg);
			break;
		case 'x':
			rc = parse_transaction(args, optarg);
			if (rc < 0)
				return -1;
			break;
		case 'v':
			args->verbose = atoi(optarg);
			break;
		case 'z':
			args->zipf = 1;
			break;
		case 'm':
			if (strcmp(optarg, "clean") == 0) {
				args->mode = MODE_CLEAN;
			} else if (strcmp(optarg, "build") == 0) {
				args->mode = MODE_BUILD;
			} else if (strcmp(optarg, "run") == 0) {
				args->mode = MODE_RUN;
			}
			break;
		case ARG_KEYLEN:
			args->key_length = atoi(optarg);
			break;
		case ARG_VALLEN:
			args->value_length = atoi(optarg);
			break;
		case ARG_TPS:
		case ARG_TPSMAX:
			args->tpsmax = atoi(optarg);
			break;
		case ARG_TPSMIN:
			args->tpsmin = atoi(optarg);
			break;
		case ARG_TPSINTERVAL:
			args->tpsinterval = atoi(optarg);
			break;
		case ARG_TPSCHANGE:
			if (strcmp(optarg, "sin") == 0)
				args->tpschange = TPS_SIN;
			else if (strcmp(optarg, "square") == 0)
				args->tpschange = TPS_SQUARE;
			else if (strcmp(optarg, "pulse") == 0)
				args->tpschange = TPS_PULSE;
			else {
				fprintf(stderr, "--tpschange must be sin, square or pulse\n");
				return -1;
			}
			break;
		case ARG_SAMPLING:
			args->sampling = atoi(optarg);
			break;
		case ARG_VERSION:
			fprintf(stderr, "Version: %d\n", FDB_API_VERSION);
			exit(0);
			break;
		case ARG_COMMITGET:
			args->commit_get = 1;
			break;
		case ARG_FLATBUFFERS:
			args->flatbuffers = 1;
			break;
		case ARG_KNOBS:
			memcpy(args->knobs, optarg, strlen(optarg) + 1);
			break;
		case ARG_LOGGROUP:
			memcpy(args->log_group, optarg, strlen(optarg) + 1);
			break;
		case ARG_PREFIXPADDING:
			args->prefixpadding = 1;
			break;
		case ARG_TRACE:
			args->trace = 1;
			break;
		case ARG_TRACEPATH:
			args->trace = 1;
			memcpy(args->tracepath, optarg, strlen(optarg) + 1);
			break;
		case ARG_TRACEFORMAT:
			if (strncmp(optarg, "json", 5) == 0) {
				args->traceformat = 1;
			} else if (strncmp(optarg, "xml", 4) == 0) {
				args->traceformat = 0;
			} else {
				fprintf(stderr, "Error: Invalid trace_format %s\n", optarg);
				return -1;
			}
			break;
		case ARG_STREAMING_MODE:
			if (strncmp(optarg, "all", 3) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
			} else if (strncmp(optarg, "iterator", 8) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_ITERATOR;
			} else if (strncmp(optarg, "small", 5) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_SMALL;
			} else if (strncmp(optarg, "medium", 6) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_MEDIUM;
			} else if (strncmp(optarg, "large", 5) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_LARGE;
			} else if (strncmp(optarg, "serial", 6) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_SERIAL;
			} else {
				fprintf(stderr, "Error: Invalid streaming mode %s\n", optarg);
				return -1;
			}
			break;
		case ARG_TXNTRACE:
			args->txntrace = atoi(optarg);
			break;

		case ARG_TXNTAGGING:
			args->txntagging = atoi(optarg);
			if (args->txntagging > 1000) {
				args->txntagging = 1000;
			}
			break;
		case ARG_TXNTAGGINGPREFIX:
			if (strlen(optarg) > TAGPREFIXLENGTH_MAX) {
				fprintf(stderr, "Error: the length of txntagging_prefix is larger than %d\n", TAGPREFIXLENGTH_MAX);
				exit(0);
			}
			memcpy(args->txntagging_prefix, optarg, strlen(optarg));
			break;
		case ARG_CLIENT_THREADS_PER_VERSION:
			args->client_threads_per_version = atoi(optarg);
			break;
		case ARG_DISABLE_RYW:
			args->disable_ryw = 1;
			break;
		case ARG_JSON_REPORT:
			if (optarg == NULL && (argv[optind] == NULL || (argv[optind] != NULL && argv[optind][0] == '-'))) {
				// if --report_json is the last option and no file is specified
				// or --report_json is followed by another option
				char default_file[] = "mako.json";
				strncpy(args->json_output_path, default_file, strlen(default_file));
			} else {
				strncpy(args->json_output_path, optarg, strlen(optarg) + 1);
			}
			break;
		case ARG_BG_FILE_PATH:
			args->bg_materialize_files = true;
			strncpy(args->bg_file_path, optarg, strlen(optarg) + 1);
		}
	}

	if ((args->tpsmin == -1) || (args->tpsmin > args->tpsmax)) {
		args->tpsmin = args->tpsmax;
	}

	if (args->verbose >= VERBOSE_DEFAULT) {
		printme = stdout;
	} else {
		printme = fopen("/dev/null", "w");
	}
	if (args->verbose >= VERBOSE_ANNOYING) {
		annoyme = stdout;
	} else {
		annoyme = fopen("/dev/null", "w");
	}
	if (args->verbose >= VERBOSE_DEBUG) {
		debugme = stdout;
	} else {
		debugme = fopen("/dev/null", "w");
	}

	return 0;
}

char* get_ops_name(int ops_code) {
	switch (ops_code) {
	case OP_GETREADVERSION:
		return "GRV";
	case OP_GET:
		return "GET";
	case OP_GETRANGE:
		return "GETRANGE";
	case OP_SGET:
		return "SGET";
	case OP_SGETRANGE:
		return "SGETRANGE";
	case OP_UPDATE:
		return "UPDATE";
	case OP_INSERT:
		return "INSERT";
	case OP_INSERTRANGE:
		return "INSERTRANGE";
	case OP_OVERWRITE:
		return "OVERWRITE";
	case OP_CLEAR:
		return "CLEAR";
	case OP_SETCLEAR:
		return "SETCLEAR";
	case OP_CLEARRANGE:
		return "CLEARRANGE";
	case OP_SETCLEARRANGE:
		return "SETCLEARRANGE";
	case OP_COMMIT:
		return "COMMIT";
	case OP_TRANSACTION:
		return "TRANSACTION";
	case OP_READ_BG:
		return "READBLOBGRANULE";
	default:
		return "";
	}
}

int validate_args(mako_args_t* args) {
	if (args->mode == MODE_INVALID) {
		fprintf(stderr, "ERROR: --mode has to be set\n");
		return -1;
	}
	if (args->rows <= 0) {
		fprintf(stderr, "ERROR: --rows must be a positive integer\n");
		return -1;
	}
	if (args->key_length < 0) {
		fprintf(stderr, "ERROR: --keylen must be a positive integer\n");
		return -1;
	}
	if (args->value_length < 0) {
		fprintf(stderr, "ERROR: --vallen must be a positive integer\n");
		return -1;
	}
	if (args->num_fdb_clusters > NUM_CLUSTERS_MAX) {
		fprintf(stderr, "ERROR: Mako is not supported to do work to more than %d clusters\n", NUM_CLUSTERS_MAX);
		return -1;
	}
	if (args->num_databases > NUM_DATABASES_MAX) {
		fprintf(stderr, "ERROR: Mako is not supported to do work to more than %d databases\n", NUM_DATABASES_MAX);
		return -1;
	}
	if (args->num_databases < args->num_fdb_clusters) {
		fprintf(stderr,
		        "ERROR: --num_databases (%d) must be >= number of clusters(%d)\n",
		        args->num_databases,
		        args->num_fdb_clusters);
		return -1;
	}
	if (args->num_threads < args->num_databases) {
		fprintf(stderr,
		        "ERROR: --threads (%d) must be >= number of databases (%d)\n",
		        args->num_threads,
		        args->num_databases);
		return -1;
	}
	if (args->key_length < 4 /* "mako" */ + digits(args->rows)) {
		fprintf(stderr,
		        "ERROR: --keylen must be larger than %d to store \"mako\" prefix "
		        "and maximum row number\n",
		        4 + digits(args->rows));
		return -1;
	}
	if (args->mode == MODE_RUN) {
		if ((args->seconds > 0) && (args->iteration > 0)) {
			fprintf(stderr, "ERROR: Cannot specify seconds and iteration together\n");
			return -1;
		}
		if ((args->seconds == 0) && (args->iteration == 0)) {
			fprintf(stderr, "ERROR: Must specify either seconds or iteration\n");
			return -1;
		}
		if (args->txntagging < 0) {
			fprintf(stderr, "ERROR: --txntagging must be a non-negative integer\n");
			return -1;
		}
	}
	return 0;
}

/* stats output formatting */
#define STR2(x) #x
#define STR(x) STR2(x)
#define STATS_TITLE_WIDTH 12
#define STATS_FIELD_WIDTH 12

void print_stats(mako_args_t* args, mako_stats_t* stats, struct timespec* now, struct timespec* prev, FILE* fp) {
	int i, j;
	int op;
	int print_err;
	static uint64_t ops_total_prev[MAX_OP] = { 0 };
	uint64_t ops_total[MAX_OP] = { 0 };
	static uint64_t errors_total_prev[MAX_OP] = { 0 };
	uint64_t errors_total[MAX_OP] = { 0 };
	uint64_t errors_diff[MAX_OP] = { 0 };
	static uint64_t totalxacts_prev = 0;
	uint64_t totalxacts = 0;
	static uint64_t conflicts_prev = 0;
	uint64_t conflicts = 0;
	double duration_nsec = (now->tv_sec - prev->tv_sec) * 1000000000.0 + (now->tv_nsec - prev->tv_nsec);

	for (i = 0; i < args->num_processes; i++) {
		for (j = 0; j < args->num_threads; j++) {
			totalxacts += stats[(i * args->num_threads) + j].xacts;
			conflicts += stats[(i * args->num_threads) + j].conflicts;
			for (op = 0; op < MAX_OP; op++) {
				ops_total[op] += stats[(i * args->num_threads) + j].ops[op];
				errors_total[op] += stats[(i * args->num_threads) + j].errors[op];
			}
		}
	}

	if (fp) {
		fwrite("{", 1, 1, fp);
	}
	printf("%" STR(STATS_TITLE_WIDTH) "s ", "OPS");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0) {
			uint64_t ops_total_diff = ops_total[op] - ops_total_prev[op];
			printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", ops_total_diff);
			if (fp) {
				fprintf(fp, "\"%s\": %" PRIu64 ",", get_ops_name(op), ops_total_diff);
			}
			errors_diff[op] = errors_total[op] - errors_total_prev[op];
			print_err = (errors_diff[op] > 0);
			ops_total_prev[op] = ops_total[op];
			errors_total_prev[op] = errors_total[op];
		}
	}
	/* TPS */
	double tps = (totalxacts - totalxacts_prev) * 1000000000.0 / duration_nsec;
	printf("%" STR(STATS_FIELD_WIDTH) ".2f ", tps);
	if (fp) {
		fprintf(fp, "\"tps\": %.2f,", tps);
	}
	totalxacts_prev = totalxacts;

	/* Conflicts */
	double conflicts_diff = (conflicts - conflicts_prev) * 1000000000.0 / duration_nsec;
	printf("%" STR(STATS_FIELD_WIDTH) ".2f\n", conflicts_diff);
	if (fp) {
		fprintf(fp, "\"conflictsPerSec\": %.2f", conflicts_diff);
	}
	conflicts_prev = conflicts;

	if (print_err) {
		printf("%" STR(STATS_TITLE_WIDTH) "s ", "Errors");
		for (op = 0; op < MAX_OP; op++) {
			if (args->txnspec.ops[op][OP_COUNT] > 0) {
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", errors_diff[op]);
				if (fp) {
					fprintf(fp, ",\"errors\": %.2f", conflicts_diff);
				}
			}
		}
		printf("\n");
	}
	if (fp) {
		fprintf(fp, "}");
	}

	return;
}

void print_stats_header(mako_args_t* args, bool show_commit, bool is_first_header_empty, bool show_op_stats) {
	int op;
	int i;

	/* header */
	if (is_first_header_empty)
		for (i = 0; i <= STATS_TITLE_WIDTH; i++)
			printf(" ");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0) {
			printf("%" STR(STATS_FIELD_WIDTH) "s ", get_ops_name(op));
		}
	}

	if (show_commit)
		printf("%" STR(STATS_FIELD_WIDTH) "s ", "COMMIT");
	if (show_op_stats) {
		printf("%" STR(STATS_FIELD_WIDTH) "s\n", "TRANSACTION");
	} else {
		printf("%" STR(STATS_FIELD_WIDTH) "s ", "TPS");
		printf("%" STR(STATS_FIELD_WIDTH) "s\n", "Conflicts/s");
	}

	for (i = 0; i < STATS_TITLE_WIDTH; i++)
		printf("=");
	printf(" ");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0) {
			for (i = 0; i < STATS_FIELD_WIDTH; i++)
				printf("=");
			printf(" ");
		}
	}

	/* COMMIT */
	if (show_commit) {
		for (i = 0; i < STATS_FIELD_WIDTH; i++)
			printf("=");
		printf(" ");
	}

	if (show_op_stats) {
		/* TRANSACTION */
		for (i = 0; i < STATS_FIELD_WIDTH; i++)
			printf("=");
		printf(" ");
	} else {
		/* TPS */
		for (i = 0; i < STATS_FIELD_WIDTH; i++)
			printf("=");
		printf(" ");

		/* Conflicts */
		for (i = 0; i < STATS_FIELD_WIDTH; i++)
			printf("=");
	}
	printf("\n");
}

void print_report(mako_args_t* args,
                  mako_stats_t* stats,
                  struct timespec* timer_now,
                  struct timespec* timer_start,
                  pid_t* pid_main,
                  FILE* fp) {
	int i, j, k, op, index;
	int first_op = 1;
	uint64_t totalxacts = 0;
	uint64_t conflicts = 0;
	uint64_t totalerrors = 0;
	uint64_t ops_total[MAX_OP] = { 0 };
	uint64_t errors_total[MAX_OP] = { 0 };
	uint64_t lat_min[MAX_OP] = { 0 };
	uint64_t lat_total[MAX_OP] = { 0 };
	uint64_t lat_samples[MAX_OP] = { 0 };
	uint64_t lat_max[MAX_OP] = { 0 };

	uint64_t duration_nsec =
	    (timer_now->tv_sec - timer_start->tv_sec) * 1000000000 + (timer_now->tv_nsec - timer_start->tv_nsec);

	for (op = 0; op < MAX_OP; op++) {
		lat_min[op] = 0xFFFFFFFFFFFFFFFF; /* uint64_t */
		lat_max[op] = 0;
		lat_total[op] = 0;
		lat_samples[op] = 0;
	}

	for (i = 0; i < args->num_processes; i++) {
		for (j = 0; j < args->num_threads; j++) {
			int idx = i * args->num_threads + j;
			totalxacts += stats[idx].xacts;
			conflicts += stats[idx].conflicts;
			for (op = 0; op < MAX_OP; op++) {
				if ((args->txnspec.ops[op][OP_COUNT] > 0) || (op == OP_TRANSACTION) || (op == OP_COMMIT)) {
					totalerrors += stats[idx].errors[op];
					ops_total[op] += stats[idx].ops[op];
					errors_total[op] += stats[idx].errors[op];
					lat_total[op] += stats[idx].latency_us_total[op];
					lat_samples[op] += stats[idx].latency_samples[op];
					if (stats[idx].latency_us_min[op] < lat_min[op]) {
						lat_min[op] = stats[idx].latency_us_min[op];
					}
					if (stats[idx].latency_us_max[op] > lat_max[op]) {
						lat_max[op] = stats[idx].latency_us_max[op];
					}
				} /* if count > 0 */
			}
		}
	}

	/* overall stats */
	double total_duration = duration_nsec * 1.0 / 1000000000;
	printf("\n====== Total Duration %6.3f sec ======\n\n", total_duration);
	printf("Total Processes:  %8d\n", args->num_processes);
	printf("Total Threads:    %8d\n", args->num_threads);
	if (args->tpsmax == args->tpsmin)
		printf("Target TPS:       %8d\n", args->tpsmax);
	else {
		printf("Target TPS (MAX): %8d\n", args->tpsmax);
		printf("Target TPS (MIN): %8d\n", args->tpsmin);
		printf("TPS Interval:     %8d\n", args->tpsinterval);
		printf("TPS Change:       ");
		switch (args->tpschange) {
		case TPS_SIN:
			printf("%8s\n", "SIN");
			break;
		case TPS_SQUARE:
			printf("%8s\n", "SQUARE");
			break;
		case TPS_PULSE:
			printf("%8s\n", "PULSE");
			break;
		}
	}
	printf("Total Xacts:      %8" PRIu64 "\n", totalxacts);
	printf("Total Conflicts:  %8" PRIu64 "\n", conflicts);
	printf("Total Errors:     %8" PRIu64 "\n", totalerrors);
	printf("Overall TPS:      %8" PRIu64 "\n\n", totalxacts * 1000000000 / duration_nsec);

	if (fp) {
		fprintf(fp, "\"results\": {");
		fprintf(fp, "\"totalDuration\": %6.3f,", total_duration);
		fprintf(fp, "\"totalProcesses\": %d,", args->num_processes);
		fprintf(fp, "\"totalThreads\": %d,", args->num_threads);
		fprintf(fp, "\"targetTPS\": %d,", args->tpsmax);
		fprintf(fp, "\"totalXacts\": %" PRIu64 ",", totalxacts);
		fprintf(fp, "\"totalConflicts\": %" PRIu64 ",", conflicts);
		fprintf(fp, "\"totalErrors\": %" PRIu64 ",", totalerrors);
		fprintf(fp, "\"overallTPS\": %" PRIu64 ",", totalxacts * 1000000000 / duration_nsec);
	}

	/* per-op stats */
	print_stats_header(args, true, true, false);

	/* OPS */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Total OPS");
	if (fp) {
		fprintf(fp, "\"totalOps\": {");
	}
	for (op = 0; op < MAX_OP; op++) {
		if ((args->txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) || op == OP_COMMIT) {
			printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", ops_total[op]);
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fprintf(fp, ",");
				}
				fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), ops_total[op]);
			}
		}
	}

	/* TPS */
	double tps = totalxacts * 1000000000.0 / duration_nsec;
	printf("%" STR(STATS_FIELD_WIDTH) ".2f ", tps);

	/* Conflicts */
	double conflicts_rate = conflicts * 1000000000.0 / duration_nsec;
	printf("%" STR(STATS_FIELD_WIDTH) ".2f\n", conflicts_rate);

	if (fp) {
		fprintf(fp, "}, \"tps\": %.2f, \"conflictsPerSec\": %.2f, \"errors\": {", tps, conflicts_rate);
	}

	/* Errors */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Errors");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) {
			printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", errors_total[op]);
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fprintf(fp, ",");
				}
				fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), errors_total[op]);
			}
		}
	}
	if (fp) {
		fprintf(fp, "}, \"numSamples\": {");
	}
	printf("\n\n");

	printf("%s", "Latency (us)");
	print_stats_header(args, true, false, true);

	/* Total Samples */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Samples");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (lat_total[op]) {
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", lat_samples[op]);
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fprintf(fp, ",");
				}
				fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), lat_samples[op]);
			}
		}
	}
	printf("\n");

	/* Min Latency */
	if (fp) {
		fprintf(fp, "}, \"minLatency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Min");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (lat_min[op] == -1) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", lat_min[op]);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), lat_min[op]);
				}
			}
		}
	}
	printf("\n");

	/* Avg Latency */
	if (fp) {
		fprintf(fp, "}, \"avgLatency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Avg");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (lat_total[op]) {
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", lat_total[op] / lat_samples[op]);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), lat_total[op] / lat_samples[op]);
				}
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
		}
	}
	printf("\n");

	/* Max Latency */
	if (fp) {
		fprintf(fp, "}, \"maxLatency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Max");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (lat_max[op] == 0) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", lat_max[op]);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), lat_max[op]);
				}
			}
		}
	}
	printf("\n");

	uint64_t* dataPoints[MAX_OP];
	uint64_t median;
	int point_99_9pct, point_99pct, point_95pct;

	/* Median Latency */
	if (fp) {
		fprintf(fp, "}, \"medianLatency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Median");
	int num_points[MAX_OP] = { 0 };
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (lat_total[op]) {
				dataPoints[op] = (uint64_t*)malloc(sizeof(uint64_t) * lat_samples[op]);
				if (dataPoints[op] == NULL) {
					printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
					continue;
				}
				k = 0;
				for (i = 0; i < args->num_processes; i++) {
					for (j = 0; j < args->num_threads; j++) {
						char file_name[NAME_MAX] = { '\0' };
						sprintf(file_name, "%s%d", TEMP_DATA_STORE, *pid_main);
						get_stats_file_name(file_name, i, j, op);
						FILE* f = fopen(file_name, "r");
						fseek(f, 0, SEEK_END);
						int numPoints = ftell(f) / sizeof(uint64_t);
						fseek(f, 0, 0);
						index = 0;
						while (index < numPoints) {
							fread(&dataPoints[op][k++], sizeof(uint64_t), 1, f);
							++index;
						}
						fclose(f);
					}
				}
				num_points[op] = k;
				quick_sort(dataPoints[op], num_points[op]);
				if (num_points[op] & 1) {
					median = dataPoints[op][num_points[op] / 2];
				} else {
					median = (dataPoints[op][num_points[op] / 2] + dataPoints[op][num_points[op] / 2 - 1]) >> 1;
				}
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", median);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), median);
				}
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
		}
	}
	printf("\n");

	/* 95%ile Latency */
	if (fp) {
		fprintf(fp, "}, \"p95Latency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "95.0 pctile");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (dataPoints[op] == NULL) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
				continue;
			}
			if (lat_total[op]) {
				point_95pct = ((float)(num_points[op]) * 0.95) - 1;
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", dataPoints[op][point_95pct]);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), dataPoints[op][point_95pct]);
				}
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
		}
	}
	printf("\n");

	/* 99%ile Latency */
	if (fp) {
		fprintf(fp, "}, \"p99Latency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "99.0 pctile");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (dataPoints[op] == NULL) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
				continue;
			}
			if (lat_total[op]) {
				point_99pct = ((float)(num_points[op]) * 0.99) - 1;
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", dataPoints[op][point_99pct]);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), dataPoints[op][point_99pct]);
				}
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
		}
	}
	printf("\n");

	/* 99.9%ile Latency */
	if (fp) {
		fprintf(fp, "}, \"p99.9Latency\": {");
	}
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "99.9 pctile");
	first_op = 1;
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (dataPoints[op] == NULL) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
				continue;
			}
			if (lat_total[op]) {
				point_99_9pct = ((float)(num_points[op]) * 0.999) - 1;
				printf("%" STR(STATS_FIELD_WIDTH) PRIu64 " ", dataPoints[op][point_99_9pct]);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fprintf(fp, ",");
					}
					fprintf(fp, "\"%s\": %" PRIu64, get_ops_name(op), dataPoints[op][point_99_9pct]);
				}
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
		}
	}
	printf("\n");
	if (fp) {
		fprintf(fp, "}}");
	}

	char command_remove[NAME_MAX] = { '\0' };
	sprintf(command_remove, "rm -rf %s%d", TEMP_DATA_STORE, *pid_main);
	system(command_remove);

	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION) {
			if (lat_total[op])
				free(dataPoints[op]);
		}
	}
}

int stats_process_main(mako_args_t* args,
                       mako_stats_t* stats,
                       volatile double* throttle_factor,
                       volatile int* signal,
                       volatile int* stopcount,
                       pid_t* pid_main) {
	struct timespec timer_start, timer_prev, timer_now;
	double sin_factor;
	int first_stats = 1;

	/* wait until the signal turn on */
	while (*signal == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	if (args->verbose >= VERBOSE_DEFAULT)
		print_stats_header(args, false, true, false);

	FILE* fp = NULL;
	if (args->json_output_path[0] != '\0') {
		fp = fopen(args->json_output_path, "w");
		fprintf(fp, "{\"makoArgs\": {");
		fprintf(fp, "\"api_version\": %d,", args->api_version);
		fprintf(fp, "\"json\": %d,", args->json);
		fprintf(fp, "\"num_processes\": %d,", args->num_processes);
		fprintf(fp, "\"num_threads\": %d,", args->num_threads);
		fprintf(fp, "\"mode\": %d,", args->mode);
		fprintf(fp, "\"rows\": %d,", args->rows);
		fprintf(fp, "\"seconds\": %d,", args->seconds);
		fprintf(fp, "\"iteration\": %d,", args->iteration);
		fprintf(fp, "\"tpsmax\": %d,", args->tpsmax);
		fprintf(fp, "\"tpsmin\": %d,", args->tpsmin);
		fprintf(fp, "\"tpsinterval\": %d,", args->tpsinterval);
		fprintf(fp, "\"tpschange\": %d,", args->tpschange);
		fprintf(fp, "\"sampling\": %d,", args->sampling);
		fprintf(fp, "\"key_length\": %d,", args->key_length);
		fprintf(fp, "\"value_length\": %d,", args->value_length);
		fprintf(fp, "\"commit_get\": %d,", args->commit_get);
		fprintf(fp, "\"verbose\": %d,", args->verbose);
		fprintf(fp, "\"cluster_files\": \"%s\",", args->cluster_files[0]);
		fprintf(fp, "\"log_group\": \"%s\",", args->log_group);
		fprintf(fp, "\"prefixpadding\": %d,", args->prefixpadding);
		fprintf(fp, "\"trace\": %d,", args->trace);
		fprintf(fp, "\"tracepath\": \"%s\",", args->tracepath);
		fprintf(fp, "\"traceformat\": %d,", args->traceformat);
		fprintf(fp, "\"knobs\": \"%s\",", args->knobs);
		fprintf(fp, "\"flatbuffers\": %d,", args->flatbuffers);
		fprintf(fp, "\"txntrace\": %d,", args->txntrace);
		fprintf(fp, "\"txntagging\": %d,", args->txntagging);
		fprintf(fp, "\"txntagging_prefix\": \"%s\",", args->txntagging_prefix);
		fprintf(fp, "\"streaming_mode\": %d,", args->streaming_mode);
		fprintf(fp, "\"disable_ryw\": %d,", args->disable_ryw);
		fprintf(fp, "\"json_output_path\": \"%s\"", args->json_output_path);
		fprintf(fp, "},\"samples\": [");
	}

	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_start);
	timer_prev.tv_sec = timer_start.tv_sec;
	timer_prev.tv_nsec = timer_start.tv_nsec;
	while (*signal != SIGNAL_RED) {
		usleep(100000); /* sleep for 100ms */
		clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);

		/* print stats every (roughly) 1 sec */
		if (timer_now.tv_sec > timer_prev.tv_sec) {

			/* adjust throttle rate if needed */
			if (args->tpsmax != args->tpsmin) {
				/* set the throttle factor between 0.0 and 1.0 */
				switch (args->tpschange) {
				case TPS_SIN:
					sin_factor =
					    sin((timer_now.tv_sec % args->tpsinterval) / (double)args->tpsinterval * M_PI * 2) / 2.0 + 0.5;
					*throttle_factor = 1 - (sin_factor * (1.0 - ((double)args->tpsmin / args->tpsmax)));
					break;
				case TPS_SQUARE:
					if (timer_now.tv_sec % args->tpsinterval < (args->tpsinterval / 2)) {
						/* set to max */
						*throttle_factor = 1.0;
					} else {
						/* set to min */
						*throttle_factor = (double)args->tpsmin / (double)args->tpsmax;
					}
					break;
				case TPS_PULSE:
					if (timer_now.tv_sec % args->tpsinterval == 0) {
						/* set to max */
						*throttle_factor = 1.0;
					} else {
						/* set to min */
						*throttle_factor = (double)args->tpsmin / (double)args->tpsmax;
					}
					break;
				}
			}

			if (args->verbose >= VERBOSE_DEFAULT) {
				if (first_stats) {
					first_stats = 0;
				} else {
					if (fp)
						fprintf(fp, ",");
				}
				print_stats(args, stats, &timer_now, &timer_prev, fp);
			}
			timer_prev.tv_sec = timer_now.tv_sec;
			timer_prev.tv_nsec = timer_now.tv_nsec;
		}
	}

	if (fp) {
		fprintf(fp, "],");
	}

	/* print report */
	if (args->verbose >= VERBOSE_DEFAULT) {
		clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
		while (*stopcount < args->num_threads * args->num_processes) {
			usleep(10000); /* 10ms */
		}
		print_report(args, stats, &timer_now, &timer_start, pid_main, fp);
	}

	if (fp) {
		fprintf(fp, "}");
		fclose(fp);
	}

	return 0;
}

int main(int argc, char* argv[]) {
	int rc;
	mako_args_t args;
	int p;
	pid_t* worker_pids = NULL;
	proc_type_t proc_type = proc_master;
	int worker_id;
	pid_t pid;
	int status;
	mako_shmhdr_t* shm; /* shmhdr + stats */
	int shmfd;
	char shmpath[NAME_MAX];
	size_t shmsize;
	mako_stats_t* stats;
	pid_t pid_main;

	setlinebuf(stdout);

	rc = init_args(&args);
	if (rc < 0) {
		fprintf(stderr, "ERROR: init_args failed\n");
		return -1;
	}
	rc = parse_args(argc, argv, &args);
	if (rc < 0) {
		/* usage printed */
		return 0;
	}

	rc = validate_args(&args);
	if (rc < 0)
		return -1;

	if (args.mode == MODE_CLEAN) {
		/* cleanup will be done from a single thread */
		args.num_processes = 1;
		args.num_threads = 1;
	}

	if (args.mode == MODE_BUILD) {
		if (args.txnspec.ops[OP_INSERT][OP_COUNT] == 0) {
			parse_transaction(&args, "i100");
		}
	}

	pid_main = getpid();
	/* create the shared memory for stats */
	sprintf(shmpath, "mako%d", pid_main);
	shmfd = shm_open(shmpath, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (shmfd < 0) {
		fprintf(stderr, "ERROR: shm_open failed\n");
		return -1;
	}

	/* allocate */
	shmsize = sizeof(mako_shmhdr_t) + (sizeof(mako_stats_t) * args.num_processes * args.num_threads);
	if (ftruncate(shmfd, shmsize) < 0) {
		shm = MAP_FAILED;
		fprintf(stderr, "ERROR: ftruncate (fd:%d size:%llu) failed\n", shmfd, (unsigned long long)shmsize);
		goto failExit;
	}

	/* map it */
	shm = (mako_shmhdr_t*)mmap(NULL, shmsize, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0);
	if (shm == MAP_FAILED) {
		fprintf(stderr, "ERROR: mmap (fd:%d size:%llu) failed\n", shmfd, (unsigned long long)shmsize);
		goto failExit;
	}

	stats = (mako_stats_t*)((void*)shm + sizeof(mako_shmhdr_t));

	/* initialize the shared memory */
	memset(shm, 0, shmsize);

	/* get ready */
	shm->signal = SIGNAL_OFF;
	shm->readycount = 0;
	shm->stopcount = 0;
	shm->throttle_factor = 1.0;

	/* fork worker processes + 1 stats process */
	worker_pids = (pid_t*)calloc(sizeof(pid_t), args.num_processes + 1);
	if (!worker_pids) {
		fprintf(stderr, "ERROR: cannot allocate worker_pids (%d processes)\n", args.num_processes);
		goto failExit;
	}

	/* forking (num_process + 1) children */
	/* last process is the stats handler */
	for (p = 0; p < args.num_processes + 1; p++) {
		pid = fork();
		if (pid != 0) {
			/* master */
			worker_pids[p] = pid;
			if (args.verbose == VERBOSE_DEBUG) {
				printf("DEBUG: worker %d (PID:%d) forked\n", p, worker_pids[p]);
			}
		} else {
			if (p < args.num_processes) {
				/* worker process */
				proc_type = proc_worker;
				worker_id = p;
			} else {
				/* stats */
				proc_type = proc_stats;
			}
			break;
		}
	}

	/* initialize the randomizer */
	srand(time(0) * getpid());

	/* initialize zipfian if necessary (per-process) */
	if (args.zipf) {
		zipfian_generator(args.rows);
	}

	if (proc_type == proc_worker) {
		/* worker process */
		worker_process_main(&args, worker_id, shm, &pid_main);
		/* worker can exit here */
		exit(0);
	} else if (proc_type == proc_stats) {
		/* stats */
		if (args.mode == MODE_CLEAN) {
			/* no stats needed for clean mode */
			exit(0);
		}
		stats_process_main(&args, stats, &shm->throttle_factor, &shm->signal, &shm->stopcount, &pid_main);
		exit(0);
	}

	/* master */
	/* wait for everyone to be ready */
	while (shm->readycount < (args.num_processes * args.num_threads)) {
		usleep(1000);
	}
	shm->signal = SIGNAL_GREEN;

	if (args.mode == MODE_RUN) {
		struct timespec timer_start, timer_now;

		/* run the benchamrk */

		/* if seconds is specified, stop child processes after the specified
		 * duration */
		if (args.seconds > 0) {
			if (args.verbose == VERBOSE_DEBUG) {
				printf("DEBUG: master sleeping for %d seconds\n", args.seconds);
			}

			clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_start);
			while (1) {
				usleep(100000); /* sleep for 100ms */
				clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
				/* doesn't have to be precise */
				if (timer_now.tv_sec - timer_start.tv_sec > args.seconds) {
					if (args.verbose == VERBOSE_DEBUG) {
						printf("DEBUG: time's up (%d seconds)\n", args.seconds);
					}
					break;
				}
			}

			/* notify everyone the time's up */
			shm->signal = SIGNAL_RED;
		}
	}

	/* wait for worker processes to exit */
	for (p = 0; p < args.num_processes; p++) {
		if (args.verbose == VERBOSE_DEBUG) {
			printf("DEBUG: waiting worker %d (PID:%d) to exit\n", p, worker_pids[p]);
		}
		pid = waitpid(worker_pids[p], &status, 0 /* or what? */);
		if (pid < 0) {
			fprintf(stderr, "ERROR: waitpid failed for worker process PID %d\n", worker_pids[p]);
		}
		if (args.verbose == VERBOSE_DEBUG) {
			printf("DEBUG: worker %d (PID:%d) exited\n", p, worker_pids[p]);
		}
	}

	/* all worker threads finished, stop the stats */
	if (args.mode == MODE_BUILD || args.iteration > 0) {
		shm->signal = SIGNAL_RED;
	}

	/* wait for stats to stop */
	pid = waitpid(worker_pids[args.num_processes], &status, 0 /* or what? */);
	if (pid < 0) {
		fprintf(stderr, "ERROR: waitpid failed for stats process PID %d\n", worker_pids[args.num_processes]);
	}

failExit:

	if (worker_pids)
		free(worker_pids);

	if (shm != MAP_FAILED)
		munmap(shm, shmsize);

	if (shmfd) {
		close(shmfd);
		shm_unlink(shmpath);
		unlink(shmpath);
	}

	return 0;
}
