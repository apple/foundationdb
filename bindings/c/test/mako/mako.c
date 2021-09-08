#include <assert.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <sys/syslimits.h>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#else
#include <limits.h>
#endif

#include "mako.h"
#include "utils.h"
#include "zipf.h"

#define check_fdb_error(_e)                                                                                            \
	do {                                                                                                               \
		if (_e) {                                                                                                      \
			fprintf(stderr, "ERROR: Failed at %s:%d (%s)\n", __FILE__, __LINE__, fdb_get_error(_e));                   \
			goto FDB_FAIL;                                                                                             \
		}                                                                                                              \
	} while (0)

#define fdb_block_wait(_f)                                                                                             \
	do {                                                                                                               \
		if ((fdb_future_block_until_ready(_f)) != 0) {                                                                 \
			fprintf(stderr, "ERROR: fdb_future_block_until_ready failed at %s:%d\n", __FILE__, __LINE__);              \
			goto FDB_FAIL;                                                                                             \
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

int commit_transaction(FDBTransaction* transaction, mako_stats_t* stats) {
	FDBFuture* f;
	fdb_error_t err = 0;
	int retry = DEFAULT_RETRY_COUNT;

	do {
		f = fdb_transaction_commit(transaction);
		err = wait_future(f);
		fdb_future_destroy(f);
		if (stats) {
			if (err == 1020 /* not_committed */)
				stats->conflicts++;
			else {
				stats->errors[OP_COMMIT]++;
			}
		}

		if (err) {
			fprintf(stderr, "ERROR: Error %d occured at fdb_transaction_commit\n", err);
			f = fdb_transaction_on_error(transaction, err);
			err = wait_future(f);
			fdb_future_destroy(f);
			if (err) {
				/* not retryable */
				fprintf(stderr, "ERROR: fdb_transaction_on_error returned %d at %s:%d\n", err, __FILE__, __LINE__);
				break;
			}
		} else {
			if (stats)
				stats->ops[OP_COMMIT]++;
			break;
		}
	} while (err && retry--);

	return err;
}

void update_op_stats(struct timespec* start, struct timespec* end, int op, mako_stats_t* stats) {
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
}

/* FDB network thread */
void* fdb_network_thread(void* args) {
	fdb_error_t err;

	if (((mako_args_t*)args)->verbose == VERBOSE_DEBUG) {
		printf("DEBUG: fdb_network_thread started\n");
	}

	err = fdb_run_network();
	if (err) {
		fprintf(stderr, "ERROR: fdb_run_network: %s\n", fdb_get_error(err));
	}

	return 0;
}

/* cleanup database */
int cleanup(FDBTransaction* transaction, mako_args_t* args) {
	struct timespec timer_start, timer_end;
	char beginstr[7];
	char endstr[7];

	strncpy(beginstr, "mako", 4);
	beginstr[4] = 0x00;
	strncpy(endstr, "mako", 4);
	endstr[4] = 0xff;
	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_start);
	fdb_transaction_clear_range(transaction, (uint8_t*)beginstr, 5, (uint8_t*)endstr, 5);
	if (commit_transaction(transaction, NULL))
		goto FDB_FAIL;

	fdb_transaction_reset(transaction);
	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_end);
	if (args->verbose >= VERBOSE_DEFAULT) {
		printf("INFO: Clear range: %6.3f sec\n",
		       ((timer_end.tv_sec - timer_start.tv_sec) * 1000000000.0 + timer_end.tv_nsec - timer_start.tv_nsec) /
		           1000000000);
	}
	return 0;

FDB_FAIL:
	fprintf(stderr, "ERROR: FDB failure in cleanup()\n");
	return -1;
}

/* populate database */
int populate(FDBTransaction* transaction,
             mako_args_t* args,
             int worker_id,
             int thread_id,
             int thread_tps,
             mako_stats_t* stats) {
	int i;
	struct timespec timer_start, timer_end;
	struct timespec timer_prev, timer_now; /* for throttling */
	struct timespec timer_per_xact_start, timer_per_xact_end;
	char* keystr;
	char* valstr;

	int begin = insert_begin(args->rows, worker_id, thread_id, args->num_processes, args->num_threads);
	int end = insert_end(args->rows, worker_id, thread_id, args->num_processes, args->num_threads);
	int xacts = 0;

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

		if ((thread_tps > 0) && (xacts >= thread_tps)) {
			/* throttling is on */

		throttle:
			clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
			if ((timer_now.tv_sec > timer_prev.tv_sec + 1) ||
			    ((timer_now.tv_sec == timer_prev.tv_sec + 1) && (timer_now.tv_nsec > timer_prev.tv_nsec))) {
				/* more than 1 second passed, no need to throttle */
				xacts = 0;
				timer_prev.tv_sec = timer_now.tv_sec;
				timer_prev.tv_nsec = timer_now.tv_nsec;
			} else {
				/* 1 second not passed, throttle */
				usleep(1000); /* sleep for 1ms */
				goto throttle;
			}
		} /* throttle */

		/* sequential keys */
		genkey(keystr, i, args->rows, args->key_length + 1);
		/* random values */
		randstr(valstr, args->value_length + 1);

		/* insert (SET) */
		fdb_transaction_set(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)valstr, strlen(valstr));
		stats->ops[OP_INSERT]++;

		/* commit every 100 inserts (default) */
		if (i % args->txnspec.ops[OP_INSERT][OP_COUNT] == 0) {

			if (commit_transaction(transaction, NULL))
				goto FDB_FAIL;

			/* xact latency stats */
			clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
			update_op_stats(&timer_per_xact_start, &timer_per_xact_end, OP_COMMIT, stats);
			stats->ops[OP_COMMIT]++;
			clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_start);

			fdb_transaction_reset(transaction);
			stats->xacts++;
			xacts++; /* for throttling */
		}
	}

	if (commit_transaction(transaction, NULL))
		goto FDB_FAIL;

	/* xact latency stats */
	clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
	update_op_stats(&timer_per_xact_start, &timer_per_xact_end, OP_COMMIT, stats);

	clock_gettime(CLOCK_MONOTONIC, &timer_end);
	stats->xacts++;

	if (args->verbose == VERBOSE_DEBUG) {
		printf("DEBUG: Populated %d rows (%d-%d): %6.3f sec\n",
		       end - begin,
		       begin,
		       end,
		       ((timer_end.tv_sec - timer_start.tv_sec) * 1000000000.0 + timer_end.tv_nsec - timer_start.tv_nsec) /
		           1000000000);
	}

	free(keystr);
	free(valstr);
	return 0;

FDB_FAIL:
	if (keystr)
		free(keystr);
	if (valstr)
		free(valstr);
	fprintf(stderr, "ERROR: FDB failure in populate()\n");
	return -1;
}

int64_t run_op_getreadversion(FDBTransaction* transaction) {
	int64_t rv = 0;
	FDBFuture* f;
	fdb_error_t err;
	int retry = DEFAULT_RETRY_COUNT;

	do {
		f = fdb_transaction_get_read_version(transaction);
		err = wait_future(f);

		if (err) {
			fdb_future_destroy(f);
			f = fdb_transaction_on_error(transaction, err);
			err = wait_future(f);
			fdb_future_destroy(f);
			if (err) {
				/* not retryable */
				break;
			}
		}
	} while (err && retry--);

	if (err) {
		fprintf(stderr, "ERROR: fdb_transaction_get_read_version: %s\n", fdb_get_error(err));
		return -1;
	}

#if FDB_API_VERSION < 620
	err = fdb_future_get_version(f, &rv);
#else
	err = fdb_future_get_int64(f, &rv);
#endif

	if (err) {
#if FDB_API_VERSION < 620
		fprintf(stderr, "ERROR: fdb_future_get_version: %s\n", fdb_get_error(err));
#else
		fprintf(stderr, "ERROR: fdb_future_get_int64: %s\n", fdb_get_error(err));
#endif
	}
	fdb_future_destroy(f);
	return rv;
}

int run_op_get(FDBTransaction* transaction, char* keystr, char* valstr, int snapshot) {
	FDBFuture* f;
	int out_present;
	char* val;
	int vallen;
	fdb_error_t err;
	int retry = DEFAULT_RETRY_COUNT;

	do {
		f = fdb_transaction_get(transaction, (uint8_t*)keystr, strlen(keystr), snapshot);
		err = wait_future(f);

		if (err) {
			fdb_future_destroy(f);
			f = fdb_transaction_on_error(transaction, err);
			err = wait_future(f);
			fdb_future_destroy(f);
			if (err) {
				/* not retryable */
				break;
			}
		}
	} while (err && retry--);

	if (err) {
		fprintf(stderr, "ERROR: fdb_transaction_get: %s\n", fdb_get_error(err));
		return -1;
	}

	err = fdb_future_get_value(f, &out_present, (const uint8_t**)&val, &vallen);
	fdb_future_destroy(f);
	if (err || !out_present) {
		/* error or value not present */
		return -1;
	}
	strncpy(valstr, val, vallen);
	valstr[vallen] = '\0';
	return 0;
}

int run_op_getrange(FDBTransaction* transaction, char* keystr, char* keystr2, char* valstr, int snapshot, int reverse) {
	FDBFuture* f;
	fdb_error_t err;
	FDBKeyValue const* out_kv;
	int out_count;
	int out_more;
	int retry = DEFAULT_RETRY_COUNT;

	do {
		f = fdb_transaction_get_range(transaction,
		                              FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)keystr, strlen(keystr)),
		                              FDB_KEYSEL_LAST_LESS_OR_EQUAL((uint8_t*)keystr2, strlen(keystr2)) + 1,
		                              0 /* limit */,
		                              0 /* target_bytes */,
		                              FDB_STREAMING_MODE_WANT_ALL /* FDBStreamingMode */,
		                              0 /* iteration */,
		                              snapshot,
		                              reverse /* reverse */);
		err = wait_future(f);

		if (err) {
			fdb_future_destroy(f);
			f = fdb_transaction_on_error(transaction, err);
			err = wait_future(f);
			fdb_future_destroy(f);
			if (err) {
				/* not retryable */
				break;
			}
		}
	} while (err && retry--);

	if (err) {
		fprintf(stderr, "ERROR: fdb_transaction_get_range: %s\n", fdb_get_error(err));
		return -1;
	}

	err = fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more);
	if (err) {
		fprintf(stderr, "ERROR: fdb_future_get_keyvalue_array: %s\n", fdb_get_error(err));
		fdb_future_destroy(f);
		return -1;
	}
	fdb_future_destroy(f);
	return 0;
}

int run_op_update(FDBTransaction* transaction, char* keystr, char* valstr) {
	FDBFuture* f;
	int out_present;
	char* val;
	int vallen;
	fdb_error_t err;
	int retry = DEFAULT_RETRY_COUNT;

	/* GET first */
	do {
		f = fdb_transaction_get(transaction, (uint8_t*)keystr, strlen(keystr), 0);
		err = wait_future(f);

		if (err) {
			fdb_future_destroy(f);
			f = fdb_transaction_on_error(transaction, err);
			err = wait_future(f);
			fdb_future_destroy(f);
			if (err) {
				/* not retryable */
				break;
			}
		}
	} while (err && retry--);

	if (err) {
		fprintf(stderr, "ERROR: fdb_transaction_get: %s\n", fdb_get_error(err));
		return -1;
	}

	err = fdb_future_get_value(f, &out_present, (const uint8_t**)&val, &vallen);
	fdb_future_destroy(f);
	if (err || !out_present) {
		/* error or value not present */
		return -1;
	}

	/* Update Value (SET) */
	fdb_transaction_set(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)valstr, strlen(valstr));
	return 0;
}

int run_op_insert(FDBTransaction* transaction, char* keystr, char* valstr) {
	fdb_transaction_set(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)valstr, strlen(valstr));
	return 0;
}

int run_op_clear(FDBTransaction* transaction, char* keystr) {
	fdb_transaction_clear(transaction, (uint8_t*)keystr, strlen(keystr));
	return 0;
}

int run_op_clearrange(FDBTransaction* transaction, char* keystr, char* keystr2) {
	fdb_transaction_clear_range(transaction, (uint8_t*)keystr, strlen(keystr), (uint8_t*)keystr2, strlen(keystr2));
	return 0;
}

/* run one transaction */
int run_transaction(FDBTransaction* transaction,
                    mako_args_t* args,
                    mako_stats_t* stats,
                    char* keystr,
                    char* keystr2,
                    char* valstr) {
	int i;
	int count;
	int rc;
	struct timespec timer_start, timer_end;
	struct timespec timer_per_xact_start, timer_per_xact_end;
	int docommit = 0;
	int keynum;
	int keyend;
	int64_t readversion;
	int randstrlen;
	int rangei;

	/* transaction */
	clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_start);
	for (i = 0; i < MAX_OP; i++) {

		if ((args->txnspec.ops[i][OP_COUNT] > 0) && (i != OP_COMMIT)) {
			for (count = 0; count < args->txnspec.ops[i][OP_COUNT]; count++) {

				/* pick a random key(s) */
				if (args->zipf) {
					keynum = zipfian_next();
				} else {
					keynum = urand(0, args->rows - 1);
				}
				genkey(keystr, keynum, args->rows, args->key_length + 1);

				/* range */
				if (args->txnspec.ops[i][OP_RANGE] > 0) {
					keyend = keynum + args->txnspec.ops[i][OP_RANGE] - 1; /* inclusive */
					if (keyend > args->rows - 1) {
						keyend = args->rows - 1;
					}
					genkey(keystr2, keyend, args->rows, args->key_length + 1);
				}

				if (stats->xacts % args->sampling == 0) {
					/* per op latency */
					clock_gettime(CLOCK_MONOTONIC, &timer_start);
				}

				switch (i) {
				case OP_GETREADVERSION:
					readversion = run_op_getreadversion(transaction);
					if (!readversion) {
						rc = -1;
					}
					break;
				case OP_GET:
					rc = run_op_get(transaction, keystr, valstr, 0);
					break;
				case OP_GETRANGE:
					rc = run_op_getrange(transaction, keystr, keystr2, valstr, 0, args->txnspec.ops[i][OP_REVERSE]);
					break;
				case OP_SGET:
					rc = run_op_get(transaction, keystr, valstr, 1);
					break;
				case OP_SGETRANGE:
					rc = run_op_getrange(transaction, keystr, keystr2, valstr, 1, args->txnspec.ops[i][OP_REVERSE]);
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
						if (rc != 0)
							break;
					}
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
					if (rc == 0) {
						/* commit insert so mutation goes to storage */
						if (commit_transaction(transaction, stats) == 0) {
							clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
							update_op_stats(&timer_per_xact_start, &timer_per_xact_end, OP_COMMIT, stats);
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
						if (rc != 0) {
							/* rollback not necessary, transaction will be reset */
							break;
						}
					}
					/* commit inserts so mutation goes to storage */
					if (commit_transaction(transaction, stats) == 0) {
						clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
						update_op_stats(&timer_per_xact_start, &timer_per_xact_end, OP_COMMIT, stats);
					}
					fdb_transaction_reset(transaction);
					rc = run_op_clearrange(transaction, keystr2, keystr);
					docommit = 1;
					break;
				default:
					fprintf(stderr, "ERROR: Unknown Operation %d\n", i);
					break;
				}

				if (stats->xacts % args->sampling == 0) {
					clock_gettime(CLOCK_MONOTONIC, &timer_end);
					if (rc == 0) {
						/* per op latency */
						update_op_stats(&timer_start, &timer_end, i, stats);
					}
				}

				/* check rc */
				if (rc != 0) {
					stats->errors[i]++;
				} else {
					stats->ops[i]++;
				}
			}
		}
	}
	if (docommit | args->commit_get) {
		if (commit_transaction(transaction, stats) == 0) {
			clock_gettime(CLOCK_MONOTONIC, &timer_per_xact_end);
			update_op_stats(&timer_per_xact_start, &timer_per_xact_end, OP_COMMIT, stats);
		}
	}
	stats->xacts++;

	fdb_transaction_reset(transaction);
	return 0;
}

int run_workload(FDBTransaction* transaction,
                 mako_args_t* args,
                 int thread_tps,
                 int thread_iters,
                 volatile int* signal,
                 mako_stats_t* stats) {
	int xacts = 0;
	int rc = 0;
	struct timespec timer_prev, timer_now;
	char* keystr;
	char* keystr2;
	char* valstr;

	if (thread_tps < 0)
		return 0;

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

	while (1) {

		if ((thread_tps > 0) && (xacts >= thread_tps)) {
			/* throttling is on */

			clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
			if ((timer_now.tv_sec > timer_prev.tv_sec + 1) ||
			    ((timer_now.tv_sec == timer_prev.tv_sec + 1) && (timer_now.tv_nsec > timer_prev.tv_nsec))) {
				/* more than 1 second passed, no need to throttle */
				xacts = 0;
				timer_prev.tv_sec = timer_now.tv_sec;
				timer_prev.tv_nsec = timer_now.tv_nsec;
			} else {
				/* 1 second not passed, throttle */
				usleep(1000);
				continue;
			}
		}

		rc = run_transaction(transaction, args, stats, keystr, keystr2, valstr);
		if (rc) {
			/* should never get here */
			fprintf(stderr, "ERROR: run_transaction failed (%d)\n", rc);
		}

		if (thread_iters > 0) {
			if (thread_iters == xacts) {
				break;
			}
		} else if (*signal == SIGNAL_RED) {
			break;
		}
		xacts++;
	}
	free(keystr);
	free(keystr2);
	free(valstr);

	return rc;
}

/* mako worker thread */
void* worker_thread(void* thread_args) {
	int worker_id = ((thread_args_t*)thread_args)->process->worker_id;
	int thread_id = ((thread_args_t*)thread_args)->thread_id;
	mako_args_t* args = ((thread_args_t*)thread_args)->process->args;
	FDBDatabase* database = ((thread_args_t*)thread_args)->process->database;
	fdb_error_t err;
	int rc;
	FDBTransaction* transaction;
	int thread_tps;
	int thread_iters = 0;
	int op;
	volatile int* signal = &((thread_args_t*)thread_args)->process->shm->signal;
	volatile int* readycount = &((thread_args_t*)thread_args)->process->shm->readycount;
	mako_stats_t* stats = (void*)((thread_args_t*)thread_args)->process->shm + sizeof(mako_shmhdr_t) /* skip header */
	                      + (sizeof(mako_stats_t) * (worker_id * args->num_threads + thread_id));

	/* init latency */
	for (op = 0; op < MAX_OP; op++) {
		stats->latency_us_min[op] = 0xFFFFFFFFFFFFFFFF; /* uint64_t */
		stats->latency_us_max[op] = 0;
		stats->latency_us_total[op] = 0;
	}

	if (args->verbose == VERBOSE_DEBUG) {
		printf("DEBUG: worker_id:%d (%d) thread_id:%d (%d) (tid:%d)\n",
		       worker_id,
		       args->num_processes,
		       thread_id,
		       args->num_threads,
		       (unsigned int)pthread_self());
	}

	if (args->tps) {
		thread_tps = compute_thread_tps(args->tps, worker_id, thread_id, args->num_processes, args->num_threads);
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
		rc = populate(transaction, args, worker_id, thread_id, thread_tps, stats);
		if (rc < 0) {
			fprintf(stderr, "ERROR: populate failed\n");
		}
	}

	/* run the workload */
	else if (args->mode == MODE_RUN) {
		rc = run_workload(transaction, args, thread_tps, thread_iters, signal, stats);
		if (rc < 0) {
			fprintf(stderr, "ERROR: run_workload failed\n");
		}
	}

	/* fall through */
FDB_FAIL:
	fdb_transaction_destroy(transaction);
	pthread_exit(0);
}

/* mako worker process */
int worker_process_main(mako_args_t* args, int worker_id, mako_shmhdr_t* shm) {
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
	process.args = args;
	process.shm = (mako_shmhdr_t*)shm;

	if (args->verbose == VERBOSE_DEBUG) {
		printf("DEBUG: worker %d started\n", worker_id);
	}

	/* Everything starts from here */
	/* Let's use the maximum API version */
	//  fprintf(stderr, "fdb_get_max_api_version: %d\n",
	//  fdb_get_max_api_version());
	err = fdb_select_api_version(fdb_get_max_api_version());
	if (err) {
		fprintf(stderr, "ERROR: Failed at %s:%d (%s)\n", __FILE__, __LINE__, fdb_get_error(err));
		return -1;
	}

	/* enable flatbuffers if specified */
	if (args->flatbuffers) {
#ifdef FDB_NET_OPTION_USE_FLATBUFFERS
		if (args->verbose >= VERBOSE_DEBUG) {
			printf("DEBUG: Using flatbuffers\n");
		}
		err = fdb_network_set_option(FDB_NET_OPTION_USE_FLATBUFFERS, (uint8_t*)&args->flatbuffers, sizeof(uint8_t));
		if (err) {
			fprintf(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_USE_FLATBUFFERS): %s\n", fdb_get_error(err));
		}
#else
		if (args->verbose >= VERBOSE_DEFAULT) {
			printf("INFO: flatbuffers is not supported in FDB API version %d\n", FDB_API_VERSION);
		}
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
		if (args->verbose >= VERBOSE_DEBUG) {
			printf("DEBUG: Enable Tracing (%s)\n",
			       (args->tracepath[0] == '\0') ? "current directory" : args->tracepath);
		}
		err = fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE, (uint8_t*)args->tracepath, strlen(args->tracepath));
		if (err) {
			fprintf(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE): %s\n", fdb_get_error(err));
		}
	}

	/* enable knobs if specified */
	if (args->knobs[0] != '\0') {
		if (args->verbose >= VERBOSE_DEBUG) {
			printf("DEBUG: Setting client konbs: %s\n", args->knobs);
		}
		err = fdb_network_set_option(FDB_NET_OPTION_KNOB, (uint8_t*)args->knobs, strlen(args->knobs));
		if (err) {
			fprintf(stderr, "ERROR: fdb_network_set_option: %s\n", fdb_get_error(err));
		}
	}

	/* Network thread must be setup before doing anything */
	if (args->verbose == VERBOSE_DEBUG) {
		printf("DEBUG: fdb_setup_network\n");
	}
	err = fdb_setup_network();
	if (err) {
		fprintf(stderr, "ERROR: Failed at %s:%d (%s)\n", __FILE__, __LINE__, fdb_get_error(err));
		return -1;
	}

	/* Each worker process will have its own network thread */
	if (args->verbose >= VERBOSE_DEBUG) {
		printf("DEBUG: creating network thread\n");
	}
	rc = pthread_create(&network_thread, NULL, fdb_network_thread, (void*)args);
	if (rc != 0) {
		fprintf(stderr, "ERROR: Cannot create a network thread\n");
		return -1;
	}

	/*** let's party! ***/

	/* set up cluster and datbase for workder threads */

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
	fdb_create_database(args->cluster_file, &process.database);
#endif

	if (args->verbose >= VERBOSE_DEBUG) {
		printf("DEBUG: creating %d worker threads\n", args->num_threads);
	}
	worker_threads = (pthread_t*)calloc(sizeof(pthread_t), args->num_threads);
	if (!worker_threads) {
		fprintf(stderr, "ERROR: cannot allocate worker_threads\n");
		goto EXIT;
	}

	/* spawn worker threads */
	thread_args = (thread_args_t*)calloc(sizeof(thread_args_t), args->num_threads);
	if (!thread_args) {
		fprintf(stderr, "ERROR: cannot allocate thread_args\n");
		goto EXIT;
	}

	for (i = 0; i < args->num_threads; i++) {
		thread_args[i].thread_id = i;
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
		if (args->verbose >= VERBOSE_DEBUG) {
			printf("DEBUG: worker_thread %d joining\n", i);
		}
		rc = pthread_join(worker_threads[i], NULL);
		if (rc != 0) {
			fprintf(stderr, "ERROR: threads %d failed to join\n", i);
		}
	}

EXIT:
	if (worker_threads)
		free(worker_threads);
	if (thread_args)
		free(thread_args);

	/* clean up database and cluster */
	fdb_database_destroy(process.database);
#if FDB_API_VERSION < 610
	fdb_cluster_destroy(cluster);
#endif

	/* stop the network thread */
	if (args->verbose >= VERBOSE_DEBUG) {
		printf("DEBUG: fdb_stop_network\n");
	}
	err = fdb_stop_network();
	check_fdb_error(err);

FDB_FAIL:

	/* wait for the network thread to join */
	if (args->verbose >= VERBOSE_DEBUG) {
		printf("DEBUG: network_thread joining\n");
	}
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
	args->json = 0;
	args->num_processes = 1;
	args->num_threads = 1;
	args->mode = MODE_INVALID;
	args->rows = 10000;
	args->seconds = 0;
	args->iteration = 0;
	args->tps = 0;
	args->sampling = 1000;
	args->key_length = 16;
	args->value_length = 16;
	args->zipf = 0;
	args->commit_get = 0;
	args->verbose = 1;
	args->flatbuffers = 0;
	args->knobs[0] = '\0';
	args->log_group[0] = '\0';
	args->trace = 0;
	args->tracepath[0] = '\0';
	for (i = 0; i < MAX_OP; i++) {
		args->txnspec.ops[i][OP_COUNT] = 0;
	}
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
		} else {
			if (args->verbose == VERBOSE_DEBUG) {
				printf("Error: Invalid transaction spec: %s\n", ptr);
			}
			error = 1;
			break;
		}

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
			printf("DEBUG: OP: %d: %d: %d\n", op, args->txnspec.ops[op][0], args->txnspec.ops[op][1]);
		}
	}

	return 0;
}

void usage() {
	printf("Usage:\n");
	printf("%-24s%s\n", "-h, --help", "Print this message");
	printf("%-24s%s\n", "    --version", "Print FDB version");
	printf("%-24s%s\n", "-v, --verbose", "Specify verbosity");
	printf("%-24s%s\n", "-c, --cluster=FILE", "Specify FDB cluster file");
	printf("%-24s%s\n", "-p, --procs=PROCS", "Specify number of worker processes");
	printf("%-24s%s\n", "-t, --threads=THREADS", "Specify number of worker threads");
	printf("%-24s%s\n", "-r, --rows=ROWS", "Specify number of records");
	printf("%-24s%s\n", "-s, --seconds=SECONDS", "Specify the test duration in seconds\n");
	printf("%-24s%s\n", "", "This option cannot be specified with --iteration.");
	printf("%-24s%s\n", "-i, --iteration=ITERS", "Specify the number of iterations.\n");
	printf("%-24s%s\n", "", "This option cannot be specified with --seconds.");
	printf("%-24s%s\n", "    --keylen=LENGTH", "Specify the key lengths");
	printf("%-24s%s\n", "    --vallen=LENGTH", "Specify the value lengths");
	printf("%-24s%s\n", "-x, --transaction=SPEC", "Transaction specification");
	printf("%-24s%s\n", "    --tps=TPS", "Specify the target TPS");
	printf("%-24s%s\n", "    --sampling=RATE", "Specify the sampling rate for latency stats");
	printf("%-24s%s\n", "-m, --mode=MODE", "Specify the mode (build, run, clean)");
	printf("%-24s%s\n", "-z, --zipf", "Use zipfian distribution instead of uniform distribution");
	printf("%-24s%s\n", "    --commitget", "Commit GETs");
	printf("%-24s %s\n", "   --loggroup=LOGGROUP", "Set client log group");
	printf("%-24s%s\n", "    --trace", "Enable tracing");
	printf("%-24s%s\n", "    --tracepath=PATH", "Set trace file path");
	printf("%-24s%s\n", "    --knobs=KNOBS", "Set client knobs");
	printf("%-24s%s\n", "    --flatbuffers", "Use flatbuffers");
}

/* parse benchmark paramters */
int parse_args(int argc, char* argv[], mako_args_t* args) {
	int rc;
	int c;
	int idx;
	while (1) {
		const char* short_options = "c:p:t:r:s:i:x:v:m:hjz";
		static struct option long_options[] = { /* name, has_arg, flag, val */
			                                    { "cluster", required_argument, NULL, 'c' },
			                                    { "procs", required_argument, NULL, 'p' },
			                                    { "threads", required_argument, NULL, 't' },
			                                    { "rows", required_argument, NULL, 'r' },
			                                    { "seconds", required_argument, NULL, 's' },
			                                    { "iteration", required_argument, NULL, 'i' },
			                                    { "keylen", required_argument, NULL, ARG_KEYLEN },
			                                    { "vallen", required_argument, NULL, ARG_VALLEN },
			                                    { "transaction", required_argument, NULL, 'x' },
			                                    { "tps", required_argument, NULL, ARG_TPS },
			                                    { "sampling", required_argument, NULL, ARG_SAMPLING },
			                                    { "verbose", required_argument, NULL, 'v' },
			                                    { "mode", required_argument, NULL, 'm' },
			                                    { "knobs", required_argument, NULL, ARG_KNOBS },
			                                    { "loggroup", required_argument, NULL, ARG_LOGGROUP },
			                                    { "tracepath", required_argument, NULL, ARG_TRACEPATH },
			                                    /* no args */
			                                    { "help", no_argument, NULL, 'h' },
			                                    { "json", no_argument, NULL, 'j' },
			                                    { "zipf", no_argument, NULL, 'z' },
			                                    { "commitget", no_argument, NULL, ARG_COMMITGET },
			                                    { "flatbuffers", no_argument, NULL, ARG_FLATBUFFERS },
			                                    { "trace", no_argument, NULL, ARG_TRACE },
			                                    { "version", no_argument, NULL, ARG_VERSION },
			                                    { NULL, 0, NULL, 0 }
		};
		idx = 0;
		c = getopt_long(argc, argv, short_options, long_options, &idx);
		if (c < 0)
			break;
		switch (c) {
		case '?':
		case 'h':
			usage();
			return -1;
		case 'c':
			strcpy(args->cluster_file, optarg);
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
			args->tps = atoi(optarg);
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
		case ARG_TRACE:
			args->trace = 1;
			break;
		case ARG_TRACEPATH:
			args->trace = 1;
			memcpy(args->tracepath, optarg, strlen(optarg) + 1);
			break;
		}
	}
	return 0;
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
	}
	return 0;
}

/* stats output formatting */
#define STR2(x) #x
#define STR(x) STR2(x)
#define STATS_TITLE_WIDTH 12
#define STATS_FIELD_WIDTH 12

void print_stats(mako_args_t* args, mako_stats_t* stats, struct timespec* now, struct timespec* prev) {
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
	double durationns = (now->tv_sec - prev->tv_sec) * 1000000000.0 + (now->tv_nsec - prev->tv_nsec);

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
	printf("%" STR(STATS_TITLE_WIDTH) "s ", "OPS");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0) {
			printf("%" STR(STATS_FIELD_WIDTH) "lld ", ops_total[op] - ops_total_prev[op]);
			errors_diff[op] = errors_total[op] - errors_total_prev[op];
			print_err = (errors_diff[op] > 0);
			ops_total_prev[op] = ops_total[op];
			errors_total_prev[op] = errors_total[op];
		}
	}
	/* TPS */
	printf("%" STR(STATS_FIELD_WIDTH) ".2f ", (totalxacts - totalxacts_prev) * 1000000000.0 / durationns);
	totalxacts_prev = totalxacts;

	/* Conflicts */
	printf("%" STR(STATS_FIELD_WIDTH) ".2f\n", (conflicts - conflicts_prev) * 1000000000.0 / durationns);
	conflicts_prev = conflicts;

	if (print_err) {
		printf("%" STR(STATS_TITLE_WIDTH) "s ", "Errors");
		for (op = 0; op < MAX_OP; op++) {
			if (args->txnspec.ops[op][OP_COUNT] > 0) {
				printf("%" STR(STATS_FIELD_WIDTH) "lld ", errors_diff[op]);
			}
		}
		printf("\n");
	}
	return;
}

void print_stats_header(mako_args_t* args) {
	int op;
	int i;

	/* header */
	for (i = 0; i <= STATS_TITLE_WIDTH; i++)
		printf(" ");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0) {
			switch (op) {
			case OP_GETREADVERSION:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "GRV");
				break;
			case OP_GET:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "GET");
				break;
			case OP_GETRANGE:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "GETRANGE");
				break;
			case OP_SGET:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "SGET");
				break;
			case OP_SGETRANGE:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "SGETRANGE");
				break;
			case OP_UPDATE:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "UPDATE");
				break;
			case OP_INSERT:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "INSERT");
				break;
			case OP_INSERTRANGE:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "INSERTRANGE");
				break;
			case OP_CLEAR:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "CLEAR");
				break;
			case OP_SETCLEAR:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "SETCLEAR");
				break;
			case OP_CLEARRANGE:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "CLEARRANGE");
				break;
			case OP_SETCLEARRANGE:
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "SETCLRRANGE");
				break;
			}
		}
	}
	printf("%" STR(STATS_FIELD_WIDTH) "s ", "TPS");
	printf("%" STR(STATS_FIELD_WIDTH) "s\n", "Conflicts/s");

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
	/* TPS */
	for (i = 0; i < STATS_FIELD_WIDTH; i++)
		printf("=");
	printf(" ");
	/* Conflicts */
	for (i = 0; i < STATS_FIELD_WIDTH; i++)
		printf("=");
	printf("\n");
}

void print_report(mako_args_t* args, mako_stats_t* stats, struct timespec* timer_now, struct timespec* timer_start) {
	int i, j, op;
	uint64_t totalxacts = 0;
	uint64_t conflicts = 0;
	uint64_t totalerrors = 0;
	uint64_t ops_total[MAX_OP] = { 0 };
	uint64_t errors_total[MAX_OP] = { 0 };
	uint64_t lat_min[MAX_OP] = { 0 };
	uint64_t lat_total[MAX_OP] = { 0 };
	uint64_t lat_samples[MAX_OP] = { 0 };
	uint64_t lat_max[MAX_OP] = { 0 };

	uint64_t durationns =
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
				if ((args->txnspec.ops[op][OP_COUNT] > 0) || (op == OP_COMMIT)) {
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
	printf("\n====== Total Duration %6.3f sec ======\n\n", (double)durationns / 1000000000);
	printf("Total Processes:  %8d\n", args->num_processes);
	printf("Total Threads:    %8d\n", args->num_threads);
	printf("Target TPS:       %8d\n", args->tps);
	printf("Total Xacts:      %8lld\n", totalxacts);
	printf("Total Conflicts:  %8lld\n", conflicts);
	printf("Total Errors:     %8lld\n", totalerrors);
	printf("Overall TPS:      %8lld\n\n", totalxacts * 1000000000 / durationns);

	/* per-op stats */
	print_stats_header(args);

	/* OPS */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Total OPS");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 && op != OP_COMMIT) {
			printf("%" STR(STATS_FIELD_WIDTH) "lld ", ops_total[op]);
		}
	}
	/* TPS */
	printf("%" STR(STATS_FIELD_WIDTH) ".2f ", totalxacts * 1000000000.0 / durationns);
	/* Conflicts */
	printf("%" STR(STATS_FIELD_WIDTH) ".2f\n", conflicts * 1000000000.0 / durationns);

	/* Errors */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Errors");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 && op != OP_COMMIT) {
			printf("%" STR(STATS_FIELD_WIDTH) "lld ", errors_total[op]);
		}
	}
	printf("\n");

	/* Min Latency */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Lat Min (us)");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_COMMIT) {
			if (lat_min[op] == -1) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "lld ", lat_min[op]);
			}
		}
	}
	printf("\n");

	/* Avg Latency */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Lat Avg (us)");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_COMMIT) {
			if (lat_total[op]) {
				printf("%" STR(STATS_FIELD_WIDTH) "lld ", lat_total[op] / lat_samples[op]);
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			}
		}
	}
	printf("\n");

	/* Max Latency */
	printf("%-" STR(STATS_TITLE_WIDTH) "s ", "Lat Max (us)");
	for (op = 0; op < MAX_OP; op++) {
		if (args->txnspec.ops[op][OP_COUNT] > 0 || op == OP_COMMIT) {
			if (lat_max[op] == 0) {
				printf("%" STR(STATS_FIELD_WIDTH) "s ", "N/A");
			} else {
				printf("%" STR(STATS_FIELD_WIDTH) "lld ", lat_max[op]);
			}
		}
	}
	printf("\n");
}

int stats_process_main(mako_args_t* args, mako_stats_t* stats, volatile int* signal) {
	struct timespec timer_start, timer_prev, timer_now;

	/* wait until the signal turn on */
	while (*signal == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	if (args->verbose >= VERBOSE_DEFAULT)
		print_stats_header(args);

	clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_start);
	timer_prev.tv_sec = timer_start.tv_sec;
	timer_prev.tv_nsec = timer_start.tv_nsec;
	while (*signal != SIGNAL_RED) {
		usleep(100000); /* sleep for 100ms */
		clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
		/* roughly 1 sec */
		if (timer_now.tv_sec > timer_prev.tv_sec) {
			if (args->verbose >= VERBOSE_DEFAULT)
				print_stats(args, stats, &timer_now, &timer_prev);
			timer_prev.tv_sec = timer_now.tv_sec;
			timer_prev.tv_nsec = timer_now.tv_nsec;
		}
	}

	/* print report */
	if (args->verbose >= VERBOSE_DEFAULT) {
		clock_gettime(CLOCK_MONOTONIC_COARSE, &timer_now);
		print_report(args, stats, &timer_now, &timer_start);
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

	/* create the shared memory for stats */
	sprintf(shmpath, "mako%d", getpid());
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
		goto EXIT;
	}

	/* map it */
	shm = (mako_shmhdr_t*)mmap(NULL, shmsize, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0);
	if (shm == MAP_FAILED) {
		fprintf(stderr, "ERROR: mmap (fd:%d size:%llu) failed\n", shmfd, (unsigned long long)shmsize);
		goto EXIT;
	}

	stats = (mako_stats_t*)((void*)shm + sizeof(mako_shmhdr_t));

	/* initialize the shared memory */
	memset(shm, 0, shmsize);

	/* get ready */
	shm->signal = SIGNAL_OFF;
	shm->readycount = 0;

	/* fork worker processes */
	worker_pids = (pid_t*)calloc(sizeof(pid_t), args.num_processes + 1);
	if (!worker_pids) {
		fprintf(stderr, "ERROR: cannot allocate worker_pids (%d processes)\n", args.num_processes);
		goto EXIT;
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
		worker_process_main(&args, worker_id, shm);
		/* worker can exit here */
		exit(0);
	} else if (proc_type == proc_stats) {
		/* stats */
		if (args.mode == MODE_CLEAN) {
			/* no stats needed for clean mode */
			exit(0);
		}
		stats_process_main(&args, stats, &shm->signal);
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

EXIT:

	if (worker_pids)
		free(worker_pids);

	if (shm != MAP_FAILED)
		munmap(shm, shmsize);

	if (shmfd) {
		close(shmfd);
		shm_unlink(shmpath);
	}

	return 0;
}
