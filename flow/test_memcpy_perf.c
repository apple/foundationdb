/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_random.h>
#include <rte_malloc.h>

#include <rte_memcpy.h>

#include "test.h"

/*
 * Set this to the maximum buffer size you want to test. If it is 0, then the
 * values in the buf_sizes[] array below will be used.
 */
#define TEST_VALUE_RANGE        0

/* List of buffer sizes to test */
#if TEST_VALUE_RANGE == 0
static size_t buf_sizes[] = {
	1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128,
	129, 191, 192, 193, 255, 256, 257, 319, 320, 321, 383, 384, 385, 447, 448,
	449, 511, 512, 513, 767, 768, 769, 1023, 1024, 1025, 1518, 1522, 1536, 1600,
	2048, 2560, 3072, 3584, 4096, 4608, 5120, 5632, 6144, 6656, 7168, 7680, 8192
};
/* MUST be as large as largest packet size above */
#define SMALL_BUFFER_SIZE       8192
#else /* TEST_VALUE_RANGE != 0 */
static size_t buf_sizes[TEST_VALUE_RANGE];
#define SMALL_BUFFER_SIZE       TEST_VALUE_RANGE
#endif /* TEST_VALUE_RANGE == 0 */


/*
 * Arrays of this size are used for measuring uncached memory accesses by
 * picking a random location within the buffer. Make this smaller if there are
 * memory allocation errors.
 */
#define LARGE_BUFFER_SIZE       (100 * 1024 * 1024)

/* How many times to run timing loop for performance tests */
#define TEST_ITERATIONS         1000000
#define TEST_BATCH_SIZE         100

/* Data is aligned on this many bytes (power of 2) */
#ifdef RTE_MACHINE_CPUFLAG_AVX512F
#define ALIGNMENT_UNIT          64
#elif defined RTE_MACHINE_CPUFLAG_AVX2
#define ALIGNMENT_UNIT          32
#else /* RTE_MACHINE_CPUFLAG */
#define ALIGNMENT_UNIT          16
#endif /* RTE_MACHINE_CPUFLAG */

/*
 * Pointers used in performance tests. The two large buffers are for uncached
 * access where random addresses within the buffer are used for each
 * memcpy. The two small buffers are for cached access.
 */
static uint8_t *large_buf_read, *large_buf_write;
static uint8_t *small_buf_read, *small_buf_write;

/* Initialise data buffers. */
static int
init_buffers(void)
{
	unsigned i;

	large_buf_read = rte_malloc("memcpy", LARGE_BUFFER_SIZE + ALIGNMENT_UNIT, ALIGNMENT_UNIT);
	if (large_buf_read == NULL)
		goto error_large_buf_read;

	large_buf_write = rte_malloc("memcpy", LARGE_BUFFER_SIZE + ALIGNMENT_UNIT, ALIGNMENT_UNIT);
	if (large_buf_write == NULL)
		goto error_large_buf_write;

	small_buf_read = rte_malloc("memcpy", SMALL_BUFFER_SIZE + ALIGNMENT_UNIT, ALIGNMENT_UNIT);
	if (small_buf_read == NULL)
		goto error_small_buf_read;

	small_buf_write = rte_malloc("memcpy", SMALL_BUFFER_SIZE + ALIGNMENT_UNIT, ALIGNMENT_UNIT);
	if (small_buf_write == NULL)
		goto error_small_buf_write;

	for (i = 0; i < LARGE_BUFFER_SIZE; i++)
		large_buf_read[i] = rte_rand();
	for (i = 0; i < SMALL_BUFFER_SIZE; i++)
		small_buf_read[i] = rte_rand();

	return 0;

error_small_buf_write:
	rte_free(small_buf_read);
error_small_buf_read:
	rte_free(large_buf_write);
error_large_buf_write:
	rte_free(large_buf_read);
error_large_buf_read:
	printf("ERROR: not enough memory\n");
	return -1;
}

/* Cleanup data buffers */
static void
free_buffers(void)
{
	rte_free(large_buf_read);
	rte_free(large_buf_write);
	rte_free(small_buf_read);
	rte_free(small_buf_write);
}

/*
 * Get a random offset into large array, with enough space needed to perform
 * max copy size. Offset is aligned, uoffset is used for unalignment setting.
 */
static inline size_t
get_rand_offset(size_t uoffset)
{
	return ((rte_rand() % (LARGE_BUFFER_SIZE - SMALL_BUFFER_SIZE)) &
			~(ALIGNMENT_UNIT - 1)) + uoffset;
}

/* Fill in source and destination addresses. */
static inline void
fill_addr_arrays(size_t *dst_addr, int is_dst_cached, size_t dst_uoffset,
				 size_t *src_addr, int is_src_cached, size_t src_uoffset)
{
	unsigned int i;

	for (i = 0; i < TEST_BATCH_SIZE; i++) {
		dst_addr[i] = (is_dst_cached) ? dst_uoffset : get_rand_offset(dst_uoffset);
		src_addr[i] = (is_src_cached) ? src_uoffset : get_rand_offset(src_uoffset);
	}
}

/*
 * WORKAROUND: For some reason the first test doing an uncached write
 * takes a very long time (~25 times longer than is expected). So we do
 * it once without timing.
 */
static void
do_uncached_write(uint8_t *dst, int is_dst_cached,
				  const uint8_t *src, int is_src_cached, size_t size)
{
	unsigned i, j;
	size_t dst_addrs[TEST_BATCH_SIZE], src_addrs[TEST_BATCH_SIZE];

	for (i = 0; i < (TEST_ITERATIONS / TEST_BATCH_SIZE); i++) {
		fill_addr_arrays(dst_addrs, is_dst_cached, 0,
						 src_addrs, is_src_cached, 0);
		for (j = 0; j < TEST_BATCH_SIZE; j++) {
			rte_memcpy(dst+dst_addrs[j], src+src_addrs[j], size);
		}
	}
}

/*
 * Run a single memcpy performance test. This is a macro to ensure that if
 * the "size" parameter is a constant it won't be converted to a variable.
 */
#define SINGLE_PERF_TEST(dst, is_dst_cached, dst_uoffset,                   \
                         src, is_src_cached, src_uoffset, size)             \
do {                                                                        \
    unsigned int iter, t;                                                   \
    size_t dst_addrs[TEST_BATCH_SIZE], src_addrs[TEST_BATCH_SIZE];          \
    uint64_t start_time, total_time = 0;                                    \
    uint64_t total_time2 = 0;                                               \
    for (iter = 0; iter < (TEST_ITERATIONS / TEST_BATCH_SIZE); iter++) {    \
        fill_addr_arrays(dst_addrs, is_dst_cached, dst_uoffset,             \
                         src_addrs, is_src_cached, src_uoffset);            \
        start_time = rte_rdtsc();                                           \
        for (t = 0; t < TEST_BATCH_SIZE; t++)                               \
            rte_memcpy(dst+dst_addrs[t], src+src_addrs[t], size);           \
        total_time += rte_rdtsc() - start_time;                             \
    }                                                                       \
    for (iter = 0; iter < (TEST_ITERATIONS / TEST_BATCH_SIZE); iter++) {    \
        fill_addr_arrays(dst_addrs, is_dst_cached, dst_uoffset,             \
                         src_addrs, is_src_cached, src_uoffset);            \
        start_time = rte_rdtsc();                                           \
        for (t = 0; t < TEST_BATCH_SIZE; t++)                               \
            memcpy(dst+dst_addrs[t], src+src_addrs[t], size);               \
        total_time2 += rte_rdtsc() - start_time;                            \
    }                                                                       \
    printf("%3.0f -", (double)total_time  / TEST_ITERATIONS);                 \
    printf("%3.0f",   (double)total_time2 / TEST_ITERATIONS);                 \
    printf("(%6.2f%%) ", ((double)total_time - total_time2)*100/total_time2); \
} while (0)

/* Run aligned memcpy tests for each cached/uncached permutation */
#define ALL_PERF_TESTS_FOR_SIZE(n)                                       \
do {                                                                     \
    if (__builtin_constant_p(n))                                         \
        printf("\nC%6u", (unsigned)n);                                   \
    else                                                                 \
        printf("\n%7u", (unsigned)n);                                    \
    SINGLE_PERF_TEST(small_buf_write, 1, 0, small_buf_read, 1, 0, n);    \
    SINGLE_PERF_TEST(large_buf_write, 0, 0, small_buf_read, 1, 0, n);    \
    SINGLE_PERF_TEST(small_buf_write, 1, 0, large_buf_read, 0, 0, n);    \
    SINGLE_PERF_TEST(large_buf_write, 0, 0, large_buf_read, 0, 0, n);    \
} while (0)

/* Run unaligned memcpy tests for each cached/uncached permutation */
#define ALL_PERF_TESTS_FOR_SIZE_UNALIGNED(n)                             \
do {                                                                     \
    if (__builtin_constant_p(n))                                         \
        printf("\nC%6u", (unsigned)n);                                   \
    else                                                                 \
        printf("\n%7u", (unsigned)n);                                    \
    SINGLE_PERF_TEST(small_buf_write, 1, 1, small_buf_read, 1, 5, n);    \
    SINGLE_PERF_TEST(large_buf_write, 0, 1, small_buf_read, 1, 5, n);    \
    SINGLE_PERF_TEST(small_buf_write, 1, 1, large_buf_read, 0, 5, n);    \
    SINGLE_PERF_TEST(large_buf_write, 0, 1, large_buf_read, 0, 5, n);    \
} while (0)

/* Run memcpy tests for constant length */
#define ALL_PERF_TEST_FOR_CONSTANT                                      \
do {                                                                    \
    TEST_CONSTANT(6U); TEST_CONSTANT(64U); TEST_CONSTANT(128U);         \
    TEST_CONSTANT(192U); TEST_CONSTANT(256U); TEST_CONSTANT(512U);      \
    TEST_CONSTANT(768U); TEST_CONSTANT(1024U); TEST_CONSTANT(1536U);    \
} while (0)

/* Run all memcpy tests for aligned constant cases */
static inline void
perf_test_constant_aligned(void)
{
#define TEST_CONSTANT ALL_PERF_TESTS_FOR_SIZE
	ALL_PERF_TEST_FOR_CONSTANT;
#undef TEST_CONSTANT
}

/* Run all memcpy tests for unaligned constant cases */
static inline void
perf_test_constant_unaligned(void)
{
#define TEST_CONSTANT ALL_PERF_TESTS_FOR_SIZE_UNALIGNED
	ALL_PERF_TEST_FOR_CONSTANT;
#undef TEST_CONSTANT
}

/* Run all memcpy tests for aligned variable cases */
static inline void
perf_test_variable_aligned(void)
{
	unsigned n = sizeof(buf_sizes) / sizeof(buf_sizes[0]);
	unsigned i;
	for (i = 0; i < n; i++) {
		ALL_PERF_TESTS_FOR_SIZE((size_t)buf_sizes[i]);
	}
}

/* Run all memcpy tests for unaligned variable cases */
static inline void
perf_test_variable_unaligned(void)
{
	unsigned n = sizeof(buf_sizes) / sizeof(buf_sizes[0]);
	unsigned i;
	for (i = 0; i < n; i++) {
		ALL_PERF_TESTS_FOR_SIZE_UNALIGNED((size_t)buf_sizes[i]);
	}
}

/* Run all memcpy tests */
static int
perf_test(void)
{
	int ret;
	struct timeval tv_begin, tv_end;
	double time_aligned, time_unaligned;
	double time_aligned_const, time_unaligned_const;

	ret = init_buffers();
	if (ret != 0)
		return ret;

#if TEST_VALUE_RANGE != 0
	/* Set up buf_sizes array, if required */
	unsigned i;
	for (i = 0; i < TEST_VALUE_RANGE; i++)
		buf_sizes[i] = i;
#endif

	/* See function comment */
	do_uncached_write(large_buf_write, 0, small_buf_read, 1, SMALL_BUFFER_SIZE);

	printf("\n** rte_memcpy() - memcpy perf. tests (C = compile-time constant) **\n"
		   "======= ================= ================= ================= =================\n"
		   "   Size   Cache to cache     Cache to mem      Mem to cache        Mem to mem\n"
		   "(bytes)          (ticks)          (ticks)           (ticks)           (ticks)\n"
		   "------- ----------------- ----------------- ----------------- -----------------");

	printf("\n================================= %2dB aligned =================================",
		ALIGNMENT_UNIT);
	/* Do aligned tests where size is a variable */
	gettimeofday(&tv_begin, NULL);
	perf_test_variable_aligned();
	gettimeofday(&tv_end, NULL);
	time_aligned = (double)(tv_end.tv_sec - tv_begin.tv_sec)
		+ ((double)tv_end.tv_usec - tv_begin.tv_usec)/1000000;
	printf("\n------- ----------------- ----------------- ----------------- -----------------");
	/* Do aligned tests where size is a compile-time constant */
	gettimeofday(&tv_begin, NULL);
	perf_test_constant_aligned();
	gettimeofday(&tv_end, NULL);
	time_aligned_const = (double)(tv_end.tv_sec - tv_begin.tv_sec)
		+ ((double)tv_end.tv_usec - tv_begin.tv_usec)/1000000;
	printf("\n================================== Unaligned ==================================");
	/* Do unaligned tests where size is a variable */
	gettimeofday(&tv_begin, NULL);
	perf_test_variable_unaligned();
	gettimeofday(&tv_end, NULL);
	time_unaligned = (double)(tv_end.tv_sec - tv_begin.tv_sec)
		+ ((double)tv_end.tv_usec - tv_begin.tv_usec)/1000000;
	printf("\n------- ----------------- ----------------- ----------------- -----------------");
	/* Do unaligned tests where size is a compile-time constant */
	gettimeofday(&tv_begin, NULL);
	perf_test_constant_unaligned();
	gettimeofday(&tv_end, NULL);
	time_unaligned_const = (double)(tv_end.tv_sec - tv_begin.tv_sec)
		+ ((double)tv_end.tv_usec - tv_begin.tv_usec)/1000000;
	printf("\n======= ================= ================= ================= =================\n\n");

	printf("Test Execution Time (seconds):\n");
	printf("Aligned variable copy size   = %8.3f\n", time_aligned);
	printf("Aligned constant copy size   = %8.3f\n", time_aligned_const);
	printf("Unaligned variable copy size = %8.3f\n", time_unaligned);
	printf("Unaligned constant copy size = %8.3f\n", time_unaligned_const);
	free_buffers();

	return 0;
}

static int
test_memcpy_perf(void)
{
	int ret;

	ret = perf_test();
	if (ret != 0)
		return -1;
	return 0;
}

REGISTER_TEST_COMMAND(memcpy_perf_autotest, test_memcpy_perf);
