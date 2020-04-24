/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <rte_common.h>
#include <rte_random.h>
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
	0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255,
	256, 257, 320, 384, 511, 512, 513, 1023, 1024, 1025, 1518, 1522, 1600,
	2048, 3072, 4096, 5120, 6144, 7168, 8192
};
/* MUST be as large as largest packet size above */
#define SMALL_BUFFER_SIZE       8192
#else /* TEST_VALUE_RANGE != 0 */
static size_t buf_sizes[TEST_VALUE_RANGE];
#define SMALL_BUFFER_SIZE       TEST_VALUE_RANGE
#endif /* TEST_VALUE_RANGE == 0 */

/* Data is aligned on this many bytes (power of 2) */
#define ALIGNMENT_UNIT          32


/*
 * Create two buffers, and initialise one with random values. These are copied
 * to the second buffer and then compared to see if the copy was successful.
 * The bytes outside the copied area are also checked to make sure they were not
 * changed.
 */
static int
test_single_memcpy(unsigned int off_src, unsigned int off_dst, size_t size)
{
	unsigned int i;
	uint8_t dest[SMALL_BUFFER_SIZE + ALIGNMENT_UNIT];
	uint8_t src[SMALL_BUFFER_SIZE + ALIGNMENT_UNIT];
	void * ret;

	/* Setup buffers */
	for (i = 0; i < SMALL_BUFFER_SIZE + ALIGNMENT_UNIT; i++) {
		dest[i] = 0;
		src[i] = (uint8_t) rte_rand();
	}

	/* Do the copy */
	ret = rte_memcpy(dest + off_dst, src + off_src, size);
	if (ret != (dest + off_dst)) {
		printf("rte_memcpy() returned %p, not %p\n",
		       ret, dest + off_dst);
	}

	/* Check nothing before offset is affected */
	for (i = 0; i < off_dst; i++) {
		if (dest[i] != 0) {
			printf("rte_memcpy() failed for %u bytes (offsets=%u,%u): "
			       "[modified before start of dst].\n",
			       (unsigned)size, off_src, off_dst);
			return -1;
		}
	}

	/* Check everything was copied */
	for (i = 0; i < size; i++) {
		if (dest[i + off_dst] != src[i + off_src]) {
			printf("rte_memcpy() failed for %u bytes (offsets=%u,%u): "
			       "[didn't copy byte %u].\n",
			       (unsigned)size, off_src, off_dst, i);
			return -1;
		}
	}

	/* Check nothing after copy was affected */
	for (i = size; i < SMALL_BUFFER_SIZE; i++) {
		if (dest[i + off_dst] != 0) {
			printf("rte_memcpy() failed for %u bytes (offsets=%u,%u): "
			       "[copied too many].\n",
			       (unsigned)size, off_src, off_dst);
			return -1;
		}
	}
	return 0;
}

/*
 * Check functionality for various buffer sizes and data offsets/alignments.
 */
static int
func_test(void)
{
	unsigned int off_src, off_dst, i;
	unsigned int num_buf_sizes = sizeof(buf_sizes) / sizeof(buf_sizes[0]);
	int ret;

	for (off_src = 0; off_src < ALIGNMENT_UNIT; off_src++) {
		for (off_dst = 0; off_dst < ALIGNMENT_UNIT; off_dst++) {
			for (i = 0; i < num_buf_sizes; i++) {
				ret = test_single_memcpy(off_src, off_dst,
				                         buf_sizes[i]);
				if (ret != 0)
					return -1;
			}
		}
	}
	return 0;
}

static int
test_memcpy(void)
{
	int ret;

	ret = func_test();
	if (ret != 0)
		return -1;
	return 0;
}

REGISTER_TEST_COMMAND(memcpy_autotest, test_memcpy);
