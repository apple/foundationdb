/*
 * linux_kaio.h
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

// AIO system calls

typedef struct io_context* io_context_t;

enum { IO_CMD_PREAD = 0, IO_CMD_PWRITE = 1, IO_CMD_FSYNC = 2, IO_CMD_FDSYNC = 3 };

struct linux_iocb {
	void* data;
	uint32_t key, unused;

	uint16_t aio_lio_opcode;
	uint16_t aio_reqprio;
	uint32_t aio_fildes;

	void* buf;
	uint64_t nbytes;
	int64_t offset;
	uint64_t unused2;
	uint32_t flags;
	uint32_t eventfd;
};

struct linux_ioresult {
	void* data;
	linux_iocb* iocb;
	unsigned long result;
	unsigned long result2;
};

static int io_setup(unsigned nr_events, io_context_t* ctxp) {
	return syscall(__NR_io_setup, nr_events, ctxp);
}
static int io_submit(io_context_t ctx_id, long nrstruct, linux_iocb** iocbpp) {
	return syscall(__NR_io_submit, ctx_id, nrstruct, iocbpp);
}
static int io_getevents(io_context_t ctx_id, long min_nr, long nr, linux_ioresult* events, struct timespec* timeout) {
	return syscall(__NR_io_getevents, ctx_id, min_nr, nr, events, timeout);
}
