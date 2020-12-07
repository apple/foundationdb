/* SPDX-License-Identifier: MIT */
#ifndef LIBURING_SYSCALL_H
#define LIBURING_SYSCALL_H

#include <signal.h>

struct io_uring_params;

/*
 * System calls
 */
extern int __sys_io_uring_setup(unsigned entries, struct io_uring_params *p);
extern int __sys_io_uring_enter(int fd, unsigned to_submit,
	unsigned min_complete, unsigned flags, sigset_t *sig);
extern int __sys_io_uring_enter2(int fd, unsigned to_submit,
	unsigned min_complete, unsigned flags, sigset_t *sig, int sz);
extern int __sys_io_uring_register(int fd, unsigned int opcode, const void *arg,
	unsigned int nr_args);

#endif
