/* SPDX-License-Identifier: MIT */
/*
 * Description: test that sigfd reading/polling works. A regression test for
 * the upstream commit:
 *
 * fd7d6de22414 ("io_uring: don't recurse on tsk->sighand->siglock with signalfd")
 */
#include <unistd.h>
#include <sys/signalfd.h>
#include <sys/epoll.h>
#include <sys/poll.h>
#include <stdio.h>
#include "liburing.h"

static int setup_signal(void)
{
	sigset_t mask;
	int sfd;

	sigemptyset(&mask);
	sigaddset(&mask, SIGINT);

	sigprocmask(SIG_BLOCK, &mask, NULL);
	sfd = signalfd(-1, &mask, SFD_NONBLOCK);
	if (sfd < 0)
		perror("signalfd");
	return sfd;
}

static int test_uring(int sfd)
{
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;
	struct io_uring ring;
	int ret;

	io_uring_queue_init(32, &ring, 0);

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_poll_add(sqe, sfd, POLLIN);
	io_uring_submit(&ring);

	kill(getpid(), SIGINT);

	io_uring_wait_cqe(&ring, &cqe);
	if (cqe->res & POLLIN) {
		ret = 0;
	} else {
		fprintf(stderr, "Unexpected poll mask %x\n", cqe->res);
		ret = 1;
	}
	io_uring_cqe_seen(&ring, cqe);
	io_uring_queue_exit(&ring);
	return ret;
}

int main(int argc, char *argv[])
{
	int sfd, ret;

	if (argc > 1)
		return 0;

	sfd = setup_signal();
	if (sfd < 0)
		return 1;

	ret = test_uring(sfd);
	if (ret)
		fprintf(stderr, "test_uring signalfd failed\n");

	close(sfd);
	return ret;
}
