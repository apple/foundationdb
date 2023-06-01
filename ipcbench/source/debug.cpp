#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <unistd.h>

#define handle_error(msg)                                                                                              \
	do {                                                                                                               \
		perror(msg);                                                                                                   \
		exit(EXIT_FAILURE);                                                                                            \
	} while (0)

int main(int argc, char* argv[]) {
	int efd, j, ret;
	uint64_t u;
	ssize_t s;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <num1> <num2>...\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	efd = eventfd(0, 0);
	if (efd == -1)
		handle_error("eventfd");

	switch (fork()) {
	case 0:
		for (j = 1; j < argc; j++) {
			printf("Child writing %s to efd\n", argv[j]);
			u = strtoull(argv[j], NULL, 0);
			s = write(efd, &u, sizeof(uint64_t));
			if (s != sizeof(uint64_t))
				handle_error("write");
		}
		printf("Child completed write loop\n");

		exit(EXIT_SUCCESS);

	default:
		struct epoll_event events[10];

		int ep_fd = epoll_create(1024);
		if (ep_fd < 0) {
			handle_error("epoll_create fail: ");
		}

		struct epoll_event read_event;

		read_event.events = EPOLLHUP | EPOLLERR | EPOLLIN;
		read_event.data.fd = efd;

		ret = epoll_ctl(ep_fd, EPOLL_CTL_ADD, efd, &read_event);
		if (ret < 0) {
			handle_error("epoll ctl failed:");
		}
		ret = epoll_wait(ep_fd, events, 30, 30000);

		printf("Parent about to read\n");
		s = read(efd, &u, sizeof(uint64_t));
		if (s != sizeof(uint64_t))
			handle_error("read");

		printf("Parent read %llu (0x%llx) from efd\n", (unsigned long long)u, (unsigned long long)u);
		exit(EXIT_SUCCESS);
	}
}