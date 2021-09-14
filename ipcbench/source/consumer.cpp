#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/bind.hpp>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <cstdlib> //std::system
#include "flow/Error.h"
#include "thread"
#include <signal.h>
#include "sys/epoll.h"
#include <sys/eventfd.h>
#include <sys/socket.h>

#include "utility.hpp"

#define READ_SIZE 1024
#define MAX_EVENTS 300

using namespace boost::interprocess;

volatile sig_atomic_t stopped = 0;
volatile sig_atomic_t started = 0;

void trace(int trace_period_milliseconds, std::atomic_int* counter, std::atomic_ullong* latency) {
	shm::bench_t prev_time = shm::now();
	// shm::bench_t prev_latency = latency->load();
	int prev_counter = counter->load();
	while (!stopped) {
		auto count = counter->load() - prev_counter;
		printf("Rate: %.3e messages/second\n", count * 1e9 / (shm::now() - prev_time));
		printf("Roundtrip latency(Avg): %.3f us\n", latency->load() / 1e3 / counter->load());
		// clear samples, recalculate metrics in the next time window
		counter->store(0);
		latency->store(0);
		prev_counter = counter->load();
		prev_time = shm::now();
		// sleep
		std::this_thread::sleep_for(std::chrono::milliseconds(trace_period_milliseconds));
	}
}

void stop_handler(int s) {
	started = 1;
	stopped = 1;
}

void start_handler(int s) {
	started = 1;
}

void nullCompletionHandler() {}

void sleep_handler(const boost::system::error_code& error, int signum, boost::asio::io_context* context) {
	if (!error && signum == SIGUSR2) {
		// printf("Received SIGUSR2!\n");
		context->post(nullCompletionHandler);
	}
}

int closeEpollFD(int epoll_fd) {
	if (close(epoll_fd)) {
		fprintf(stderr, "Failed to close epoll file descriptor\n");
		return 1;
	}
	return 0;
}

// void sendFD(int socket, int fd) // send fd by socket
// {
// 	struct msghdr msg = { 0 };
// 	char buf[CMSG_SPACE(sizeof(fd))];
// 	memset(buf, '\0', sizeof(buf));
// 	char m_buffer[128];
// 	memset(m_buffer, '\0', sizeof(m_buffer));
// 	struct iovec io = { .iov_base = m_buffer, .iov_len = sizeof(m_buffer) };

// 	msg.msg_iov = &io;
// 	msg.msg_iovlen = 1;
// 	msg.msg_control = buf;
// 	msg.msg_controllen = sizeof(buf);

// 	struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
// 	cmsg->cmsg_level = SOL_SOCKET;
// 	cmsg->cmsg_type = SCM_RIGHTS;
// 	cmsg->cmsg_len = CMSG_LEN(sizeof(fd));

// 	// *((int *) CMSG_DATA(cmsg)) = fd;
// 	memcpy((int*)CMSG_DATA(cmsg), &fd, sizeof(int));

// 	int bytes = sendmsg(socket, &msg, 0);
// 	if (bytes < 0) {
// 		fprintf(stderr, "Failed to send FD\n");
// 		printf("Error: %s\n", strerror(errno));
// 	} else {
// 		printf("Event fd:%d, send bytes: %d\n", fd, bytes);
// 	}
// }

static void sendfd(int sockfd, int fd) {
	struct msghdr msgh;
	struct iovec iov;
	int data;
	struct cmsghdr* cmsgp;

	/* Allocate a char array of suitable size to hold the ancillary data.
	   However, since this buffer is in reality a 'struct cmsghdr', use a
	   union to ensure that it is suitably aligned. */
	union {
		char buf[CMSG_SPACE(sizeof(int))];
		/* Space large enough to hold an 'int' */
		struct cmsghdr align;
	} controlMsg;

	/* The 'msg_name' field can be used to specify the address of the
	   destination socket when sending a datagram. However, we do not
	   need to use this field because 'sockfd' is a connected socket. */

	msgh.msg_name = NULL;
	msgh.msg_namelen = 0;

	/* On Linux, we must transmit at least one byte of real data in
	   order to send ancillary data. We transmit an arbitrary integer
	   whose value is ignored by recvfd(). */

	msgh.msg_iov = &iov;
	msgh.msg_iovlen = 1;
	iov.iov_base = &data;
	iov.iov_len = sizeof(int);
	data = 12345;

	/* Set 'msghdr' fields that describe ancillary data */
	msgh.msg_control = controlMsg.buf;
	msgh.msg_controllen = sizeof(controlMsg.buf);
	printf("Size: %d\n", msgh.msg_controllen);

	/* Set up ancillary data describing file descriptor to send */

	cmsgp = CMSG_FIRSTHDR(&msgh);
	cmsgp->cmsg_level = SOL_SOCKET;
	cmsgp->cmsg_type = SCM_RIGHTS;
	cmsgp->cmsg_len = CMSG_LEN(sizeof(int));
	memcpy(CMSG_DATA(cmsgp), &fd, sizeof(int));

	/* Send real plus ancillary data */
	int bytes = sendmsg(sockfd, &msgh, 0);
	if (bytes < 0) {
		fprintf(stderr, "Failed to send FD\n");
		printf("Error: %s\n", strerror(errno));
	} else {
		printf("Event fd:%d, send bytes: %d\n", fd, bytes);
	}
}

// Consumer, receive the request and send back the reply
int main(int argc, char* argv[]) {
	// signal(SIGINT, stop_handler);
	// signal(SIGUSR1, start_handler);

	// while (!started) {
	// 	std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	// }
	// printf("Received signal from producer to start\n");

	// sigset_t mask;
	// sigemptyset(&mask);
	// sigaddset(&mask, SIGTERM);
	// sigaddset(&mask, SIGINT);
	// int r = sigprocmask(SIG_BLOCK, &mask, 0);
	// if (r == -1) {
	// 	fprintf(stderr, "Failed: sigprocmask\n");
	// 	return 1;
	// }
	int event_fd = eventfd(0, 0);
	if (event_fd == -1) {
		fprintf(stderr, "Failed: event_fd\n");
		return 1;
	}

	int event_count;
	size_t bytes_read;
	char read_buffer[READ_SIZE + 1];
	struct epoll_event event, events[MAX_EVENTS];
	int epoll_fd = epoll_create1(0);

	if (epoll_fd == -1) {
		fprintf(stderr, "Failed to create epoll file descriptor\n");
		return 1;
	}

	event.events = EPOLLHUP | EPOLLERR | EPOLLIN;
	event.data.fd = event_fd;

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_fd, &event)) {
		fprintf(stderr, "Failed to add file descriptor to epoll\n");
		printf("Error: %s\n", strerror(errno));
		return closeEpollFD(epoll_fd);
	}

	int listenSocket, connectSocket;

	unsigned short int listenPort = 6000;

	socklen_t clientAddressLength;

	struct sockaddr_in clientAddress, serverAddress;

	// Create socket for listening for client connection requests.

	listenSocket = socket(AF_INET, SOCK_STREAM, 0);

	if (listenSocket < 0) {
		fprintf(stderr, "cannot create listen socket\n");
		closeEpollFD(epoll_fd);
		close(event_fd);
		return 1;
	}

	// Bind listen socket to listen port.  First set various fields in

	// the serverAddress structure, then call bind().

	// htonl() and htons() convert long integers and short integers

	// (respectively) from host byte order (on x86 this is Least

	// Significant Byte first) to network byte order (Most Significant

	// Byte first).

	serverAddress.sin_family = AF_INET;

	serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);

	serverAddress.sin_port = htons(listenPort);

	if (bind(listenSocket,

	         (struct sockaddr*)&serverAddress,

	         sizeof(serverAddress)) < 0) {

		fprintf(stderr, "cannot bind socket\n");
		closeEpollFD(epoll_fd);
		close(event_fd);
		return 1;
	}

	// Wait for connections from clients.

	// This is a non-blocking call; i.e., it registers this program with

	// the system as expecting connections on this socket, and then

	// this thread of execution continues on.

	listen(listenSocket, 5);

	while (1) {

		std::cout << "Waiting for TCP connection on port " << listenPort << " ...\n";

		// Accept a connection with a client that is requesting one.  The

		// accept() call is a blocking call; i.e., this thread of

		// execution stops until a connection comes in.

		// connectSocket is a new socket that the system provides,

		// separate from listenSocket.  We *could* accept more

		// connections on listenSocket, before connectSocket is closed,

		// but this program doesn't do that.

		clientAddressLength = sizeof(clientAddress);

		connectSocket = accept(listenSocket,

		                       (struct sockaddr*)&clientAddress,

		                       &clientAddressLength);

		if (connectSocket < 0) {
			fprintf(stderr, "cannot accept connection\n");
			closeEpollFD(epoll_fd);
			close(event_fd);
			return 1;
		}

		// Show the IP address of the client.

		// inet_ntoa() converts an IP address from binary form to the

		// standard "numbers and dots" notation.

		std::cout << "  connected to " << inet_ntoa(clientAddress.sin_addr);

		// Show the client's port number.

		// ntohs() converts a short int from network byte order (which is

		// Most Significant Byte first) to host byte order (which on x86,

		// for example, is Least Significant Byte first).

		std::cout << ":" << ntohs(clientAddress.sin_port) << "\n";
		break;
	}

	sendfd(connectSocket, event_fd);

	printf("\nPolling for input...\n");
	event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, 30000);
	printf("%d ready events\n", event_count);
	for (int i = 0; i < event_count; i++) {
		printf("Reading file descriptor '%d' -- ", events[i].data.fd);
		bytes_read = read(events[i].data.fd, read_buffer, READ_SIZE);
		printf("%zd bytes read.\n", bytes_read);
		read_buffer[bytes_read] = '\0';
		printf("Read '%s'\n", read_buffer);
	}

	closeEpollFD(epoll_fd);
	close(event_fd);

	// std::atomic_int count = 0;
	// std::atomic_ullong latency = 0;
	// std::thread traceT{ trace, 1000, &count, &latency };

	// wake up if stopped
	// std::thread stopThread([&context] {
	// 	while (true) {
	// 		if (stopped) {
	// 			context.post(nullCompletionHandler);
	// 			break;
	// 		}
	// 		// check every 1 second
	// 		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	// 	}
	// });

	// int size = std::stoi(argv[1]);
	// // this will throw error if memory not exists
	// managed_shared_memory segment(open_only, "MySharedMemory");

	// shm::message_queue* request_queue;
	// // Find the message queue using the name
	// try {
	// 	request_queue = segment.find<shm::message_queue>("request_queue").first;
	// } catch (Error& e) {
	// 	printf("Error\n");
	// 	return 0;
	// }
	// // sleeping flag
	// std::atomic_bool* isSleeping = segment.find<std::atomic_bool>("sleeping_flag").first;

	// char* reply = static_cast<char*>(segment.allocate(size * sizeof(char)));
	// void* buffer = malloc(size * sizeof(char));

	// while (!stopped) {
	// 	// ptr to the reply message
	// 	offset_ptr<shm::message> ptr;
	// 	bool poped = false;
	// 	for (auto i = 0; i < 100000; ++i) {
	// 		if (request_queue->pop(ptr)) {
	// 			// printf("Consumer pops once\n");
	// 			poped = true;
	// 			break;
	// 		}
	// 	}
	// 	// read the request
	// 	if (poped) {
	// 		memcpy(buffer, ptr->data, size);
	// 		latency.fetch_add(shm::now() - ptr->start_time);
	// 		count.fetch_add(1);
	// 		poped = false;
	// 	}
	// 	// Start an asynchronous wait for one of the signals to occur.
	// 	// signals.async_wait(boost::bind(sleep_handler, _1, _2, &context));
	// 	// sleep
	// 	isSleeping->store(true);
	// 	// pull once to avoid deadlock
	// 	if (request_queue->pop(ptr)) {
	// 		memcpy(buffer, ptr->data, size);
	// 		latency.fetch_add(shm::now() - ptr->start_time);
	// 		count.fetch_add(1);
	// 	}
	// 	// context.run_one();
	// 	isSleeping->store(false);
	// }
	// segment.deallocate(reply);
	// // stopThread.join();
	// traceT.join();
	// free(buffer);
	std::cout << "Consumer finished.\n";
}