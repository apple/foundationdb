#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/bind.hpp>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <cstdlib> //std::system
#include "thread"
#include "sys/epoll.h"
#include <sys/socket.h>

#include "utility.hpp"

/**************************************************************************/
/* Constants used by this program                                         */
/**************************************************************************/
#define SERVER_PATH "/tmp/server"
#define READ_SIZE 1024
#define MAX_EVENTS 300
#define FALSE 0

using namespace boost::interprocess;
namespace ipc = boost::interprocess;

volatile sig_atomic_t stopped = 0;

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
	stopped = 1;
}

void nullCompletionHandler() {}

void sleep_handler(const boost::system::error_code& error, int signum, boost::asio::io_context* context) {
	if (!error && signum == SIGUSR2) {
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

static int recvfd(int sockfd) {
	struct msghdr msgh;
	struct iovec iov;
	int data, fd;
	ssize_t nr;

	/* Allocate a char buffer for the ancillary data. See the comments
	   in sendfd() */
	union {
		char buf[CMSG_SPACE(sizeof(int))];
		struct cmsghdr align;
	} controlMsg;
	struct cmsghdr* cmsgp;

	/* The 'msg_name' field can be used to obtain the address of the
	   sending socket. However, we do not need this information. */

	msgh.msg_name = NULL;
	msgh.msg_namelen = 0;

	/* Specify buffer for receiving real data */

	msgh.msg_iov = &iov;
	msgh.msg_iovlen = 1;
	iov.iov_base = &data; /* Real data is an 'int' */
	iov.iov_len = sizeof(int);

	/* Set 'msghdr' fields that describe ancillary data */

	msgh.msg_control = controlMsg.buf;
	msgh.msg_controllen = sizeof(controlMsg.buf);

	/* Receive real plus ancillary data; real data is ignored */

	nr = recvmsg(sockfd, &msgh, 0);
	if (nr < 0) {
		fprintf(stderr, "Failed to receive message\n");
		printf("Error: %s\n", strerror(errno));
	} else {
		printf("Received bytes: %d\n", nr);
	}

	cmsgp = CMSG_FIRSTHDR(&msgh);

	/* Check the validity of the 'cmsghdr' */

	if (cmsgp == NULL || cmsgp->cmsg_len != CMSG_LEN(sizeof(int)) || cmsgp->cmsg_level != SOL_SOCKET ||
	    cmsgp->cmsg_type != SCM_RIGHTS) {
		errno = EINVAL;
		fprintf(stderr,
		        "Error: invalid cmsghdr, %s, %s, %s, %s\n",
		        cmsgp == NULL ? "y" : "n",
		        cmsgp->cmsg_len != CMSG_LEN(sizeof(int)) ? "y" : "n",
		        cmsgp->cmsg_level != SOL_SOCKET ? "y" : "n",
		        cmsgp->cmsg_type != SCM_RIGHTS ? "y" : "n");
		return -1;
	}

	/* Return the received file descriptor to our caller */

	memcpy(&fd, CMSG_DATA(cmsgp), sizeof(int));
	return fd;
}

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
	signal(SIGINT, stop_handler);

	// create event fd
	int event_fd = eventfd(0, 0);
	if (event_fd == -1) {
		fprintf(stderr, "Failed to create event fd\n");
		return 1;
	}

	/***********************************************************************/
	/* Variable and structure definitions.                                 */
	/***********************************************************************/
	int sd = -1, connectSocket = -1, rc;
	struct sockaddr_un serveraddr;
	/***********************************************************************/
	/* A do/while(FALSE) loop is used to make error cleanup easier.  The   */
	/* close() of each of the socket descriptors is only done once at the  */
	/* very end of the program.                                            */
	/***********************************************************************/
	do {
		/********************************************************************/
		/* The socket() function returns a socket descriptor, which represents   */
		/* an endpoint.  The statement also identifies that the UNIX        */
		/* address family with the stream transport (SOCK_STREAM) will be   */
		/* used for this socket.                                            */
		/********************************************************************/
		sd = socket(AF_UNIX, SOCK_STREAM, 0);
		if (sd < 0) {
			perror("socket() failed");
			break;
		}

		/********************************************************************/
		/* After the socket descriptor is created, a bind() function gets a */
		/* unique name for the socket.                                      */
		/********************************************************************/
		memset(&serveraddr, 0, sizeof(serveraddr));
		serveraddr.sun_family = AF_UNIX;
		strcpy(serveraddr.sun_path, SERVER_PATH);

		rc = bind(sd, (struct sockaddr*)&serveraddr, SUN_LEN(&serveraddr));
		if (rc < 0) {
			perror("bind() failed");
			break;
		}

		/********************************************************************/
		/* The listen() function allows the server to accept incoming       */
		/* client connections.  In this example, the backlog is set to 10.  */
		/* This means that the system will queue 10 incoming connection     */
		/* requests before the system starts rejecting the incoming         */
		/* requests.                                                        */
		/********************************************************************/
		rc = listen(sd, 10);
		if (rc < 0) {
			perror("listen() failed");
			break;
		}

		printf("Ready for producer to connect...\n");

		/********************************************************************/
		/* The server uses the accept() function to accept an incoming      */
		/* connection request.  The accept() call will block indefinitely   */
		/* waiting for the incoming connection to arrive.                   */
		/********************************************************************/
		socklen_t clientAddressLength;
		struct sockaddr_in clientAddress;
		connectSocket = accept(sd, (struct sockaddr*)&clientAddress, &clientAddressLength);
		if (connectSocket < 0) {
			perror("accept() failed");
			break;
		}

		std::cout << "Connected to " << inet_ntoa(clientAddress.sin_addr) << std::endl;

		// send the fd to producer
		sendfd(connectSocket, event_fd);

		/********************************************************************/
		/* Program complete                                                 */
		/********************************************************************/

	} while (FALSE);

	/***********************************************************************/
	/* Close down any open socket descriptors                              */
	/***********************************************************************/
	if (sd != -1)
		close(sd);

	if (connectSocket != -1)
		close(connectSocket);

	/***********************************************************************/
	/* Remove the UNIX path name from the file system                      */
	/***********************************************************************/
	unlink(SERVER_PATH);

	// main process : epoll wait
	int event_count;
	struct epoll_event event, events[MAX_EVENTS];
	int epoll_fd = epoll_create1(0);

	if (epoll_fd == -1) {
		fprintf(stderr, "Failed to create epoll file descriptor\n");
		return 1;
	}

	event.events = EPOLLIN;// | EPOLLET;
	event.data.fd = event_fd;

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_fd, &event)) {
		fprintf(stderr, "Failed to add file descriptor to epoll\n");
		printf("Error: %s\n", strerror(errno));
		return 1;
	}

	std::atomic_int count = 0;
	std::atomic_ullong latency = 0;
	std::thread traceT{ trace, 1000, &count, &latency };
	uint64_t num;

	int size = std::stoi(argv[1]);
	// this will throw error if memory not exists
	managed_shared_memory segment(open_only, "MySharedMemory");

	shm::message_queue* request_queue;
	// Find the message queue using the name
	try {
		request_queue = segment.find<shm::message_queue>("request_queue").first;
	} catch (ipc::interprocess_exception& e) {
		printf("Error %s\n", e.what());
		return 0;
	}
	// sleeping flag
	std::atomic_bool* isSleeping = segment.find<std::atomic_bool>("sleeping_flag").first;

	void* buffer = malloc(size * sizeof(char));

	while (!stopped) {
		// ptr to the reply message
		offset_ptr<shm::message> ptr;
		for (auto i = 0; i < 1; ++i) {
			if (request_queue->pop(ptr)) {
				memcpy(buffer, ptr->data, size);
				latency.fetch_add(shm::now() - ptr->start_time);
				count.fetch_add(1);
			}
		}
		// sleep
		isSleeping->store(true);
		// pull once to avoid deadlock
		if (request_queue->pop(ptr)) {
			memcpy(buffer, ptr->data, size);
			latency.fetch_add(shm::now() - ptr->start_time);
			count.fetch_add(1);
		}
		event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, 50000);
		if (stopped)
			break;
		int s = read(event_fd, &num, sizeof(uint64_t));
		if (s != sizeof(uint64_t)) {
			fprintf(stderr, "Failed to read from event fd-%d\n", event_fd);
		}
		isSleeping->store(false);
	}
	traceT.join();
	free(buffer);
	close(epoll_fd);
	close(event_fd);

	std::cout << "Consumer finished.\n";
}