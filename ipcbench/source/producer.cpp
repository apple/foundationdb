#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/sync/named_condition.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <stdlib.h>
#include <string>
#include "utility.hpp"
#include <cstdlib> //std::system
#include <thread>
#include <signal.h>

#define SERVER_PATH "/tmp/server"
#define FALSE 0
#define MAX_EVENTS 300

using namespace boost::interprocess;

volatile sig_atomic_t stopped = 0;

void my_handler(int s) {
	stopped = 1;
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

// Main function. For producer
int main(int argc, char* argv[]) {

	signal(SIGINT, my_handler);

	std::atomic_int kill_count = 0;
	int waiting_interval = 0;
	if (argc == 2)
		waiting_interval = std::stoi(argv[1]);

	// Remove shared memory on construction and destruction
	struct shm_remove {
		shm_remove() { shared_memory_object::remove("MySharedMemory"); }
		~shm_remove() { shared_memory_object::remove("MySharedMemory"); }
	} remover;

	// Create a new segment with given name and size
	managed_shared_memory segment(create_only, "MySharedMemory", 65536);
	// Create two lock-free queue with given name
	// size is given as 10
	// here we are doing a ping-pong test so the size if okay
	shm::message_queue* request_queue = segment.construct<shm::message_queue>("request_queue")();
	// shm::message_queue* latency_queue = segment.construct<shm::message_queue>("latency_queue")();

	// create a flag to indicate whether the consumer is sleeping
	std::atomic_bool* sleepingFlag = segment.construct<std::atomic_bool>("sleeping_flag")();
	sleepingFlag->store(false);

	shm::bench_t last_print_time = shm::now();

	shm::message* msg = static_cast<shm::message*>(segment.allocate(sizeof(shm::message)));

	int producer_fd = eventfd(0, 0);
	if (producer_fd == -1) {
		fprintf(stderr, "Failed to create event fd\n");
		return 1;
	}

	/***********************************************************************/
	/* Variable and structure definitions.                                 */
	/***********************************************************************/
	int sd = -1, rc, event_fd;
	struct sockaddr_un serveraddr;

	/***********************************************************************/
	/* A do/while(FALSE) loop is used to make error cleanup easier.  The   */
	/* close() of the socket descriptor is only done once at the very end  */
	/* of the program.                                                     */
	/***********************************************************************/
	do {
		/********************************************************************/
		/* The socket() function returns a socket descriptor, which represents   */
		/* an endpoint.  The statement also identifies that the UNIX  */
		/* address family with the stream transport (SOCK_STREAM) will be   */
		/* used for this socket.                                            */
		/********************************************************************/
		sd = socket(AF_UNIX, SOCK_STREAM, 0);
		if (sd < 0) {
			perror("socket() failed");
			break;
		}

		/********************************************************************/
		/* If an argument was passed in, use this as the server, otherwise  */
		/* use the #define that is located at the top of this program.      */
		/********************************************************************/
		memset(&serveraddr, 0, sizeof(serveraddr));
		serveraddr.sun_family = AF_UNIX;
		if (argc > 2)
			strcpy(serveraddr.sun_path, argv[2]);
		else
			strcpy(serveraddr.sun_path, SERVER_PATH);

		/********************************************************************/
		/* Use the connect() function to establish a connection to the      */
		/* server.                                                          */
		/********************************************************************/
		rc = connect(sd, (struct sockaddr*)&serveraddr, SUN_LEN(&serveraddr));
		if (rc < 0) {
			perror("connect() failed");
			break;
		}

		event_fd = recvfd(sd);
		sendfd(sd, producer_fd);

	} while (FALSE);

	/***********************************************************************/
	/* Close down any open socket descriptors                              */
	/***********************************************************************/
	if (sd != -1)
		close(sd);
	
	// main process : epoll wait
	int event_count;
	struct epoll_event event, events[MAX_EVENTS];
	int epoll_fd = epoll_create1(0);

	if (epoll_fd == -1) {
		fprintf(stderr, "Failed to create epoll file descriptor\n");
		return 1;
	}

	event.events = EPOLLIN;// | EPOLLET;
	event.data.fd = producer_fd;

	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, producer_fd, &event)) {
		fprintf(stderr, "Failed to add file descriptor to epoll\n");
		printf("Error: %s\n", strerror(errno));
		return 1;
	}

	uint64_t num = 19950811;
	while (!stopped) {
		if (waiting_interval)
			std::this_thread::sleep_for(std::chrono::microseconds(waiting_interval));
		else {
			// somehow the busy loop will die quickly
			// add a very short sleep can avoid it
			// 10 nano seconds is small enough to be ignored considering the latency is ~10us
			// std::this_thread::sleep_for(std::chrono::nanoseconds(10));
		}
		// write the message
		memset(msg->data, '-', 100);
		msg->start_time = shm::now();
		while (!request_queue->push(msg) && !stopped) {
			// fails to push, just retry
		};
		if (sleepingFlag->load()) {
			int ret = write(event_fd, &num, sizeof(num));
			if (ret < 0) {
				fprintf(stderr, "write event fd fail\n");
				printf("Error: %s\n", strerror(errno));
				break;
			}
			++kill_count;
			if (shm::now() - last_print_time > 1e9) {
				printf("%f wake/seconds\n", kill_count * 1e9 / (shm::now() - last_print_time));
				last_print_time = shm::now();
				kill_count = 0;
			}
		}
		event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, 50000);
	}
	close(event_fd);
	close(producer_fd);
	close(epoll_fd);

	segment.deallocate(msg);
	// When done, destroy the queues from the segment
	segment.destroy<shm::message>("request_queue");
	segment.destroy<std::atomic_bool>("sleeping_flag");
	printf("Producer destroyed.\n");
}