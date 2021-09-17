#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <cstdlib> //std::system
#include "sys/epoll.h"
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <unistd.h>
#include <fcntl.h>
#include <thread>
#include <sys/un.h>

#define SERVER_PATH "/tmp/server"
#define MAX_EVENTS 300

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

void report(const char* msg, int terminate) {
	perror(msg);
	if (terminate)
		exit(-1); /* failure */
}

int main(int argc, char* argv[]) {

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

		printf("Ready for client connect().\n");

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

		// epoll wait
		int event_count;
        uint64_t u;
		struct epoll_event event, events[MAX_EVENTS];
		int epoll_fd = epoll_create1(0);

		if (epoll_fd == -1) {
			fprintf(stderr, "Failed to create epoll file descriptor\n");
			return 1;
		}

		int event_fd = eventfd(0, 0);
		if (event_fd == -1) {
			fprintf(stderr, "Failed to create event fd\n");
			return 1;
		}

		event.events = EPOLLIN;// | EPOLLET;
		event.data.fd = event_fd;

		if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, event_fd, &event)) {
			fprintf(stderr, "Failed to add file descriptor to epoll\n");
			printf("Error: %s\n", strerror(errno));
		} else {
			sendfd(connectSocket, event_fd);
			printf("\nPolling for input...\n");
			event_count = epoll_wait(epoll_fd, events, MAX_EVENTS, 10000);
			printf("%d ready events\n", event_count);
			// for (int i = 0; i < event_count; i++) {
			// 	printf("Reading file descriptor '%d' -- ", events[i].data.fd);
			// 	bytes_read = read(events[i].data.fd, read_buffer, READ_SIZE);
			// 	printf("%zd bytes read.\n", bytes_read);
			// 	read_buffer[bytes_read] = '\0';
			// 	printf("Read '%s'\n", read_buffer);
			// }
            int s = read(event_fd , &u, sizeof(uint64_t));
            if (s != sizeof(uint64_t)) {
                fprintf(stderr, "Failed to read from event fd\n");
            }

		    printf("Event fd read %ld from efd\n", u);
		}
        close(event_fd);
		close(epoll_fd);

		/********************************************************************/
		/* Program complete                                                 */
		/********************************************************************/

	} while (0);

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
}