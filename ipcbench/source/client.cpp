#include <cstdint>
#include <cstdio>
#include <netdb.h>

#include <netinet/in.h>
#include <unistd.h>

#include <iostream>

/**************************************************************************/
/* Header files needed for this sample program                            */
/**************************************************************************/
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

/**************************************************************************/
/* Constants used by this program                                         */
/**************************************************************************/
#define SERVER_PATH "/tmp/server"
#define BUFFER_LENGTH 250
#define FALSE 0
#define MAX_LINE 100

#define LINE_ARRAY_SIZE (MAX_LINE + 1)

using namespace std;

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
	if (cmsgp == NULL) {
		fprintf(stderr, "Debug: %d, %d\n", msgh.msg_controllen, sizeof(struct cmsghdr));
		return -1;
	}

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

int main(int argc, char* argv[]) {

	/***********************************************************************/
	/* Variable and structure definitions.                                 */
	/***********************************************************************/
	int sd = -1, rc;
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
		if (argc > 1)
			strcpy(serveraddr.sun_path, argv[1]);
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

		// write to the server
		uint64_t num = 0;
		std::cout << "Number to send: ";
		std::cin >> num;
		/********************************************************************/
		/* Send bytes of a's to the server                              */
		/********************************************************************/
		int event_fd = recvfd(sd);

		int ret = write(event_fd, &num, sizeof(num));
		if (ret < 0) {
			fprintf(stderr, "write event fd fail\n");
			printf("Error: %s\n", strerror(errno));
		}
		close(event_fd);

	} while (FALSE);

	/***********************************************************************/
	/* Close down any open socket descriptors                              */
	/***********************************************************************/
	if (sd != -1)
		close(sd);
}