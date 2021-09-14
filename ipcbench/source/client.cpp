#include <cstdio>
#include <netdb.h>

#include <netinet/in.h>

#include <sys/socket.h>
#include <unistd.h>

#include <iostream>

#define MAX_LINE 100

#define LINE_ARRAY_SIZE (MAX_LINE + 1)

using namespace std;

// int receiveFD(int socket) // receive fd from socket
// {
// 	struct msghdr msg = { 0 };
// 	char m_buffer[256];
// 	memset(m_buffer, '\0', sizeof(m_buffer));
// 	struct iovec io = { .iov_base = m_buffer, .iov_len = sizeof(m_buffer) };
// 	msg.msg_iov = &io;
// 	msg.msg_iovlen = 1;

// 	char buf[CMSG_SPACE(sizeof(int))];
// 	msg.msg_control = buf;
// 	msg.msg_controllen = sizeof(buf);

// 	int bytes = recvmsg(socket, &msg, 0);
// 	if (bytes < 0) {
// 		fprintf(stderr, "Failed to receive message\n");
// 		printf("Error: %s\n", strerror(errno));
// 	} else {
// 		printf("Received bytes: %d\n", bytes);
// 	}

// 	struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);

//     if (cmsg == NULL) {
//         fprintf(stderr, "Error: invalid cmsg\n");
//     }

// 	printf("About to extract fd\n");
// 	int fd;
// 	memcpy(&fd, (int*)CMSG_DATA(cmsg), sizeof(int));
// 	printf("Extracted fd %d\n", fd);

// 	return fd;
// }

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
        fprintf(stderr, "Debug 1, %d, %d\n", msgh.msg_controllen, sizeof(struct cmsghdr));
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

int main()

{

	int socketDescriptor;

	unsigned short int serverPort = 6000;

	struct sockaddr_in serverAddress;

	struct hostent* hostInfo;

	char buf[LINE_ARRAY_SIZE], c;

	cout << "Enter server host name or IP address: ";

	cin.get(buf, MAX_LINE, '\n');

	// gethostbyname() takes a host name or ip address in "numbers and

	// dots" notation, and returns a pointer to a hostent structure,

	// which we'll need later.  It's not important for us what this

	// structure is actually composed of.

	hostInfo = gethostbyname(buf);

	if (hostInfo == NULL) {

		cout << "problem interpreting host: " << buf << "\n";

		exit(1);
	}

	cin.get(c); // dispose of the newline

	// Create a socket.  "AF_INET" means it will use the IPv4 protocol.

	// "SOCK_STREAM" means it will be a reliable connection (i.e., TCP;

	// for UDP use SOCK_DGRAM), and I'm not sure what the 0 for the last

	// parameter means, but it seems to work.

	socketDescriptor = socket(AF_INET, SOCK_STREAM, 0);

	if (socketDescriptor < 0) {

		cerr << "cannot create socket\n";

		exit(1);
	}

	// Connect to server.  First we have to set some fields in the

	// serverAddress structure.  The system will assign me an arbitrary

	// local port that is not in use.

	serverAddress.sin_family = hostInfo->h_addrtype;

	memcpy((char*)&serverAddress.sin_addr.s_addr,

	       hostInfo->h_addr_list[0],
	       hostInfo->h_length);

	serverAddress.sin_port = htons(serverPort);

	if (connect(socketDescriptor,

	            (struct sockaddr*)&serverAddress,

	            sizeof(serverAddress)) < 0) {

		cerr << "cannot connect\n";

		exit(1);
	}

	int eventFD = recvfd(socketDescriptor);

	// Prompt the user for input, then read in the input, up to MAX_LINE

	// charactars, and then dispose of the rest of the line, including

	// the newline character.

	cout << "Input: ";

	cin.get(buf, MAX_LINE, '\n');

	int count = 0;

	while (cin.get(c) && c != '\n') {
		++count;
	}

	int ret = write(eventFD, &count, sizeof(count));
	if (ret < 0) {
		fprintf(stderr, "write event fd fail\n");
		printf("Error: %s\n", strerror(errno));
	}

	close(eventFD);
	close(socketDescriptor);

	return 0;
}