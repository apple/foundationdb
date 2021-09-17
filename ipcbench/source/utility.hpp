#include <boost/interprocess/offset_ptr.hpp>
#include <cstring>
#include "boost/lockfree/spsc_queue.hpp"
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/named_condition.hpp>
#include <queue>

namespace bip = boost::interprocess;

namespace shm {

typedef unsigned long long bench_t;

struct message {
	char data[100];
	bench_t start_time;
	message() {
		// default message
		memset(data, '*', 100);
		start_time = 0;
	}
};

template <typename T>
using alloc = bip::allocator<T, bip::managed_shared_memory::segment_manager>;

using message_alloc = alloc<message>;

using message_queue = boost::lockfree::spsc_queue<bip::offset_ptr<message>, boost::lockfree::capacity<10>>;
// using message_queue = std::queue<bip::offset_ptr<char>>;

bench_t now() {
	struct timespec ts;
	timespec_get(&ts, TIME_UTC);
	return ts.tv_sec * 1e9 + ts.tv_nsec;
};

int recvfd(int sockfd);

void sendfd(int sockfd, int fd);

} // namespace shm