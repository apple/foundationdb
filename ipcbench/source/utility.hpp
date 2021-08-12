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
	message() {
		// default message
		memset(data, '*', 100);
	}
};

template <typename T>
using alloc = bip::allocator<T, bip::managed_shared_memory::segment_manager>;

using message_alloc = alloc<message>;

using message_queue = boost::lockfree::spsc_queue<bip::offset_ptr<char>, boost::lockfree::capacity<10>>;
// using message_queue = std::queue<bip::offset_ptr<char>>;

bench_t now();

} // namespace shm