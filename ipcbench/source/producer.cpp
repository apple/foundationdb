#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/sync/named_condition.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <chrono>
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

using namespace boost::interprocess;

volatile sig_atomic_t stopped = 0;

void my_handler(int s) {
	// printf("Producer: catch ctrl-C\n");
	stopped = 1;
}

// Main function. For producer
int main(int argc, char* argv[]) {

	signal(SIGINT, my_handler);

	int size = std::stoi(argv[1]);
	int waiting_interval = 0;
	if (argc == 3)
		waiting_interval = std::stoi(argv[2]);

	// Remove shared memory on construction and destruction
	struct shm_remove {
		shm_remove() {
			shared_memory_object::remove("MySharedMemory");
			named_mutex::remove("consumer_mutex");
			named_condition::remove("consumer_cond");
		}
		~shm_remove() {
			shared_memory_object::remove("MySharedMemory");
			named_mutex::remove("consumer_mutex");
			named_condition::remove("consumer_cond");
		}
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
	named_mutex mutex(open_or_create, "consumer_mutex");
	named_condition cond(open_or_create, "consumer_cond");
	printf("Created cond and mutex\n");

	shm::message* msg = static_cast<shm::message*>(segment.allocate(sizeof(shm::message)));
	// char* msg = static_cast<char*>(segment.allocate(size*sizeof(char)));
	void* buffer = malloc(size * sizeof(char));
	while (!stopped) {
		if (waiting_interval)
			std::this_thread::sleep_for(std::chrono::milliseconds(waiting_interval));
		else {
			// somehow the busy loop will die quickly
			// add a very short sleep can avoid it
			// 10 nano seconds is small enough to be ignored considering the latency is ~10us
			std::this_thread::sleep_for(std::chrono::nanoseconds(10));
		}
		// shm::message* msg = static_cast<shm::message*>(segment.allocate(sizeof(shm::message)));
		// write the message
		memset(msg->data, '-', 100);
		msg->start_time = shm::now();
		while (!request_queue->push(msg) && !stopped) {
			// fails to push, just retry
			// printf("Retry once\n");
		};
		// printf("Producer pushed once\n");
		if (sleepingFlag->load()) {
			// printf("Notify once\n");
			cond.notify_all();
		}
		// while (!reply_queue->pop(ptr) && !stopped) {
		// 	// fails to pop, just retry
		// }
		// printf("Producer sleep\n");
		// reply_queue->pop(ptr);
		// read the reply
		// memcpy(buffer, ptr.get(), size);
	}
	segment.deallocate(msg);
	free(buffer);
	// When done, destroy the queues from the segment
	segment.destroy<shm::message>("request_queue");
	// segment.destroy<shm::message>("reply_queue");
	segment.destroy<std::atomic_bool>("sleeping_flag");
	printf("Producer destroyed.\n");
}