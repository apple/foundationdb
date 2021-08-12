#include <atomic>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
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

void trace(int trace_period_milliseconds, std::atomic_int* counter, std::atomic_ullong* latency) {
	shm::bench_t prev_time = shm::now();
    // shm::bench_t prev_latency = latency->load();
	int prev_counter = counter->load();
	while (!stopped) {
        printf("Rate: %.3e messages/second\n", (counter->load() - prev_counter) * 1e9 / (shm::now() - prev_time));
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

shm::bench_t shm::now() {
	struct timespec ts;
	timespec_get(&ts, TIME_UTC);
	return ts.tv_sec * 1e9 + ts.tv_nsec;
};

void my_handler(int s){
    stopped = 1;
}

// Main function. For producer
int main(int argc, char* argv[]) {
	
	signal(SIGINT, my_handler);

    int size = std::stoi(argv[1]);
    int waiting_interval = 0;
	if (argc == 3) waiting_interval = std::stoi(argv[2]);

	// Remove shared memory on construction and destruction
	struct shm_remove {
		shm_remove() { 
			shared_memory_object::remove("MySharedMemory");
		}
		~shm_remove() { shared_memory_object::remove("MySharedMemory"); }
	} remover;

	// Create a new segment with given name and size
	managed_shared_memory segment(create_only, "MySharedMemory", 65536);
	// Create two lock-free queue with given name
	// size is given as 10
	// here we are doing a ping-pong test so the size if okay
	shm::message_queue* request_queue = segment.construct<shm::message_queue>("request_queue")();
	shm::message_queue* reply_queue = segment.construct<shm::message_queue>("reply_queue")();
	
	std::atomic_int count = 0;
	std::atomic_ullong latency = 0;
	std::thread traceT{ trace, 1000, &count, &latency };
    // shm::message* msg = static_cast<shm::message*>(segment.allocate(sizeof(shm::message)));
    char* msg = static_cast<char*>(segment.allocate(size*sizeof(char)));
    void* buffer = malloc(size*sizeof(char));
	while (!stopped) {
        if (waiting_interval)
            std::this_thread::sleep_for(std::chrono::milliseconds(waiting_interval));
		// shm::message* msg = static_cast<shm::message*>(segment.allocate(sizeof(shm::message)));
		// write the message
        memset(msg, '-', size);

		shm::bench_t start_time = shm::now();
		// shm::bench_t push_start = shm::now();
		while (!request_queue->push(msg) && !stopped) {
			// fails to push, just retry
		};
		offset_ptr<char> ptr;
		while (!reply_queue->pop(ptr) && !stopped) {
			// fails to pop, just retry
		}
		// read the reply
		memcpy(buffer, ptr.get(), size);
		latency.fetch_add(shm::now() - start_time);
        count.fetch_add(1);
	}
    segment.deallocate(msg);
    free(buffer);
	// When done, destroy the queues from the segment
	segment.destroy<shm::message>("request_queue");
	segment.destroy<shm::message>("reply_queue");
	traceT.join();
	printf("Producer destroyed.\n");
}