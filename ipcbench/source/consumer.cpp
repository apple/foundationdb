#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <cstdlib> //std::system
#include "flow/Error.h"
#include "thread"
#include <signal.h>

#include "utility.hpp"

using namespace boost::interprocess;

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

void my_handler(int s){
    stopped = 1;
}

void nullCompletionHandler() {}

// Consumer, receive the request and send back the reply
int main(int argc, char *argv[])
{
    signal(SIGINT, my_handler);

    int size = std::stoi(argv[1]);
    // this will throw error if memory not exists
    managed_shared_memory segment(open_only, "MySharedMemory");

    shm::message_queue *request_queue;
    //Find the message queue using the name
    try {
        request_queue = segment.find<shm::message_queue>("request_queue").first;
        // reply_queue = segment.find<shm::message_queue>("reply_queue").first;
    } catch (Error& e) {
        printf("Error\n");
        return 0;
    }
    // sleeping flag
    std::atomic_bool* isSleeping = segment.find<std::atomic_bool>("sleeping_flag").first;
    // asio stuff
	boost::asio::io_context context;
	// give it some work, to prevent premature exit
	boost::asio::executor_work_guard<decltype(context.get_executor())> work{context.get_executor()};
    named_mutex mutex(open_only, "consumer_mutex");
	named_condition cond(open_only, "consumer_cond");

    std::atomic_int count = 0;
	std::atomic_ullong latency = 0;
	std::thread traceT{ trace, 1000, &count, &latency };

    // wake up if stopped
	std::thread stopThread([&cond]{
		while (true) {
			if (stopped) {
				cond.notify_all();
				break;
			}
			// check every 1 second
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		}
	});

    std::thread observer([&mutex, &cond, &context]{
		while (!stopped) {
			// printf("Producer observer thread waiting...\n");
			boost::interprocess::scoped_lock<named_mutex> lock(mutex);
			cond.wait(lock);
			context.post(nullCompletionHandler);
			// printf("Producer 2 thread get notification\n");
		}
	});

    char* reply = static_cast<char*>(segment.allocate(size*sizeof(char)));
    void* buffer = malloc(size*sizeof(char));

    while (!stopped) {
        // ptr to the reply message
		offset_ptr<shm::message> ptr;
        bool poped = false;
		for (auto i = 0; i < 100000; ++i) {
			if (request_queue->pop(ptr)) {
                // printf("Consumer pops once\n");
                poped = true;
				break;
            }
		}
        // read the request
        if (poped) {
            memcpy(buffer, ptr->data, size);
            latency.fetch_add(shm::now() - ptr->start_time);
            count.fetch_add(1);
            poped = false;
        }
        // sleep
		isSleeping->store(true);
        // pull once to avoid deadlock
        if (request_queue->pop(ptr)) {
            memcpy(buffer, ptr->data, size);
            latency.fetch_add(shm::now() - ptr->start_time);
            count.fetch_add(1);
        }
		context.run_one();
		// printf("Consumer waked up!");
		isSleeping->store(false);
        // write the reply
        // memset(reply, '*', size);
        // while (!reply_queue->push(reply) && !stopped) {
        //     // push failed, retry
        // };
    }
    segment.deallocate(reply);
    observer.join();
    stopThread.join();
    traceT.join();
    free(buffer);
    std::cout << "Consumer finished.\n";
}