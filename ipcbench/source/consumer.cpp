#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/bind.hpp>
#include <csignal>
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
volatile sig_atomic_t started = 0;

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
	started = 1;
	stopped = 1;
}

void start_handler(int s) {
	started = 1;
}

void nullCompletionHandler() {}

void sleep_handler(const boost::system::error_code& error, int signum, boost::asio::io_context* context) {
	if (!error && signum == SIGUSR2) {
		// printf("Received SIGUSR2!\n");
		context->post(nullCompletionHandler);
	}
}

// Consumer, receive the request and send back the reply
int main(int argc, char* argv[]) {
	signal(SIGINT, stop_handler);
	signal(SIGUSR1, start_handler);

	while (!started) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
	printf("Received signal from producer to start\n");

	// asio stuff
	boost::asio::io_context context;
	// give it some work, to prevent premature exit
	boost::asio::executor_work_guard<decltype(context.get_executor())> work{ context.get_executor() };

	// Construct a signal set registered for process termination.
	boost::asio::signal_set signals(context, SIGUSR2);

	std::atomic_int count = 0;
	std::atomic_ullong latency = 0;
	std::thread traceT{ trace, 1000, &count, &latency };

	// wake up if stopped
	std::thread stopThread([&context] {
		while (true) {
			if (stopped) {
				context.post(nullCompletionHandler);
				break;
			}
			// check every 1 second
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
		}
	});

	int size = std::stoi(argv[1]);
	// this will throw error if memory not exists
	managed_shared_memory segment(open_only, "MySharedMemory");

	shm::message_queue* request_queue;
	// Find the message queue using the name
	try {
		request_queue = segment.find<shm::message_queue>("request_queue").first;
	} catch (Error& e) {
		printf("Error\n");
		return 0;
	}
	// sleeping flag
	std::atomic_bool* isSleeping = segment.find<std::atomic_bool>("sleeping_flag").first;

	char* reply = static_cast<char*>(segment.allocate(size * sizeof(char)));
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
		// Start an asynchronous wait for one of the signals to occur.
		signals.async_wait(boost::bind(sleep_handler, _1, _2, &context));
		// sleep
		isSleeping->store(true);
		// pull once to avoid deadlock
		if (request_queue->pop(ptr)) {
			memcpy(buffer, ptr->data, size);
			latency.fetch_add(shm::now() - ptr->start_time);
			count.fetch_add(1);
		}
		context.run_one();
		isSleeping->store(false);
	}
	segment.deallocate(reply);
	stopThread.join();
	traceT.join();
	free(buffer);
	std::cout << "Consumer finished.\n";
}