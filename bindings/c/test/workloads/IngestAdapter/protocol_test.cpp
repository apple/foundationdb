#include "ConsumerAdapter.h"
#include "ConsumerAdapterUtils.h"
#include "ConsumerClient.h"
#include "ProducerFuzz.h"
#include <boost/array.hpp>
#include <iostream>
#include <set>
#include <sys/types.h>
#include <sys/wait.h>

using namespace std;

void SIG_Handler(int sig) {
	auto log = Log::get("pvTrace");
	log.trace("ProtocolTestReceivedSIGNAL", { { "Signal", STR(sig) } });
}

int main(int argc, char** argv) {
	// register signal handler
	signal(SIGTERM, SIG_Handler);
	signal(SIGINT, SIG_Handler);
	// seed random number generator
	srand(time(NULL));

	std::shared_ptr<Log> log(new Log("pvTrace", "test1_trace.json"));
	// trace->set_level(spdlog::level::debug);
	// create and run fake producer
	// fake producer forks and creates external consumer

	// get the cluster file
	// default:
	std::string clusterFile = "./fdb.cluster";

	if (argc > 1) {
		clusterFile = argv[1];
	}

	int test = 1;
	/* protocol test */
	if (test == 0) {
		try {
			log->trace("ProtocolTest0BeginTest");
			boost::asio::io_context io_context;
			auto prodFuzz = ProducerFuzz::create(io_context, 4613, log);
			prodFuzz->requestGen.init(log, 1000, 2550, 1000, 100, 3, 10);
			prodFuzz->start(10000, 100, 100);

			std::shared_ptr<ConsumerClientIF> consumerClient = std::make_shared<ConsumerClientTester>(io_context, log);
			auto consumerAdapter = ConsumerAdapter::create(io_context, 4613, consumerClient, log);

			consumerAdapter->connect();

			log->trace("ProtocolTest0RunIOEventLoop");
			io_context.run(); // blocks as long as there are async callbacks in epoll queue
			log->trace("ProtocolTest0Complete");
		} catch (std::exception& e) {

			log->trace(LogLevel::Error, "ProtocolTestFailure", { { "Reason", e.what() } });
			return -1;
		}
	}
	/*  fdb6 client test */
	if (test == 1) {
		try {

			log->trace("ProtocolTest1BeginTest", { { "ClusterFile", clusterFile } });
			boost::asio::io_context io_context;
			auto prodFuzz = ProducerFuzz::create(io_context, 4613, log);
			prodFuzz->requestGen.init(log, 1000000 /* total key range */, 10000 /* max value size*/,
			                          100 /* max mutations in batch */, 10 /* max keyRange size*/,
			                          3 /* max ranges in verifyRange request */, 10 /* max waiting verifyRanges */);
			prodFuzz->start(100000, 100, 20);

			std::shared_ptr<ConsumerClientIF> consumerClient = std::make_shared<ConsumerClientFDB6>(clusterFile, log);
			auto consumerAdapter = ConsumerAdapter::create(io_context, 4613, consumerClient, log);

			int err = consumerAdapter->start();
			if (err) {
				log->trace(LogLevel::Error, "ProtocolTestFailure",
				           { { "Reason", "FDB6 Client Failed To Start FDB Network" }, { "Error", STR(err) } });
				return err;
			}
			consumerAdapter->connect();
			log->trace("ProtocolTest1RunIOEventLoop");
			io_context.run();
			err = consumerAdapter->stop();
			if (err) {

				log->trace(LogLevel::Error, "ProtocolTestFailure",
				           { { "Reason", "FDB6 Client Failed To Stop FDB Network" }, { "Error", STR(err) } });
				return err;
			}

			log->trace("ProtocolTest1Complete");
		} catch (std::exception& e) {
        log->trace(LogLevel::Error, "ProtocolTestFailure", { { "CaughtError", e.what() } });
			return -1;
		}
	}
	log->trace("ProtocolTestsComplete");

	return 0;
}
