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
	log.trace("ConsumerAdapterMainReceivedSIGNAL", { { "Signal", STR(sig) } });
}

int main(int argc, char** argv, char** env) {
	// register signal handler
	signal(SIGTERM, SIG_Handler);
	signal(SIGINT, SIG_Handler);
	// seed random number generator
	srand(time(NULL));

	std::shared_ptr<Log> log(new Log("pvTrace", "test1_trace.json"));

	// get the cluster file and port
	// TODO: pass the log file name
	std::string clusterFile = "./fdb.cluster";
	int port;
	if (argc < 2 || argc > 3) {
		port = atoi(argv[1]);
		return 1;
	}
	if (argc > 2) {
		clusterFile = argv[2];
	}
	// while (*env)
	//  cout << "env var:" << *env++ << endl;

	try {

		log->trace("ConsumerAdapterCreate");
		boost::asio::io_context io_context;
		std::shared_ptr<ConsumerClientIF> consumerClient = std::make_shared<ConsumerClientFDB6>(clusterFile, log);
		auto consumerAdapter = ConsumerAdapter::create(io_context, 4613, consumerClient, log);

		int err = consumerAdapter->start();
		if (err) {
			log->trace(LogLevel::Error, "ConsumerAdapterFailure",
			           { { "Reason", "FDB6 Client Failed To Start FDB Network" }, { "Error", STR(err) } });
			return err;
		}
		consumerAdapter->connect();

		io_context.run();
		err = consumerAdapter->stop();
		if (err) {
			log->trace(LogLevel::Error, "ConsumerAdapterFailure",
			           { { "Reason", "FDB6 Client Failed To Stop FDB Network" }, { "Error", STR(err) } });

			return err;
		}

		log->trace("ConsumerAdapterEnded");
	} catch (std::exception& e) {
		log->trace(LogLevel::Error, "ConsumerAdapterMainLoopFailure", { { "CaughtError", e.what() } });
		return -1;
	}
	return 0;
}
