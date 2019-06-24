#include "ConsumerAdapter.h"
#include "ConsumerAdapterUtils.h"
#include "ConsumerClient.h"
#include "FakeSnowCannon.h"
#include <boost/array.hpp>
#include <iostream>
#include <set>
#include <sys/types.h>
#include <sys/wait.h>

using namespace std;

void SIG_Handler(int sig) {
  auto trace = spdlog::get("pvTrace");
  trace->info("Consumer Adapter received SIGNAL:{}", sig);
}

int main(int argc, char **argv, char **env) {
  // register signal handler
  signal(SIGTERM, SIG_Handler);
  signal(SIGINT, SIG_Handler);
  // seed random number generator
  srand(time(NULL));

  auto trace =
      spdlog::rotating_logger_mt("pvTrace", "adapter_trace.json", 1000000, 10);

  trace->flush_on(spdlog::level::info);
  trace->set_pattern(
      "{\"Severity\": \"%l\", \"Time\": \"%E\", \"DateTime\": "
      "\"%Y-%m-%dT%T\", \"ThreadID\": \"%t\" \"Type\": \"%v\"} ");

  // get the cluster file
  // default:

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

    trace->info("Create Consumer Adapter");
    boost::asio::io_context io_context;
    std::shared_ptr<ConsumerClientIF> consumerClient =
        std::make_shared<ConsumerClientFDB6>(clusterFile);
    auto consumerAdapter =
        ConsumerAdapter::create(io_context, 4613, consumerClient);

    int err = consumerAdapter->start();
    if (err) {
      trace->error("Consumer Adapter failed to start fdb network err:{}", err);
      return err;
    }
    consumerAdapter->connect();

    io_context.run();
    err = consumerAdapter->stop();
    if (err) {
      trace->error("Consumer Adapter failed to stop fdb network err:{}", err);
      return err;
    }
    trace->info("Consumer Adapter ended");
  } catch (std::exception &e) {
    trace->error("Consumer Adapter main loop failure: ({})", e.what());
    return -1;
  }
  return 0;
}
