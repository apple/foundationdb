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
  trace->info("protocol test received SIGNAL:{}", sig);
}

int main(int argc, char **argv) {
  // register signal handler
  signal(SIGTERM, SIG_Handler);
  signal(SIGINT, SIG_Handler);
  // seed random number generator
  srand(time(NULL));

  auto trace =
      spdlog::rotating_logger_mt("pvTrace", "test1_trace.json", 1000000, 10);

  trace->flush_on(spdlog::level::info);
  trace->set_pattern(
      "{\"Severity\": \"%l\", \"Time\": \"%E\", \"DateTime\": "
      "\"%Y-%m-%dT%T\", \"ThreadID\": \"%t\" \"Type\": \"%v\"} ");
  // trace->set_level(spdlog::level::debug);
  // create and run fake snowcannon
  // fake snowcannon forks and creates external consumer

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

      trace->info("Protocol Test!");
      boost::asio::io_context io_context;
      auto fakeSC = FakeSnowCannon::create(io_context, 4613);
      fakeSC->snowcannonGen.init(1000, 2550, 1000, 100, 3, 10);
      fakeSC->start(10000, 100, 100);

      std::shared_ptr<ConsumerClientIF> consumerClient =
          std::make_shared<ConsumerClientTester>(io_context);
      auto consumerAdapter =
          ConsumerAdapter::create(io_context, 4613, consumerClient);

      consumerAdapter->connect();

      trace->info("running SC io event handler");
      io_context
          .run(); // blocks as long as there are async callbacks in epoll queue
      trace->info("test complete");
    } catch (std::exception &e) {

      trace->error("test failure: ({})", e.what());
      return -1;
    }
  }
  /*  fdb6 client test */
  if (test == 1) {
    try {

      trace->info("fdb6 client test start clusFile:'{}'", clusterFile);
      boost::asio::io_context io_context;
      auto fakeSC = FakeSnowCannon::create(io_context, 4613);
      fakeSC->snowcannonGen.init(
          1000000 /* total key range */, 10000 /* max value size*/,
          100 /* max mutations in batch */, 10 /* max keyRange size*/,
          3 /* max ranges in verifyRange request */,
          10 /* max waiting verifyRanges */);
      fakeSC->start(100000, 100, 20);

      std::shared_ptr<ConsumerClientIF> consumerClient =
          std::make_shared<ConsumerClientFDB6>(clusterFile);
      auto consumerAdapter =
          ConsumerAdapter::create(io_context, 4613, consumerClient);

      int err = consumerAdapter->start();
      if (err) {
        trace->error("fdb6 client test failed to start fdb network err:{}",
                     err);
        return err;
      }
      consumerAdapter->connect();

      io_context.run();
      err = consumerAdapter->stop();
      if (err) {
        trace->error("fdb6 client test failed to stop fdb network err:{}", err);
        return err;
      }

      trace->info("fdb6 client test complete");
    } catch (std::exception &e) {

      trace->error("fdb6 client test failure: ({})", e.what());
      return -1;
    }
  }

  trace->info("test complete");

  return 0;
}
