#include "ConsumerClient.h"
#include "ConsumerAdapterUtils.h"

// simulate behavior of a client
// TODO:
// randomly return correct or incorrect values
// randomly return an error
// add delays

using namespace std;
using namespace ConsAdapter::serialization;

// External Client Test Code

ConsumerClientTester::ConsumerClientTester(boost::asio::io_context &io_context)
    : io_context(io_context) {
  // trace = spdlog::get("pvTrace");
  // log.trace("creating ConsumerClientTester");
}

int ConsumerClientTester::startNetwork() {
  // log.trace("ConsumerClientTester start network");
  return 0;
}

int ConsumerClientTester::stopNetwork() {
  // log.trace("ConsumerClientTester stop network");
  return 0;
}

// Get replicator state
// checks if it matches given replicator state
int ConsumerClientTester::beginTxn(MessageBuffer *reqBuffer) {
  ReplicatorStateExt statePassed(reqBuffer->getRepState());
  // log.trace("ConsumerClient getReplicatorState arg:{} endpoint:{}",
  //           statePassed.toStr(), reqBuffer->endpoint);

  io_context.post(
      boost::bind(&ConsumerClientTester::getReplicatorState, this, reqBuffer));

  return 0;
}

void ConsumerClientTester::getReplicatorState(MessageBuffer *reqBuffer) {
  // just pass the same state back
  reqBuffer->replyRepState = reqBuffer->getRepState();
  switch (reqBuffer->type) {
  case MessageBufferType::T_GetReplicatorStateReq:
    consumerTxnResponseCB(reqBuffer, true);
    break;
  case MessageBufferType::T_SetReplicatorStateReq:
    io_context.post(boost::bind(&ConsumerClientTester::setReplicatorStateCommit,
                                this, reqBuffer));
    break;
  case MessageBufferType::T_PushBatchReq:
    io_context.post(
        boost::bind(&ConsumerClientTester::pushBatchCommit, this, reqBuffer));
    break;
  case MessageBufferType::T_VerifyRangeReq:
    io_context.post(
        boost::bind(&ConsumerClientTester::verifyRange, this, reqBuffer));
    break;
  default:
    // log.trace("bad req type");
    assert(0);
  }
  // log.trace("ConsumerClient getReplicatorState response endpoint:{}",
  //           reqBuffer->endpoint);
}

void ConsumerClientTester::setReplicatorStateCommit(MessageBuffer *reqBuffer) {

  // log.trace("ConsumerClient setReplicatorState committed state endpoint:{}",
  //            reqBuffer->endpoint);
  consumerTxnResponseCB(reqBuffer, true);
}

void ConsumerClientTester::pushBatchCommit(MessageBuffer *reqBuffer) {
  // log.trace("ConsumerClient pushBatch endpoint:{}", reqBuffer->endpoint);

  reqBuffer->replyRepState = reqBuffer->getRepState();
  consumerTxnResponseCB(reqBuffer, true);
}

void ConsumerClientTester::verifyRange(MessageBuffer *reqBuffer) {

  // log.trace("ConsumerClient verifyRange begin endpoint:{}",
  //            reqBuffer->endpoint);
  consumerTxnResponseCB(reqBuffer, false);
  reqBuffer->curVerifyRange++;
  io_context.post(
      boost::bind(&ConsumerClientTester::verifyRangeCB, this, reqBuffer));
}

void ConsumerClientTester::verifyRangeCB(MessageBuffer *reqBuffer) {
  if (reqBuffer->curVerifyRange < reqBuffer->getRanges()->Length()) {

    // log.trace(
    //     "ConsumerClient verifyRange:'{}' range:{} endpoint:{}",
    //      reqBuffer->curVerifyRange,
    //     printObj(*reqBuffer->getRanges()->Get(reqBuffer->curVerifyRange)),
    //    reqBuffer->endpoint);
    reqBuffer->curVerifyRange++;
    io_context.post(
        boost::bind(&ConsumerClientTester::verifyRangeCB, this, reqBuffer));
  } else {
    // log.trace("ConsumerClient verifyRange done endpoint:{}",
    //            reqBuffer->curVerifyRange, reqBuffer->endpoint);
    consumerTxnResponseCB(reqBuffer, true);
  }
}
