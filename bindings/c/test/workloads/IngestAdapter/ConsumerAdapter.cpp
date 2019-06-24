#include "ConsumerAdapter.h"
#include "ConsumerAdapterUtils.h"
#include <boost/lexical_cast.hpp>

using boost::asio::ip::tcp;
using namespace std;
using namespace ConsAdapter::serialization;

ConsumerAdapter::ConsumerAdapter(boost::asio::io_context &io_context,
                                   unsigned p,
                                   std::shared_ptr<ConsumerClientIF> ec)
    : port(p), io_context(io_context), socket(io_context), resolver(io_context),
      consumerClient(ec), signals(io_context, SIGTERM, SIGINT),
      responseQStrand(io_context) {

  trace = spdlog::get("pvTrace");
  trace->flush_on(spdlog::level::info);

  // register the server
  assert(g_consumerAdapter == NULL);
  g_consumerAdapter = this;
  signals.async_wait(handle_sig);

  trace->info("consumerAdapter created");
}

ConsumerAdapter::~ConsumerAdapter() {
  trace->info("consumerAdapter destroyed");
  g_consumerAdapter = NULL;
}

int ConsumerAdapter::start() {
  started = true;
  return consumerClient->startNetwork();
}
int ConsumerAdapter::stop() {
  if (started) {
    started = false;
    return consumerClient->stopNetwork();
  }
  return 0;
}

void ConsumerAdapter::connect() {
  tcp::resolver::iterator epIt =
      resolver.resolve(tcp::resolver::query(tcp::v4(), "localhost", "4613"));
  tcp::endpoint ep = epIt->endpoint();
  trace->info("consumerAdapter connecting to '{}'...",
              boost::lexical_cast<std::string>(ep));

  consumerClient->registerTxnResponseCallback(boost::bind(
      &ConsumerAdapter::txnResponseCB, shared_from_this(), _1, _2));

  socket.async_connect(*epIt, boost::bind(&ConsumerAdapter::handleConnect,
                                          shared_from_this(),
                                          boost::asio::placeholders::error));
}

void ConsumerAdapter::handleConnect(const boost::system::error_code &error) {
  trace->info("consumerAdapter connected! err{}", error.message());
  connected = true;
  waitForRequests();
}

void ConsumerAdapter::waitForRequests() {
  // read the header
  trace->debug("consumerAdapter waiting to read header...");

  // allocate new buffer
  std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();

  boost::asio::async_read(
      socket,
      boost::asio::buffer((void *)&reqBuffer->header,
                          sizeof(reqBuffer->header)),
      boost::asio::transfer_exactly(sizeof(MessageHeader)),
      boost::bind(&ConsumerAdapter::handleHeader, shared_from_this(),
                  reqBuffer, boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
}

void ConsumerAdapter::handleHeader(
    std::shared_ptr<MessageBuffer> reqBuffer,
    const boost::system::error_code &error, // Result of operation.
    std::size_t bytesRead) {

  if (error) {
    // TODO: close
    trace->info("consumerAdapter reading header ERROR:{}", error.message());
  }
  if (bytesRead < sizeof(MessageHeader)) {
    trace->debug("consumerAdapter handle header ERROR: transfered only {}",
                 bytesRead);
    // TODO: close
  }
  trace->debug("consumerAdapter handle header:{} id:{}",
               printObj(reqBuffer->header), reqBuffer->id);
  reqBuffer->readBuffer.resize(reqBuffer->header.size);

  boost::asio::async_read(
      socket, boost::asio::buffer(reqBuffer->readBuffer),
      boost::asio::transfer_exactly(reqBuffer->header.size),
      boost::bind(&ConsumerAdapter::handleRequest, shared_from_this(),
                  reqBuffer, boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
}

void ConsumerAdapter::handleRequest(
    std::shared_ptr<MessageBuffer> reqBuffer,
    const boost::system::error_code &error, // Result of operation.
    std::size_t bytesRead) {

  trace->info("consumerAdapter service handle request read:{}, header:{}, id:{}",
              bytesRead, reqBuffer->header.toStr(), reqBuffer->id);
  if (error) {
    trace->info("consumerAdapter service reading request ERROR:{}",
                error.message());
    assert(0);
  }
  if (bytesRead < reqBuffer->header.size) {
    trace->error("consumerAdapter service handle request ERROR: transfered only {}",
                 bytesRead);
    assert(0);
  }

  // check checksum
  Crc32 crc;
  uint32_t checksum =
      crc.sum(static_cast<const void *>(reqBuffer->readBuffer.data()),
              reqBuffer->header.size);
  trace->info("consumerAdapter server check req sum:{}", checksum);
  if (checksum != reqBuffer->header.checksum) {
    trace->info("consumerAdapter read request ERROR: failed checksum");
    assert(0);
  }

  responseQStrand.post(boost::bind(&ConsumerAdapter::serviceRequest,
                                   shared_from_this(), reqBuffer));
  waitForRequests();
}

int ConsumerAdapter::queueResponse(std::shared_ptr<MessageBuffer> respBuffer) {
  respBuffer->prepareWrite();

  trace->debug("consumerAdapter preparing resp Header:{}",
               printObj(respBuffer->header));
  for (auto b : respBuffer->writeBuffers) {
    trace->debug("buff size:{}", b.size());
  }

  trace->debug("consumerAdapter queue response");
  respQueue.push_back(respBuffer);
  if (respQueue.size() == 1) {
    sendResponse();
  }
  return 0;
}

void ConsumerAdapter::sendResponse() {
  if (respQueue.empty()) {
    trace->error("consumerAdapter sendResponse no work");
    return;
  }

  trace->info("consumerAdapter sendResponse");
  auto messageBuf = respQueue.front();

  boost::asio::async_write(
      socket, messageBuf->getWriteBuffers(),
      boost::bind(&ConsumerAdapter::handleFinishSendResponse,
                  shared_from_this(), boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
}

void ConsumerAdapter::handleFinishSendResponse(
    const boost::system::error_code &error, std::size_t bytes_transferred) {
  trace->info("consumerAdapter write complete err:{}, bytes:{}", error.message(),
              bytes_transferred);
  respQueue.pop_front();
  if (!respQueue.empty()) {
    sendResponse();
  }
}

void ConsumerAdapter::handle_sig(const boost::system::error_code &error,
                                  int signal_number) {

  auto trace = spdlog::get("pvTrace");
  trace->info("consumerAdapter handle SIGNAL:{} err:{}", signal_number,
              error.value());
  g_consumerAdapter->stop();
}

// not thread safe
// initialize and save messageBuffer
void ConsumerAdapter::serviceRequest(
    std::shared_ptr<MessageBuffer> reqBuffer) {
  auto polevaultReq = GetConsumerAdapterRequest(
      static_cast<const void *>(reqBuffer->readBuffer.data()));

  int endpoint = polevaultReq->endpoint();
  reqBuffer->endpoint = endpoint;
  trace->info("consumerAdapter service message... ep:{} id:{}", endpoint,
              reqBuffer->id);

  switch (polevaultReq->request_type()) {
  case Request_GetReplicatorStateReq:
    reqBuffer->type = MessageBufferType::T_GetReplicatorStateReq;
    break;
  case Request_SetReplicatorStateReq:
    reqBuffer->type = MessageBufferType::T_SetReplicatorStateReq;
    break;
  case Request_PushBatchReq:
    reqBuffer->type = MessageBufferType::T_PushBatchReq;
    break;
  case Request_VerifyRangeReq:
    reqBuffer->type = MessageBufferType::T_VerifyRangeReq;
    break;
  default:
    trace->error("consumerAdapter ERROR: bad req type");
    assert(0);
  }

  int err = consumerClient->beginTxn(reqBuffer.get());
  activeReqBuffers[reqBuffer->id] = reqBuffer;

  if (err) {
    trace->error("consumerAdapter service message err:'{}' Endpoint:{} id:{}", err,
                 endpoint, reqBuffer->id);
    responseQStrand.post(boost::bind(&ConsumerAdapter::createAndQueueResponse,
                                     shared_from_this(), reqBuffer.get(), true,
                                     MessageResponseType::T_ReplyResp));
  }
}

// Not thread safe.
// create response, free request buffer
int ConsumerAdapter::createAndQueueResponse(MessageBuffer *reqBuffer,
                                             bool freeBuffer,
                                             MessageResponseType respType) {

  std::shared_ptr<MessageBuffer> respBuffer = std::make_shared<MessageBuffer>();
  respBuffer->endpoint = reqBuffer->endpoint;
  // Create either a REPLY or a FINISH response and queue
  if (respType == MessageResponseType::T_ReplyResp) {
    auto repStateSerialized = reqBuffer->replyRepState.serialize();
    auto reply = CreateReplyResp(respBuffer->serializer, &repStateSerialized,
                                 reqBuffer->error);
    auto resp = CreateConsumerAdapterResponse(
        respBuffer->serializer, Response_ReplyResp, reply.Union(),
        respBuffer->endpoint);
    trace->info("consumerAdapter queue REPLY response ep:{}", respBuffer->endpoint);
    respBuffer->serializer.Finish(resp);
  } else if (respType == MessageResponseType::T_FinishResp) {
    auto checksums =
        respBuffer->serializer.CreateVector(reqBuffer->replyChecksums);
    auto finish =
        CreateFinishResp(respBuffer->serializer, checksums, reqBuffer->error);
    auto resp = CreateConsumerAdapterResponse(
        respBuffer->serializer, Response_FinishResp, finish.Union(),
        respBuffer->endpoint);

    trace->info("consumerAdapter queue FINISH response ep:{} id:{}",
                respBuffer->endpoint, respBuffer->id);
    respBuffer->serializer.Finish(resp);
  } else {
    assert(0);
  }
  if (freeBuffer) {
    activeReqBuffers.erase(reqBuffer->id);
  }
  queueResponse(respBuffer);
  return 0;
}

void ConsumerAdapter::txnResponseCB(MessageBuffer *reqBuffer,
                                     bool freeBuffer) {
  trace->info("consumerAdapter txn response free:{} endpoint:{} id:{}", freeBuffer,
              reqBuffer->endpoint, reqBuffer->id);
  // Send respType separately, since the buffer may change before we have a
  // chance to send the first response
  responseQStrand.post(boost::bind(&ConsumerAdapter::createAndQueueResponse,
                                   shared_from_this(), reqBuffer, freeBuffer,
                                   reqBuffer->respType));
}
