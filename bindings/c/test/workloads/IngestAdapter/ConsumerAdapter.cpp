#include "ConsumerAdapter.h"
#include "ConsumerAdapterUtils.h"
#include <boost/lexical_cast.hpp>

using boost::asio::ip::tcp;
using namespace std;
using namespace ConsAdapter::serialization;

ConsumerAdapter::ConsumerAdapter(boost::asio::io_context& io_context, unsigned p, std::shared_ptr<ConsumerClientIF> ec,
                                 shared_ptr<Log> l)
  : port(p), io_context(io_context), socket(io_context), resolver(io_context), consumerClient(ec),
    signals(io_context, SIGTERM, SIGINT), responseQStrand(io_context), log(l) {

	// register the server
	assert(g_consumerAdapter == NULL);
	g_consumerAdapter = this;
	signals.async_wait(handle_sig);

	log->trace("ConsumerAdapterCreated", { { "Port", STR(port) } });
}

ConsumerAdapter::~ConsumerAdapter() {
	log->trace("ConsumerAdapterDestroyed");
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
	tcp::resolver::iterator epIt = resolver.resolve(tcp::resolver::query(tcp::v4(), "localhost", "4613"));
	tcp::endpoint ep = epIt->endpoint();

	log->trace("ConsumerAdapterConnecting", { { "address", boost::lexical_cast<std::string>(ep) } });

	consumerClient->registerTxnResponseCallback(
	    boost::bind(&ConsumerAdapter::txnResponseCB, shared_from_this(), _1, _2));

	socket.async_connect(
	    *epIt, boost::bind(&ConsumerAdapter::handleConnect, shared_from_this(), boost::asio::placeholders::error));
}

void ConsumerAdapter::handleConnect(const boost::system::error_code& error) {

	log->trace("ConsumerAdapterConnected", { { "Error", error.message() } });
	connected = true;
	waitForRequests();
}

void ConsumerAdapter::waitForRequests() {
	// read the header

	log->trace("ConsumerAdapterWaitingForRequests");

	// allocate new buffer
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();

	boost::asio::async_read(socket, boost::asio::buffer((void*)&reqBuffer->header, sizeof(reqBuffer->header)),
	                        boost::asio::transfer_exactly(sizeof(MessageHeader)),
	                        boost::bind(&ConsumerAdapter::handleHeader, shared_from_this(), reqBuffer,
	                                    boost::asio::placeholders::error,
	                                    boost::asio::placeholders::bytes_transferred));
}

void ConsumerAdapter::handleHeader(std::shared_ptr<MessageBuffer> reqBuffer,
                                   const boost::system::error_code& error, // Result of operation.
                                   std::size_t bytesRead) {

	if (error) {
		log->trace(LogLevel::Error, "ConsumerAdapter_HandleRequestHeaderError", { { "Error", error.message() } });
		// TODO: close
	}
	if (bytesRead < sizeof(MessageHeader)) {
		log->trace(LogLevel::Error, "ConsumerAdapter_HandleRequestHeaderIncomplete",
		           { { "BytesTransferred", STR(bytesRead) } });
		// TODO: close
	}

	log->trace(LogLevel::Debug, "ConsumerAdapter_HandleRequestHeader",
	           { { "Header", printObj(reqBuffer->header) }, { "ReqID", STR(reqBuffer->id) } });
	reqBuffer->readBuffer.resize(reqBuffer->header.size);

	boost::asio::async_read(
	    socket, boost::asio::buffer(reqBuffer->readBuffer), boost::asio::transfer_exactly(reqBuffer->header.size),
	    boost::bind(&ConsumerAdapter::handleRequest, shared_from_this(), reqBuffer, boost::asio::placeholders::error,
	                boost::asio::placeholders::bytes_transferred));
}

void ConsumerAdapter::handleRequest(std::shared_ptr<MessageBuffer> reqBuffer,
                                    const boost::system::error_code& error, // Result of operation.
                                    std::size_t bytesRead) {

	log->trace("ConsumerAdapter_HandleRequest", { { "Header", printObj(reqBuffer->header) },
	                                              { "ReqID", STR(reqBuffer->id) },
	                                              { "BytesTransferred", STR(bytesRead) } });
	if (error) {
		log->trace(LogLevel::Error, "ConsumerAdapter_HandleRequestError", { { "Error", error.message() } });
		assert(0);
	}
	if (bytesRead < reqBuffer->header.size) {
		log->trace(LogLevel::Error, "ConsumerAdapter_HandleRequestHeaderIncomplete",
		           { { "BytesTransferred", STR(bytesRead) } });
		assert(0);
	}

	// check checksum
	Crc32 crc;
	uint32_t checksum = crc.sum(static_cast<const void*>(reqBuffer->readBuffer.data()), reqBuffer->header.size);

	log->trace(LogLevel::Debug, "ConsumerAdapter_HandleRequestCheckChecksum", { { "Checksum", STR(checksum) } });
	if (checksum != reqBuffer->header.checksum) {
		log->trace(LogLevel::Error, "ConsumerAdapter_HandleRequestErrorChecksumMismatch",
		           { { "Checksum", STR(checksum) } });
		assert(0);
	}

	responseQStrand.post(boost::bind(&ConsumerAdapter::serviceRequest, shared_from_this(), reqBuffer));
	waitForRequests();
}

int ConsumerAdapter::queueResponse(std::shared_ptr<MessageBuffer> respBuffer) {
	respBuffer->prepareWrite();
	log->trace(LogLevel::Debug, "ConsumerAdapter_QueueResponse",
	           {
	               { "Header", printObj(respBuffer->header) },
	               { "HeaderBuffSize", STR(respBuffer->writeBuffers[0].size()) },
	               { "MsgBuffSize", STR(respBuffer->writeBuffers[1].size()) },
	           });
	respQueue.push_back(respBuffer);
	if (respQueue.size() == 1) {
		sendResponse();
	}
	return 0;
}

void ConsumerAdapter::sendResponse() {
	if (respQueue.empty()) {
		log->trace(LogLevel::Debug, "ConsumerAdapter_WaitForResponsesToSend");
		return;
	}
	auto messageBuf = respQueue.front();

	log->trace("ConsumerAdapter_SendResponse",
	           { { "Endpoint", STR(messageBuf->endpoint) }, { "MsgID", STR(messageBuf->id) } });

	boost::asio::async_write(socket, messageBuf->getWriteBuffers(),
	                         boost::bind(&ConsumerAdapter::handleFinishSendResponse, shared_from_this(),
	                                     boost::asio::placeholders::error,
	                                     boost::asio::placeholders::bytes_transferred));
}

void ConsumerAdapter::handleFinishSendResponse(const boost::system::error_code& error, std::size_t bytes_transferred) {
	log->trace("ConsumerAdapter_ResponseWriteComplete", { { "Error", error.message() },
	                                                      { "BytesTransferred", STR(bytes_transferred) },
	                                                      { "QueueSize", STR(respQueue.size()) } });
	respQueue.pop_front();
	if (!respQueue.empty()) {
		sendResponse();
	}
}

// not thread safe
// initialize and save messageBuffer
void ConsumerAdapter::serviceRequest(std::shared_ptr<MessageBuffer> reqBuffer) {
	auto polevaultReq = GetConsumerAdapterRequest(static_cast<const void*>(reqBuffer->readBuffer.data()));

	int endpoint = polevaultReq->endpoint();
	reqBuffer->endpoint = endpoint;
	log->trace("ConsumerAdapter_ServeRequest", { { "MsgID", STR(reqBuffer->id) }, { "Endpoint", STR(endpoint) } });

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
		log->trace(LogLevel::Error, "ConsumerAdapter_BadRequestType");
		assert(0);
	}

	int err = consumerClient->beginTxn(reqBuffer.get());
	activeReqBuffers[reqBuffer->id] = reqBuffer;

	if (err) {
		log->trace(LogLevel::Error, "ConsumerAdapter_ServeRequestError",
		           { { "MsgID", STR(reqBuffer->id) }, { "Endpoint", STR(endpoint) }, { "Error", STR(err) } });

		responseQStrand.post(boost::bind(&ConsumerAdapter::createAndQueueResponse, shared_from_this(), reqBuffer.get(),
		                                 true, MessageResponseType::T_ReplyResp));
	}
}

// Not thread safe.
// create response, free request buffer
int ConsumerAdapter::createAndQueueResponse(MessageBuffer* reqBuffer, bool freeBuffer, MessageResponseType respType) {

	std::shared_ptr<MessageBuffer> respBuffer = std::make_shared<MessageBuffer>();
	respBuffer->endpoint = reqBuffer->endpoint;
	// Create either a REPLY or a FINISH response and queue
	if (respType == MessageResponseType::T_ReplyResp) {
		auto repStateSerialized = reqBuffer->replyRepState.serialize();
		auto reply = CreateReplyResp(respBuffer->serializer, &repStateSerialized, reqBuffer->error);
		auto resp = CreateConsumerAdapterResponse(respBuffer->serializer, Response_ReplyResp, reply.Union(),
		                                          respBuffer->endpoint);
		log->trace("ConsumerAdapter_CreateAndQueueReply",
		           { { "Endpoint", STR(respBuffer->endpoint) }, { "ID", STR(respBuffer->id) } });

		respBuffer->serializer.Finish(resp);
	} else if (respType == MessageResponseType::T_FinishResp) {
		auto checksums = respBuffer->serializer.CreateVector(reqBuffer->replyChecksums);
		auto finish = CreateFinishResp(respBuffer->serializer, checksums, reqBuffer->error);
		auto resp = CreateConsumerAdapterResponse(respBuffer->serializer, Response_FinishResp, finish.Union(),
		                                          respBuffer->endpoint);
		log->trace("ConsumerAdapter_CreateAndQueueVerifyFinish",
		           { { "Endpoint", STR(respBuffer->endpoint) }, { "ID", STR(respBuffer->id) } });
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

void ConsumerAdapter::txnResponseCB(MessageBuffer* reqBuffer, bool freeBuffer) {
    log->trace("ConsumerAdapter_TXNResponse", { { "DoFree", STR(freeBuffer) }, { "Endpoint", STR(reqBuffer->endpoint) },
	                                            { "ID", STR(reqBuffer->id) } });
	// Send respType separately, since the buffer may change before we have a
	// chance to send the first response
	responseQStrand.post(boost::bind(&ConsumerAdapter::createAndQueueResponse, shared_from_this(), reqBuffer,
	                                 freeBuffer, reqBuffer->respType));
}

void ConsumerAdapter::handle_sig(const boost::system::error_code& error, int signal_number) {

	auto log = Log::get("pvTrace");
	log.trace("ConsumerAdapter_TERMINATED", { { "Signal", STR(signal_number) }, { "Error", error.message() } });

	g_consumerAdapter->stop();
}
