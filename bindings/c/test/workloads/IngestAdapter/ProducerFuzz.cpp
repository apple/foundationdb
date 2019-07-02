#include "ProducerFuzz.h"
#include "ConsumerAdapterUtils.h"
#include <boost/lexical_cast.hpp>

using boost::asio::ip::tcp;
using namespace std;
using namespace ConsAdapter::serialization;

ProducerFuzz::ProducerFuzz(boost::asio::io_context& io_context, unsigned p, std::shared_ptr<Log> l)
  : port(p), io_context(io_context), socket(io_context), acceptor_(io_context, tcp::endpoint(tcp::v4(), p)),
    signals(io_context, SIGTERM, SIGINT), writeQStrand(io_context), log(l) {
	if (g_pf == NULL) {
		g_pf = this;
	}
	signals.async_wait(handle_sig);

	// todo: create request queue
	/// make handleResponse asyn
}

void ProducerFuzz::start(int reqsToServe, int maxReqsQueued, int maxReqsWaiting) {
	testStartTS = chrono::system_clock::now();
	lastResponseTS = chrono::system_clock::now();
	requestsToServe = reqsToServe;
	maxRequestsQueued = maxReqsQueued;
	maxRequestsWaiting = maxReqsWaiting;
	// register waitForResponses callback
	log->trace("ProducerFuzz_TestStart",
	           { { "RequestsToServe", STR(requestsToServe) }, { "MaxReqsQueued", STR(maxRequestsQueued) } });
	log->trace("ProducerFuzz_WaitForConnect");
	setRepState(); // always start with setting the replicator state
	requestGenerator();
	acceptor_.async_accept(
	    socket, boost::bind(&ProducerFuzz::handleAccept, shared_from_this(), boost::asio::placeholders::error));
}
void ProducerFuzz::handleAccept(const boost::system::error_code& error) {
	log->trace("ProducerFuzz_Connected", { { "Error", error.message() } });
	connected = true;
	waitForResponses();
}

ProducerFuzz::~ProducerFuzz() {
	log->trace(LogLevel::Debug, "ProducerFuzz_Destruct");
}

int ProducerFuzz::queueRequest(std::shared_ptr<MessageBuffer> reqBuffer, int endpoint) {
	reqBuffer->prepareWrite();
	reqBuffer->endpoint = endpoint;

	log->trace(LogLevel::Debug, "ProducerFuzz_PrepareRequest",
	           { { "Header", printObj(reqBuffer->header) },
	             { "HeaderBuffSize", STR(reqBuffer->writeBuffers[0].size()) },
	             { "MsgBuffSize", STR(reqBuffer->writeBuffers[1].size()) },
	             { "QueueSize", STR(reqQueue.size()) },
	             { "Endpoint", STR(reqBuffer->endpoint) } });
	reqQueue.push_back(reqBuffer);
	if (reqQueue.size() == 1) {
		sendRequest();
	}
	return 0;
}

void ProducerFuzz::sendRequest() {
	if (reqQueue.empty()) {
		log->trace(LogLevel::Warn, "ProducerFuzz_SendRequestNoWork");
		return;
	}
	if (currentIDSending == reqQueue.front()->id) {
		log->trace("ProducerFuzz_SendRequestSkipDup", { { "ID", STR(currentIDSending) } });
		return;
	}

	auto messageBuf = reqQueue.front();
	currentIDSending = messageBuf->id;
	requestGen.updateEndpointSendTime(messageBuf->endpoint);

	log->trace("ProducerFuzz_SendRequest",
	           { { "ID", STR(currentIDSending) }, { "Endpoint", STR(messageBuf->endpoint) } });

	boost::asio::async_write(
	    socket, messageBuf->getWriteBuffers(),
	    boost::asio::bind_executor(writeQStrand, boost::bind(&ProducerFuzz::handleFinishWrite, shared_from_this(),
	                                                         currentIDSending, boost::asio::placeholders::error,
	                                                         boost::asio::placeholders::bytes_transferred)));
}

// Can't run concurrently with other handlers on writeQStrand
void ProducerFuzz::handleFinishWrite(int id, const boost::system::error_code& error, std::size_t bytes_transferred) {
	log->trace("ProducerFuzz_RequestWriteComplete", { { "Error", error.message() },
	                                           { "ID", STR(id) },
	                                           { "BytesTransferred", STR(bytes_transferred) },
	                                           { "QueueSize", STR(reqQueue.size()) } });

	assert(id == currentIDSending);
	assert(!reqQueue.empty());
	assert(id == reqQueue.front()->id);
	currentIDSending = 0;
	if (!error) {
		requestsServed++;
		bytesSent += bytes_transferred;
		// int ep = reqQueue.front()->endpoint;
		log->trace("ProducerFuzz_WriteCompletePopMessageEP",
		           { { "ID", STR(id) }, { "Endpoint", STR(reqQueue.front()->endpoint) } });
		reqQueue.pop_front();
	}
	if (!reqQueue.empty()) {
		sendRequest();
	}
}

void ProducerFuzz::waitForResponses() {

	log->trace("ProducerFuzz_WaitForResponses");

	boost::asio::async_read(socket, boost::asio::buffer((void*)&respBuffer.header, sizeof(respBuffer.header)),
	                        boost::asio::transfer_exactly(sizeof(MessageHeader)),
	                        boost::bind(&ProducerFuzz::handleHeader, shared_from_this(),
	                                    boost::asio::placeholders::error,
	                                    boost::asio::placeholders::bytes_transferred));
}

void ProducerFuzz::handleHeader(const boost::system::error_code& error, // Result of operation.

                                std::size_t bytes_transferred) {
	if (error) {
		log->trace(LogLevel::Error, "ProducerFuzz_HandleResponseHeaderError", { { "Error", error.message() } });
		// TODO: close
	}
	if (bytes_transferred != sizeof(MessageHeader)) {
		log->trace(LogLevel::Error, "ProducerFuzz_HandleResponseHeaderIncomplete",
		           { { "BytesTransferred", STR(bytes_transferred) } });
		// TODO: close
	}

	log->trace("ProducerFuzz_HandleResponseHeader", { { "Header", printObj(respBuffer.header) } });

	respBuffer.readBuffer.resize(respBuffer.header.size);

	boost::asio::async_read(
	    socket, boost::asio::buffer(respBuffer.readBuffer), boost::asio::transfer_exactly(respBuffer.header.size),
	    boost::bind(&ProducerFuzz::handleResponse, shared_from_this(), boost::asio::placeholders::error,
	                boost::asio::placeholders::bytes_transferred));
}

void ProducerFuzz::handleResponse(const boost::system::error_code& error, // Result of operation.
                                  std::size_t bytes_transferred) {

	log->trace("ProducerFuzz_HandleResponse",
	           { { "Header", respBuffer.header.toStr() }, { "BytesTransferred", STR(bytes_transferred) } });
	if (error) {
		log->trace(LogLevel::Error, "ProducerFuzz_HandleResponseError", { { "Error", error.message() } });
		// TODO: close
	}
	if (bytes_transferred != respBuffer.header.size) {
		log->trace(LogLevel::Error, "ProducerFuzz_HandleResponseIncomplete",
		           { { "BytesTransferred", STR(bytes_transferred) } });
		// TODO: close
	}

	// check message checksum
	Crc32 crc;
	uint32_t checksum = crc.sum(static_cast<const void*>(respBuffer.readBuffer.data()), respBuffer.header.size);

	log->trace(LogLevel::Debug, "ProducerFuzz_HandleResponseCheckChecksum", { { "Checksum", STR(checksum) } });
	if (checksum != respBuffer.header.checksum) {
      log->trace(LogLevel::Error, "ProducerFuzz_HandleResponseErrorChecksumMismatch", { { "Checksum", STR(checksum) } });
		return;
	}

	auto polevaultResp = flatbuffers::GetRoot<ConsumerAdapterResponse>(respBuffer.readBuffer.data());

	log->trace(LogLevel::Debug, "ProducerFuzz_ServeResponse", { { "Endpoint", STR(polevaultResp->endpoint()) } });
	int ret;
	MessageStats epStats;
	switch (polevaultResp->response_type()) {
	case Response_ReplyResp: {
		auto resp = static_cast<const ReplyResp*>(polevaultResp->response());
		log->trace(LogLevel::Debug, "ProducerFuzz_ServeReply",
		           { { "Error", STR(resp->error()) },
		             { "Reply", printObj(*resp->repState()) },
		             { "Endpoint", STR(polevaultResp->endpoint()) } });

		epStats = requestGen.waitingEPGotReply(polevaultResp->endpoint(), resp->error());
		break;
	}
	case Response_FinishResp: {
		auto resp = static_cast<const FinishResp*>(polevaultResp->response());
		log->trace(LogLevel::Debug, "ProducerFuzz_ServeVerifyFinish",
		           { { "Error", STR(resp->error()) }, { "Endpoint", STR(polevaultResp->endpoint()) } });
		epStats = requestGen.waitingEPGotVerifyFinish(polevaultResp->endpoint());

		break;
	}
	}
	updateStatsOnResponse(polevaultResp, epStats);
	log->trace(LogLevel::Debug, "ProducerFuzz_HandleResponsePrintWaiting",
	           { { "ReqsServed", STR(requestsServed) },
	             { "EndpointsWaitingForReply", STR(requestGen.endpointsWaitingForReply()) },
	             { "EndpointsWaitingForFinish", STR(requestGen.endpointsWaitingForVerifyFinish()) } });

	lastResponseTS = chrono::system_clock::now();
	if (!checkTestEnd()) {
		waitForResponses();
	}
}
int ProducerFuzz::getRepState() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();
	auto endpoint = requestGen.getGetRepStateReq(reqBuffer->serializer);
	log->trace(LogLevel::Debug, "ProducerFuzz_QueueGetRepRequest");
	queueRequest(reqBuffer, endpoint);
	return 0;
}

int ProducerFuzz::setRepState() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();
	auto endpoint = requestGen.getSetRepStateReq(reqBuffer->serializer);
	log->trace(LogLevel::Debug, "ProducerFuzz_QueueSetRepRequest");
	queueRequest(reqBuffer, endpoint);
	return 0;
}

int ProducerFuzz::pushBatch() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();
	auto endpoint = requestGen.getPushBatchReq(reqBuffer->serializer);
	log->trace(LogLevel::Debug, "ProducerFuzz_QueuePushBatchRequest");
	queueRequest(reqBuffer, endpoint);
	return 0;
}

int ProducerFuzz::verifyRange() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();
	auto endpoint = requestGen.getVerifyRangesReq(reqBuffer->serializer);
	log->trace(LogLevel::Debug, "ProducerFuzz_QueueVerifyRangeRequest");
	queueRequest(reqBuffer, endpoint);
	return 0;
}
void ProducerFuzz::updateStatsOnResponse(const ConsumerAdapterResponse* resp, MessageStats epStats) {
	int err = 0;
	if (resp->response_type() == Response_ReplyResp) {
		auto r = static_cast<const ReplyResp*>(resp->response());
		err = r->error();
	} else {

		auto r = static_cast<const FinishResp*>(resp->response());
		err = r->error();
	}
	if (err) {
		errorsReturned[err] += 1;
	}
	if (resp->response_type() == Response_FinishResp ||
	    resp->response_type() == Response_ReplyResp && epStats.type != MessageBufferType::T_VerifyRangeReq) {
		if (!err) {
			// success
			verifiesSuccess += epStats.ranges;
			if (epStats.type == MessageBufferType::T_PushBatchReq) {
				bytesPushed += epStats.bytes;
				pushReqsSuccess++;
			}
			if (epStats.type == MessageBufferType::T_GetReplicatorStateReq) {
				getReqsSuccess++;
			}
		} else {

			log->trace(LogLevel::Error, "ProducerFuzz_RequestTxnFailed", { { "Error", STR(err) } });
			report();
			close();
			exit(err);
		}
		verifiesComplete += epStats.ranges;
		if (epStats.type == MessageBufferType::T_PushBatchReq) {
			pushReqsComplete++;
		}
		if (epStats.type == MessageBufferType::T_GetReplicatorStateReq) {
			getReqsComplete++;
		}
	}
	// TODO: calculate avg latency and throughput with elapsed
}

void ProducerFuzz::report() {
	int errorCount = 0;
	auto now = chrono::system_clock::now();
	auto elapsed = chrono::duration_cast<std::chrono::seconds>(now - testStartTS).count();

	log->trace("ProducerFuzz_TestReport", { { "TimeElapsed", STR(elapsed) },
	                                        { "RequestsServed", STR(requestsServed) },
	                                        { "RequestsLeft", STR(requestsToServe) },
	                                        { "PushBatchReqsComplete", STR(pushReqsComplete) },
	                                        { "PushBatchReqsSuccess", STR(pushReqsSuccess) },
	                                        { "VerifyRangeReqsComplete", STR(verifiesComplete) },
	                                        { "VerifyRangeReqsSuccess", STR(verifiesSuccess) },
	                                        { "GetRepStateReqsComplete", STR(getReqsComplete) },
	                                        { "GetRepStateReqsSuccess", STR(getReqsSuccess) },
	                                        { "BytesPushed", STR(bytesPushed) },
	                                        { "BytesSent", STR(bytesSent) } });
	vector<std::pair<std::string, std::string>> errorDetails;
	for (auto eIt : errorsReturned) {
		errorDetails.push_back({ STR(eIt.first), STR(eIt.second) });
		errorCount += eIt.second;
	}
	errorDetails.push_back({ "TotalErrors", STR(errorCount) });
	log->trace("ProducerFuzz_TestReportErrors", errorDetails);
}

bool ProducerFuzz::checkTestEnd() {
	auto now = chrono::system_clock::now();
	auto timeSinceResponse = chrono::duration_cast<std::chrono::seconds>(now - lastResponseTS).count();
	if (requestsServed >= requestsToServe && requestGen.endpointsWaitingForReply() == 0 &&
	    requestGen.endpointsWaitingForVerifyFinish() == 0) {
		log->trace("ProducerFuzz_TestComplete");
		report();
		close();
		return true;
	} else if (timeSinceResponse >= timeout) {

		log->trace("ProducerFuzz_TestErrorTimeout", { { "TimeSinceLastResponse", STR(timeSinceResponse) } });
		report();
		close();
		return true;
	}
	return false;
}

int ProducerFuzz::close() {
	try {

		log->trace("ProducerFuzz_CloseConnection");
		socket.close();
	} catch (std::exception& e) {
		log->trace("ProducerFuzz_CloseConnectionError", { { "Error", e.what() } });
		return -1;
	}
	io_context.stop();
	return 0;
}

// Can't run concurrently with other handlers on writeQStrand
void ProducerFuzz::requestGenerator() {
	// fill queue with requests
	while (requestGen.endpointsWaitingForSet() == 0 && requestGen.endpointsWaitingForReply() < maxRequestsWaiting &&
	       reqQueue.size() < maxRequestsQueued && requestsServed < requestsToServe) {
		int chooseReq = rand() % 100;
		// Note: could run setRepState, but if we don't drain all reqs first,
		// external consumer will fail existing requests and close.
		if (chooseReq < 10) {
			getRepState();
		} else if (chooseReq < 90 || requestGen.verifyReqsWaitingToSend() == 0) {
			pushBatch();
		} else {
			verifyRange();
		}

		log->trace("ProducerFuzz_QueueGeneratedRequest", { { "EPsWaiting", STR(requestGen.endpointsWaitingForReply()) },
		                                                   { "QueueSize", STR(reqQueue.size()) },
		                                                   { "RequestsServed", STR(requestsServed) } });
	}
	if (!checkTestEnd()) {
		writeQStrand.post(boost::bind(&ProducerFuzz::requestGenerator, shared_from_this()));
	}
}

void ProducerFuzz::handle_sig(const boost::system::error_code& error, int signal_number) {
	auto log = Log::get("pvTrace");
	log.trace("ProducerFuzz_TERMINATED", { { "Signal", STR(signal_number) }, { "Error", error.message() } });
	if (!error) {
		g_pf->report();
		g_pf->close();
	}
}
