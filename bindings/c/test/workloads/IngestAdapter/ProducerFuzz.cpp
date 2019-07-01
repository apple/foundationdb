#include "ProducerFuzz.h"
#include "ConsumerAdapterUtils.h"
#include <boost/lexical_cast.hpp>

using boost::asio::ip::tcp;
using namespace std;
using namespace ConsAdapter::serialization;

ProducerFuzz::ProducerFuzz(boost::asio::io_context& io_context, unsigned p)
  : port(p), io_context(io_context), socket(io_context), acceptor_(io_context, tcp::endpoint(tcp::v4(), p)),
    signals(io_context, SIGTERM, SIGINT), writeQStrand(io_context) {

	trace = spdlog::get("pvTrace");
	trace->flush_on(spdlog::level::info);
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
	trace->info("producerFuzz test start: requests to serve:{} max queued:{}", requestsToServe, maxRequestsQueued);
	trace->info("producerFuzz waiting to connect");
	setRepState(); // always start with setting the replicator state
	requestGenerator();
	acceptor_.async_accept(
	    socket, boost::bind(&ProducerFuzz::handleAccept, shared_from_this(), boost::asio::placeholders::error));
}
void ProducerFuzz::handleAccept(const boost::system::error_code& error) {
	trace->info("producerFuzz connected err:{}", error.message());
	connected = true;
	waitForResponses();
}

ProducerFuzz::~ProducerFuzz() {
	trace->info("producerFuzz DELETE");
}

int ProducerFuzz::queueRequest(std::shared_ptr<MessageBuffer> reqBuffer, int endpoint) {
	reqBuffer->prepareWrite();
	reqBuffer->endpoint = endpoint;

	trace->info("producerFuzz preparing req Header:{}", printObj(reqBuffer->header));
	for (auto b : reqBuffer->writeBuffers) {
		trace->info("buff size:{}", b.size());
	}
	reqQueue.push_back(reqBuffer);
	trace->info("producerFuzz queue request size:{} endpoint:{}", reqQueue.size(), reqBuffer->endpoint);
	if (reqQueue.size() == 1) {
		sendRequest();
	}
	return 0;
}

void ProducerFuzz::sendRequest() {
	if (reqQueue.empty()) {
		trace->warn("producerFuzz sendRequest no work");
		return;
	}
	if (currentIDSending == reqQueue.front()->id) {
		trace->info("producerFuzz skip duplicate send for id {}", currentIDSending);
		return;
	}

	auto messageBuf = reqQueue.front();
	currentIDSending = messageBuf->id;
	requestGen.updateEndpointSendTime(messageBuf->endpoint);
	trace->info("producerFuzz sendRequest endpoint:{} id:{}", messageBuf->endpoint, currentIDSending);

	boost::asio::async_write(
	    socket, messageBuf->getWriteBuffers(),
	    boost::asio::bind_executor(writeQStrand, boost::bind(&ProducerFuzz::handleFinishWrite, shared_from_this(),
	                                                         currentIDSending, boost::asio::placeholders::error,
	                                                         boost::asio::placeholders::bytes_transferred)));
}

// Can't run concurrently with other handlers on writeQStrand
void ProducerFuzz::handleFinishWrite(int id, const boost::system::error_code& error, std::size_t bytes_transferred) {
	{
		trace->info("producerFuzz write complete err:{}, id:{} bytes:{} queue size:{}", error.message(), id,
		            bytes_transferred, reqQueue.size());
	}
	assert(id == currentIDSending);
	assert(!reqQueue.empty());
	assert(id == reqQueue.front()->id);
	currentIDSending = 0;
	if (!error) {
		requestsServed++;
		bytesSent += bytes_transferred;
		// int ep = reqQueue.front()->endpoint;
		trace->info("producerFuzz pop message endpoint:{} id:", reqQueue.front()->endpoint, id);
		reqQueue.pop_front();
	}
	if (!reqQueue.empty()) {
		sendRequest();
	}
}

void ProducerFuzz::waitForResponses() {
	trace->info("producerFuzz waiting for responses ");

	boost::asio::async_read(socket, boost::asio::buffer((void*)&respBuffer.header, sizeof(respBuffer.header)),
	                        boost::asio::transfer_exactly(sizeof(MessageHeader)),
	                        boost::bind(&ProducerFuzz::handleHeader, shared_from_this(),
	                                    boost::asio::placeholders::error,
	                                    boost::asio::placeholders::bytes_transferred));
}

void ProducerFuzz::handleHeader(const boost::system::error_code& error, // Result of operation.

                                std::size_t bytes_transferred) {
	if (error) {
		// TODO: close
		trace->error("producerFuzz response reading header ERROR:{}", error.message());
	}
	if (bytes_transferred != sizeof(MessageHeader)) {
		trace->error("producerFuzz response handle header ERROR: transfered only {}", bytes_transferred);
		// TODO: close
	}

	trace->info("producerFuzz response handle header:{}", printObj(respBuffer.header));

	respBuffer.readBuffer.resize(respBuffer.header.size);

	boost::asio::async_read(
	    socket, boost::asio::buffer(respBuffer.readBuffer), boost::asio::transfer_exactly(respBuffer.header.size),
	    boost::bind(&ProducerFuzz::handleResponse, shared_from_this(), boost::asio::placeholders::error,
	                boost::asio::placeholders::bytes_transferred));
}

void ProducerFuzz::handleResponse(const boost::system::error_code& error, // Result of operation.
                                  std::size_t bytes_transferred) {

	trace->info("producerFuzz response handle message read:{} header:{}", bytes_transferred, respBuffer.header.toStr());
	if (error) {
		// TODO: close
		trace->error("producerFuzz response reading response ERROR:{}", error.message());
	}
	if (bytes_transferred != respBuffer.header.size) {
		trace->error("producerFuzz response handle message ERROR: transfered only {}", bytes_transferred);
		// TODO: close
	}

	// check message checksum
	Crc32 crc;
	uint32_t checksum = crc.sum(static_cast<const void*>(respBuffer.readBuffer.data()), respBuffer.header.size);
	trace->info("producerFuzz response check response sum:{}", checksum);
	if (checksum != respBuffer.header.checksum) {
		trace->error("ERROR: producerFuzz response failed checksum");
		return;
	}

	auto polevaultResp = flatbuffers::GetRoot<ConsumerAdapterResponse>(respBuffer.readBuffer.data());

	trace->info("producerFuzz response service response... ep:{}", polevaultResp->endpoint());
	int ret;
	MessageStats epStats;
	switch (polevaultResp->response_type()) {
	case Response_ReplyResp: {
		auto resp = static_cast<const ReplyResp*>(polevaultResp->response());
		trace->info("producerFuzz response Reply:{} Endpoint:{} Err:{}", printObj(*resp->repState()),
		            polevaultResp->endpoint(), resp->error());
		epStats = requestGen.waitingEPGotReply(polevaultResp->endpoint(), resp->error());
		break;
	}
	case Response_FinishResp: {
		auto resp = static_cast<const FinishResp*>(polevaultResp->response());
		trace->info("producerFuzz response Finish (VerifyRange) Endpoint:{} Err:{}", polevaultResp->endpoint(),
		            resp->error());
		epStats = requestGen.waitingEPGotVerifyFinish(polevaultResp->endpoint());

		break;
	}
	}
	updateStatsOnResponse(polevaultResp, epStats);
	trace->debug("producerFuzz response reqs served:{} endpoints waiting for reply:'{}' waiting "
	             "for finish:'{}'...",
	             requestsServed, requestGen.endpointsWaitingForReply(), requestGen.endpointsWaitingForVerifyFinish());
	lastResponseTS = chrono::system_clock::now();
	if (!checkTestEnd()) {
		waitForResponses();
	}
}
int ProducerFuzz::getRepState() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();

	auto endpoint = requestGen.getGetRepStateReq(reqBuffer->serializer);
	trace->info("get rep state queue done");
	queueRequest(reqBuffer, endpoint);
	return 0;
}

int ProducerFuzz::setRepState() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();

	auto endpoint = requestGen.getSetRepStateReq(reqBuffer->serializer);
	trace->info("set rep state queue done");
	queueRequest(reqBuffer, endpoint);
	return 0;
}

int ProducerFuzz::pushBatch() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();

	auto endpoint = requestGen.getPushBatchReq(reqBuffer->serializer);
	trace->info("pushBatch queue done");
	queueRequest(reqBuffer, endpoint);
	return 0;
}

int ProducerFuzz::verifyRange() {
	std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();

	auto endpoint = requestGen.getVerifyRangesReq(reqBuffer->serializer);
	trace->info("verifyRange queue done");
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
			trace->error("FATAL: txn failed:{}", err);
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
	trace->info("producerFuzz test report:");

	trace->info("......time elapsed:{}", elapsed);
	trace->info("......requests served:{}", requestsServed);
	trace->info("......requests left to serve:{}", requestsToServe);
	trace->info("......pushBatches requests completed:{} success:{}", pushReqsComplete, pushReqsSuccess);
	trace->info("......verifyRanges completed:{} success:{}", verifiesComplete, verifiesSuccess);
	trace->info("......getRepState requests completed:{} success:{}", getReqsComplete, getReqsSuccess);
	trace->info("......bytes pushed:{}", bytesPushed);
	trace->info("......bytes sent:{}", bytesSent);
	for (auto eIt : errorsReturned) {
		trace->info("......error:{} count:{}", eIt.first, eIt.second);
		errorCount += eIt.second;
	}
	trace->info("......total errors returned:{}", errorCount);
}

bool ProducerFuzz::checkTestEnd() {
	auto now = chrono::system_clock::now();
	auto timeSinceResponse = chrono::duration_cast<std::chrono::seconds>(now - lastResponseTS).count();
	if (requestsServed >= requestsToServe && requestGen.endpointsWaitingForReply() == 0 &&
	    requestGen.endpointsWaitingForVerifyFinish() == 0) {

		trace->info("producerFuzz test COMPLETE");
		report();
		close();
		return true;
	} else if (timeSinceResponse >= timeout) {
		trace->info("producerFuzz test ERROR timeout: time since last resp:{}", timeSinceResponse);
		report();
		close();
		return true;
	}
	return false;
}

int ProducerFuzz::close() {
	try {
		trace->info("close connection");
		socket.close();
	} catch (std::exception& e) {
		trace->error("ERR: {}", e.what());
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
		trace->info("producerFuzz req generator queued request epsWaiting:{} qSize:{} "
		            "reqsServed:{}",
		            requestGen.endpointsWaitingForReply(), reqQueue.size(), requestsServed);
	}
	if (!checkTestEnd()) {
		writeQStrand.post(boost::bind(&ProducerFuzz::requestGenerator, shared_from_this()));
	}
}

void ProducerFuzz::handle_sig(const boost::system::error_code& error, int signal_number) {
	auto trace = spdlog::get("pvTrace");
	trace->info("producerFuzz TERMINATED sig:{} err:{}", signal_number, error.value());
	if (!error) {
		g_pf->report();
		g_pf->close();
	}
}
