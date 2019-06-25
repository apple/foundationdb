#ifndef POLEVAULTCLIENT_H
#define POLEVAULTCLIENT_H
#pragma once

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include "ConsumerAdapterUtils.h"
#include <boost/enable_shared_from_this.hpp>
#include <string>

#include <spdlog/spdlog.h>

#include "ConsumerAdapter.h"
#include "ConsumerAdapterProtocol_generated.h"
#include "ConsumerClient.h"
#include "EndpointLoadGenerator.h"
#include <list>
#include <set>

class ProducerFuzz : public std::enable_shared_from_this<ProducerFuzz> {
private:
	std::mutex writeLock;
	boost::asio::io_context& io_context;
	boost::asio::ip::tcp::socket socket;
	unsigned port;
	std::shared_ptr<spdlog::logger> trace;
	boost::asio::ip::tcp::acceptor acceptor_;
	boost::asio::signal_set signals;
	boost::asio::io_context::strand writeQStrand;
	bool connected = false;
	int currentIDSending = 0;

	std::list<std::shared_ptr<MessageBuffer>> reqQueue;
	MessageBuffer respBuffer;

	// testing:
	int timeout = 5; // fail test if we don't get a response in 5 seconds
	int maxRequestsQueued;
	int maxRequestsWaiting;
	int requestsServed = 0;
	int requestsToServe;
	uint64_t bytesPushed = 0;
	uint64_t bytesSent = 0;
	uint64_t verifiesComplete = 0;
	uint64_t verifiesSuccess = 0;
	uint64_t getReqsComplete = 0;
	uint64_t getReqsSuccess = 0;
	uint64_t pushReqsComplete = 0;
	uint64_t pushReqsSuccess = 0;
	uint64_t timeElapsed = 0;
	// uint64_t avgPushThroughput = 0;
	// uint64_t avgPushLatency = 0;
	std::map<int, int> errorsReturned;
	std::chrono::system_clock::time_point testStartTS;
	std::chrono::system_clock::time_point lastResponseTS;

public:
	EndpointLoadGenerator requestGen;
	static std::shared_ptr<ProducerFuzz> create(boost::asio::io_context& io_context, unsigned p) {
		return std::shared_ptr<ProducerFuzz>(new ProducerFuzz(io_context, p));
	}
	~ProducerFuzz();

	void start(int reqsToServe, int maxReqsQueued = 10, int maxReqsWaiting = 10);
	void report();

private:
	ProducerFuzz(boost::asio::io_context& io_context, unsigned p);
	int queueRequest(std::shared_ptr<MessageBuffer> reqBuffer, int endpoint);
	void sendRequest();
	void waitForResponses();
	void handleAccept(const boost::system::error_code& error);
	void handleFinishWrite(int endpoint, const boost::system::error_code& error, std::size_t bytes_transferred);
	void handleHeader(const boost::system::error_code& error, std::size_t bytes_transferred);
	void handleResponse(const boost::system::error_code& error, std::size_t bytes_transferred);

	static void handle_sig(const boost::system::error_code& error, int signal_number);

	void requestGenerator();
	int getRepState();
	int setRepState();
	int pushBatch();
	int verifyRange();

	bool checkTestEnd();
	int close();

	void updateStatsOnResponse(const ConsAdapter::serialization::ConsumerAdapterResponse* resp, MessageStats epStats);
};

static ProducerFuzz* g_pf = NULL;

#endif
