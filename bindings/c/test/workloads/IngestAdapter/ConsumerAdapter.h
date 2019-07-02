#ifndef POLEVAULTSERVER_H
#define POLEVAULTSERVER_H
#pragma once

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/none_t.hpp>
#include <boost/optional.hpp>
#include <boost/array.hpp>

#include "ConsumerClient.h"
#include "ConsumerAdapterProtocol_generated.h"
#include "ConsumerAdapterUtils.h"

typedef boost::array<char, 1024> Buffer;

class ConsumerAdapter : public std::enable_shared_from_this<ConsumerAdapter> {
private:
	bool connected = false;
	bool started = false;
	boost::asio::io_context& io_context;

	boost::asio::ip::tcp::resolver resolver;
	uint32_t port;
	std::shared_ptr<ConsumerClientIF> consumerClient;
	std::shared_ptr<Log> log;

	boost::asio::ip::tcp::socket socket;
	boost::asio::streambuf _buffer;
	Buffer buffer;
	boost::asio::signal_set signals;

	std::map<int, std::shared_ptr<MessageBuffer>> activeReqBuffers;
	std::list<std::shared_ptr<MessageBuffer>> respQueue;
	boost::asio::io_context::strand responseQStrand;

public:
	static std::shared_ptr<ConsumerAdapter> create(boost::asio::io_context& io_context, unsigned p,
	                                               std::shared_ptr<ConsumerClientIF> ec, std::shared_ptr<Log> l) {
		return std::shared_ptr<ConsumerAdapter>(new ConsumerAdapter(io_context, p, ec, l));
	}

	~ConsumerAdapter();

	void connect();
	int start();
	int stop();

	// using cont ptr to choose template definition is tricky.
	// template <class PV_REQUEST> int serviceRequest(const PV_REQUEST *req);

private:
	ConsumerAdapter(boost::asio::io_context& io_context, unsigned p, std::shared_ptr<ConsumerClientIF> ec,
	                std::shared_ptr<Log> l);

	void waitForRequests();
	void handleConnect(const boost::system::error_code& error);
	void handleHeader(std::shared_ptr<MessageBuffer> reqBuffer, const boost::system::error_code& error,
	                  std::size_t bytesRead);
	void handleRequest(std::shared_ptr<MessageBuffer> reqBuffer, const boost::system::error_code& error,
	                   std::size_t bytesRead);

	void serviceRequest(std::shared_ptr<MessageBuffer> reqBuffer);

	static void handle_sig(const boost::system::error_code& error, int signal_number);

	void txnResponseCB(MessageBuffer* reqBuffer, bool freeBuffer);
	int createAndQueueResponse(MessageBuffer* reqBuffer, bool freeBuffer, MessageResponseType respType);
	int queueResponse(std::shared_ptr<MessageBuffer> respBuffer);
	void sendResponse();
	void handleFinishSendResponse(const boost::system::error_code& error, std::size_t bytes_transferred);
};

static ConsumerAdapter* g_consumerAdapter;

#endif
