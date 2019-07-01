#ifndef EXTERNALCLIENT_H
#define EXTERNALCLIENT_H
#pragma once

#define FDB_API_VERSION 610

#include "ConsumerAdapterProtocol_generated.h"
#include "ConsumerAdapterUtils.h"
#include <boost/bind.hpp>

#include <boost/function.hpp>

// Ugly hack until we can make the client a discoverable module
// in both projects
#ifdef INGEST_ADAPTER_SIM_TEST
#include "foundationdb/fdb_c.h"
#else
#include <fdb_c.h>
#endif

#include <mutex>
#include <shared_mutex>
#include <vector>

class ConsumerClientIF {
public:
	virtual int beginTxn(MessageBuffer* msgBuf) = 0;
	virtual int startNetwork() = 0;
	virtual int stopNetwork() = 0;
	virtual void registerTxnResponseCallback(boost::function<void(MessageBuffer* reqBuffer, bool freeBuffer)> cb) = 0;
};

#ifndef INGEST_ADAPTER_SIM_TEST
class ConsumerClientTester : public ConsumerClientIF {
private:
	boost::asio::io_context& io_context;
	Log log;
	boost::function<void(MessageBuffer* reqBuffer, bool freeBuffer)> consumerTxnResponseCB;

public:
	ConsumerClientTester(boost::asio::io_context& io_context);
	int beginTxn(MessageBuffer* msgBuf) override;
	int startNetwork() override;
	int stopNetwork() override;
	void registerTxnResponseCallback(boost::function<void(MessageBuffer* reqBuffer, bool freeBuffer)> cb) override {
		consumerTxnResponseCB = cb;
	};

private:
	void getReplicatorState(MessageBuffer* reqBuffer);
	void setReplicatorStateCommit(MessageBuffer* reqBuffer);
	void pushBatchCommit(MessageBuffer* reqBuffer);
	void verifyRange(MessageBuffer* reqBuffer);
	void verifyRangeCB(MessageBuffer* reqBuffer);
};
#endif

class ConsumerClientFDB6 : public ConsumerClientIF {
private:
	static ConsumerClientFDB6* g_FDB6Client;
	std::shared_ptr<Log> log;

#ifndef INGEST_ADAPTER_SIM_TEST
	pthread_t network_thread;
#endif
	std::string clusterFile;
	FDBDatabase* database;
	std::map<uint64_t, FDBTransaction*> txnMap;
	boost::function<void(MessageBuffer* reqBuffer, bool freeBuffer)> consumerTxnResponseCB;
	bool doNetworkTrace = 1;

	std::string debugTxnID = "TXN1234";
	std::string networkTracePath = "";
	std::string networkKnobJson = "knob_trace_json=1";
	mutable std::shared_mutex txnMutex;

public:
	static std::string repStateKey;

public:
#ifndef INGEST_ADAPTER_SIM_TEST
	ConsumerClientFDB6(std::string clusterFile, std::shared_ptr<Log> log);
#else
	ConsumerClientFDB6(FDBDatabase* db, std::shared_ptr<Log> log);
#endif
	~ConsumerClientFDB6() {
		log->trace("ConsumerClientFDB6Destroy");
		g_FDB6Client = NULL;
	};
	static ConsumerClientFDB6* instance();
	int beginTxn(MessageBuffer* msgBuf) override;
	int startNetwork() override;
	int stopNetwork() override;
	void registerTxnResponseCallback(boost::function<void(MessageBuffer* reqBuffer, bool freeBuffer)> cb) override {
		consumerTxnResponseCB = cb;
	};

private:
	FDBTransaction* createTransaction(MessageBuffer* buffer, fdb_error_t& err);
	FDBTransaction* getTransaction(MessageBuffer* buffer);
	void cleanTransaction(MessageBuffer* buffer);
	void sendResponse(MessageBuffer* buffer, bool free);

	static void retryTxnCB(FDBFuture* fut, void* arg);
	static void commitTxnCB(FDBFuture* fut, void* arg);
	static void checkReplicatorStateCB(FDBFuture* fut, void* arg);
	static void verifyRangeCB(FDBFuture* fut, void* arg);

	void setReplicatorState(MessageBuffer* reqBuffer);
	void pushBatch(MessageBuffer* reqBuffer);
	void verifyRange(MessageBuffer* reqBuffer);
};

#endif
