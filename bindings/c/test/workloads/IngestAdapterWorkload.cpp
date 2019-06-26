#define FDB_API_VERSION 610
#include "foundationdb/fdb_c.h"
#undef DLLEXPORT
#include "workloads.h"
#include "IngestAdapter/EndpointLoadGenerator.h"
#include "IngestAdapter/ConsumerClient.h"

#include "IngestAdapter/ConsumerAdapterUtils.h"

#include <random>

namespace {

struct IngestAdapterWorkload : FDBWorkload {
	static const std::string name;
	bool success = true;
	FDBWorkloadContext* cxt;
	struct ActorRunner {
		GenericPromise<bool> done;
		IngestAdapterWorkload* self;
	};
	std::unique_ptr<ActorRunner> runner;

	EndpointLoadGenerator requestGen;
	std::shared_ptr<ConsumerClientIF> consumerClient;

	std::map<int, std::shared_ptr<MessageBuffer>> activeReqBuffers;

	// stats:
	int timeout = 5; // fail test if we don't get a response in 5 seconds
	double requestsServed = 0;
	double bytesPushed = 0;
	double bytesSent = 0;
	double verifiesComplete = 0;
	double verifiesSuccess = 0;
	double getReqsComplete = 0;
	double getReqsSuccess = 0;
	double pushReqsComplete = 0;
	double pushReqsSuccess = 0;
	// uint64_t avgPushThroughput = 0;
	// uint64_t avgPushLatency = 0;
	std::map<int, int> errorsReturned;

	// test args
	int requestsToServe;
	int maxRequestsWaiting;

	uint32_t random() { return cxt->rnd(); }

	std::string description() const override { return name; }
	bool init(FDBWorkloadContext* context) override {
		cxt = context;
		consumerClient->registerTxnResponseCallback(boost::bind(&IngestAdapterWorkload::txnResponseCB, this, _1, _2));
		requestsToServe = context->getOption("requestsToServe", 100000ul);
		maxRequestsWaiting = context->getOption("maxRequestsWaiting", 100ul);
		requestGen.init(1000000 /* total key range */, 10000 /* max value size*/, 100 /* max mutations in batch */,
		                10 /* max keyRange size*/, 3 /* max ranges in verifyRange request */,
		                10 /* max waiting verifyRanges */);
		return true;
	}
	void setup(FDBDatabase* db, GenericPromise<bool> done) override { done.send(true); }

	void start(FDBDatabase* db, GenericPromise<bool> done) override {
		runner.reset(new ActorRunner{ std::move(done), this });
		// set the replicator state first
		sendRequest(MessageBufferType::T_SetReplicatorStateReq);
	}

	void check(FDBDatabase* db, GenericPromise<bool> done) override { done.send(success); }
	void getMetrics(std::vector<FDBPerfMetric>& out) const override {
		out.emplace_back(FDBPerfMetric{ "reqsServed", requestsServed, false });
		out.emplace_back(FDBPerfMetric{ "verifies", verifiesComplete, false });
		out.emplace_back(FDBPerfMetric{ "verifiesSuccess", verifiesSuccess, false });
		out.emplace_back(FDBPerfMetric{ "pushReqs", pushReqsComplete, false });
		out.emplace_back(FDBPerfMetric{ "pushReqsSuccess", pushReqsSuccess, false });
		out.emplace_back(FDBPerfMetric{ "getStateReqs", getReqsComplete, false });
		out.emplace_back(FDBPerfMetric{ "getStateReqsSuccess", getReqsSuccess, false });
		out.emplace_back(FDBPerfMetric{ "bytesPushed", bytesPushed, false });
	}

	void sendRequests() {

		cxt->trace(FDBSeverity::Info, "IngestWorkloadSendRequests", {});
		while (requestGen.endpointsWaitingForSet() == 0 && requestGen.endpointsWaitingForReply() < maxRequestsWaiting) {
			auto chooseReq = random();
			if (chooseReq < 10) {
				sendRequest(MessageBufferType::T_GetReplicatorStateReq);
			} else if (chooseReq < 90 || requestGen.verifyReqsWaitingToSend() == 0) {
				sendRequest(MessageBufferType::T_PushBatchReq);
			} else {
				sendRequest(MessageBufferType::T_VerifyRangeReq);
			}
		}
	}

	void sendRequest(MessageBufferType reqType) {

		cxt->trace(FDBSeverity::Info, "IngestWorkloadSendRequest", { { "type", printRequestType(reqType) } });
		std::shared_ptr<MessageBuffer> reqBuffer = std::make_shared<MessageBuffer>();
		int ep;
		switch (reqType) {
		case MessageBufferType::T_GetReplicatorStateReq:
			ep = requestGen.getGetRepStateReq(reqBuffer->serializer);
			break;
		case MessageBufferType::T_SetReplicatorStateReq:
			ep = requestGen.getSetRepStateReq(reqBuffer->serializer);
			break;
		case MessageBufferType::T_PushBatchReq:
			ep = requestGen.getPushBatchReq(reqBuffer->serializer);
			break;
		case MessageBufferType::T_VerifyRangeReq:
			ep = requestGen.getVerifyRangesReq(reqBuffer->serializer);
			break;
		default:
			assert(0);
			break;
		}
		activeReqBuffers[ep] = reqBuffer;
		requestsServed++;
		consumerClient->beginTxn(reqBuffer.get());
	}

	void endTestOrSendMoreRequests() {
		if (requestsServed >= requestsToServe) {
			runner->done.send(true);
		} else {
			// send another batch of requests
			// someday: choose to change the replicator registration
			sendRequests();
		}
	}

	void txnResponseCB(MessageBuffer* reqBuffer, bool freeBuffer) {
		cxt->trace(FDBSeverity::Info, "IngestWorkloadTxnCB",
		           { { "buffer", reqBuffer->toStr() }, { "free", STR(freeBuffer) } });
		handleResponse(reqBuffer); // handle response directly

		// simulate createAndQueueResponse in ConsumerAdapter
		if (freeBuffer) {
			activeReqBuffers.erase(reqBuffer->id);
		}
		endTestOrSendMoreRequests();
		// end test or send more requests?
	}
	// update stats and endpoint tracker
	void handleResponse(MessageBuffer* buf) {

		// Simulate handleResponse in ProducerFuzz
		MessageStats epStats;
		if (buf->respType == MessageResponseType::T_ReplyResp) {
			epStats = requestGen.waitingEPGotReply(buf->endpoint);
		} else {
			epStats = requestGen.waitingEPGotVerifyFinish(buf->endpoint);
		}

		int err = buf->error;
		if (err) {
			errorsReturned[buf->error] += 1;
		}

		if (buf->respType == MessageResponseType::T_FinishResp ||
		    (buf->respType == MessageResponseType::T_ReplyResp && buf->type != MessageBufferType::T_VerifyRangeReq)) {
			if (!err) {
				// success
				verifiesSuccess += epStats.ranges;
				if (buf->type == MessageBufferType::T_PushBatchReq) {
					bytesPushed += epStats.bytes;
					pushReqsSuccess++;
				}
				if (buf->type == MessageBufferType::T_GetReplicatorStateReq) {
					getReqsSuccess++;
				}
			}
			verifiesComplete += epStats.ranges;
			if (epStats.type == MessageBufferType::T_PushBatchReq) {
				pushReqsComplete++;
			}
			if (epStats.type == MessageBufferType::T_GetReplicatorStateReq) {
				getReqsComplete++;
			}
		}
	}
};

const std::string IngestAdapterWorkload::name = "IngestAdapterWorkload";

} // namespace

FDBWorkloadFactoryT<IngestAdapterWorkload> ingestAdapterWorkload(IngestAdapterWorkload::name);
