#include "ConsumerAdapterUtils.h"
#include "ConsumerClient.h"

using namespace std;
using namespace ConsAdapter::serialization;

std::string ConsumerClientFDB6::repStateKey = "\xff/snowCannonProxy/ReplicatorState";
ConsumerClientFDB6* ConsumerClientFDB6::g_FDB6Client = NULL;

#ifndef INGEST_ADAPTER_SIM_TEST
ConsumerClientFDB6::ConsumerClientFDB6(std::string clusterFile, std::shared_ptr<Log> log)
  : clusterFile(clusterFile), log(log) {
	log->trace("ConsumerClient6Create");
	assert(g_FDB6Client == NULL);
	g_FDB6Client = this;

	fdb_error_t err;
	err = fdb_select_api_version(fdb_get_max_api_version());
	if (err) {
		log->trace("ConsumerClientFDB6ErrorCheckingAPIVersion", { { "Error", STR(fdb_get_error(err)) } });
	}
}
int ConsumerClientFDB6::stopNetwork() {
	log->trace("ConsumerClientFDB6StopNetwork");
	auto err = fdb_stop_network();
	if (err) {
		log->trace("ConsumerClientFDB6StopNetworkError", { { "Error", STR(fdb_get_error(err)) } });
	}
	err = pthread_join(network_thread, NULL);
	if (err) {
		log->trace("ConsumerClientFDB6StopNetworkError", { { "Error", STR(fdb_get_error(err)) } });
	}
	return err;
}

/* FDB network thread */
void* fdb_network_thread(void* args) {
	Log log;
	log->trace("ConsumerClientFDB6StartNetwork");
	fdb_error_t err = fdb_run_network();
	if (err) {
		log->trace(LogLevel::Error, "ConsumerClientFDB6StartNetworkError", { { "Error", STR(fdb_get_error(err)) } });
	}
	return NULL;
}

int ConsumerClientFDB6::startNetwork() {
	fdb_error_t err;

	if (doNetworkTrace) {
		log->trace("Client6EnableNetworkTracing", { { "Path", networkTracePath } });
		err = fdb_network_set_option(FDB_NET_OPTION_TRACE_ENABLE, (uint8_t*)networkTracePath.c_str(),
		                             networkTracePath.size());
		if (err) {
			log->trace(LogLevel::Error, "Client6EnableNetworkTracingError", { { "Error", STR(fdb_get_error(err)) } });
		}
		err = fdb_network_set_option(FDB_NET_OPTION_KNOB, (uint8_t*)networkKnobJson.c_str(), networkKnobJson.size());
		if (err) {
			log->trace(LogLevel::Error, "Client6EnableJsonError", { { "Error", STR(fdb_get_error(err)) } });
		}
	}

	err = fdb_setup_network();
	if (err) {
		log->trace(LogLevel::Error, "Client6SetupNetworkError", { { "Error", STR(fdb_get_error(err)) } });

		return err;
	}

	err = pthread_create(&network_thread, NULL, fdb_network_thread, NULL);
	if (err != 0) {
		log->trace(LogLevel::Error, "Client6StartNetworkThreadError", { { "Error", STR(fdb_get_error(err)) } });

		return -1;
	}

	err = fdb_create_database(clusterFile.c_str(), &database);
	if (err) {
		log->trace(LogLevel::Error, "Client6CreateDatabaseError", { { "Error", STR(fdb_get_error(err)) } });
		return err;
	}
	return err;
}
#else
ConsumerClientFDB6::ConsumerClientFDB6(FDBDatabase *db, std::shared_ptr<Log> log) : database(db), log(log) {
	log->trace("ConsumerClient6Create");
	assert(g_FDB6Client == NULL);
	g_FDB6Client = this;

	fdb_error_t err;
	err = fdb_select_api_version(fdb_get_max_api_version());
	if (err) {
		log->trace("ConsumerClientFDB6ErrorCheckingAPIVersion", { { "Error", STR(fdb_get_error(err)) } });
	}
}
int ConsumerClientFDB6::stopNetwork() {
	return 0;
}
int ConsumerClientFDB6::startNetwork() {
	return 0;
}
#endif

// FDB transaction and error  handling
#define SETUP_CONTEXT()                                                                                                \
	std::shared_ptr<Log> log = g_FDB6Client->log;                                                                      \
	MessageBuffer* reqBuffer = static_cast<MessageBuffer*>(arg);

#define SETUP_CB_CONTEXT()                                                                                             \
	SETUP_CONTEXT()                                                                                                    \
	auto txn = g_FDB6Client->getTransaction(reqBuffer);

// assumes FDBFuture *fut, fdb_error_t err, trace, and the reqBuffer are defined
#define CHECK_TXN_FUTURE_CB(fut, tag)                                                                                  \
	{                                                                                                                  \
		auto ferr = fdb_future_get_error(fut);                                                                         \
		log->trace(fmt::format("Client6_{}_TxnFutCBCheck", tag), { { "Error", STR(fdb_get_error(ferr)) } });             \
		if (ferr) {                                                                                                    \
			log->trace(fmt::format("Client6_{}_TxnRetryOnError", tag),                                                 \
			           { { "Error", STR(fdb_get_error(ferr)) }, { "Buffer", reqBuffer->toStr() } });                     \
			FDBFuture* futErr = fdb_transaction_on_error(txn, ferr);                                                   \
			fdb_error_t err2 = fdb_future_set_callback(futErr, &ConsumerClientFDB6::retryTxnCB, reqBuffer);            \
			if (err2) {                                                                                                \
				log->trace(LogLevel::Error, fmt::format("Client6_{}_TxnRetryFAIL", tag),                               \
				           { { "Error", STR(fdb_get_error(err2)) } });                                                   \
				fdb_future_destroy(futErr);                                                                            \
				reqBuffer->error = err2;                                                                               \
				g_FDB6Client->cleanTransaction(reqBuffer);                                                             \
				g_FDB6Client->sendResponse(reqBuffer, true);                                                           \
			}                                                                                                          \
			fdb_future_destroy(fut);                                                                                   \
			return;                                                                                                    \
		}                                                                                                              \
	}

// checks error on future operations after already checking the future's
// error.  Should never catch
#define CHECK(err, tag)                                                                                                \
	if (err) {                                                                                                         \
		log->trace(LogLevel::Error, fmt::format("Client6_{}_UnexpectedError", tag),                                    \
		           { { "Error", STR(fdb_get_error(err)) } }),                                                            \
		    assert(0);                                                                                                 \
	}

FDBTransaction* ConsumerClientFDB6::createTransaction(MessageBuffer* buffer, fdb_error_t& err) {
	std::unique_lock lock(txnMutex);
	log->trace("Client6CreateTxn", { { "Buffer", buffer->toStr() } });
	if (txnMap.find(buffer->id) != txnMap.end()) {
		log->trace(LogLevel::Error, "Client6CreateTxnAlreadyExistsError", { { "ID", STR(buffer->id) } });
		assert(0);
	}
	FDBTransaction* txn;
	err = fdb_database_create_transaction(database, &txn);
	txnMap[buffer->id] = txn;
	return txnMap[buffer->id];
}

FDBTransaction* ConsumerClientFDB6::getTransaction(MessageBuffer* buffer) {
	std::shared_lock lock(txnMutex);
	log->trace("Client6GetTxn", { { "Buffer", buffer->toStr() } });
	if (txnMap.find(buffer->id) == txnMap.end()) {
		log->trace(LogLevel::Error, "Client6NoTxnAssociatedWithBuffer", { { "ID", STR(buffer->id) } });
		assert(0);
	}
	return txnMap[buffer->id];
}

void ConsumerClientFDB6::cleanTransaction(MessageBuffer* buffer) {
	std::unique_lock lock(txnMutex);
	log->trace("Client6CleanTxn", { { "Buffer", buffer->toStr() } });
	if (txnMap.find(buffer->id) != txnMap.end()) {
		fdb_transaction_destroy(txnMap[buffer->id]);
		txnMap.erase(buffer->id);
	} else {
		log->trace(LogLevel::Error, "Client6CleanNoTxnAssociatedWithBuffer", { { "ID", STR(buffer->id) } });
		assert(0);
	}
}

void ConsumerClientFDB6::retryTxnCB(FDBFuture* fut, void* arg) {
	SETUP_CONTEXT();
	fdb_error_t err = fdb_future_get_error(fut);
	if (err) {
		log->trace(LogLevel::Error, "Client6RetryTxnError", { { "Error", STR(fdb_get_error(err)) } });
		g_FDB6Client->sendResponse(reqBuffer, true);
		return;
	}
	log->trace("ConsumerClientTxnRetry");
	g_FDB6Client->cleanTransaction(reqBuffer);
	g_FDB6Client->beginTxn(reqBuffer);
}

void ConsumerClientFDB6::sendResponse(MessageBuffer* buffer, bool free) {
	if (free) {
		log->trace("Client6FreeTxnOnResponse", { { "Buffer", buffer->toStr() } });
		cleanTransaction(buffer);
	}
	consumerTxnResponseCB(buffer, free);
}

// Get replicator state
// checks if it matches given replicator state
int ConsumerClientFDB6::beginTxn(MessageBuffer* reqBuffer) {
	// reset buffer reply (in case of retry)
	// create new transaction for this request
	reqBuffer->resetReply();
	fdb_error_t err = 0;
	auto txn = createTransaction(reqBuffer, err);
	ReplicatorStateExt statePassed(reqBuffer->getRepState());

	log->trace("Client6BeginTxnGetRepState", { { "Arg", statePassed.toStr() }, { "Buffer", reqBuffer->toStr() } });

	err = fdb_transaction_set_option(txn, FDB_TR_OPTION_ACCESS_SYSTEM_KEYS, NULL, 0);
	//err = fdb_transaction_set_option(txn, FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, (uint8_t*)debugTxnID.c_str(),
	//                                 debugTxnID.size());
	//err = fdb_transaction_set_option(txn, FDB_TR_OPTION_LOG_TRANSACTION, NULL, 0);

	CHECK(err, "TxnSetOptionAccessSystemKeys");

	// Get operation
	FDBFuture* f = fdb_transaction_get(txn, (uint8_t*)ConsumerClientFDB6::repStateKey.c_str(),
	                                   ConsumerClientFDB6::repStateKey.size(), false);
	log->trace("GETKEY", { { "Key", ConsumerClientFDB6::repStateKey } });
	err = fdb_future_set_callback(f, &ConsumerClientFDB6::checkReplicatorStateCB, reqBuffer);
	CHECK(err, "BeginTxnSetCB");
	return err;
}

void ConsumerClientFDB6::checkReplicatorStateCB(FDBFuture* fut, void* arg) {
	SETUP_CB_CONTEXT();
	CHECK_TXN_FUTURE_CB(fut, "GetReplicatorState");

	log->trace("Client6GetRepStateCB", { { "Buffer", reqBuffer->toStr() } });

	fdb_error_t err = 0;
	char* val;
	int valLen;
	int out_present;
	err = fdb_future_get_value(fut, &out_present, (const uint8_t**)&val, &valLen);
	CHECK(err, "GetValue");
	auto repState = flatbuffers::GetRoot<ConsAdapter::serialization::ReplicatorState>(static_cast<const void*>(val));

	if (out_present) {
		reqBuffer->replyRepState = ReplicatorStateExt(*repState);

		log->trace("Client6GetRepStateCBState", { { "RepState", reqBuffer->replyRepState.toStr() } });
	} else {
		log->trace("Client6GetRepStateCBStateNotFound");
	}

	fdb_future_destroy(fut);
	if (reqBuffer->type != MessageBufferType::T_SetReplicatorStateReq && !reqBuffer->checkReplicatorIDRegistry(log)) {
		reqBuffer->error = -1;

		log->trace("Client6GetRepStateCBIDMismatchAbort");
		g_FDB6Client->sendResponse(reqBuffer, true);
		return;
	}

	switch (reqBuffer->type) {
	case MessageBufferType::T_GetReplicatorStateReq:
		g_FDB6Client->sendResponse(reqBuffer, true);
		break;
	case MessageBufferType::T_SetReplicatorStateReq:
		g_FDB6Client->setReplicatorState(reqBuffer);
		break;
	case MessageBufferType::T_PushBatchReq:
		g_FDB6Client->pushBatch(reqBuffer);
		break;
	case MessageBufferType::T_VerifyRangeReq:
		g_FDB6Client->sendResponse(reqBuffer, false);
		g_FDB6Client->verifyRange(reqBuffer);
		break;
	default:
		log->trace(LogLevel::Error, "Client6BadRequestType");
		assert(0);
	}
}

void ConsumerClientFDB6::setReplicatorState(MessageBuffer* reqBuffer) {
	fdb_error_t err;
	FDBFuture* fut;
	auto txn = getTransaction(reqBuffer);
	CHECK(err, "SetRepState_GetTxn");

	ReplicatorStateExt setState = reqBuffer->replyRepState;
	setState.id = reqBuffer->getRepState().id;
	log->trace("Client6SetRepState", { { "Buffer", reqBuffer->toStr() },
	                                   { "Arg", reqBuffer->getRepState().toStr() },
	                                   { "Cur", reqBuffer->replyRepState.toStr() },
	                                   { "New", setState.toStr() } });

	reqBuffer->setStateObj = setState.serialize();

	fdb_transaction_set(txn, (uint8_t*)ConsumerClientFDB6::repStateKey.c_str(), ConsumerClientFDB6::repStateKey.size(),
	                    (uint8_t*)&reqBuffer->setStateObj, sizeof(ReplicatorState));

	// TEST: make sure accessing the object location in the vtable and copying
	// sizeof(object) gets us the whole serialized object
	/*{
	  uint8_t *tmp = (uint8_t *)malloc(sizeof(ReplicatorState));
	  memcpy(tmp, (uint8_t *)&reqBuffer->setStateObj, sizeof(ReplicatorState));
	  auto tmpRepState =
	      flatbuffers::GetRoot<ConsAdapter::serialization::ReplicatorState>(
	          static_cast<const void *>(tmp));
	  log->trace("TEST SERIALIZATION:{}", printObj(*tmpRepState));
	  free(tmp);
	  }*/

	fut = fdb_transaction_commit(txn);
	err = fdb_future_set_callback(fut, &ConsumerClientFDB6::commitTxnCB, reqBuffer);
	CHECK(err, "SetRepState_SetCB");
}

void ConsumerClientFDB6::pushBatch(MessageBuffer* reqBuffer) {
	log->trace("Client6PushBatch", { { "Buffer", reqBuffer->toStr() } });
	fdb_error_t err;
	auto txn = getTransaction(reqBuffer);

	for (int i = 0; i < reqBuffer->getMutations()->Length(); i++) {
		auto m = reqBuffer->getMutations()->Get(i);
		log->trace(LogLevel::Debug, "Client6PushBatchTraceMutation", { { "Mut", printObj(*m) } });
		if (m->type() == 0) { // FDBMutationType == SetValue
			log->trace(LogLevel::Debug, "Client6PushBatchTraceMutKey", { { "Key", m->param1()->str() } });
			fdb_transaction_set(txn, (uint8_t*)m->param1()->c_str(), m->param1()->str().size(),
			                    (uint8_t*)m->param2()->c_str(), m->param2()->str().size());
		} else if (m->type() == 1) { // FDBMutationType == ClearRange
			log->trace(LogLevel::Debug, "Client6PushBatchTraceMutClearRange",
			           { { "Key1", m->param1()->str() }, { "Key2", m->param2()->str() } });
			fdb_transaction_clear_range(txn, (uint8_t*)m->param1()->c_str(), m->param1()->str().size(),
			                            (uint8_t*)m->param2()->c_str(), m->param2()->str().size());
		} else {
			log->trace(LogLevel::Error, "Client6PushBatchMutationTypeNotSupported", { { "MsgType", STR(m->type()) } });
		}
	}
	FDBFuture* fut = fdb_transaction_commit(txn);
	err = fdb_future_set_callback(fut, &ConsumerClientFDB6::commitTxnCB, reqBuffer);
	CHECK(err, "PushBatch_SetCB");
}

void ConsumerClientFDB6::commitTxnCB(FDBFuture* fut, void* arg) {
	SETUP_CB_CONTEXT();
	CHECK_TXN_FUTURE_CB(fut, "TxnFinishCommit");

	log->trace("Client6CommitTxnCB", { { "Buffer", reqBuffer->toStr() } });
	g_FDB6Client->sendResponse(reqBuffer, true);

	fdb_future_destroy(fut);
}

void ConsumerClientFDB6::verifyRange(MessageBuffer* reqBuffer) {
	fdb_error_t err;
	FDBFuture* fut;
	auto txn = getTransaction(reqBuffer);

	log->trace("Client6VerifyRange", { { "Buffer", reqBuffer->toStr() },
	                                   { "RangeCount", STR(reqBuffer->getRanges()->Length()) },
	                                   { "CurrentRange", STR(reqBuffer->curVerifyRange) } });
	if (reqBuffer->curVerifyRange >= reqBuffer->getRanges()->Length()) {
		log->trace("Client6VerifyRangeComplete");
		g_FDB6Client->sendResponse(reqBuffer, true);
		return;
	}
	auto range = reqBuffer->getRanges()->Get(reqBuffer->curVerifyRange);
	log->trace("Client6VerifyRangeGetRange", { { "Range", printObj(*range) } });

	fut = fdb_transaction_get_range(
	    txn, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)range->begin()->c_str(), range->begin()->str().size()),
	    FDB_KEYSEL_FIRST_GREATER_THAN((uint8_t*)range->end()->c_str(), range->end()->str().size()), 0 /* limit */,
	    0 /* target_bytes */, FDB_STREAMING_MODE_WANT_ALL /* FDBStreamingMode */, 0 /* iteration */, 0 /* snapshot */,
	    0 /* reverse */
	);

	err = fdb_future_set_callback(fut, &ConsumerClientFDB6::verifyRangeCB, reqBuffer);
	CHECK(err, "VerifyRange_SetCB");
}

void ConsumerClientFDB6::verifyRangeCB(FDBFuture* fut, void* arg) {
	SETUP_CB_CONTEXT();
	CHECK_TXN_FUTURE_CB(fut, "VerifyRange");
	fdb_error_t err;
	FDBKeyValue const* out_kv;
	int out_count;
	int out_more;
	uint64_t totalSize = 0;
	Crc32 crc;
	uint32_t checksum = 0;
	uint32_t argChecksum = reqBuffer->getChecksum(reqBuffer->curVerifyRange);
	string beginKey;
	string endKey;
	string beginValue;
	string endValue;
	reqBuffer->respType = MessageResponseType::T_FinishResp; // change response type

	err = fdb_future_get_keyvalue_array(fut, &out_kv, &out_count, &out_more);
	log->trace("Client6VerifyRangeCB", { { "Buffer", reqBuffer->toStr() }, { "OutCount", STR(out_count) } });

	CHECK(err, "VerifyRangeCB_ReadKeyValueArray");
	if (out_more) {
		log->trace(LogLevel::Error, "Client6VerifyRangeFAILRangeTooLarge");
		// TODO: what error to set this to?
		reqBuffer->error = -1;
		g_FDB6Client->sendResponse(reqBuffer, true);
		return;
	}

	for (int i = 0; i < out_count; i++) {
		string key = string((const char*)out_kv[i].key, out_kv[i].key_length);
		string value = string((const char*)out_kv[i].value, out_kv[i].value_length);
		if (i == 0) {
			beginKey = key;
			beginValue = value;
		}
		if (i == out_count - 1) {
			endKey = key;
			endValue = value;
		}

		log->trace(LogLevel::Debug, "Client6VerifyRangeCheckKV", { { "Key", key }, { "Value", value } });
		crc.block(out_kv[i].key, out_kv[i].key_length);
		crc.block(out_kv[i].value, out_kv[i].value_length);
		totalSize += out_kv[i].key_length;
		totalSize += out_kv[i].value_length;
	}
	checksum = crc.sum();
	if (checksum != argChecksum) {
		log->trace(LogLevel::Error, "Client6VerifyRangeFAILChecksumMismatch",
		           { { "BeginKey", beginKey },
		             { "EndKey", endValue },
		             { "Calculated", STR(checksum) },
		             { "Expected", STR(argChecksum) },
		             { "KVSize", STR(totalSize) },
		             { "Index", STR(reqBuffer->curVerifyRange) } });
		g_FDB6Client->sendResponse(reqBuffer, true);
		return;
	} else {
		log->trace("Client6VerifyRangeSUCCESSTrace", { { "BeginKey", beginKey },
		                                               { "EndKey", endValue },
		                                               { "Calculated", STR(checksum) },
		                                               { "Expected", STR(argChecksum) },
		                                               { "KVSize", STR(totalSize) },
		                                               { "Index", STR(reqBuffer->curVerifyRange) } });
	}

	reqBuffer->curVerifyRange++;
	reqBuffer->replyChecksums.push_back(checksum);
	// loop
	g_FDB6Client->verifyRange(reqBuffer);
	fdb_future_destroy(fut);
}
