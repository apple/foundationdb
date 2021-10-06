#pragma once
#include <string>
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H
#include "fdbserver/RemoteIKeyValueStore.actor.g.h"
#elif !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbrpc/FlowTransport.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"

#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

enum RemoteIKVSWellKnownEndpoints {
	WLTOKEN_IKVS_PROCESS_SERVER = WLTOKEN_FIRST_AVAILABLE,
	WLTOKEN_IKVS_GET,
	WLTOKEN_IKVS_SET,
	WLTOKEN_IKVS_CLEAR,
	WLTOKEN_IKVS_COMMIT,
	WLTOKEN_IKVS_READ_PREFIX,
	WLTOKEN_IKVS_READ_RANGE,
	WLTOKEN_IKVS_GET_STORAGE_BYTES,
	WLTOKEN_IKVS_GET_ERROR,
	WLTOKEN_IKVS_ON_CLOSED,
	WLTOKEN_IKVS_DISPOSE,
	WLTOKEN_IKVS_CLOSE
};

struct IKVSProcessInterface {

	constexpr static FileIdentifier file_identifier = 3491838;
	RequestStream<struct GetIKVSProcessInterfaceRequest> getProcessInterface;
	RequestStream<struct OpenKVStoreRequest> openKVStore;

	UID uniqueID = deterministicRandom()->randomUniqueID();

	UID id() const { return uniqueID; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getProcessInterface, openKVStore);
	}
};

struct IKVSInterface {
	constexpr static FileIdentifier file_identifier = 4929113;
	RequestStream<struct IKVSGetValueRequest> getValue;
	RequestStream<struct IKVSSetRequest> set;
	RequestStream<struct IKVSClearRequest> clear;
	RequestStream<struct IKVSCommitRequest> commit;
	RequestStream<struct IKVSReadValuePrefixRequest> readValuePrefix;
	RequestStream<struct IKVSReadRangeRequest> readRange;
	RequestStream<struct IKVSGetStorageByteRequest> getStorageBytes;
	RequestStream<struct IKVSGetErrorRequest> getError;
	RequestStream<struct IKVSOnClosedRequest> onClosed;
	RequestStream<struct IKVSDisposeRequest> dispose;
	RequestStream<struct IKVSCloseRequest> close;

	UID uniqueID = deterministicRandom()->randomUniqueID();

	std::string tempId;

	UID id() const { return uniqueID; }

	KeyValueStoreType storeType;

	KeyValueStoreType type() const { return storeType; }

	void initializeEndpoints() {
		getValue.makeWellKnownEndpoint(WLTOKEN_IKVS_GET, TaskPriority::DefaultEndpoint);
		set.makeWellKnownEndpoint(WLTOKEN_IKVS_SET, TaskPriority::DefaultEndpoint);
		clear.makeWellKnownEndpoint(WLTOKEN_IKVS_CLEAR, TaskPriority::DefaultEndpoint);
		commit.makeWellKnownEndpoint(WLTOKEN_IKVS_COMMIT, TaskPriority::DefaultEndpoint);
		readValuePrefix.makeWellKnownEndpoint(WLTOKEN_IKVS_READ_PREFIX, TaskPriority::DefaultEndpoint);
		readRange.makeWellKnownEndpoint(WLTOKEN_IKVS_READ_RANGE, TaskPriority::DefaultEndpoint);
		getStorageBytes.makeWellKnownEndpoint(WLTOKEN_IKVS_GET_STORAGE_BYTES, TaskPriority::DefaultEndpoint);
		getError.makeWellKnownEndpoint(WLTOKEN_IKVS_GET_ERROR, TaskPriority::DefaultEndpoint);
		onClosed.makeWellKnownEndpoint(WLTOKEN_IKVS_ON_CLOSED, TaskPriority::DefaultEndpoint);
		dispose.makeWellKnownEndpoint(WLTOKEN_IKVS_DISPOSE, TaskPriority::DefaultEndpoint);
		close.makeWellKnownEndpoint(WLTOKEN_IKVS_CLOSE, TaskPriority::DefaultEndpoint);
	}

	IKVSInterface() {}

	IKVSInterface(KeyValueStoreType type) : storeType(type) {}

	// IKVSInterface()
	//   : getValue(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_GET)), set(Endpoint::wellKnown({ addr },
	//   WLTOKEN_IKVS_SET)),
	//     clear(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_CLEAR)),
	//     commit(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_COMMIT)),
	//     readValuePrefix(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_READ_PREFIX)),
	//     readRange(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_READ_RANGE)),
	//     getStorageBytes(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_GET_STORAGE_BYTES)),
	//     getError(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_GET_ERROR)),
	//     onClosed(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_ON_CLOSED)),
	//     dispose(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_DISPOSE)),
	//     close(Endpoint::wellKnown({ addr }, WLTOKEN_IKVS_CLOSE)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           getValue,
		           set,
		           clear,
		           commit,
		           readValuePrefix,
		           readRange,
		           getStorageBytes,
		           getError,
		           onClosed,
		           dispose,
		           close,
		           uniqueID,
		           tempId);
	}
};

struct GetIKVSProcessInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 8382983;
	ReplyPromise<struct IKVSProcessInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct OpenKVStoreRequest {
	constexpr static FileIdentifier file_identifier = 5918682;
	KeyValueStoreType storeType;
	std::string filename;
	UID logID;
	int64_t memoryLimit;
	bool checkChecksums;
	bool checkIntegrity;
	ReplyPromise<struct IKVSInterface> reply;

	OpenKVStoreRequest(){};

	OpenKVStoreRequest(KeyValueStoreType storeType,
	                   std::string filename,
	                   UID logID,
	                   int64_t memoryLimit,
	                   bool checkChecksums = false,
	                   bool checkIntegrity = false)
	  : storeType(storeType), filename(filename), logID(logID), memoryLimit(memoryLimit),
	    checkChecksums(checkChecksums), checkIntegrity(checkIntegrity) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, storeType, filename, logID, memoryLimit, checkChecksums, checkIntegrity, reply);
	}
};

struct IKVSGetValueRequest {
	constexpr static FileIdentifier file_identifier = 1029439;
	KeyRef key;
	Optional<UID> debugID = Optional<UID>();
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, debugID, reply);
	}
};

struct IKVSSetRequest {
	constexpr static FileIdentifier file_identifier = 7283948;
	KeyValueRef keyValue;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyValue, reply);
	}
};

struct IKVSClearRequest {
	constexpr static FileIdentifier file_identifier = 2838575;
	KeyRangeRef range;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, reply);
	}
};

struct IKVSCommitRequest {
	constexpr static FileIdentifier file_identifier = 2985129;
	bool sequential;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sequential, reply);
	}
};

struct IKVSReadValuePrefixRequest {
	constexpr static FileIdentifier file_identifier = 1928374;
	KeyRef key;
	int maxLength;
	Optional<UID> debugID = Optional<UID>();
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, maxLength, debugID, reply);
	}
};

struct IKVSReadRangeRequest {
	constexpr static FileIdentifier file_identifier = 5918394;
	KeyRangeRef keys;
	int rowLimit = 1 << 30;
	int byteLimit = 1 << 30;
	ReplyPromise<RangeResult> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, reply);
	}
};

struct IKVSGetStorageByteRequest {
	constexpr static FileIdentifier file_identifier = 3512344;
	ReplyPromise<StorageBytes> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct IKVSGetErrorRequest {
	constexpr static FileIdentifier file_identifier = 3942891;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct IKVSOnClosedRequest {
	constexpr static FileIdentifier file_identifier = 1923894;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct IKVSDisposeRequest {
	constexpr static FileIdentifier file_identifier = 1235952;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct IKVSCloseRequest {
	constexpr static FileIdentifier file_identifier = 13859172;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct RemoteIKeyValueStore : public IKeyValueStore {
	IKVSInterface interf;
	Future<Void> initialized;
	RemoteIKeyValueStore() {}

	Future<Void> init() override {

		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote init");
		return initialized;
	}
	// TODO: Implement all
	Future<Void> getError() override {
		return interf.getError.getReply(IKVSGetErrorRequest{});
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote get error");
	}
	Future<Void> onClosed() override {
		return interf.onClosed.getReply(IKVSOnClosedRequest{});
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote onclosed");
	}
	// remove reply fields
	void dispose() override {
		interf.dispose.send(IKVSDisposeRequest{});
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote dispose");
	}
	void close() override {
		interf.close.send(IKVSCloseRequest{});
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote close");
	}

	KeyValueStoreType getType() const override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote getType");
		return interf.type();
	}

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote set");
		interf.set.send(IKVSSetRequest{ keyValue });
	}
	void clear(KeyRangeRef range, const Arena* arena = nullptr) override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote readRange");
		interf.clear.send(IKVSClearRequest{ range });
	}

	Future<Void> commit(bool sequential = false) override {
		std::cout << "sending commit request\n";
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote commit");
		return interf.commit.getReply(IKVSCommitRequest{ sequential });
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID = Optional<UID>()) override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote readValue");
		return interf.getValue.getReply(IKVSGetValueRequest{ key, debugID });
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        Optional<UID> debugID = Optional<UID>()) override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote readValuePrefix");
		return interf.readValuePrefix.getReply(IKVSReadValuePrefixRequest{ key, maxLength, debugID });
	}

	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit = 1 << 30, int byteLimit = 1 << 30) override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote read range");
		return interf.readRange.getReply(IKVSReadRangeRequest{ keys, rowLimit, byteLimit });
	}

	StorageBytes getStorageBytes() const override {
		TraceEvent(SevWarnAlways, "RemoteKVStore").detail("Action", "remote getStorageByte");
		return StorageBytes{};
	}
};

ACTOR Future<Void> runRemoteServer();

#include "flow/unactorcompiler.h"
#endif