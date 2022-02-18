#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H
#include "fdbserver/RemoteIKeyValueStore.actor.g.h"
#elif !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowProcess.actor.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "flow/ActorCollection.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"

#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

enum RemoteIKVSWellKnownEndpoints {
	WLTOKEN_IKVS_PROCESS_SERVER = WLTOKEN_FIRST_AVAILABLE,
	WLTOKEN_IKVS_RESERVED_COUNT
};

struct IKVSCommitReply {
	constexpr static FileIdentifier file_identifier = 3958189;
	StorageBytes storeBytes;

	IKVSCommitReply() : storeBytes(0, 0, 0, 0) {}
	IKVSCommitReply(const StorageBytes& sb) : storeBytes(sb) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, storeBytes);
	}
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

	UID id() const { return uniqueID; }

	KeyValueStoreType storeType;

	KeyValueStoreType type() const { return storeType; }

	IKVSInterface() {}

	IKVSInterface(KeyValueStoreType type) : storeType(type) {}

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
		           uniqueID);
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
	IKeyValueStore::ReadType type;
	Optional<UID> debugID = Optional<UID>();
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, type, debugID, reply);
	}
};

struct IKVSSetRequest {
	constexpr static FileIdentifier file_identifier = 7283948;
	KeyValueRef keyValue;
	uint64_t order;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyValue, order, reply);
	}
};

struct IKVSClearRequest {
	constexpr static FileIdentifier file_identifier = 2838575;
	KeyRangeRef range;
	uint64_t order;
	ReplyPromise<Void> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, order, reply);
	}
};

struct IKVSCommitRequest {
	constexpr static FileIdentifier file_identifier = 2985129;
	bool sequential;
	ReplyPromise<IKVSCommitReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sequential, reply);
	}
};

struct IKVSReadValuePrefixRequest {
	constexpr static FileIdentifier file_identifier = 1928374;
	KeyRef key;
	int maxLength;
	IKeyValueStore::ReadType type;
	Optional<UID> debugID = Optional<UID>();
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, maxLength, type, debugID, reply);
	}
};

struct IKVSReadRangeReply {
	constexpr static FileIdentifier file_identifier = 6682449;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	bool more;
	Optional<KeyRef> readThrough;
	bool readToBegin;
	bool readThroughEnd;

	IKVSReadRangeReply() = default;

	explicit IKVSReadRangeReply(const RangeResult& res)
	  : arena(res.arena()), data(static_cast<const VectorRef<KeyValueRef>&>(res)), more(res.more),
	    readThrough(res.readThrough), readToBegin(res.readToBegin), readThroughEnd(res.readThroughEnd) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, data, more, readThrough, readToBegin, readThroughEnd, arena);
	}

	RangeResult toRangeResult() const {
		RangeResult r(RangeResultRef(data, more, readThrough), arena);
		r.readToBegin = readToBegin;
		r.readThroughEnd = readThroughEnd;
		return r;
	}
};

struct IKVSReadRangeRequest {
	constexpr static FileIdentifier file_identifier = 5918394;
	KeyRangeRef keys;
	int rowLimit;
	int byteLimit;
	IKeyValueStore::ReadType type;
	ReplyPromise<RangeResult> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, rowLimit, byteLimit, type, reply);
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

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface);

struct KeyValueStoreProcess : FlowProcess {
	IKVSProcessInterface kvsIf;
	Standalone<StringRef> serializedIf;

	Endpoint ssProcess; // endpoint for the storage process
	RequestStream<FlowProcessRegistrationRequest> ssRequestStream;
	// Interface of the parent storage server
	IKeyValueStore* ssInterface;

	KeyValueStoreProcess() : ssInterface(nullptr) {
		TraceEvent(SevDebug, "InitKeyValueStoreProcess").log();
		ObjectWriter writer(IncludeVersion());
		writer.serialize(kvsIf);
		serializedIf = writer.toString();
	}

	void setSSInterface(IKeyValueStore* store) { ssInterface = store; }

	void registerEndpoint(Endpoint p) override {
		ssProcess = p;
		ssRequestStream = RequestStream<FlowProcessRegistrationRequest>(p);
	}

	StringRef name() const override { return "KeyValueStoreProcess"_sr; }
	StringRef serializedInterface() const override { return serializedIf; }

	Future<Void> onClosed() const override {
		ASSERT(ssInterface && g_network->isSimulated());
		return ssInterface->onClosed();
	}

	void consumeInterface(StringRef intf) override {
		kvsIf = ObjectReader::fromStringRef<IKVSProcessInterface>(intf, IncludeVersion());
	}

	ACTOR static Future<Void> _run(KeyValueStoreProcess* self) {
		state ActorCollection actors(true);
		TraceEvent(SevDebug, "WaitingForOpenKVStoreRequest").log();
		loop {
			choose {
				when(OpenKVStoreRequest req = waitNext(self->kvsIf.openKVStore.getFuture())) {
					TraceEvent(SevDebug, "OpenKVStoreRequestReceived").log();
					IKVSInterface reply;
					actors.add(runIKVS(req, reply));
				}
				when(ErrorOr<Void> e = wait(errorOr(actors.getResult()))) {
					if (e.isError()) {
						TraceEvent(SevDebug, "KeyValueStoreProcessRunActorError").error(e.getError(), true);
						throw e.getError();
					} else {
						TraceEvent(SevDebug, "KeyValueStoreProcessFinished").log();
						return e.get();
					}
				}
			}
		}
	}

	Future<Void> run() override { return _run(this); }
};

struct RemoteIKeyValueStore : public IKeyValueStore {
	IKVSInterface interf;
	Future<Void> initialized;
	KeyValueStoreProcess ikvsProcess;
	StorageBytes storageBytes;
	uint64_t set_counter, clear_counter;
	RemoteIKeyValueStore() : ikvsProcess(), storageBytes(0, 0, 0, 0), set_counter(0), clear_counter(0) {
		ikvsProcess.setSSInterface(this);
	}

	Future<Void> init() override {
		TraceEvent(SevInfo, "RemoteIKeyValueStoreInit").log();
		return initialized;
	}

	Future<Void> getError() const override { return getErrorImpl(this); }
	Future<Void> onClosed() const override { return onCloseImpl(this); }

	void dispose() override {
		TraceEvent(SevDebug, "RemoteIKVSDisposeRequest").backtrace();
		interf.dispose.send(IKVSDisposeRequest{});
		delete this;
	}
	void close() override {
		TraceEvent(SevDebug, "RemoteIKVSCloseRequest").backtrace();
		interf.close.send(IKVSCloseRequest{});
		delete this;
	}

	KeyValueStoreType getType() const override { return interf.type(); }

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		interf.set.send(IKVSSetRequest{ keyValue, set_counter++ });
	}
	void clear(KeyRangeRef range, const Arena* arena = nullptr) override {
		interf.clear.send(IKVSClearRequest{ range, clear_counter++ });
	}

	Future<Void> commit(bool sequential = false) override {
		Future<IKVSCommitReply> commitReply = interf.commit.getReply(IKVSCommitRequest{ sequential });
		return commitAndGetStorageBytes(this, commitReply);
	}

	Future<Optional<Value>> readValue(KeyRef key,
	                                  ReadType type = ReadType::NORMAL,
	                                  Optional<UID> debugID = Optional<UID>()) override {
		return readValueImpl(this, IKVSGetValueRequest{ key, type, debugID });
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        ReadType type = ReadType::NORMAL,
	                                        Optional<UID> debugID = Optional<UID>()) override {
		return interf.readValuePrefix.getReply(IKVSReadValuePrefixRequest{ key, maxLength, type, debugID });
	}

	Future<RangeResult> readRange(KeyRangeRef keys,
	                              int rowLimit = 1 << 30,
	                              int byteLimit = 1 << 30,
	                              ReadType type = ReadType::NORMAL) override {
		IKVSReadRangeRequest req{ keys, rowLimit, byteLimit, type };
		return interf.readRange.getReply(req);
		// return fmap([](const IKVSReadRangeReply& reply) { return reply.toRangeResult(); },
		//             interf.readRange.getReply(req));
	}

	StorageBytes getStorageBytes() const override { return storageBytes; }

	ACTOR static Future<Void> commitAndGetStorageBytes(RemoteIKeyValueStore* self,
	                                                   Future<IKVSCommitReply> commitReplyFuture) {
		IKVSCommitReply commitReply = wait(commitReplyFuture);
		self->storageBytes = commitReply.storeBytes;
		return Void();
	}

	ACTOR static Future<Optional<Value>> readValueImpl(RemoteIKeyValueStore* self, IKVSGetValueRequest req) {
		// wait(self->init());
		Optional<Value> val = wait(self->interf.getValue.getReply(req));
		return val;
	}

	ACTOR static Future<Void> getErrorImpl(const RemoteIKeyValueStore* self) {
		wait(self->initialized);
		try {
			choose {
				when(wait(self->interf.getError.getReply(IKVSGetErrorRequest{}))) {
					TraceEvent(SevDebug, "RemoteIKVSGetError").log();
				}
				when(int res = wait(self->ikvsProcess.returnCode())) {
					TraceEvent(res < 0 ? SevError : SevInfo, "SpawnedProcessHasDied").detail("Res", res);
				}
			}
		} catch (Error& e) {
			TraceEvent(SevInfo, "GetErrorImplError").error(e, true).backtrace();
			throw;
		}
		return Void();
	}

	ACTOR static Future<Void> onCloseImpl(const RemoteIKeyValueStore* self) {
		try {
			wait(self->initialized);
			TraceEvent(SevInfo, "RemoteIKVSOnCloseImplInitialized");
			wait(self->interf.onClosed.getReply(IKVSOnClosedRequest{}));
			TraceEvent(SevInfo, "RemoteIKVSOnCloseImplOnClosedFinished");
		} catch (Error& e) {
			TraceEvent(SevInfo, "RemoteIKVSOnCloseImplError").detail("Error", e.code()).backtrace();
			throw;
		}
		return Void();
	}
};

#include "flow/unactorcompiler.h"
#endif