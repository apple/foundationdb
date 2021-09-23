#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H
#include "fdbserver/RemoteIKeyValueStore.actor.g.h"
#elif !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/IKeyValueStore.h"

#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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
	// RequestStream<struct IKVSSetRequest> set;
	// RequestStream<ReplyPromise<Void>> commit;

	UID uniqueID = deterministicRandom()->randomUniqueID();

	UID id() const { return uniqueID; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, getValue);
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
	// std::string const filename;
	UID logID;
	int64_t memoryLimit;
	bool checkChecksums = false;
	bool checkIntegrity = false;
	ReplyPromise<struct IKVSInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, storeType, logID, memoryLimit, checkChecksums, checkIntegrity, reply);
	}
};

struct IKVSGetValueRequest {
	constexpr static FileIdentifier file_identifier = 1029439;
	Key key;
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, reply);
	}
};

struct RemoteIKeyValueStore : public IKeyValueStore {
	IKVSInterface interf;
	KeyValueStoreType type;
	// RemoteIKeyValueStore;

	// TODO: Implement all
	Future<Void> getError() override;
	Future<Void> onClosed() override;
	void dispose() override;
	void close() override;

	KeyValueStoreType getType() const override;
	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override;
	void clear(KeyRangeRef range, const Arena* arena = nullptr) override;
	Future<Void> commit(bool sequential = false) override;

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID = Optional<UID>()) override;

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        Optional<UID> debugID = Optional<UID>()) override;

	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit = 1 << 30, int byteLimit = 1 << 30) override;

	StorageBytes getStorageBytes() const override;
};

ACTOR Future<Void> runRemoteServer();

#include "flow/unactorcompiler.h"
#endif