/*
 * RemoteIKeyValueStore.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_G_H
#include "fdbserver/RemoteIKeyValueStore.actor.g.h"
#elif !defined(FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H)
#define FDBSERVER_REMOTE_IKEYVALUESTORE_ACTOR_H

#include "flow/ActorCollection.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "fdbrpc/FlowProcess.actor.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"

#include "flow/actorcompiler.h" // This must be the last #include.

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

struct RemoteKVSProcessInterface {

	constexpr static FileIdentifier file_identifier = 3491838;
	RequestStream<struct GetRemoteKVSProcessInterfaceRequest> getProcessInterface;
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
	RequestStream<struct IKVSGetParametersRequest> getParameters;
	RequestStream<struct IKVSSetParametersRequest> setParameters;
	RequestStream<struct IKVSSetParametersRequest> checkCompatibility;

	UID uniqueID;

	UID id() const { return uniqueID; }

	KeyValueStoreType storeType;

	KeyValueStoreType type() const { return storeType; }

	IKVSInterface() {}

	IKVSInterface(KeyValueStoreType type) : uniqueID(deterministicRandom()->randomUniqueID()), storeType(type) {}

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
		           getParameters,
		           setParameters,
		           checkCompatibility,
		           uniqueID);
	}
};

struct GetRemoteKVSProcessInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 8382983;
	ReplyPromise<struct RemoteKVSProcessInterface> reply;

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
	EncryptionAtRestMode encryptionMode;
	Optional<StorageEngineParamSet> params;
	ReplyPromise<struct IKVSInterface> reply;

	OpenKVStoreRequest(){};

	OpenKVStoreRequest(KeyValueStoreType storeType,
	                   std::string filename,
	                   UID logID,
	                   int64_t memoryLimit,
	                   bool checkChecksums = false,
	                   bool checkIntegrity = false,
	                   EncryptionAtRestMode mode = EncryptionAtRestMode::DISABLED,
	                   Optional<StorageEngineParamSet> params = {})
	  : storeType(storeType), filename(filename), logID(logID), memoryLimit(memoryLimit),
	    checkChecksums(checkChecksums), checkIntegrity(checkIntegrity), encryptionMode(mode), params(params) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, storeType, filename, logID, memoryLimit, checkChecksums, checkIntegrity, encryptionMode, params, reply);
	}
};

struct IKVSGetValueRequest {
	constexpr static FileIdentifier file_identifier = 1029439;
	KeyRef key;
	Optional<ReadOptions> options;
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, options, reply);
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
	Optional<ReadOptions> options;
	ReplyPromise<Optional<Value>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, maxLength, options, reply);
	}
};

// Use this instead of RangeResult as reply for better serialization performance
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
	Optional<ReadOptions> options;
	ReplyPromise<IKVSReadRangeReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, rowLimit, byteLimit, options, reply);
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

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct IKVSCloseRequest {
	constexpr static FileIdentifier file_identifier = 13859172;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct IKVSGetParametersRequest {
	constexpr static FileIdentifier file_identifier = 13859173;
	ReplyPromise<GetStorageEngineParamsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct IKVSSetParametersRequest {
	constexpr static FileIdentifier file_identifier = 13859174;
	StorageEngineParamSet params;
	ReplyPromise<StorageEngineParamResult> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, params, reply);
	}
};

ACTOR Future<Void> runIKVS(OpenKVStoreRequest openReq, IKVSInterface ikvsInterface);

struct KeyValueStoreProcess : FlowProcess {
	RemoteKVSProcessInterface kvsIf;
	Standalone<StringRef> serializedIf;

	Endpoint ssProcess; // endpoint for the storage process
	RequestStream<FlowProcessRegistrationRequest> ssRequestStream;

	KeyValueStoreProcess() {
		TraceEvent(SevDebug, "InitKeyValueStoreProcess").log();
		ObjectWriter writer(IncludeVersion());
		writer.serialize(kvsIf);
		serializedIf = writer.toString();
	}

	void registerEndpoint(Endpoint p) override {
		ssProcess = p;
		ssRequestStream = RequestStream<FlowProcessRegistrationRequest>(p);
	}

	StringRef name() const override { return _name; }
	StringRef serializedInterface() const override { return serializedIf; }

	ACTOR static Future<Void> _run(KeyValueStoreProcess* self) {
		state ActorCollection actors(true);
		TraceEvent("WaitingForOpenKVStoreRequest").log();
		loop {
			choose {
				when(OpenKVStoreRequest req = waitNext(self->kvsIf.openKVStore.getFuture())) {
					TraceEvent("OpenKVStoreRequestReceived").log();
					IKVSInterface interf;
					actors.add(runIKVS(req, interf));
				}
				when(ErrorOr<Void> e = wait(errorOr(actors.getResult()))) {
					if (e.isError()) {
						TraceEvent("KeyValueStoreProcessRunActorError").errorUnsuppressed(e.getError());
						throw e.getError();
					} else {
						TraceEvent("KeyValueStoreProcessFinished").log();
						return e.get();
					}
				}
			}
		}
	}

	Future<Void> run() override { return _run(this); }

	static StringRef _name;
};

struct RemoteIKeyValueStore : public IKeyValueStore {
	RemoteKVSProcessInterface kvsProcess;
	IKVSInterface interf;
	Future<Void> initialized;
	Future<int> returnCode;
	StorageBytes storageBytes;

	RemoteIKeyValueStore() : storageBytes(0, 0, 0, 0) {}

	Future<Void> init() override {
		TraceEvent(SevInfo, "RemoteIKeyValueStoreInit").log();
		return initialized;
	}

	Future<Void> getError() const override { return getErrorImpl(this, returnCode); }
	Future<Void> onClosed() const override { return onCloseImpl(this); }

	void dispose() override {
		TraceEvent(SevDebug, "RemoteIKVSDisposeRequest").backtrace();
		interf.dispose.send(IKVSDisposeRequest{});
		// hold the future to not cancel the spawned process
		uncancellable(returnCode);
		delete this;
	}
	void close() override {
		TraceEvent(SevDebug, "RemoteIKVSCloseRequest").backtrace();
		interf.close.send(IKVSCloseRequest{});
		// hold the future to not cancel the spawned process
		uncancellable(returnCode);
		delete this;
	}

	KeyValueStoreType getType() const override { return interf.type(); }

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		interf.set.send(IKVSSetRequest{ keyValue, ReplyPromise<Void>() });
	}
	void clear(KeyRangeRef range,
	           const StorageServerMetrics* storageMetrics = nullptr,
	           const Arena* arena = nullptr) override {
		interf.clear.send(IKVSClearRequest{ range, ReplyPromise<Void>() });
	}

	Future<Void> commit(bool sequential = false) override {
		Future<IKVSCommitReply> commitReply =
		    interf.commit.getReply(IKVSCommitRequest{ sequential, ReplyPromise<IKVSCommitReply>() });
		return commitAndGetStorageBytes(this, commitReply);
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> options = Optional<ReadOptions>()) override {
		return readValueImpl(this, IKVSGetValueRequest{ key, options, ReplyPromise<Optional<Value>>() });
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        Optional<ReadOptions> options = Optional<ReadOptions>()) override {
		return interf.readValuePrefix.getReply(
		    IKVSReadValuePrefixRequest{ key, maxLength, options, ReplyPromise<Optional<Value>>() });
	}

	Future<RangeResult> readRange(KeyRangeRef keys,
	                              int rowLimit = 1 << 30,
	                              int byteLimit = 1 << 30,
	                              Optional<ReadOptions> options = Optional<ReadOptions>()) override {
		IKVSReadRangeRequest req{ keys, rowLimit, byteLimit, options, ReplyPromise<IKVSReadRangeReply>() };
		return fmap([](const IKVSReadRangeReply& reply) { return reply.toRangeResult(); },
		            interf.readRange.getReply(req));
	}

	Future<StorageEngineParamSet> getParameters() const override {
		return fmap([](const GetStorageEngineParamsReply& reply) { return reply.params; },
		            interf.getParameters.getReply(IKVSGetParametersRequest{}));
	}

	Future<StorageEngineParamResult> setParameters(StorageEngineParamSet const& params) override {
		return interf.setParameters.getReply(
		    IKVSSetParametersRequest{ params, ReplyPromise<StorageEngineParamResult>() });
	}

	Future<StorageEngineParamResult> checkCompatibility(StorageEngineParamSet const& params) override {
		return interf.checkCompatibility.getReply(
		    IKVSSetParametersRequest{ params, ReplyPromise<StorageEngineParamResult>() });
	}

	StorageBytes getStorageBytes() const override { return storageBytes; }

	void consumeInterface(StringRef intf) {
		kvsProcess = ObjectReader::fromStringRef<RemoteKVSProcessInterface>(intf, IncludeVersion());
	}

	ACTOR static Future<Void> commitAndGetStorageBytes(RemoteIKeyValueStore* self,
	                                                   Future<IKVSCommitReply> commitReplyFuture) {
		IKVSCommitReply commitReply = wait(commitReplyFuture);
		self->storageBytes = commitReply.storeBytes;
		return Void();
	}

	ACTOR static Future<Optional<Value>> readValueImpl(RemoteIKeyValueStore* self, IKVSGetValueRequest req) {
		Optional<Value> val = wait(self->interf.getValue.getReply(req));
		return val;
	}

	ACTOR static Future<Void> getErrorImpl(const RemoteIKeyValueStore* self, Future<int> returnCode) {
		choose {
			when(wait(self->initialized)) {}
			when(wait(delay(SERVER_KNOBS->REMOTE_KV_STORE_MAX_INIT_DURATION))) {
				TraceEvent(SevError, "RemoteIKVSInitTooLong")
				    .detail("TimeLimit", SERVER_KNOBS->REMOTE_KV_STORE_MAX_INIT_DURATION);
				throw please_reboot_kv_store(); // this will reboot the kv store
			}
		}
		state Future<Void> connectionCheckingDelay = delay(FLOW_KNOBS->FAILURE_DETECTION_DELAY);
		state Future<ErrorOr<Void>> storeError = errorOr(self->interf.getError.getReply(IKVSGetErrorRequest{}));
		state NetworkAddress childAddr = self->interf.getError.getEndpoint().getPrimaryAddress();
		loop choose {
			when(ErrorOr<Void> e = wait(storeError)) {
				TraceEvent(SevDebug, "RemoteIKVSGetError")
				    .errorUnsuppressed(e.isError() ? e.getError() : success())
				    .backtrace();
				if (e.isError())
					throw e.getError();
				else
					return Never();
			}
			when(int res = wait(returnCode)) {
				TraceEvent(res != 0 ? SevError : SevInfo, "SpawnedProcessDied").detail("Res", res);
				if (res)
					throw please_reboot_kv_store(); // this will reboot the kv store
				else
					return Never();
			}
			when(wait(connectionCheckingDelay)) {
				// for the corner case where the child process stuck and waitpid also does not give update on it
				// In this scenario, we need to manually reboot the storage engine process
				if (IFailureMonitor::failureMonitor().getState(childAddr).isFailed()) {
					TraceEvent(SevError, "RemoteKVStoreConnectionStuck").log();
					throw please_reboot_kv_store(); // this will reboot the kv store
				}
				connectionCheckingDelay = delay(FLOW_KNOBS->FAILURE_DETECTION_DELAY);
			}
		}
	}

	ACTOR static Future<Void> onCloseImpl(const RemoteIKeyValueStore* self) {
		try {
			wait(self->initialized);
			wait(self->interf.onClosed.getReply(IKVSOnClosedRequest{}));
			TraceEvent(SevDebug, "RemoteIKVSOnCloseImplOnClosedFinished");
		} catch (Error& e) {
			TraceEvent(SevInfo, "RemoteIKVSOnCloseImplError").errorUnsuppressed(e).backtrace();
			throw;
		}
		return Void();
	}

	Future<EncryptionAtRestMode> encryptionMode() override {
		return EncryptionAtRestMode(EncryptionAtRestMode::DISABLED);
	}
};

Future<Void> runFlowProcess(std::string const& name, Endpoint endpoint);

#include "flow/unactorcompiler.h"
#endif