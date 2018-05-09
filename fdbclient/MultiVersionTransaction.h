/*
 * MultiVersionTransaction.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_MULTIVERSIONTRANSACTION_H
#define FDBCLIENT_MULTIVERSIONTRANSACTION_H
#pragma once

#include "FDBOptions.g.h"
#include "FDBTypes.h"
#include "IClientApi.h"

#include "flow/ThreadHelper.actor.h"

struct FdbCApi : public ThreadSafeReferenceCounted<FdbCApi> {
	typedef struct future FDBFuture;
	typedef struct cluster FDBCluster;
	typedef struct database FDBDatabase;
	typedef struct transaction FDBTransaction;

#pragma pack(push, 4)
	typedef struct keyvalue {
		const void *key;
		int keyLength;
		const void *value;
		int valueLength;
	} FDBKeyValue;
#pragma pack(pop)

	typedef int fdb_error_t;
	typedef int fdb_bool_t;

	typedef void (*FDBCallback)(FDBFuture *future, void *callback_parameter);

	//Network
	fdb_error_t (*selectApiVersion)(int runtimeVersion, int headerVersion);
	const char* (*getClientVersion)();
	fdb_error_t (*setNetworkOption)(FDBNetworkOptions::Option option, uint8_t const *value, int valueLength);
	fdb_error_t (*setupNetwork)();
	fdb_error_t (*runNetwork)();
	fdb_error_t (*stopNetwork)();
	FDBFuture* (*createCluster)(const char *clusterFilePath);

	//Cluster
	FDBFuture* (*clusterCreateDatabase)(FDBCluster *cluster, uint8_t *dbName, int dbNameLength);
	fdb_error_t (*clusterSetOption)(FDBCluster *cluster, FDBClusterOptions::Option option, uint8_t const *value, int valueLength);
	void (*clusterDestroy)(FDBCluster *cluster);

	//Database
	fdb_error_t (*databaseCreateTransaction)(FDBDatabase *database, FDBTransaction **tr);
	fdb_error_t (*databaseSetOption)(FDBDatabase *database, FDBDatabaseOptions::Option option, uint8_t const *value, int valueLength);
	void (*databaseDestroy)(FDBDatabase *database);	

	//Transaction
	fdb_error_t (*transactionSetOption)(FDBTransaction *tr, FDBTransactionOptions::Option option, uint8_t const *value, int valueLength);
	void (*transactionDestroy)(FDBTransaction *tr);

	void (*transactionSetReadVersion)(FDBTransaction *tr, int64_t version);
	FDBFuture* (*transactionGetReadVersion)(FDBTransaction *tr);
	
	FDBFuture* (*transactionGet)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength, fdb_bool_t snapshot);
	FDBFuture* (*transactionGetKey)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength, fdb_bool_t orEqual, int offset, fdb_bool_t snapshot);
	FDBFuture* (*transactionGetAddressesForKey)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength);
	FDBFuture* (*transactionGetRange)(FDBTransaction *tr, uint8_t const *beginKeyName, int beginKeyNameLength, fdb_bool_t beginOrEqual, int beginOffset,
										uint8_t const *endKeyName, int endKeyNameLength, fdb_bool_t endOrEqual, int endOffset, int limit, int targetBytes,
										FDBStreamingModes::Option mode, int iteration, fdb_bool_t snapshot, fdb_bool_t reverse);
	FDBFuture* (*transactionGetVersionstamp)(FDBTransaction* tr);

	void (*transactionSet)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength, uint8_t const *value, int valueLength);
	void (*transactionClear)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength);
	void (*transactionClearRange)(FDBTransaction *tr, uint8_t const *beginKeyName, int beginKeyNameLength, uint8_t const *endKeyName, int endKeyNameLength);
	void (*transactionAtomicOp)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength, uint8_t const *param, int paramLength, FDBMutationTypes::Option operationType);
	
	FDBFuture* (*transactionCommit)(FDBTransaction *tr);
	fdb_error_t (*transactionGetCommittedVersion)(FDBTransaction *tr, int64_t *outVersion);
	FDBFuture* (*transactionWatch)(FDBTransaction *tr, uint8_t const *keyName, int keyNameLength);
	FDBFuture* (*transactionOnError)(FDBTransaction *tr, fdb_error_t error);
	void (*transactionReset)(FDBTransaction *tr);
	void (*transactionCancel)(FDBTransaction *tr);

	fdb_error_t (*transactionAddConflictRange)(FDBTransaction *tr, uint8_t const *beginKeyName, int beginKeyNameLength, 
												uint8_t const *endKeyName, int endKeyNameLength, FDBConflictRangeTypes::Option);

	//Future
	fdb_error_t (*futureGetCluster)(FDBFuture *f, FDBCluster **outCluster);
	fdb_error_t (*futureGetDatabase)(FDBFuture *f, FDBDatabase **outDb);
	fdb_error_t (*futureGetVersion)(FDBFuture *f, int64_t *outVersion);
	fdb_error_t (*futureGetError)(FDBFuture *f);
	fdb_error_t (*futureGetKey)(FDBFuture *f, uint8_t const **outKey, int *outKeyLength);
	fdb_error_t (*futureGetValue)(FDBFuture *f, fdb_bool_t *outPresent, uint8_t const **outValue, int *outValueLength);
	fdb_error_t (*futureGetStringArray)(FDBFuture *f, const char ***outStrings, int *outCount);
	fdb_error_t (*futureGetKeyValueArray)(FDBFuture *f, FDBKeyValue const ** outKV, int *outCount, fdb_bool_t *outMore);
	fdb_error_t (*futureSetCallback)(FDBFuture *f, FDBCallback callback, void *callback_parameter);
	void (*futureCancel)(FDBFuture *f);
	void (*futureDestroy)(FDBFuture *f);
};

class DLTransaction : public ITransaction, ThreadSafeReferenceCounted<DLTransaction> {
public:
	DLTransaction(Reference<FdbCApi> api, FdbCApi::FDBTransaction *tr) : api(api), tr(tr) {}
	~DLTransaction() { api->transactionDestroy(tr); }

	void cancel();
	void setVersion(Version v);
	ThreadFuture<Version> getReadVersion();

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot=false);
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys, int limit, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange( const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key);
	ThreadFuture<Standalone<StringRef>> getVersionstamp();
 
	void addReadConflictRange(const KeyRangeRef& keys);

	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType);
	void set(const KeyRef& key, const ValueRef& value);
	void clear(const KeyRef& begin, const KeyRef& end);
	void clear(const KeyRangeRef& range);
	void clear(const KeyRef& key);

	ThreadFuture<Void> watch(const KeyRef& key);

	void addWriteConflictRange(const KeyRangeRef& keys);

	ThreadFuture<Void> commit();
	Version getCommittedVersion();

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value=Optional<StringRef>());

	ThreadFuture<Void> onError(Error const& e);
	void reset();

	void addref() { ThreadSafeReferenceCounted<DLTransaction>::addref(); }
	void delref() { ThreadSafeReferenceCounted<DLTransaction>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBTransaction* const tr;
};

class DLDatabase : public IDatabase, ThreadSafeReferenceCounted<DLDatabase> {
public:
	DLDatabase(Reference<FdbCApi> api, FdbCApi::FDBDatabase *db) : api(api), db(db) {}
	~DLDatabase() { api->databaseDestroy(db); }

	Reference<ITransaction> createTransaction();
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

	void addref() { ThreadSafeReferenceCounted<DLDatabase>::addref(); }
	void delref() { ThreadSafeReferenceCounted<DLDatabase>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBDatabase* const db;
};

class DLCluster : public ICluster, ThreadSafeReferenceCounted<DLCluster> {
public:
	DLCluster(Reference<FdbCApi> api, FdbCApi::FDBCluster *cluster) : api(api), cluster(cluster) {}
	~DLCluster() { api->clusterDestroy(cluster); }

	ThreadFuture<Reference<IDatabase>> createDatabase(Standalone<StringRef> dbName);
	void setOption(FDBClusterOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

	void addref() { ThreadSafeReferenceCounted<DLCluster>::addref(); }
	void delref() { ThreadSafeReferenceCounted<DLCluster>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBCluster* const cluster;
};

class DLApi : public IClientApi {
public:
	DLApi(std::string fdbCPath);

	void selectApiVersion(int apiVersion);
	const char* getClientVersion();

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>());
	void setupNetwork();
	void runNetwork();
	void stopNetwork();

	ThreadFuture<Reference<ICluster>> createCluster(const char *clusterFilePath);

	void addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter);

private:
	const std::string fdbCPath;
	const Reference<FdbCApi> api;
	int headerVersion;
	bool networkSetup;

	Mutex lock;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;

	void init();
};

class MultiVersionDatabase;

class MultiVersionTransaction : public ITransaction, ThreadSafeReferenceCounted<MultiVersionTransaction> {
public:
	MultiVersionTransaction(Reference<MultiVersionDatabase> db);

	void cancel();
	void setVersion(Version v);
	ThreadFuture<Version> getReadVersion();

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot=false);
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys, int limit, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<RangeResultRef>> getRange( const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot=false, bool reverse=false);
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key);
	ThreadFuture<Standalone<StringRef>> getVersionstamp();
 
	void addReadConflictRange(const KeyRangeRef& keys);

	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType);
	void set(const KeyRef& key, const ValueRef& value);
	void clear(const KeyRef& begin, const KeyRef& end);
	void clear(const KeyRangeRef& range);
	void clear(const KeyRef& key);

	ThreadFuture<Void> watch(const KeyRef& key);

	void addWriteConflictRange(const KeyRangeRef& keys);

	ThreadFuture<Void> commit();
	Version getCommittedVersion();

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value=Optional<StringRef>());

	ThreadFuture<Void> onError(Error const& e);
	void reset();

	void addref() { ThreadSafeReferenceCounted<MultiVersionTransaction>::addref(); }
	void delref() { ThreadSafeReferenceCounted<MultiVersionTransaction>::delref(); }

private:
	const Reference<MultiVersionDatabase> db;
	ThreadSpinLock lock;

	struct TransactionInfo {
		Reference<ITransaction> transaction;
		ThreadFuture<Void> onChange;
	};

	TransactionInfo transaction;

	TransactionInfo getTransaction();
	void updateTransaction();
};

class MultiVersionCluster;

class MultiVersionDatabase : public IDatabase, ThreadSafeReferenceCounted<MultiVersionDatabase> {
public:
	MultiVersionDatabase(Reference<MultiVersionCluster> cluster, Standalone<StringRef> dbName, Reference<IDatabase> db, ThreadFuture<Void> changed);
	~MultiVersionDatabase();

	Reference<ITransaction> createTransaction();
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

	void addref() { ThreadSafeReferenceCounted<MultiVersionDatabase>::addref(); }
	void delref() { ThreadSafeReferenceCounted<MultiVersionDatabase>::delref(); }

	static Reference<IDatabase> debugCreateFromExistingDatabase(Reference<IDatabase> db);

private:
	struct DatabaseState : ThreadCallback, ThreadSafeReferenceCounted<DatabaseState> {
		DatabaseState(Reference<MultiVersionCluster> cluster, Standalone<StringRef> dbName, Reference<IDatabase> db, ThreadFuture<Void> changed);

		void updateDatabase();
		void cancelCallbacks();

		bool canFire(int notMadeActive) { return true; }
		void fire(const Void &unused, int& userParam);
		void error(const Error& e, int& userParam);

		const Reference<MultiVersionCluster> cluster;

		Reference<IDatabase> db;
		const Reference<ThreadSafeAsyncVar<Reference<IDatabase>>> dbVar;
		const Standalone<StringRef> dbName;

		ThreadFuture<Reference<IDatabase>> dbFuture;
		ThreadFuture<Void> changed;

		bool cancelled;

		std::vector<std::pair<FDBDatabaseOptions::Option, Optional<Standalone<StringRef>>>> options;
		Mutex optionLock;
	};

	const Reference<DatabaseState> dbState;
	friend class MultiVersionTransaction;
};

struct ClientInfo : ThreadSafeReferenceCounted<ClientInfo> {
	uint64_t protocolVersion;
	IClientApi *api;
	std::string libPath;
	bool external;
	bool failed;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;

	ClientInfo() : protocolVersion(0), api(NULL), external(false), failed(true) {}
	ClientInfo(IClientApi *api) : protocolVersion(0), api(api), libPath("internal"), external(false), failed(false) {}
	ClientInfo(IClientApi *api, std::string libPath) : protocolVersion(0), api(api), libPath(libPath), external(true), failed(false) {}

	void loadProtocolVersion();
	bool canReplace(Reference<ClientInfo> other) const;
};

class MultiVersionApi;

class MultiVersionCluster : public ICluster, ThreadSafeReferenceCounted<MultiVersionCluster> {
public:
	MultiVersionCluster() : clusterState(new ClusterState()) {} // Used in testing workloads
	MultiVersionCluster(MultiVersionApi *api, std::string clusterFilePath, Reference<ICluster> cluster);
	~MultiVersionCluster();

	ThreadFuture<Reference<IDatabase>> createDatabase(Standalone<StringRef> dbName);
	void setOption(FDBClusterOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

	void addref() { ThreadSafeReferenceCounted<MultiVersionCluster>::addref(); }
	void delref() { ThreadSafeReferenceCounted<MultiVersionCluster>::delref(); }

private:
	struct ClusterState;

	struct Connector : ThreadCallback, ThreadSafeReferenceCounted<Connector> {
		Connector(Reference<ClusterState> clusterState, Reference<ClientInfo> client, std::string clusterFilePath) : clusterState(clusterState), client(client), clusterFilePath(clusterFilePath), connected(false), cancelled(false) {}

		void connect();
		void cancel();

		bool canFire(int notMadeActive) { return true; }
		void fire(const Void &unused, int& userParam);
		void error(const Error& e, int& userParam);

		const Reference<ClientInfo> client;
		const std::string clusterFilePath;

		const Reference<ClusterState> clusterState;

		ThreadFuture<Void> connectionFuture;

		Reference<ICluster> candidateCluster;
		Reference<ITransaction> tr;

		bool connected;
		bool cancelled;
	};

	struct ClusterState : ThreadSafeReferenceCounted<ClusterState> {
		ClusterState() : clusterVar(new ThreadSafeAsyncVar<Reference<ICluster>>(Reference<ICluster>(NULL))), currentClientIndex(-1) {}

		void stateChanged();
		void addConnection(Reference<ClientInfo> client, std::string clusterFilePath);
		void startConnections();
		void cancelConnections();

		Reference<ICluster> cluster;
		const Reference<ThreadSafeAsyncVar<Reference<ICluster>>> clusterVar;

		int currentClientIndex;
		std::vector<Reference<ClientInfo>> clients;
		std::vector<Reference<Connector>> connectionAttempts;

		std::vector<std::pair<FDBClusterOptions::Option, Optional<Standalone<StringRef>>>> options;
		Mutex optionLock;
	};

	const Reference<ClusterState> clusterState;
	friend class MultiVersionDatabase;
};

class MultiVersionApi : public IClientApi {
public:
	void selectApiVersion(int apiVersion);
	const char* getClientVersion();

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>());
	void setupNetwork();
	void runNetwork();
	void stopNetwork();
	void addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter);

	ThreadFuture<Reference<ICluster>> createCluster(const char *clusterFilePath);
	static MultiVersionApi* api;

	Reference<ClientInfo> getLocalClient();
	void runOnExternalClients(std::function<void(Reference<ClientInfo>)>, bool runOnFailedClients=false);

	void updateSupportedVersions();

	bool callbackOnMainThread;
	bool localClientDisabled;

private:
	MultiVersionApi();

	void loadEnvironmentVariableNetworkOptions();

	void disableMultiVersionClientApi();
	void setCallbacksOnExternalThreads();
	void addExternalLibrary(std::string path);
	void addExternalLibraryDirectory(std::string path);
	void disableLocalClient();
	void setSupportedClientVersions(Standalone<StringRef> versions);

	void setNetworkOptionInternal(FDBNetworkOptions::Option option, Optional<StringRef> value);

	Reference<ClientInfo> localClient;
	std::map<std::string, Reference<ClientInfo>> externalClients;

	bool networkStartSetup;
	volatile bool networkSetup;
	volatile bool bypassMultiClientApi;
	volatile bool externalClient;
	int apiVersion;

	Mutex lock;
	std::vector<std::pair<FDBNetworkOptions::Option, Optional<Standalone<StringRef>>>> options;
	std::map<FDBNetworkOptions::Option, std::set<Standalone<StringRef>>> setEnvOptions;
	volatile bool envOptionsLoaded;
};


#endif
