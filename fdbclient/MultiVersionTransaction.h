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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"

#include "flow/ThreadHelper.actor.h"

struct FdbCApi : public ThreadSafeReferenceCounted<FdbCApi> {
	typedef struct future FDBFuture;
	typedef struct cluster FDBCluster;
	typedef struct database FDBDatabase;
	typedef struct transaction FDBTransaction;

#pragma pack(push, 4)
	typedef struct keyvalue {
		const void* key;
		int keyLength;
		const void* value;
		int valueLength;
	} FDBKeyValue;
#pragma pack(pop)

	typedef int fdb_error_t;
	typedef int fdb_bool_t;

	typedef void (*FDBCallback)(FDBFuture* future, void* callback_parameter);

	// Network
	fdb_error_t (*selectApiVersion)(int runtimeVersion, int headerVersion);
	const char* (*getClientVersion)();
	fdb_error_t (*setNetworkOption)(FDBNetworkOptions::Option option, uint8_t const* value, int valueLength);
	fdb_error_t (*setupNetwork)();
	fdb_error_t (*runNetwork)();
	fdb_error_t (*stopNetwork)();
	fdb_error_t (*createDatabase)(const char* clusterFilePath, FDBDatabase** db);

	// Database
	fdb_error_t (*databaseCreateTransaction)(FDBDatabase* database, FDBTransaction** tr);
	fdb_error_t (*databaseSetOption)(FDBDatabase* database,
	                                 FDBDatabaseOptions::Option option,
	                                 uint8_t const* value,
	                                 int valueLength);
	void (*databaseDestroy)(FDBDatabase* database);

	// Transaction
	fdb_error_t (*transactionSetOption)(FDBTransaction* tr,
	                                    FDBTransactionOptions::Option option,
	                                    uint8_t const* value,
	                                    int valueLength);
	void (*transactionDestroy)(FDBTransaction* tr);

	void (*transactionSetReadVersion)(FDBTransaction* tr, int64_t version);
	FDBFuture* (*transactionGetReadVersion)(FDBTransaction* tr);

	FDBFuture* (*transactionGet)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength, fdb_bool_t snapshot);
	FDBFuture* (*transactionGetKey)(FDBTransaction* tr,
	                                uint8_t const* keyName,
	                                int keyNameLength,
	                                fdb_bool_t orEqual,
	                                int offset,
	                                fdb_bool_t snapshot);
	FDBFuture* (*transactionGetAddressesForKey)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength);
	FDBFuture* (*transactionGetRange)(FDBTransaction* tr,
	                                  uint8_t const* beginKeyName,
	                                  int beginKeyNameLength,
	                                  fdb_bool_t beginOrEqual,
	                                  int beginOffset,
	                                  uint8_t const* endKeyName,
	                                  int endKeyNameLength,
	                                  fdb_bool_t endOrEqual,
	                                  int endOffset,
	                                  int limit,
	                                  int targetBytes,
	                                  FDBStreamingModes::Option mode,
	                                  int iteration,
	                                  fdb_bool_t snapshot,
	                                  fdb_bool_t reverse);
	FDBFuture* (*transactionGetVersionstamp)(FDBTransaction* tr);

	void (*transactionSet)(FDBTransaction* tr,
	                       uint8_t const* keyName,
	                       int keyNameLength,
	                       uint8_t const* value,
	                       int valueLength);
	void (*transactionClear)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength);
	void (*transactionClearRange)(FDBTransaction* tr,
	                              uint8_t const* beginKeyName,
	                              int beginKeyNameLength,
	                              uint8_t const* endKeyName,
	                              int endKeyNameLength);
	void (*transactionAtomicOp)(FDBTransaction* tr,
	                            uint8_t const* keyName,
	                            int keyNameLength,
	                            uint8_t const* param,
	                            int paramLength,
	                            FDBMutationTypes::Option operationType);

	FDBFuture* (*transactionGetEstimatedRangeSizeBytes)(FDBTransaction* tr,
	                                                    uint8_t const* begin_key_name,
	                                                    int begin_key_name_length,
	                                                    uint8_t const* end_key_name,
	                                                    int end_key_name_length);

	FDBFuture* (*transactionCommit)(FDBTransaction* tr);
	fdb_error_t (*transactionGetCommittedVersion)(FDBTransaction* tr, int64_t* outVersion);
	FDBFuture* (*transactionGetApproximateSize)(FDBTransaction* tr);
	FDBFuture* (*transactionWatch)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength);
	FDBFuture* (*transactionOnError)(FDBTransaction* tr, fdb_error_t error);
	void (*transactionReset)(FDBTransaction* tr);
	void (*transactionCancel)(FDBTransaction* tr);

	fdb_error_t (*transactionAddConflictRange)(FDBTransaction* tr,
	                                           uint8_t const* beginKeyName,
	                                           int beginKeyNameLength,
	                                           uint8_t const* endKeyName,
	                                           int endKeyNameLength,
	                                           FDBConflictRangeTypes::Option);

	// Future
	fdb_error_t (*futureGetDatabase)(FDBFuture* f, FDBDatabase** outDb);
	fdb_error_t (*futureGetInt64)(FDBFuture* f, int64_t* outValue);
	fdb_error_t (*futureGetError)(FDBFuture* f);
	fdb_error_t (*futureGetKey)(FDBFuture* f, uint8_t const** outKey, int* outKeyLength);
	fdb_error_t (*futureGetValue)(FDBFuture* f, fdb_bool_t* outPresent, uint8_t const** outValue, int* outValueLength);
	fdb_error_t (*futureGetStringArray)(FDBFuture* f, const char*** outStrings, int* outCount);
	fdb_error_t (*futureGetKeyValueArray)(FDBFuture* f, FDBKeyValue const** outKV, int* outCount, fdb_bool_t* outMore);
	fdb_error_t (*futureSetCallback)(FDBFuture* f, FDBCallback callback, void* callback_parameter);
	void (*futureCancel)(FDBFuture* f);
	void (*futureDestroy)(FDBFuture* f);

	// Legacy Support
	FDBFuture* (*createCluster)(const char* clusterFilePath);
	FDBFuture* (*clusterCreateDatabase)(FDBCluster* cluster, uint8_t* dbName, int dbNameLength);
	void (*clusterDestroy)(FDBCluster* cluster);
	fdb_error_t (*futureGetCluster)(FDBFuture* f, FDBCluster** outCluster);
};

class DLTransaction : public ITransaction, ThreadSafeReferenceCounted<DLTransaction> {
public:
	DLTransaction(Reference<FdbCApi> api, FdbCApi::FDBTransaction* tr) : api(api), tr(tr) {}
	~DLTransaction() { api->transactionDestroy(tr); }

	void cancel() override;
	void setVersion(Version v) override;
	ThreadFuture<Version> getReadVersion() override;

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot = false) override;
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin,
	                                                  const KeySelectorRef& end,
	                                                  int limit,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin,
	                                                  const KeySelectorRef& end,
	                                                  GetRangeLimits limits,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys,
	                                                  int limit,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys,
	                                                  GetRangeLimits limits,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;

	void addReadConflictRange(const KeyRangeRef& keys) override;

	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) override;
	void set(const KeyRef& key, const ValueRef& value) override;
	void clear(const KeyRef& begin, const KeyRef& end) override;
	void clear(const KeyRangeRef& range) override;
	void clear(const KeyRef& key) override;

	ThreadFuture<Void> watch(const KeyRef& key) override;

	void addWriteConflictRange(const KeyRangeRef& keys) override;

	ThreadFuture<Void> commit() override;
	Version getCommittedVersion() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	ThreadFuture<Void> onError(Error const& e) override;
	void reset() override;

	void addref() override { ThreadSafeReferenceCounted<DLTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<DLTransaction>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBTransaction* const tr;
};

class DLDatabase : public IDatabase, ThreadSafeReferenceCounted<DLDatabase> {
public:
	DLDatabase(Reference<FdbCApi> api, FdbCApi::FDBDatabase* db) : api(api), db(db), ready(Void()) {}
	DLDatabase(Reference<FdbCApi> api, ThreadFuture<FdbCApi::FDBDatabase*> dbFuture);
	~DLDatabase() {
		if (db) {
			api->databaseDestroy(db);
		}
	}

	ThreadFuture<Void> onReady();

	Reference<ITransaction> createTransaction() override;
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) override;

	void addref() override { ThreadSafeReferenceCounted<DLDatabase>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<DLDatabase>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBDatabase*
	    db; // Always set if API version >= 610, otherwise guaranteed to be set when onReady future is set
	ThreadFuture<Void> ready;
};

class DLApi : public IClientApi {
public:
	DLApi(std::string fdbCPath, bool unlinkOnLoad = false);

	void selectApiVersion(int apiVersion) override;
	const char* getClientVersion() override;

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	void setupNetwork() override;
	void runNetwork() override;
	void stopNetwork() override;

	Reference<IDatabase> createDatabase(const char* clusterFilePath) override;
	Reference<IDatabase> createDatabase609(const char* clusterFilePath); // legacy database creation

	void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) override;

private:
	const std::string fdbCPath;
	const Reference<FdbCApi> api;
	const bool unlinkOnLoad;
	int headerVersion;
	bool networkSetup;

	Mutex lock;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;

	void init();
};

class MultiVersionDatabase;

class MultiVersionTransaction : public ITransaction, ThreadSafeReferenceCounted<MultiVersionTransaction> {
public:
	MultiVersionTransaction(Reference<MultiVersionDatabase> db,
	                        UniqueOrderedOptionList<FDBTransactionOptions> defaultOptions);

	~MultiVersionTransaction() override;

	void cancel() override;
	void setVersion(Version v) override;
	ThreadFuture<Version> getReadVersion() override;

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot = false) override;
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin,
	                                                  const KeySelectorRef& end,
	                                                  int limit,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeySelectorRef& begin,
	                                                  const KeySelectorRef& end,
	                                                  GetRangeLimits limits,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys,
	                                                  int limit,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<RangeResultRef>> getRange(const KeyRangeRef& keys,
	                                                  GetRangeLimits limits,
	                                                  bool snapshot = false,
	                                                  bool reverse = false) override;
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;

	void addReadConflictRange(const KeyRangeRef& keys) override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;

	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) override;
	void set(const KeyRef& key, const ValueRef& value) override;
	void clear(const KeyRef& begin, const KeyRef& end) override;
	void clear(const KeyRangeRef& range) override;
	void clear(const KeyRef& key) override;

	ThreadFuture<Void> watch(const KeyRef& key) override;

	void addWriteConflictRange(const KeyRangeRef& keys) override;

	ThreadFuture<Void> commit() override;
	Version getCommittedVersion() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	ThreadFuture<Void> onError(Error const& e) override;
	void reset() override;

	void addref() override { ThreadSafeReferenceCounted<MultiVersionTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<MultiVersionTransaction>::delref(); }

private:
	const Reference<MultiVersionDatabase> db;
	ThreadSpinLock lock;

	struct TransactionInfo {
		Reference<ITransaction> transaction;
		ThreadFuture<Void> onChange;
	};

	// Timeout related variables for MultiVersionTransaction objects that do not have an underlying ITransaction

	// The time when the MultiVersionTransaction was last created or reset
	std::atomic<double> startTime;

	// A lock that needs to be held if using timeoutTsav or currentTimeout
	ThreadSpinLock timeoutLock;

	// A single assignment var (i.e. promise) that gets set with an error when the timeout elapses or the transaction
	// is reset or destroyed.
	Reference<ThreadSingleAssignmentVar<Void>> timeoutTsav;

	// A reference to the current actor waiting for the timeout. This actor will set the timeoutTsav promise.
	ThreadFuture<Void> currentTimeout;

	// Configure a timeout based on the options set for this transaction. This timeout only applies
	// if we don't have an underlying database object to connect with.
	void setTimeout(Optional<StringRef> value);

	// Creates a ThreadFuture<T> that will signal an error if the transaction times out.
	template <class T>
	ThreadFuture<T> makeTimeout();

	TransactionInfo transaction;

	TransactionInfo getTransaction();
	void updateTransaction();
	void setDefaultOptions(UniqueOrderedOptionList<FDBTransactionOptions> options);

	std::vector<std::pair<FDBTransactionOptions::Option, Optional<Standalone<StringRef>>>> persistentOptions;
};

struct ClientDesc {
	std::string const libPath;
	bool const external;

	ClientDesc(std::string libPath, bool external) : libPath(libPath), external(external) {}
};

struct ClientInfo : ClientDesc, ThreadSafeReferenceCounted<ClientInfo> {
	ProtocolVersion protocolVersion;
	IClientApi* api;
	bool failed;
	std::atomic_bool initialized;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;

	ClientInfo()
	  : ClientDesc(std::string(), false), protocolVersion(0), api(nullptr), failed(true), initialized(false) {}
	ClientInfo(IClientApi* api)
	  : ClientDesc("internal", false), protocolVersion(0), api(api), failed(false), initialized(false) {}
	ClientInfo(IClientApi* api, std::string libPath)
	  : ClientDesc(libPath, true), protocolVersion(0), api(api), failed(false), initialized(false) {}

	void loadProtocolVersion();
	bool canReplace(Reference<ClientInfo> other) const;
};

class MultiVersionApi;

class MultiVersionDatabase : public IDatabase, ThreadSafeReferenceCounted<MultiVersionDatabase> {
public:
	MultiVersionDatabase(MultiVersionApi* api,
	                     int threadIdx,
	                     std::string clusterFilePath,
	                     Reference<IDatabase> db,
	                     Reference<IDatabase> versionMonitorDb,
	                     bool openConnectors = true);

	~MultiVersionDatabase() override;

	Reference<ITransaction> createTransaction() override;
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) override;

	void addref() override { ThreadSafeReferenceCounted<MultiVersionDatabase>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<MultiVersionDatabase>::delref(); }

	// Create a MultiVersionDatabase that wraps an already created IDatabase object
	// For internal use in testing
	static Reference<IDatabase> debugCreateFromExistingDatabase(Reference<IDatabase> db);

	// private:

	struct LegacyVersionMonitor;

	// A struct that manages the current connection state of the MultiVersionDatabase. This wraps the underlying
	// IDatabase object that is currently interacting with the cluster.
	struct DatabaseState : ThreadSafeReferenceCounted<DatabaseState> {
		DatabaseState(std::string clusterFilePath, Reference<IDatabase> versionMonitorDb);

		// Replaces the active database connection with a new one. Must be called from the main thread.
		void updateDatabase(Reference<IDatabase> newDb, Reference<ClientInfo> client);

		// Called when a change to the protocol version of the cluster has been detected.
		// Must be called from the main thread
		void protocolVersionChanged(ProtocolVersion protocolVersion);

		// Adds a client (local or externally loaded) that can be used to connect to the cluster
		void addClient(Reference<ClientInfo> client);

		// Watch the cluster protocol version for changes and update the database state when it does.
		// Must be called from the main thread
		ThreadFuture<Void> monitorProtocolVersion();

		// Starts version monitors for old client versions that don't support connect packet monitoring (<= 5.0).
		// Must be called from the main thread
		void startLegacyVersionMonitors();

		// Cleans up state for the legacy version monitors to break reference cycles
		void close();

		Reference<IDatabase> db;
		const Reference<ThreadSafeAsyncVar<Reference<IDatabase>>> dbVar;
		std::string clusterFilePath;

		// Used to monitor the cluster protocol version. Will be the same as db unless we have either not connected
		// yet or if the client version associated with db does not support protocol monitoring. In those cases,
		// this will be a specially created local db.
		Reference<IDatabase> versionMonitorDb;

		bool closed;

		ThreadFuture<Void> changed;
		ThreadFuture<Void> dbReady;
		ThreadFuture<Void> protocolVersionMonitor;

		// Versions older than 6.1 do not benefit from having their database connections closed. Additionally,
		// there are various issues that result in negative behavior in some cases if the connections are closed.
		// Therefore, we leave them open.
		std::map<ProtocolVersion, Reference<IDatabase>> legacyDatabaseConnections;

		// Versions 5.0 and older do not support connection packet monitoring and require alternate techniques to
		// determine the cluster version.
		std::list<Reference<LegacyVersionMonitor>> legacyVersionMonitors;

		Optional<ProtocolVersion> dbProtocolVersion;

		// This maps a normalized protocol version to the client associated with it. This prevents compatible
		// differences in protocol version not matching each other.
		std::map<ProtocolVersion, Reference<ClientInfo>> clients;

		std::vector<std::pair<FDBDatabaseOptions::Option, Optional<Standalone<StringRef>>>> options;
		UniqueOrderedOptionList<FDBTransactionOptions> transactionDefaultOptions;
		Mutex optionLock;
	};

	// A struct that enables monitoring whether the cluster is running an old version (<= 5.0) that doesn't support
	// connect packet monitoring.
	struct LegacyVersionMonitor : ThreadSafeReferenceCounted<LegacyVersionMonitor> {
		LegacyVersionMonitor(Reference<ClientInfo> const& client) : client(client), monitorRunning(false) {}

		// Terminates the version monitor to break reference cycles
		void close();

		// Starts the connection monitor by creating a database object at an old version.
		// Must be called from the main thread
		void startConnectionMonitor(Reference<DatabaseState> dbState);

		// Runs a GRV probe on the cluster to determine if the client version is compatible with the cluster.
		// Must be called from main thread
		void runGrvProbe(Reference<DatabaseState> dbState);

		Reference<ClientInfo> client;
		Reference<IDatabase> db;
		Reference<ITransaction> tr;

		ThreadFuture<Void> versionMonitor;
		bool monitorRunning;
	};

	const Reference<DatabaseState> dbState;
	friend class MultiVersionTransaction;
};

class MultiVersionApi : public IClientApi {
public:
	void selectApiVersion(int apiVersion) override;
	const char* getClientVersion() override;

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	void setupNetwork() override;
	void runNetwork() override;
	void stopNetwork() override;
	void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) override;

	// Creates an IDatabase object that represents a connection to the cluster
	Reference<IDatabase> createDatabase(const char* clusterFilePath) override;
	static MultiVersionApi* api;

	Reference<ClientInfo> getLocalClient();
	void runOnExternalClients(int threadId,
	                          std::function<void(Reference<ClientInfo>)>,
	                          bool runOnFailedClients = false);
	void runOnExternalClientsAllThreads(std::function<void(Reference<ClientInfo>)>, bool runOnFailedClients = false);

	void updateSupportedVersions();

	bool callbackOnMainThread;
	bool localClientDisabled;

	static bool apiVersionAtLeast(int minVersion);

private:
	MultiVersionApi();

	void loadEnvironmentVariableNetworkOptions();

	void disableMultiVersionClientApi();
	void setCallbacksOnExternalThreads();
	void addExternalLibrary(std::string path);
	void addExternalLibraryDirectory(std::string path);
	// Return a vector of (pathname, unlink_on_close) pairs.  Makes threadCount - 1 copies of the library stored in
	// path, and returns a vector of length threadCount.
	std::vector<std::pair<std::string, bool>> copyExternalLibraryPerThread(std::string path);
	void disableLocalClient();
	void setSupportedClientVersions(Standalone<StringRef> versions);

	void setNetworkOptionInternal(FDBNetworkOptions::Option option, Optional<StringRef> value);

	Reference<ClientInfo> localClient;
	std::map<std::string, ClientDesc> externalClientDescriptions;
	std::map<std::string, std::vector<Reference<ClientInfo>>> externalClients;

	bool networkStartSetup;
	volatile bool networkSetup;
	volatile bool bypassMultiClientApi;
	volatile bool externalClient;
	int apiVersion;

	int nextThread = 0;
	int threadCount;

	Mutex lock;
	std::vector<std::pair<FDBNetworkOptions::Option, Optional<Standalone<StringRef>>>> options;
	std::map<FDBNetworkOptions::Option, std::set<Standalone<StringRef>>> setEnvOptions;
	volatile bool envOptionsLoaded;
};

#endif
