/*
 * ThreadSafeTransaction.cpp
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

#include "fdbclient/BlobGranuleFiles.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/versions.h"
#include "fdbclient/NativeAPI.actor.h"

// Users of ThreadSafeTransaction might share Reference<ThreadSafe...> between different threads as long as they don't
// call addRef (e.g. C API follows this). Therefore, it is unsafe to call (explicitly or implicitly) this->addRef in any
// of these functions.

ThreadFuture<Void> ThreadSafeDatabase::onConnected() {
	DatabaseContext* db = this->db;
	return onMainThread([db]() -> Future<Void> {
		db->checkDeferredError();
		return db->onConnected();
	});
}

ThreadFuture<Reference<IDatabase>> ThreadSafeDatabase::createFromExistingDatabase(Database db) {
	return onMainThread([db]() {
		db->checkDeferredError();
		DatabaseContext* cx = db.getPtr();
		cx->addref();
		return Future<Reference<IDatabase>>(Reference<IDatabase>(new ThreadSafeDatabase(cx)));
	});
}

Reference<ITransaction> ThreadSafeDatabase::createTransaction() {
	auto type = isConfigDB ? ISingleThreadTransaction::Type::SIMPLE_CONFIG : ISingleThreadTransaction::Type::RYW;
	return Reference<ITransaction>(new ThreadSafeTransaction(db, type));
}

void ThreadSafeDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBDatabaseOptions::optionInfo.find(option);
	if (itr != FDBDatabaseOptions::optionInfo.end()) {
		TraceEvent("SetDatabaseOption").detail("Option", itr->second.name);
	} else {
		TraceEvent("UnknownDatabaseOption").detail("Option", option);
		throw invalid_option();
	}
	if (itr->first == FDBDatabaseOptions::USE_CONFIG_DATABASE) {
		isConfigDB = true;
	}

	DatabaseContext* db = this->db;
	Standalone<Optional<StringRef>> passValue = value;

	// ThreadSafeDatabase is not allowed to do anything with options except pass them through to RYW.
	onMainThreadVoid(
	    [db, option, passValue]() {
		    db->checkDeferredError();
		    db->setOption(option, passValue.contents());
	    },
	    &db->deferredError);
}

ThreadFuture<int64_t> ThreadSafeDatabase::rebootWorker(const StringRef& address, bool check, int duration) {
	DatabaseContext* db = this->db;
	Key addressKey = address;
	return onMainThread([db, addressKey, check, duration]() -> Future<int64_t> {
		return db->rebootWorker(addressKey, check, duration);
	});
}

ThreadFuture<Void> ThreadSafeDatabase::forceRecoveryWithDataLoss(const StringRef& dcid) {
	DatabaseContext* db = this->db;
	Key dcidKey = dcid;
	return onMainThread([db, dcidKey]() -> Future<Void> { return db->forceRecoveryWithDataLoss(dcidKey); });
}

ThreadFuture<Void> ThreadSafeDatabase::createSnapshot(const StringRef& uid, const StringRef& snapshot_command) {
	DatabaseContext* db = this->db;
	Key snapUID = uid;
	Key cmd = snapshot_command;
	return onMainThread([db, snapUID, cmd]() -> Future<Void> { return db->createSnapshot(snapUID, cmd); });
}

// Return the main network thread busyness
double ThreadSafeDatabase::getMainThreadBusyness() {
	ASSERT(g_network);
	return g_network->networkInfo.metrics.networkBusyness;
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> ThreadSafeDatabase::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	DatabaseContext* db = this->db;
	return onMainThread(
	    [db, expectedVersion]() -> Future<ProtocolVersion> { return db->getClusterProtocol(expectedVersion); });
}

ThreadSafeDatabase::ThreadSafeDatabase(std::string connFilename, int apiVersion) {
	ClusterConnectionFile* connFile =
	    new ClusterConnectionFile(ClusterConnectionFile::lookupClusterFileName(connFilename).first);

	// Allocate memory for the Database from this thread (so the pointer is known for subsequent method calls)
	// but run its constructor on the main thread
	DatabaseContext* db = this->db = DatabaseContext::allocateOnForeignThread();

	onMainThreadVoid(
	    [db, connFile, apiVersion]() {
		    try {
			    Database::createDatabase(
			        Reference<ClusterConnectionFile>(connFile), apiVersion, IsInternal::False, LocalityData(), db)
			        .extractPtr();
		    } catch (Error& e) {
			    new (db) DatabaseContext(e);
		    } catch (...) {
			    new (db) DatabaseContext(unknown_error());
		    }
	    },
	    nullptr);
}

ThreadSafeDatabase::~ThreadSafeDatabase() {
	DatabaseContext* db = this->db;
	onMainThreadVoid([db]() { db->delref(); }, nullptr);
}

ThreadSafeTransaction::ThreadSafeTransaction(DatabaseContext* cx, ISingleThreadTransaction::Type type) {
	// Allocate memory for the transaction from this thread (so the pointer is known for subsequent method calls)
	// but run its constructor on the main thread

	// It looks strange that the DatabaseContext::addref is deferred by the onMainThreadVoid call, but it is safe
	// because the reference count of the DatabaseContext is solely managed from the main thread.  If cx is destructed
	// immediately after this call, it will defer the DatabaseContext::delref (and onMainThread preserves the order of
	// these operations).
	auto tr = this->tr = ISingleThreadTransaction::allocateOnForeignThread(type);
	// No deferred error -- if the construction of the RYW transaction fails, we have no where to put it
	onMainThreadVoid(
	    [tr, cx]() {
		    cx->addref();
		    tr->setDatabase(Database(cx));
	    },
	    nullptr);
}

// This constructor is only used while refactoring fdbcli and only called from the main thread
ThreadSafeTransaction::ThreadSafeTransaction(ReadYourWritesTransaction* ryw) : tr(ryw) {
	if (tr)
		tr->addref();
}

ThreadSafeTransaction::~ThreadSafeTransaction() {
	ISingleThreadTransaction* tr = this->tr;
	if (tr)
		onMainThreadVoid([tr]() { tr->delref(); }, nullptr);
}

void ThreadSafeTransaction::cancel() {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->cancel(); }, nullptr);
}

void ThreadSafeTransaction::setVersion(Version v) {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, v]() { tr->setVersion(v); }, &tr->deferredError);
}

ThreadFuture<Version> ThreadSafeTransaction::getReadVersion() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Version> {
		tr->checkDeferredError();
		return tr->getReadVersion();
	});
}

ThreadFuture<Optional<Value>> ThreadSafeTransaction::get(const KeyRef& key, bool snapshot) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k, snapshot]() -> Future<Optional<Value>> {
		tr->checkDeferredError();
		return tr->get(k, Snapshot{ snapshot });
	});
}

ThreadFuture<Key> ThreadSafeTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	KeySelector k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k, snapshot]() -> Future<Key> {
		tr->checkDeferredError();
		return tr->getKey(k, Snapshot{ snapshot });
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, r]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getEstimatedRangeSizeBytes(r);
	});
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> ThreadSafeTransaction::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                       int64_t chunkSize) {
	KeyRange r = range;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, r, chunkSize]() -> Future<Standalone<VectorRef<KeyRef>>> {
		tr->checkDeferredError();
		return tr->getRangeSplitPoints(r, chunkSize);
	});
}

ThreadFuture<RangeResult> ThreadSafeTransaction::getRange(const KeySelectorRef& begin,
                                                          const KeySelectorRef& end,
                                                          int limit,
                                                          bool snapshot,
                                                          bool reverse) {
	KeySelector b = begin;
	KeySelector e = end;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, b, e, limit, snapshot, reverse]() -> Future<RangeResult> {
		tr->checkDeferredError();
		return tr->getRange(b, e, limit, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<RangeResult> ThreadSafeTransaction::getRange(const KeySelectorRef& begin,
                                                          const KeySelectorRef& end,
                                                          GetRangeLimits limits,
                                                          bool snapshot,
                                                          bool reverse) {
	KeySelector b = begin;
	KeySelector e = end;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, b, e, limits, snapshot, reverse]() -> Future<RangeResult> {
		tr->checkDeferredError();
		return tr->getRange(b, e, limits, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<Standalone<VectorRef<const char*>>> ThreadSafeTransaction::getAddressesForKey(const KeyRef& key) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k]() -> Future<Standalone<VectorRef<const char*>>> {
		tr->checkDeferredError();
		return tr->getAddressesForKey(k);
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> ThreadSafeTransaction::getBlobGranuleRanges(
    const KeyRangeRef& keyRange) {
	ISingleThreadTransaction* tr = this->tr;
	KeyRange r = keyRange;

	return onMainThread([tr, r]() -> Future<Standalone<VectorRef<KeyRangeRef>>> {
		tr->checkDeferredError();
		return tr->getBlobGranuleRanges(r);
	});
}

ThreadFuture<RangeResult> ThreadSafeTransaction::readBlobGranules(const KeyRangeRef& keyRange,
                                                                  Version beginVersion,
                                                                  Optional<Version> readVersion,
                                                                  ReadBlobGranuleContext granule_context) {
	// In V1 of api this is required, field is just for forward compatibility
	ASSERT(beginVersion == 0);

	bool doMaterialize = !granule_context.debugNoMaterialize;

	ISingleThreadTransaction* tr = this->tr;
	KeyRange r = keyRange;

	int64_t readVersionOut;
	ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> getFilesFuture = onMainThread(
	    [tr, r, beginVersion, readVersion, &readVersionOut]() -> Future<Standalone<VectorRef<BlobGranuleChunkRef>>> {
		    tr->checkDeferredError();
		    return tr->readBlobGranules(r, beginVersion, readVersion, &readVersionOut);
	    });

	// FIXME: can this safely avoid another main thread jump?
	getFilesFuture.blockUntilReadyCheckOnMainThread();

	// propagate error to client
	if (getFilesFuture.isError()) {
		return ThreadFuture<RangeResult>(getFilesFuture.getError());
	}

	Standalone<VectorRef<BlobGranuleChunkRef>> files = getFilesFuture.get();

	try {
		RangeResult results;
		// FIXME: could submit multiple chunks to start_load_f in parallel?
		for (BlobGranuleChunkRef& chunk : files) {
			RangeResult chunkRows;

			int64_t snapshotLoadId;
			int64_t deltaLoadIds[chunk.deltaFiles.size()];

			// FIXME: move to transactions?
			if (doMaterialize) {
				// Start load process for all files in chunk
				// In V1 of api snapshot is required, optional is just for forward compatibility
				ASSERT(chunk.snapshotFile.present());
				std::string snapshotFname = chunk.snapshotFile.get().filename.toString();
				snapshotLoadId = granule_context.start_load_f(snapshotFname.c_str(),
				                                              snapshotFname.size(),
				                                              chunk.snapshotFile.get().offset,
				                                              chunk.snapshotFile.get().length,
				                                              granule_context.userContext);
				int64_t deltaLoadIds[chunk.deltaFiles.size()];
				int64_t deltaLoadLengths[chunk.deltaFiles.size()];
				StringRef deltaData[chunk.deltaFiles.size()];
				for (int deltaFileIdx = 0; deltaFileIdx < chunk.deltaFiles.size(); deltaFileIdx++) {
					std::string deltaFName = chunk.deltaFiles[deltaFileIdx].filename.toString();
					deltaLoadIds[deltaFileIdx] = granule_context.start_load_f(deltaFName.c_str(),
					                                                          deltaFName.size(),
					                                                          chunk.deltaFiles[deltaFileIdx].offset,
					                                                          chunk.deltaFiles[deltaFileIdx].length,
					                                                          granule_context.userContext);
					deltaLoadLengths[deltaFileIdx] = chunk.deltaFiles[deltaFileIdx].length;
				}

				// once all loads kicked off, load data for chunk
				StringRef snapshotData(granule_context.get_load_f(snapshotLoadId, granule_context.userContext),
				                       chunk.snapshotFile.get().length);
				for (int i = 0; i < chunk.deltaFiles.size(); i++) {
					deltaData[i] = StringRef(granule_context.get_load_f(deltaLoadIds[i], granule_context.userContext),
					                         chunk.deltaFiles[i].length);
				}

				// materialize rows from chunk
				chunkRows = materializeBlobGranule(chunk, keyRange, readVersionOut, snapshotData, deltaData);
			}

			results.arena().dependsOn(chunkRows.arena());
			results.append(results.arena(), chunkRows.begin(), chunkRows.size());

			if (doMaterialize) {
				granule_context.free_load_f(snapshotLoadId, granule_context.userContext);
				for (int i = 0; i < chunk.deltaFiles.size(); i++) {
					granule_context.free_load_f(deltaLoadIds[i], granule_context.userContext);
				}
			}
		}
		return results;
	} catch (Error& e) {
		return ThreadFuture<RangeResult>(e);
	}
}

void ThreadSafeTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->addReadConflictRange(r); }, &tr->deferredError);
}

void ThreadSafeTransaction::makeSelfConflicting() {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->makeSelfConflicting(); }, &tr->deferredError);
}

void ThreadSafeTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	Key k = key;
	Value v = value;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, k, v, operationType]() { tr->atomicOp(k, v, operationType); }, &tr->deferredError);
}

void ThreadSafeTransaction::set(const KeyRef& key, const ValueRef& value) {
	Key k = key;
	Value v = value;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, k, v]() { tr->set(k, v); }, &tr->deferredError);
}

void ThreadSafeTransaction::clear(const KeyRangeRef& range) {
	KeyRange r = range;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->clear(r); }, &tr->deferredError);
}

void ThreadSafeTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	Key b = begin;
	Key e = end;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid(
	    [tr, b, e]() {
		    if (b > e)
			    throw inverted_range();

		    tr->clear(KeyRangeRef(b, e));
	    },
	    &tr->deferredError);
}

void ThreadSafeTransaction::clear(const KeyRef& key) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, k]() { tr->clear(k); }, &tr->deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::watch(const KeyRef& key) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k]() -> Future<Void> {
		tr->checkDeferredError();
		return tr->watch(k);
	});
}

void ThreadSafeTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->addWriteConflictRange(r); }, &tr->deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::commit() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Void> {
		tr->checkDeferredError();
		return tr->commit();
	});
}

Version ThreadSafeTransaction::getCommittedVersion() {
	// This should be thread safe when called legally, but it is fragile
	return tr->getCommittedVersion();
}

ThreadFuture<int64_t> ThreadSafeTransaction::getApproximateSize() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<int64_t> { return tr->getApproximateSize(); });
}

ThreadFuture<Standalone<StringRef>> ThreadSafeTransaction::getVersionstamp() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Standalone<StringRef>> { return tr->getVersionstamp(); });
}

void ThreadSafeTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBTransactionOptions::optionInfo.find(option);
	if (itr == FDBTransactionOptions::optionInfo.end()) {
		TraceEvent("UnknownTransactionOption").detail("Option", option);
		throw invalid_option();
	}
	ISingleThreadTransaction* tr = this->tr;
	Standalone<Optional<StringRef>> passValue = value;

	// ThreadSafeTransaction is not allowed to do anything with options except pass them through to RYW.
	onMainThreadVoid([tr, option, passValue]() { tr->setOption(option, passValue.contents()); }, &tr->deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::checkDeferredError() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() {
		try {
			tr->checkDeferredError();
		} catch (Error& e) {
			tr->deferredError = Error();
			return Future<Void>(e);
		}
		return Future<Void>(Void());
	});
}

ThreadFuture<Void> ThreadSafeTransaction::onError(Error const& e) {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, e]() { return tr->onError(e); });
}

void ThreadSafeTransaction::operator=(ThreadSafeTransaction&& r) noexcept {
	tr = r.tr;
	r.tr = nullptr;
}

ThreadSafeTransaction::ThreadSafeTransaction(ThreadSafeTransaction&& r) noexcept {
	tr = r.tr;
	r.tr = nullptr;
}

void ThreadSafeTransaction::reset() {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->reset(); }, nullptr);
}

extern const char* getSourceVersion();

ThreadSafeApi::ThreadSafeApi()
  : apiVersion(-1), clientVersion(format("%s,%s,%llx", FDB_VT_VERSION, getSourceVersion(), currentProtocolVersion)),
    transportId(0) {}

void ThreadSafeApi::selectApiVersion(int apiVersion) {
	this->apiVersion = apiVersion;
}

const char* ThreadSafeApi::getClientVersion() {
	// There is only one copy of the ThreadSafeAPI, and it never gets deleted. Also, clientVersion is never modified.
	return clientVersion.c_str();
}

void ThreadSafeApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if (option == FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID) {
		if (value.present()) {
			transportId = std::stoull(value.get().toString().c_str());
		}
	} else {
		::setNetworkOption(option, value);
	}
}

void ThreadSafeApi::setupNetwork() {
	::setupNetwork(transportId);
}

void ThreadSafeApi::runNetwork() {
	Optional<Error> runErr;
	try {
		::runNetwork();
	} catch (Error& e) {
		TraceEvent(SevError, "RunNetworkError").error(e);
		runErr = e;
	} catch (std::exception& e) {
		runErr = unknown_error();
		TraceEvent(SevError, "RunNetworkError").error(unknown_error()).detail("RootException", e.what());
	} catch (...) {
		runErr = unknown_error();
		TraceEvent(SevError, "RunNetworkError").error(unknown_error());
	}

	for (auto& hook : threadCompletionHooks) {
		try {
			hook.first(hook.second);
		} catch (Error& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(e);
		} catch (std::exception& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error()).detail("RootException", e.what());
		} catch (...) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error());
		}
	}

	if (runErr.present()) {
		throw runErr.get();
	}

	TraceEvent("RunNetworkTerminating");
}

void ThreadSafeApi::stopNetwork() {
	::stopNetwork();
}

Reference<IDatabase> ThreadSafeApi::createDatabase(const char* clusterFilePath) {
	return Reference<IDatabase>(new ThreadSafeDatabase(clusterFilePath, apiVersion));
}

void ThreadSafeApi::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	if (!g_network) {
		throw network_not_setup();
	}

	MutexHolder holder(lock); // We could use the network thread to protect this action, but then we can't guarantee
	                          // upon return that the hook is set.
	threadCompletionHooks.emplace_back(hook, hookParameter);
}

IClientApi* ThreadSafeApi::api = new ThreadSafeApi();
