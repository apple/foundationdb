/*
 * fdb_flow.actor.cpp
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

#include "fdb_flow.h"

#include <cstdint>
#include <stdio.h>
#include <cinttypes>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/DeterministicRandom.h"
#include "flow/SystemMonitor.h"
#include "flow/TLSConfig.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace FDB;

THREAD_FUNC networkThread(void* fdb) {
	((FDB::API*)fdb)->runNetwork();
	THREAD_RETURN;
}

ACTOR Future<Void> _test() {
	API* fdb = FDB::API::selectAPIVersion(710);
	auto db = fdb->createDatabase();
	state Reference<Transaction> tr = db->createTransaction();

	// tr->setVersion(1);

	Version ver = wait(tr->getReadVersion());
	fmt::print("{}\n", ver);

	state std::vector<Future<Version>> versions;

	state double starttime = timer_monotonic();
	state int i;
	// for (i = 0; i < 100000; i++) {
	// 	Version v = wait( tr->getReadVersion() );
	// }
	for (i = 0; i < 100000; i++) {
		versions.push_back(tr->getReadVersion());
	}
	for (i = 0; i < 100000; i++) {
		Version v = wait(versions[i]);
	}
	// wait( waitForAllReady( versions ) );
	printf("Elapsed: %lf\n", timer_monotonic() - starttime);

	tr->set(LiteralStringRef("foo"), LiteralStringRef("bar"));

	Optional<FDBStandalone<ValueRef>> v = wait(tr->get(LiteralStringRef("foo")));
	if (v.present()) {
		printf("%s\n", v.get().toString().c_str());
	}

	FDBStandalone<RangeResultRef> r =
	    wait(tr->getRange(KeyRangeRef(LiteralStringRef("a"), LiteralStringRef("z")), 100));

	for (auto kv : r) {
		printf("%s is %s\n", kv.key.toString().c_str(), kv.value.toString().c_str());
	}

	g_network->stop();
	return Void();
}

void fdb_flow_test() {
	API* fdb = FDB::API::selectAPIVersion(710);
	fdb->setupNetwork();
	startThread(networkThread, fdb);

	g_network = newNet2(TLSConfig());

	openTraceFile(NetworkAddress(), 1000000, 1000000, ".");
	systemMonitor();
	uncancellable(recurring(&systemMonitor, 5.0, TaskPriority::FlushTrace));

	Future<Void> t = _test();

	g_network->run();
}

// FDB object used by bindings
namespace FDB {
class DatabaseImpl : public Database, NonCopyable {
public:
	virtual ~DatabaseImpl() { fdb_database_destroy(db); }

	Reference<Transaction> createTransaction() override;
	void setDatabaseOption(FDBDatabaseOption option, Optional<StringRef> value = Optional<StringRef>()) override;
	Future<int64_t> rebootWorker(const StringRef& address, bool check = false, int duration = 0) override;
	Future<Void> forceRecoveryWithDataLoss(const StringRef& dcid) override;
	Future<Void> createSnapshot(const StringRef& uid, const StringRef& snap_command) override;

private:
	FDBDatabase* db;
	explicit DatabaseImpl(FDBDatabase* db) : db(db) {}

	friend class API;
};

class TransactionImpl : public Transaction, private NonCopyable, public FastAllocated<TransactionImpl> {
	friend class DatabaseImpl;

public:
	virtual ~TransactionImpl() {
		if (tr) {
			fdb_transaction_destroy(tr);
		}
	}

	void setReadVersion(Version v) override;
	Future<Version> getReadVersion() override;

	Future<Optional<FDBStandalone<ValueRef>>> get(const Key& key, bool snapshot = false) override;
	Future<FDBStandalone<KeyRef>> getKey(const KeySelector& key, bool snapshot = false) override;

	Future<Void> watch(const Key& key) override;

	using Transaction::getRange;
	Future<FDBStandalone<RangeResultRef>> getRange(const KeySelector& begin,
	                                               const KeySelector& end,
	                                               GetRangeLimits limits = GetRangeLimits(),
	                                               bool snapshot = false,
	                                               bool reverse = false,
	                                               FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) override;

	Future<int64_t> getEstimatedRangeSizeBytes(const KeyRange& keys) override;
	Future<FDBStandalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRange& range, int64_t chunkSize) override;

	void addReadConflictRange(KeyRangeRef const& keys) override;
	void addReadConflictKey(KeyRef const& key) override;
	void addWriteConflictRange(KeyRangeRef const& keys) override;
	void addWriteConflictKey(KeyRef const& key) override;

	void atomicOp(const KeyRef& key, const ValueRef& operand, FDBMutationType operationType) override;
	void set(const KeyRef& key, const ValueRef& value) override;
	void clear(const KeyRangeRef& range) override;
	void clear(const KeyRef& key) override;

	Future<Void> commit() override;
	Version getCommittedVersion() override;
	Future<FDBStandalone<StringRef>> getVersionstamp() override;

	void setOption(FDBTransactionOption option, Optional<StringRef> value = Optional<StringRef>()) override;

	Future<int64_t> getApproximateSize() override;
	Future<Void> onError(Error const& e) override;

	void cancel() override;
	void reset() override;

	TransactionImpl() : tr(nullptr) {}
	TransactionImpl(TransactionImpl&& r) noexcept {
		tr = r.tr;
		r.tr = nullptr;
	}
	TransactionImpl& operator=(TransactionImpl&& r) noexcept {
		tr = r.tr;
		r.tr = nullptr;
		return *this;
	}

private:
	FDBTransaction* tr;

	explicit TransactionImpl(FDBDatabase* db);
};

static inline void throw_on_error(fdb_error_t e) {
	if (e)
		throw Error(e);
}

void CFuture::blockUntilReady() {
	throw_on_error(fdb_future_block_until_ready(f));
}

void backToFutureCallback(FDBFuture* f, void* data) {
	g_network->onMainThread(Promise<Void>((SAV<Void>*)data),
	                        TaskPriority::DefaultOnMainThread); // SOMEDAY: think about this priority
}

// backToFuture<Type>( FDBFuture*, (FDBFuture* -> Type) ) -> Future<Type>
// Takes an FDBFuture (from the alien client world, with callbacks potentially firing on an alien thread)
//   and converts it into a Future<T> (with callbacks working on this thread, cancellation etc).
// You must pass as the second parameter a function which takes a ready FDBFuture* and returns a value of Type
ACTOR template <class T, class Function>
static Future<T> backToFuture(FDBFuture* _f, Function convertValue) {
	state Reference<CFuture> f(new CFuture(_f));

	Promise<Void> ready;
	Future<Void> onReady = ready.getFuture();

	throw_on_error(fdb_future_set_callback(f->f, backToFutureCallback, ready.extractRawPointer()));
	wait(onReady);

	return convertValue(f);
}

void API::setNetworkOption(FDBNetworkOption option, Optional<StringRef> value) {
	if (value.present())
		throw_on_error(fdb_network_set_option(option, value.get().begin(), value.get().size()));
	else
		throw_on_error(fdb_network_set_option(option, nullptr, 0));
}

API* API::instance = nullptr;
API::API(int version) : version(version) {}

API* API::selectAPIVersion(int apiVersion) {
	if (API::instance) {
		if (apiVersion != API::instance->version) {
			throw api_version_already_set();
		} else {
			return API::instance;
		}
	}

	if (apiVersion < 500 || apiVersion > FDB_API_VERSION) {
		throw api_version_not_supported();
	}

	throw_on_error(fdb_select_api_version_impl(apiVersion, FDB_API_VERSION));

	API::instance = new API(apiVersion);
	return API::instance;
}

bool API::isAPIVersionSelected() {
	return API::instance != nullptr;
}

API* API::getInstance() {
	if (API::instance == nullptr) {
		throw api_version_unset();
	} else {
		return API::instance;
	}
}

void API::setupNetwork() {
	throw_on_error(fdb_setup_network());
}

void API::runNetwork() {
	throw_on_error(fdb_run_network());
}

void API::stopNetwork() {
	throw_on_error(fdb_stop_network());
}

bool API::evaluatePredicate(FDBErrorPredicate pred, Error const& e) {
	return fdb_error_predicate(pred, e.code());
}

Reference<Database> API::createDatabase(std::string const& connFilename) {
	FDBDatabase* db;
	throw_on_error(fdb_create_database(connFilename.c_str(), &db));
	return Reference<Database>(new DatabaseImpl(db));
}

int API::getAPIVersion() const {
	return version;
}

Reference<Transaction> DatabaseImpl::createTransaction() {
	return Reference<Transaction>(new TransactionImpl(db));
}

void DatabaseImpl::setDatabaseOption(FDBDatabaseOption option, Optional<StringRef> value) {
	if (value.present())
		throw_on_error(fdb_database_set_option(db, option, value.get().begin(), value.get().size()));
	else
		throw_on_error(fdb_database_set_option(db, option, nullptr, 0));
}

Future<int64_t> DatabaseImpl::rebootWorker(const StringRef& address, bool check, int duration) {
	return backToFuture<int64_t>(fdb_database_reboot_worker(db, address.begin(), address.size(), check, duration),
	                             [](Reference<CFuture> f) {
		                             int64_t res;

		                             throw_on_error(fdb_future_get_int64(f->f, &res));

		                             return res;
	                             });
}

Future<Void> DatabaseImpl::forceRecoveryWithDataLoss(const StringRef& dcid) {
	return backToFuture<Void>(fdb_database_force_recovery_with_data_loss(db, dcid.begin(), dcid.size()),
	                          [](Reference<CFuture> f) {
		                          throw_on_error(fdb_future_get_error(f->f));
		                          return Void();
	                          });
}

Future<Void> DatabaseImpl::createSnapshot(const StringRef& uid, const StringRef& snap_command) {
	return backToFuture<Void>(
	    fdb_database_create_snapshot(db, uid.begin(), uid.size(), snap_command.begin(), snap_command.size()),
	    [](Reference<CFuture> f) {
		    throw_on_error(fdb_future_get_error(f->f));
		    return Void();
	    });
}

TransactionImpl::TransactionImpl(FDBDatabase* db) {
	throw_on_error(fdb_database_create_transaction(db, &tr));
}

void TransactionImpl::setReadVersion(Version v) {
	fdb_transaction_set_read_version(tr, v);
}

Future<Version> TransactionImpl::getReadVersion() {
	return backToFuture<Version>(fdb_transaction_get_read_version(tr), [](Reference<CFuture> f) {
		Version value;

		throw_on_error(fdb_future_get_int64(f->f, &value));

		return value;
	});
}

Future<Optional<FDBStandalone<ValueRef>>> TransactionImpl::get(const Key& key, bool snapshot) {
	return backToFuture<Optional<FDBStandalone<ValueRef>>>(
	    fdb_transaction_get(tr, key.begin(), key.size(), snapshot), [](Reference<CFuture> f) {
		    fdb_bool_t present;
		    uint8_t const* value;
		    int value_length;

		    throw_on_error(fdb_future_get_value(f->f, &present, &value, &value_length));

		    if (present) {
			    return Optional<FDBStandalone<ValueRef>>(FDBStandalone<ValueRef>(f, ValueRef(value, value_length)));
		    } else {
			    return Optional<FDBStandalone<ValueRef>>();
		    }
	    });
}

Future<Void> TransactionImpl::watch(const Key& key) {
	return backToFuture<Void>(fdb_transaction_watch(tr, key.begin(), key.size()), [](Reference<CFuture> f) {
		throw_on_error(fdb_future_get_error(f->f));
		return Void();
	});
}

Future<FDBStandalone<KeyRef>> TransactionImpl::getKey(const KeySelector& key, bool snapshot) {
	return backToFuture<FDBStandalone<KeyRef>>(
	    fdb_transaction_get_key(tr, key.key.begin(), key.key.size(), key.orEqual, key.offset, snapshot),
	    [](Reference<CFuture> f) {
		    uint8_t const* key;
		    int key_length;

		    throw_on_error(fdb_future_get_key(f->f, &key, &key_length));

		    return FDBStandalone<KeyRef>(f, KeyRef(key, key_length));
	    });
}

Future<FDBStandalone<RangeResultRef>> TransactionImpl::getRange(const KeySelector& begin,
                                                                const KeySelector& end,
                                                                GetRangeLimits limits,
                                                                bool snapshot,
                                                                bool reverse,
                                                                FDBStreamingMode streamingMode) {
	// FIXME: iteration
	return backToFuture<FDBStandalone<RangeResultRef>>(
	    fdb_transaction_get_range(tr,
	                              begin.key.begin(),
	                              begin.key.size(),
	                              begin.orEqual,
	                              begin.offset,
	                              end.key.begin(),
	                              end.key.size(),
	                              end.orEqual,
	                              end.offset,
	                              limits.rows,
	                              limits.bytes,
	                              streamingMode,
	                              1,
	                              snapshot,
	                              reverse),
	    [](Reference<CFuture> f) {
		    FDBKeyValue const* kv;
		    int count;
		    fdb_bool_t more;

		    throw_on_error(fdb_future_get_keyvalue_array(f->f, &kv, &count, &more));

		    return FDBStandalone<RangeResultRef>(f,
		                                         RangeResultRef(VectorRef<KeyValueRef>((KeyValueRef*)kv, count), more));
	    });
}

Future<int64_t> TransactionImpl::getEstimatedRangeSizeBytes(const KeyRange& keys) {
	return backToFuture<int64_t>(fdb_transaction_get_estimated_range_size_bytes(
	                                 tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size()),
	                             [](Reference<CFuture> f) {
		                             int64_t bytes;
		                             throw_on_error(fdb_future_get_int64(f->f, &bytes));
		                             return bytes;
	                             });
}

Future<FDBStandalone<VectorRef<KeyRef>>> TransactionImpl::getRangeSplitPoints(const KeyRange& range,
                                                                              int64_t chunkSize) {
	return backToFuture<FDBStandalone<VectorRef<KeyRef>>>(
	    fdb_transaction_get_range_split_points(
	        tr, range.begin.begin(), range.begin.size(), range.end.begin(), range.end.size(), chunkSize),
	    [](Reference<CFuture> f) {
		    FDBKey const* ks;
		    int count;
		    throw_on_error(fdb_future_get_key_array(f->f, &ks, &count));

		    return FDBStandalone<VectorRef<KeyRef>>(f, VectorRef<KeyRef>((KeyRef*)ks, count));
	    });
}

void TransactionImpl::addReadConflictRange(KeyRangeRef const& keys) {
	throw_on_error(fdb_transaction_add_conflict_range(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDB_CONFLICT_RANGE_TYPE_READ));
}

void TransactionImpl::addReadConflictKey(KeyRef const& key) {
	return addReadConflictRange(KeyRange(KeyRangeRef(key, keyAfter(key))));
}

void TransactionImpl::addWriteConflictRange(KeyRangeRef const& keys) {
	throw_on_error(fdb_transaction_add_conflict_range(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDB_CONFLICT_RANGE_TYPE_WRITE));
}

void TransactionImpl::addWriteConflictKey(KeyRef const& key) {
	return addWriteConflictRange(KeyRange(KeyRangeRef(key, keyAfter(key))));
}

void TransactionImpl::atomicOp(const KeyRef& key, const ValueRef& operand, FDBMutationType operationType) {
	fdb_transaction_atomic_op(tr, key.begin(), key.size(), operand.begin(), operand.size(), operationType);
}

void TransactionImpl::set(const KeyRef& key, const ValueRef& value) {
	fdb_transaction_set(tr, key.begin(), key.size(), value.begin(), value.size());
}

void TransactionImpl::clear(const KeyRangeRef& range) {
	fdb_transaction_clear_range(tr, range.begin.begin(), range.begin.size(), range.end.begin(), range.end.size());
}

void TransactionImpl::clear(const KeyRef& key) {
	fdb_transaction_clear(tr, key.begin(), key.size());
}

Future<Void> TransactionImpl::commit() {
	return backToFuture<Void>(fdb_transaction_commit(tr), [](Reference<CFuture> f) {
		throw_on_error(fdb_future_get_error(f->f));
		return Void();
	});
}

Version TransactionImpl::getCommittedVersion() {
	Version v;

	throw_on_error(fdb_transaction_get_committed_version(tr, &v));
	return v;
}

Future<FDBStandalone<StringRef>> TransactionImpl::getVersionstamp() {
	return backToFuture<FDBStandalone<KeyRef>>(fdb_transaction_get_versionstamp(tr), [](Reference<CFuture> f) {
		uint8_t const* key;
		int key_length;

		throw_on_error(fdb_future_get_key(f->f, &key, &key_length));

		return FDBStandalone<StringRef>(f, StringRef(key, key_length));
	});
}

void TransactionImpl::setOption(FDBTransactionOption option, Optional<StringRef> value) {
	if (value.present()) {
		throw_on_error(fdb_transaction_set_option(tr, option, value.get().begin(), value.get().size()));
	} else {
		throw_on_error(fdb_transaction_set_option(tr, option, nullptr, 0));
	}
}

Future<int64_t> TransactionImpl::getApproximateSize() {
	return backToFuture<int64_t>(fdb_transaction_get_approximate_size(tr), [](Reference<CFuture> f) {
		int64_t size = 0;
		throw_on_error(fdb_future_get_int64(f->f, &size));
		return size;
	});
}

Future<Void> TransactionImpl::onError(Error const& e) {
	return backToFuture<Void>(fdb_transaction_on_error(tr, e.code()), [](Reference<CFuture> f) {
		throw_on_error(fdb_future_get_error(f->f));
		return Void();
	});
}

void TransactionImpl::cancel() {
	fdb_transaction_cancel(tr);
}

void TransactionImpl::reset() {
	fdb_transaction_reset(tr);
}

} // namespace FDB
