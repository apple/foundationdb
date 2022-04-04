/*
 * fdb_api.hpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

// A collection of C++ classes to wrap the C API to improve memory management
// and add types to futures. Using the old C API may look something like:
//
//   FDBTransaction *tr;
//   fdb_database_create_transaction(db, &tr);
//   FDBFuture *f = fdb_transaction_get(tr, (const uint8_t*)"foo", 3, true);
//   fdb_future_block_until_ready(f);
//   fdb_future_get_value(f, ...);
//   fdb_future_destroy(f);
//   fdb_transaction_destroy(tr);
//
// Using the wrapper classes defined here, it will instead look like:
//
//   fdb::Transaction tr(db);
//   fdb::ValueFuture f = tr.get((const uint8_t*)"foo", 3, true);
//   f.block_until_ready();
//   f.get_value(f, ...);
//

#pragma once

#define FDB_API_VERSION 710
#include <foundationdb/fdb_c.h>

#include <string>
#include <string_view>

namespace fdb {

// Wrapper parent class to manage memory of an FDBFuture pointer. Cleans up
// FDBFuture when this instance goes out of scope.
class Future {
public:
	virtual ~Future() = 0;

	// Wrapper around fdb_future_is_ready.
	bool is_ready();
	// Wrapper around fdb_future_block_until_ready.
	fdb_error_t block_until_ready();
	// Wrapper around fdb_future_set_callback.
	fdb_error_t set_callback(FDBCallback callback, void* callback_parameter);
	// Wrapper around fdb_future_get_error.
	fdb_error_t get_error();
	// Wrapper around fdb_future_release_memory.
	void release_memory();
	// Wrapper around fdb_future_cancel.
	void cancel();

	// Conversion operator to allow Future instances to work interchangeably as
	// an FDBFuture object.
	// operator FDBFuture* () const {
	//   return future_;
	// }

protected:
	Future(FDBFuture* f) : future_(f) {}
	FDBFuture* future_;
};

class Int64Future : public Future {
public:
	// Call this function instead of fdb_future_get_int64 when using the
	// Int64Future type. It's behavior is identical to fdb_future_get_int64.
	fdb_error_t get(int64_t* out);

private:
	friend class Transaction;
	friend class Database;
	Int64Future(FDBFuture* f) : Future(f) {}
};

class KeyFuture : public Future {
public:
	// Call this function instead of fdb_future_get_key when using the KeyFuture
	// type. It's behavior is identical to fdb_future_get_key.
	fdb_error_t get(const uint8_t** out_key, int* out_key_length);

private:
	friend class Transaction;
	KeyFuture(FDBFuture* f) : Future(f) {}
};

class ValueFuture : public Future {
public:
	// Call this function instead of fdb_future_get_value when using the
	// ValueFuture type. It's behavior is identical to fdb_future_get_value.
	fdb_error_t get(fdb_bool_t* out_present, const uint8_t** out_value, int* out_value_length);

private:
	friend class Transaction;
	ValueFuture(FDBFuture* f) : Future(f) {}
};

class StringArrayFuture : public Future {
public:
	// Call this function instead of fdb_future_get_string_array when using the
	// StringArrayFuture type. It's behavior is identical to
	// fdb_future_get_string_array.
	fdb_error_t get(const char*** out_strings, int* out_count);

private:
	friend class Transaction;
	StringArrayFuture(FDBFuture* f) : Future(f) {}
};

class KeyValueArrayFuture : public Future {
public:
	// Call this function instead of fdb_future_get_keyvalue_array when using
	// the KeyValueArrayFuture type. It's behavior is identical to
	// fdb_future_get_keyvalue_array.
	fdb_error_t get(const FDBKeyValue** out_kv, int* out_count, fdb_bool_t* out_more);

private:
	friend class Transaction;
	KeyValueArrayFuture(FDBFuture* f) : Future(f) {}
};

class MappedKeyValueArrayFuture : public Future {
public:
	// Call this function instead of fdb_future_get_mappedkeyvalue_array when using
	// the MappedKeyValueArrayFuture type. Its behavior is identical to
	// fdb_future_get_mappedkeyvalue_array.
	fdb_error_t get(const FDBMappedKeyValue** out_kv, int* out_count, fdb_bool_t* out_more);

private:
	friend class Transaction;
	MappedKeyValueArrayFuture(FDBFuture* f) : Future(f) {}
};

class KeyRangeArrayFuture : public Future {
public:
	// Call this function instead of fdb_future_get_keyrange_array when using
	// the KeyRangeArrayFuture type. It's behavior is identical to
	// fdb_future_get_keyrange_array.
	fdb_error_t get(const FDBKeyRange** out_keyranges, int* out_count);

private:
	friend class Transaction;
	KeyRangeArrayFuture(FDBFuture* f) : Future(f) {}
};

class EmptyFuture : public Future {
private:
	friend class Transaction;
	friend class Database;
	EmptyFuture(FDBFuture* f) : Future(f) {}
};

class Result {
public:
	virtual ~Result() = 0;

protected:
	Result(FDBResult* r) : result_(r) {}
	FDBResult* result_;
};

class KeyValueArrayResult : public Result {
public:
	// Call this function instead of fdb_result_get_keyvalue_array when using
	// the KeyValueArrayREsult type. It's behavior is identical to
	// fdb_result_get_keyvalue_array.
	fdb_error_t get(const FDBKeyValue** out_kv, int* out_count, fdb_bool_t* out_more);

private:
	friend class Transaction;
	KeyValueArrayResult(FDBResult* r) : Result(r) {}
};

// Wrapper around FDBDatabase, providing database-level API
class Database final {
public:
	static Int64Future reboot_worker(FDBDatabase* db,
	                                 const uint8_t* address,
	                                 int address_length,
	                                 fdb_bool_t check,
	                                 int duration);
	static EmptyFuture force_recovery_with_data_loss(FDBDatabase* db, const uint8_t* dcid, int dcid_length);
	static EmptyFuture create_snapshot(FDBDatabase* db,
	                                   const uint8_t* uid,
	                                   int uid_length,
	                                   const uint8_t* snap_command,
	                                   int snap_command_length);
};

class Tenant final {
public:
	Tenant(FDBDatabase* db, const uint8_t* name, int name_length);
	~Tenant();
	Tenant(const Tenant&) = delete;
	Tenant& operator=(const Tenant&) = delete;
	Tenant(Tenant&&) = delete;
	Tenant& operator=(Tenant&&) = delete;

private:
	friend class Transaction;
	FDBTenant* tenant;
};

// Wrapper around FDBTransaction, providing the same set of calls as the C API.
// Handles cleanup of memory, removing the need to call
// fdb_transaction_destroy.
class Transaction final {
public:
	// Given an FDBDatabase, initializes a new transaction.
	Transaction(FDBDatabase* db);
	Transaction(Tenant& tenant);
	~Transaction();

	// Wrapper around fdb_transaction_reset.
	void reset();

	// Wrapper around fdb_transaction_cancel.
	void cancel();

	// Wrapper around fdb_transaction_set_option.
	fdb_error_t set_option(FDBTransactionOption option, const uint8_t* value, int value_length);

	// Wrapper around fdb_transaction_set_read_version.
	void set_read_version(int64_t version);

	// Returns a future which will be set to the transaction read version.
	Int64Future get_read_version();

	// Returns a future which will be set to the approximate transaction size so far.
	Int64Future get_approximate_size();

	// Returns a future which will be set to the versionstamp which was used by
	// any versionstamp operations in the transaction.
	KeyFuture get_versionstamp();

	// Returns a future which will be set to the value of `key` in the database.
	ValueFuture get(std::string_view key, fdb_bool_t snapshot);

	// Returns a future which will be set to the key in the database matching the
	// passed key selector.
	KeyFuture get_key(const uint8_t* key_name,
	                  int key_name_length,
	                  fdb_bool_t or_equal,
	                  int offset,
	                  fdb_bool_t snapshot);

	// Returns a future which will be set to an array of strings.
	StringArrayFuture get_addresses_for_key(std::string_view key);

	// Returns a future which will be set to an FDBKeyValue array.
	KeyValueArrayFuture get_range(const uint8_t* begin_key_name,
	                              int begin_key_name_length,
	                              fdb_bool_t begin_or_equal,
	                              int begin_offset,
	                              const uint8_t* end_key_name,
	                              int end_key_name_length,
	                              fdb_bool_t end_or_equal,
	                              int end_offset,
	                              int limit,
	                              int target_bytes,
	                              FDBStreamingMode mode,
	                              int iteration,
	                              fdb_bool_t snapshot,
	                              fdb_bool_t reverse);

	// WARNING: This feature is considered experimental at this time. It is only allowed when using snapshot isolation
	// AND disabling read-your-writes. Returns a future which will be set to an FDBKeyValue array.
	MappedKeyValueArrayFuture get_mapped_range(const uint8_t* begin_key_name,
	                                           int begin_key_name_length,
	                                           fdb_bool_t begin_or_equal,
	                                           int begin_offset,
	                                           const uint8_t* end_key_name,
	                                           int end_key_name_length,
	                                           fdb_bool_t end_or_equal,
	                                           int end_offset,
	                                           const uint8_t* mapper_name,
	                                           int mapper_name_length,
	                                           int limit,
	                                           int target_bytes,
	                                           FDBStreamingMode mode,
	                                           int iteration,
	                                           fdb_bool_t snapshot,
	                                           fdb_bool_t reverse);

	// Wrapper around fdb_transaction_watch. Returns a future representing an
	// empty value.
	EmptyFuture watch(std::string_view key);

	// Wrapper around fdb_transaction_commit. Returns a future representing an
	// empty value.
	EmptyFuture commit();

	// Wrapper around fdb_transaction_on_error. Returns a future representing an
	// empty value.
	EmptyFuture on_error(fdb_error_t err);

	// Wrapper around fdb_transaction_clear.
	void clear(std::string_view key);

	// Wrapper around fdb_transaction_clear_range.
	void clear_range(std::string_view begin_key, std::string_view end_key);

	// Wrapper around fdb_transaction_set.
	void set(std::string_view key, std::string_view value);

	// Wrapper around fdb_transaction_atomic_op.
	void atomic_op(std::string_view key, const uint8_t* param, int param_length, FDBMutationType operationType);

	// Wrapper around fdb_transaction_get_committed_version.
	fdb_error_t get_committed_version(int64_t* out_version);

	// Wrapper around fdb_transaction_add_conflict_range.
	fdb_error_t add_conflict_range(std::string_view begin_key, std::string_view end_key, FDBConflictRangeType type);

	KeyRangeArrayFuture get_blob_granule_ranges(std::string_view begin_key, std::string_view end_key);
	KeyValueArrayResult read_blob_granules(std::string_view begin_key,
	                                       std::string_view end_key,
	                                       int64_t beginVersion,
	                                       int64_t endVersion,
	                                       FDBReadBlobGranuleContext granule_context);

private:
	FDBTransaction* tr_;
};

} // namespace fdb
