/*
 * fdb_api.cpp
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

#include "fdb_api.hpp"

#include <iostream>

namespace fdb {

// Future

Future::~Future() {
	fdb_future_destroy(future_);
}

bool Future::is_ready() {
	return fdb_future_is_ready(future_);
}

[[nodiscard]] fdb_error_t Future::block_until_ready() {
	return fdb_future_block_until_ready(future_);
}

[[nodiscard]] fdb_error_t Future::set_callback(FDBCallback callback, void* callback_parameter) {
	return fdb_future_set_callback(future_, callback, callback_parameter);
}

[[nodiscard]] fdb_error_t Future::get_error() {
	return fdb_future_get_error(future_);
}

void Future::release_memory() {
	fdb_future_release_memory(future_);
}

void Future::cancel() {
	fdb_future_cancel(future_);
}

// Int64Future

[[nodiscard]] fdb_error_t Int64Future::get(int64_t* out) {
	return fdb_future_get_int64(future_, out);
}

// ValueFuture

[[nodiscard]] fdb_error_t ValueFuture::get(fdb_bool_t* out_present, const uint8_t** out_value, int* out_value_length) {
	return fdb_future_get_value(future_, out_present, out_value, out_value_length);
}

// KeyFuture

[[nodiscard]] fdb_error_t KeyFuture::get(const uint8_t** out_key, int* out_key_length) {
	return fdb_future_get_key(future_, out_key, out_key_length);
}

// StringArrayFuture

[[nodiscard]] fdb_error_t StringArrayFuture::get(const char*** out_strings, int* out_count) {
	return fdb_future_get_string_array(future_, out_strings, out_count);
}

// KeyRangeArrayFuture

[[nodiscard]] fdb_error_t KeyRangeArrayFuture::get(const FDBKeyRange** out_keyranges, int* out_count) {
	return fdb_future_get_keyrange_array(future_, out_keyranges, out_count);
}

// KeyValueArrayFuture

[[nodiscard]] fdb_error_t KeyValueArrayFuture::get(const FDBKeyValue** out_kv, int* out_count, fdb_bool_t* out_more) {
	return fdb_future_get_keyvalue_array(future_, out_kv, out_count, out_more);
}

// MappedKeyValueArrayFuture

[[nodiscard]] fdb_error_t MappedKeyValueArrayFuture::get(const FDBMappedKeyValue** out_kv,
                                                         int* out_count,
                                                         fdb_bool_t* out_more) {
	return fdb_future_get_mappedkeyvalue_array(future_, out_kv, out_count, out_more);
}

// Result

Result::~Result() {
	fdb_result_destroy(result_);
}

// KeyValueArrayResult
[[nodiscard]] fdb_error_t KeyValueArrayResult::get(const FDBKeyValue** out_kv, int* out_count, fdb_bool_t* out_more) {
	return fdb_result_get_keyvalue_array(result_, out_kv, out_count, out_more);
}

// Database
Int64Future Database::reboot_worker(FDBDatabase* db,
                                    const uint8_t* address,
                                    int address_length,
                                    fdb_bool_t check,
                                    int duration) {
	return Int64Future(fdb_database_reboot_worker(db, address, address_length, check, duration));
}

EmptyFuture Database::force_recovery_with_data_loss(FDBDatabase* db, const uint8_t* dcid, int dcid_length) {
	return EmptyFuture(fdb_database_force_recovery_with_data_loss(db, dcid, dcid_length));
}

EmptyFuture Database::create_snapshot(FDBDatabase* db,
                                      const uint8_t* uid,
                                      int uid_length,
                                      const uint8_t* snap_command,
                                      int snap_command_length) {
	return EmptyFuture(fdb_database_create_snapshot(db, uid, uid_length, snap_command, snap_command_length));
}

// Tenant
Tenant::Tenant(FDBDatabase* db, const uint8_t* name, int name_length) {
	if (fdb_error_t err = fdb_database_open_tenant(db, name, name_length, &tenant)) {
		std::cerr << fdb_get_error(err) << std::endl;
		std::abort();
	}
}

Tenant::~Tenant() {
	if (tenant != nullptr) {
		fdb_tenant_destroy(tenant);
	}
}

// Transaction
Transaction::Transaction(FDBDatabase* db) {
	if (fdb_error_t err = fdb_database_create_transaction(db, &tr_)) {
		std::cerr << fdb_get_error(err) << std::endl;
		std::abort();
	}
}

Transaction::Transaction(Tenant& tenant) {
	if (fdb_error_t err = fdb_tenant_create_transaction(tenant.tenant, &tr_)) {
		std::cerr << fdb_get_error(err) << std::endl;
		std::abort();
	}
}

Transaction::~Transaction() {
	fdb_transaction_destroy(tr_);
}

void Transaction::reset() {
	fdb_transaction_reset(tr_);
}

void Transaction::cancel() {
	fdb_transaction_cancel(tr_);
}

[[nodiscard]] fdb_error_t Transaction::set_option(FDBTransactionOption option, const uint8_t* value, int value_length) {
	return fdb_transaction_set_option(tr_, option, value, value_length);
}

void Transaction::set_read_version(int64_t version) {
	fdb_transaction_set_read_version(tr_, version);
}

Int64Future Transaction::get_read_version() {
	return Int64Future(fdb_transaction_get_read_version(tr_));
}

Int64Future Transaction::get_approximate_size() {
	return Int64Future(fdb_transaction_get_approximate_size(tr_));
}

KeyFuture Transaction::get_versionstamp() {
	return KeyFuture(fdb_transaction_get_versionstamp(tr_));
}

ValueFuture Transaction::get(std::string_view key, fdb_bool_t snapshot) {
	return ValueFuture(fdb_transaction_get(tr_, (const uint8_t*)key.data(), key.size(), snapshot));
}

KeyFuture Transaction::get_key(const uint8_t* key_name,
                               int key_name_length,
                               fdb_bool_t or_equal,
                               int offset,
                               fdb_bool_t snapshot) {
	return KeyFuture(fdb_transaction_get_key(tr_, key_name, key_name_length, or_equal, offset, snapshot));
}

StringArrayFuture Transaction::get_addresses_for_key(std::string_view key) {
	return StringArrayFuture(fdb_transaction_get_addresses_for_key(tr_, (const uint8_t*)key.data(), key.size()));
}

KeyValueArrayFuture Transaction::get_range(const uint8_t* begin_key_name,
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
                                           fdb_bool_t reverse) {
	return KeyValueArrayFuture(fdb_transaction_get_range(tr_,
	                                                     begin_key_name,
	                                                     begin_key_name_length,
	                                                     begin_or_equal,
	                                                     begin_offset,
	                                                     end_key_name,
	                                                     end_key_name_length,
	                                                     end_or_equal,
	                                                     end_offset,
	                                                     limit,
	                                                     target_bytes,
	                                                     mode,
	                                                     iteration,
	                                                     snapshot,
	                                                     reverse));
}

MappedKeyValueArrayFuture Transaction::get_mapped_range(const uint8_t* begin_key_name,
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
                                                        fdb_bool_t reverse) {
	return MappedKeyValueArrayFuture(fdb_transaction_get_mapped_range(tr_,
	                                                                  begin_key_name,
	                                                                  begin_key_name_length,
	                                                                  begin_or_equal,
	                                                                  begin_offset,
	                                                                  end_key_name,
	                                                                  end_key_name_length,
	                                                                  end_or_equal,
	                                                                  end_offset,
	                                                                  mapper_name,
	                                                                  mapper_name_length,
	                                                                  limit,
	                                                                  target_bytes,
	                                                                  mode,
	                                                                  iteration,
	                                                                  snapshot,
	                                                                  reverse));
}

EmptyFuture Transaction::watch(std::string_view key) {
	return EmptyFuture(fdb_transaction_watch(tr_, (const uint8_t*)key.data(), key.size()));
}

EmptyFuture Transaction::commit() {
	return EmptyFuture(fdb_transaction_commit(tr_));
}

EmptyFuture Transaction::on_error(fdb_error_t err) {
	return EmptyFuture(fdb_transaction_on_error(tr_, err));
}

void Transaction::clear(std::string_view key) {
	return fdb_transaction_clear(tr_, (const uint8_t*)key.data(), key.size());
}

void Transaction::clear_range(std::string_view begin_key, std::string_view end_key) {
	fdb_transaction_clear_range(
	    tr_, (const uint8_t*)begin_key.data(), begin_key.size(), (const uint8_t*)end_key.data(), end_key.size());
}

void Transaction::set(std::string_view key, std::string_view value) {
	fdb_transaction_set(tr_, (const uint8_t*)key.data(), key.size(), (const uint8_t*)value.data(), value.size());
}

void Transaction::atomic_op(std::string_view key,
                            const uint8_t* param,
                            int param_length,
                            FDBMutationType operationType) {
	return fdb_transaction_atomic_op(tr_, (const uint8_t*)key.data(), key.size(), param, param_length, operationType);
}

[[nodiscard]] fdb_error_t Transaction::get_committed_version(int64_t* out_version) {
	return fdb_transaction_get_committed_version(tr_, out_version);
}

fdb_error_t Transaction::add_conflict_range(std::string_view begin_key,
                                            std::string_view end_key,
                                            FDBConflictRangeType type) {
	return fdb_transaction_add_conflict_range(
	    tr_, (const uint8_t*)begin_key.data(), begin_key.size(), (const uint8_t*)end_key.data(), end_key.size(), type);
}

KeyRangeArrayFuture Transaction::get_blob_granule_ranges(std::string_view begin_key, std::string_view end_key) {
	return KeyRangeArrayFuture(fdb_transaction_get_blob_granule_ranges(
	    tr_, (const uint8_t*)begin_key.data(), begin_key.size(), (const uint8_t*)end_key.data(), end_key.size()));
}
KeyValueArrayResult Transaction::read_blob_granules(std::string_view begin_key,
                                                    std::string_view end_key,
                                                    int64_t beginVersion,
                                                    int64_t readVersion,
                                                    FDBReadBlobGranuleContext granuleContext) {
	return KeyValueArrayResult(fdb_transaction_read_blob_granules(tr_,
	                                                              (const uint8_t*)begin_key.data(),
	                                                              begin_key.size(),
	                                                              (const uint8_t*)end_key.data(),
	                                                              end_key.size(),
	                                                              beginVersion,
	                                                              readVersion,
	                                                              granuleContext));
}

} // namespace fdb
