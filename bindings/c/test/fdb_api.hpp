/*
 * fdb_api.hpp
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

#ifndef FDB_API_HPP
#define FDB_API_HPP
#pragma once

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 720
#endif

#include <cassert>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <fmt/format.h>

// introduce the option enums
#include <fdb_c_options.g.h>

namespace fdb {

// hide C API to discourage mixing C/C++ API
namespace native {
#include <foundationdb/fdb_c.h>
}

using ByteString = std::basic_string<uint8_t>;
using BytesRef = std::basic_string_view<uint8_t>;
using CharsRef = std::string_view;
using KeyRef = BytesRef;
using ValueRef = BytesRef;

inline uint8_t const* toBytePtr(char const* ptr) noexcept {
	return reinterpret_cast<uint8_t const*>(ptr);
}

// get bytestring view from charstring: e.g. std::basic_string{_view}<char>
template <template <class...> class StringLike, class Char>
BytesRef toBytesRef(const StringLike<Char>& s) noexcept {
	static_assert(sizeof(Char) == 1);
	return BytesRef(reinterpret_cast<uint8_t const*>(s.data()), s.size());
}

// get charstring view from bytestring: e.g. std::basic_string{_view}<uint8_t>
template <template <class...> class StringLike, class Char>
CharsRef toCharsRef(const StringLike<Char>& s) noexcept {
	static_assert(sizeof(Char) == 1);
	return CharsRef(reinterpret_cast<char const*>(s.data()), s.size());
}

[[maybe_unused]] constexpr const bool OverflowCheck = false;

inline int intSize(BytesRef b) {
	if constexpr (OverflowCheck) {
		if (b.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
			throw std::overflow_error("byte strlen goes beyond int bounds");
	}
	return static_cast<int>(b.size());
}

class Error {
public:
	using CodeType = native::fdb_error_t;

	Error() noexcept : err(0) {}

	explicit Error(CodeType err) noexcept : err(err) {}

	char const* what() noexcept { return native::fdb_get_error(err); }

	explicit operator bool() const noexcept { return err != 0; }

	bool is(CodeType other) const noexcept { return err == other; }

	CodeType code() const noexcept { return err; }

	bool retryable() const noexcept { return native::fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err) != 0; }

private:
	CodeType err;
};

/* Traits of value types held by ready futures.
   Holds type and value extraction function. */
namespace future_var {
struct None {
	struct Type {};
	static Error extract(native::FDBFuture*, Type&) noexcept { return Error(0); }
};
struct Int64 {
	using Type = int64_t;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		return Error(native::fdb_future_get_int64(f, &out));
	}
};
struct Key {
	using Type = std::pair<uint8_t const*, int>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_key, out_key_length] = out;
		return Error(native::fdb_future_get_key(f, &out_key, &out_key_length));
	}
};
struct Value {
	using Type = std::tuple<bool, uint8_t const*, int>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_present, out_value, out_value_length] = out;
		auto out_present_native = native::fdb_bool_t{};
		auto err = native::fdb_future_get_value(f, &out_present_native, &out_value, &out_value_length);
		out_present = (out_present_native != 0);
		return Error(err);
	}
};
struct StringArray {
	using Type = std::pair<const char**, int>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_strings, out_count] = out;
		return Error(native::fdb_future_get_string_array(f, &out_strings, &out_count));
	}
};
struct KeyValueArray {
	using Type = std::tuple<native::FDBKeyValue const*, int, bool>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_kv, out_count, out_more] = out;
		auto out_more_native = native::fdb_bool_t{};
		auto err = native::fdb_future_get_keyvalue_array(f, &out_kv, &out_count, &out_more_native);
		out_more = (out_more_native != 0);
		return Error(err);
	}
};
} // namespace future_var

[[noreturn]] inline void throwError(std::string_view preamble, Error err) {
	auto msg = std::string(preamble);
	msg.append(err.what());
	throw std::runtime_error(msg);
}

inline int maxApiVersion() {
	return native::fdb_get_max_api_version();
}

inline Error selectApiVersionNothrow(int version) {
	return Error(native::fdb_select_api_version(version));
}

inline void selectApiVersion(int version) {
	if (auto err = selectApiVersionNothrow(version)) {
		throwError(fmt::format("ERROR: fdb_select_api_version({}): ", version), err);
	}
}

namespace network {

inline Error setOptionNothrow(FDBNetworkOption option, BytesRef str) noexcept {
	return Error(native::fdb_network_set_option(option, str.data(), intSize(str)));
}

inline Error setOptionNothrow(FDBNetworkOption option, int64_t value) noexcept {
	return Error(native::fdb_network_set_option(
	    option, reinterpret_cast<const uint8_t*>(&value), static_cast<int>(sizeof(value))));
}

inline void setOption(FDBNetworkOption option, BytesRef str) {
	if (auto err = setOptionNothrow(option, str)) {
		throwError(fmt::format("ERROR: fdb_network_set_option({}): ",
		                       static_cast<std::underlying_type_t<FDBNetworkOption>>(option)),
		           err);
	}
}

inline void setOption(FDBNetworkOption option, int64_t value) {
	if (auto err = setOptionNothrow(option, value)) {
		throwError(fmt::format("ERROR: fdb_network_set_option({}, {}): ",
		                       static_cast<std::underlying_type_t<FDBNetworkOption>>(option),
		                       value),
		           err);
	}
}

inline Error setupNothrow() noexcept {
	return Error(native::fdb_setup_network());
}

inline void setup() {
	if (auto err = setupNothrow())
		throwError("ERROR: fdb_network_setup(): ", err);
}

inline Error run() {
	return Error(native::fdb_run_network());
}

inline Error stop() {
	return Error(native::fdb_stop_network());
}

} // namespace network

class Transaction;
class Database;

class Result {
	friend class Transaction;
	std::shared_ptr<native::FDBResult> r;

	Result(native::FDBResult* result) {
		if (result)
			r = std::shared_ptr<native::FDBResult>(result, &native::fdb_result_destroy);
	}

public:
	using KeyValueArray = future_var::KeyValueArray::Type;

	Error getKeyValueArrayNothrow(KeyValueArray& out) const noexcept {
		auto out_more_native = native::fdb_bool_t{};
		auto& [out_kv, out_count, out_more] = out;
		auto err_raw = native::fdb_result_get_keyvalue_array(r.get(), &out_kv, &out_count, &out_more_native);
		out_more = out_more_native != 0;
		return Error(err_raw);
	}

	KeyValueArray getKeyValueArray() const {
		auto ret = KeyValueArray{};
		if (auto err = getKeyValueArrayNothrow(ret))
			throwError("ERROR: result_get_keyvalue_array(): ", err);
		return ret;
	}
};

class Future {
protected:
	friend class Transaction;
	std::shared_ptr<native::FDBFuture> f;

	Future(native::FDBFuture* future) {
		if (future)
			f = std::shared_ptr<native::FDBFuture>(future, &native::fdb_future_destroy);
	}

	// wrap any capturing lambda as callback passable to fdb_future_set_callback().
	// destroy after invocation.
	template <class Fn>
	static void callback(native::FDBFuture*, void* param) {
		auto fp = static_cast<Fn*>(param);
		try {
			(*fp)();
		} catch (const std::exception& e) {
			fmt::print(stderr, "ERROR: Exception thrown in user callback: {}", e.what());
		}
		delete fp;
	}

	// set as callback user-defined completion handler of signature void(Future)
	template <class FutureType, class UserFunc>
	void then(UserFunc&& fn) {
		auto cb = [fut = FutureType(*this), fn = std::forward<UserFunc>(fn)]() { fn(fut); };
		using cb_type = std::decay_t<decltype(cb)>;
		auto fp = new cb_type(std::move(cb));
		if (auto err = Error(native::fdb_future_set_callback(f.get(), &callback<cb_type>, fp))) {
			throwError("ERROR: future_set_callback: ", err);
		}
	}

public:
	Future() noexcept : Future(nullptr) {}
	Future(const Future&) noexcept = default;
	Future& operator=(const Future&) noexcept = default;

	bool valid() const noexcept { return f != nullptr; }

	explicit operator bool() const noexcept { return valid(); }

	bool ready() const noexcept {
		assert(valid());
		return native::fdb_future_is_ready(f.get()) != 0;
	}

	Error blockUntilReady() const noexcept {
		assert(valid());
		return Error(native::fdb_future_block_until_ready(f.get()));
	}

	Error error() const noexcept {
		assert(valid());
		return Error(native::fdb_future_get_error(f.get()));
	}

	void cancel() noexcept { native::fdb_future_cancel(f.get()); }

	template <class VarTraits>
	typename VarTraits::Type get() const {
		assert(valid());
		assert(!error());
		auto out = typename VarTraits::Type{};
		if (auto err = VarTraits::extract(f.get(), out)) {
			throwError("future_get: ", err);
		}
		return out;
	}

	template <class VarTraits>
	Error getNothrow(typename VarTraits::Type& var) const noexcept {
		assert(valid());
		assert(!error());
		auto out = typename VarTraits::Type{};
		return VarTraits::extract(f.get(), out);
	}

	template <class UserFunc>
	void then(UserFunc&& fn) {
		then<Future>(std::forward<UserFunc>(fn));
	}
};

template <typename VarTraits>
class TypedFuture : public Future {
	friend class Future;
	friend class Transaction;
	using SelfType = TypedFuture<VarTraits>;
	using Future::Future;
	// hide type-unsafe inherited functions
	using Future::get;
	using Future::getNothrow;
	using Future::then;
	TypedFuture(const Future& f) noexcept : Future(f) {}

public:
	using ContainedType = typename VarTraits::Type;

	Future eraseType() const noexcept { return static_cast<Future const&>(*this); }

	ContainedType get() const { return get<VarTraits>(); }

	Error getNothrow(ContainedType& out) const noexcept { return getNothrow<VarTraits>(out); }

	template <class UserFunc>
	void then(UserFunc&& fn) {
		Future::then<SelfType>(std::forward<UserFunc>(fn));
	}
};

struct KeySelector {
	const uint8_t* key;
	int keyLength;
	bool orEqual;
	int offset;
};

namespace key_select {

inline KeySelector firstGreaterThan(KeyRef key, int offset = 0) {
	return KeySelector{ FDB_KEYSEL_FIRST_GREATER_THAN(key.data(), intSize(key)) + offset };
}

inline KeySelector firstGreaterOrEqual(KeyRef key, int offset = 0) {
	return KeySelector{ FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(key.data(), intSize(key)) + offset };
}

inline KeySelector lastLessThan(KeyRef key, int offset = 0) {
	return KeySelector{ FDB_KEYSEL_LAST_LESS_THAN(key.data(), intSize(key)) + offset };
}

inline KeySelector lastLessOrEqual(KeyRef key, int offset = 0) {
	return KeySelector{ FDB_KEYSEL_LAST_LESS_OR_EQUAL(key.data(), intSize(key)) + offset };
}

} // namespace key_select

class Transaction {
	friend class Database;
	std::shared_ptr<native::FDBTransaction> tr;

	explicit Transaction(native::FDBTransaction* tr_raw) {
		if (tr_raw)
			tr = std::shared_ptr<native::FDBTransaction>(tr_raw, &native::fdb_transaction_destroy);
	}

public:
	Transaction() noexcept : Transaction(nullptr) {}
	Transaction(const Transaction&) noexcept = default;
	Transaction& operator=(const Transaction&) noexcept = default;

	bool valid() const noexcept { return tr != nullptr; }

	explicit operator bool() const noexcept { return valid(); }

	Error setOptionNothrow(FDBTransactionOption option, int64_t value) noexcept {
		return Error(native::fdb_transaction_set_option(
		    tr.get(), option, reinterpret_cast<const uint8_t*>(&value), static_cast<int>(sizeof(value))));
	}

	Error setOptionNothrow(FDBTransactionOption option, BytesRef str) noexcept {
		return Error(native::fdb_transaction_set_option(tr.get(), option, str.data(), intSize(str)));
	}

	void setOption(FDBTransactionOption option, int64_t value) {
		if (auto err = setOptionNothrow(option, value)) {
			throwError(fmt::format("transaction_set_option({}, {}) returned error: ",
			                       static_cast<std::underlying_type_t<FDBTransactionOption>>(option),
			                       value),
			           err);
		}
	}

	void setOption(FDBTransactionOption option, BytesRef str) {
		if (auto err = setOptionNothrow(option, str)) {
			throwError(fmt::format("transaction_set_option({}) returned error: ",
			                       static_cast<std::underlying_type_t<FDBTransactionOption>>(option)),
			           err);
		}
	}

	TypedFuture<future_var::Int64> getReadVersion() { return native::fdb_transaction_get_read_version(tr.get()); }

	Error getCommittedVersionNothrow(int64_t& out) {
		return Error(native::fdb_transaction_get_committed_version(tr.get(), &out));
	}

	int64_t getCommittedVersion() {
		auto out = int64_t{};
		if (auto err = getCommittedVersionNothrow(out)) {
			throwError("get_committed_version: ", err);
		}
		return out;
	}

	TypedFuture<future_var::Key> getKey(KeySelector sel, bool snapshot) {
		return native::fdb_transaction_get_key(tr.get(), sel.key, sel.keyLength, sel.orEqual, sel.offset, snapshot);
	}

	TypedFuture<future_var::Value> get(KeyRef key, bool snapshot) {
		return native::fdb_transaction_get(tr.get(), key.data(), intSize(key), snapshot);
	}

	// Usage: tx.getRange(key_select::firstGreaterOrEqual(firstKey), key_select::lastLessThan(lastKey), ...)
	// gets key-value pairs in key range [begin, end)
	TypedFuture<future_var::KeyValueArray> getRange(KeySelector first,
	                                                KeySelector last,
	                                                int limit,
	                                                int target_bytes,
	                                                FDBStreamingMode mode,
	                                                int iteration,
	                                                bool snapshot,
	                                                bool reverse) {
		return native::fdb_transaction_get_range(tr.get(),
		                                         first.key,
		                                         first.keyLength,
		                                         first.orEqual,
		                                         first.offset,
		                                         last.key,
		                                         last.keyLength,
		                                         last.orEqual,
		                                         last.offset,
		                                         limit,
		                                         target_bytes,
		                                         mode,
		                                         iteration,
		                                         snapshot,
		                                         reverse);
	}

	Result readBlobGranules(KeyRef begin,
	                        KeyRef end,
	                        int64_t begin_version,
	                        int64_t read_version,
	                        native::FDBReadBlobGranuleContext context) {
		return Result(native::fdb_transaction_read_blob_granules(
		    tr.get(), begin.data(), intSize(begin), end.data(), intSize(end), begin_version, read_version, context));
	}

	TypedFuture<future_var::None> commit() { return native::fdb_transaction_commit(tr.get()); }

	TypedFuture<future_var::None> onError(Error err) { return native::fdb_transaction_on_error(tr.get(), err.code()); }

	void reset() { return native::fdb_transaction_reset(tr.get()); }

	void set(KeyRef key, ValueRef value) {
		native::fdb_transaction_set(tr.get(), key.data(), intSize(key), value.data(), intSize(value));
	}

	void clear(KeyRef key) { native::fdb_transaction_clear(tr.get(), key.data(), intSize(key)); }

	void clearRange(KeyRef begin, KeyRef end) {
		native::fdb_transaction_clear_range(tr.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}
};

class Database {
	std::shared_ptr<native::FDBDatabase> db;

public:
	Database(const Database&) noexcept = default;
	Database& operator=(const Database&) noexcept = default;
	Database(const std::string& cluster_file_path) : db(nullptr) {
		auto db_raw = static_cast<native::FDBDatabase*>(nullptr);
		if (auto err = Error(native::fdb_create_database(cluster_file_path.c_str(), &db_raw)))
			throwError(fmt::format("Failed to create database with '{}': ", cluster_file_path), err);
		db = std::shared_ptr<native::FDBDatabase>(db_raw, &native::fdb_database_destroy);
	}
	Database() noexcept : db(nullptr) {}

	Error setOptionNothrow(FDBDatabaseOption option, int64_t value) noexcept {
		return Error(native::fdb_database_set_option(
		    db.get(), option, reinterpret_cast<const uint8_t*>(&value), static_cast<int>(sizeof(value))));
	}

	Error setOptionNothrow(FDBDatabaseOption option, BytesRef str) noexcept {
		return Error(native::fdb_database_set_option(db.get(), option, str.data(), intSize(str)));
	}

	void setOption(FDBDatabaseOption option, int64_t value) {
		if (auto err = setOptionNothrow(option, value)) {
			throwError(fmt::format("database_set_option({}, {}) returned error: ",
			                       static_cast<std::underlying_type_t<FDBDatabaseOption>>(option),
			                       value),
			           err);
		}
	}

	void setOption(FDBDatabaseOption option, BytesRef str) {
		if (auto err = setOptionNothrow(option, str)) {
			throwError(fmt::format("database_set_option({}) returned error: ",
			                       static_cast<std::underlying_type_t<FDBDatabaseOption>>(option)),
			           err);
		}
	}

	Transaction createTransaction() {
		if (!db)
			throw std::runtime_error("create_transaction from null database");
		auto tx_native = static_cast<native::FDBTransaction*>(nullptr);
		auto err = Error(native::fdb_database_create_transaction(db.get(), &tx_native));
		if (err)
			throwError("Failed to create transaction: ", err);
		return Transaction(tx_native);
	}
};

} // namespace fdb

#endif /*FDB_API_HPP*/
