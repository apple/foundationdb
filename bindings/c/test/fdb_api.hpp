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
#define FDB_USE_LATEST_API_VERSION
#endif

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <fmt/format.h>

// introduce the option enums
#include <fdb_c_options.g.h>

namespace fdb {

// hide C API to discourage mixing C/C++ API
namespace native {
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_internal.h>
#include <foundationdb/fdb_c_requests.h>
} // namespace native

using ByteString = std::basic_string<uint8_t>;
using BytesRef = std::basic_string_view<uint8_t>;
using CharsRef = std::string_view;
using Key = ByteString;
using KeyRef = BytesRef;
using Value = ByteString;
using ValueRef = BytesRef;

struct KeyRangeRef : native::FDBKeyRange {
	KeyRef beginKey() const noexcept { return KeyRef(native::FDBKeyRange::begin_key, begin_key_length); }
	KeyRef endKey() const noexcept { return KeyRef(native::FDBKeyRange::end_key, end_key_length); }
};

struct KeyValueRef : native::FDBKeyValue {
	KeyRef key() const noexcept { return KeyRef(native::FDBKeyValue::key, key_length); }
	ValueRef value() const noexcept { return ValueRef(native::FDBKeyValue::value, value_length); }
};

struct MappedKeyValueRef : native::FDBMappedKeyValue {
	KeyRef key() const noexcept {
		return KeyRef(native::FDBMappedKeyValue::key.key, native::FDBMappedKeyValue::key.key_length);
	}
	ValueRef value() const noexcept {
		return ValueRef(native::FDBMappedKeyValue::value.key, native::FDBMappedKeyValue::value.key_length);
	}
	KeyRef rangeBeginKey() const noexcept {
		return KeyRef(native::FDBMappedKeyValue::getRange.begin.key.key, native::FDBMappedKeyValue::getRange.begin.key.key_length);
	}
	KeyRef rangeEndKey() const noexcept {
		return KeyRef(native::FDBMappedKeyValue::getRange.end.key.key, native::FDBMappedKeyValue::getRange.end.key.key_length);
	}
	int rangeSize() const noexcept {
		return native::FDBMappedKeyValue::getRange.m_size;
	}
	KeyValueRef rangeKeyValue(int i) const noexcept {
		return (KeyValueRef&)native::FDBMappedKeyValue::getRange.data[i];
	}
};

struct KeyValue {
	Key key;
	Value value;
};
struct KeyRange {
	Key beginKey;
	Key endKey;
};

template <class T>
class VectorRef {
public:
	VectorRef(T* data, int size) : m_data(data), m_size(size) {}
	VectorRef() = default;
	VectorRef(const VectorRef& r) = default;
	VectorRef(VectorRef&& r) noexcept = default;
	VectorRef& operator=(const VectorRef& r) = default;
	VectorRef& operator=(VectorRef&& r) noexcept = default;

	int size() { return m_size; }
	T* begin() { return m_data; }
	T* end() { return m_data + m_size; }
	T& front() { return *begin(); }
	T& back() { return end()[-1]; }
	T& operator[](int i) { return m_data[i]; }

	const T* begin() const { return m_data; }
	const T* end() const { return m_data + m_size; }
	T const& front() const { return *begin(); }
	T const& back() const { return end()[-1]; }
	int size() const { return m_size; }
	bool empty() const { return m_size == 0; }
	const T& operator[](int i) const { return m_data[i]; }

private:
	T* m_data;
	int m_size;
};

struct GranuleSummary {
	KeyRange keyRange;
	int64_t snapshotVersion;
	int64_t snapshotSize;
	int64_t deltaVersion;
	int64_t deltaSize;

	GranuleSummary(const native::FDBGranuleSummary& nativeSummary) {
		keyRange.beginKey = Key(nativeSummary.key_range.begin_key, nativeSummary.key_range.begin_key_length);
		keyRange.endKey = Key(nativeSummary.key_range.end_key, nativeSummary.key_range.end_key_length);
		snapshotVersion = nativeSummary.snapshot_version;
		snapshotSize = nativeSummary.snapshot_size;
		deltaVersion = nativeSummary.delta_version;
		deltaSize = nativeSummary.delta_size;
	}
};

// fdb_future_readbg_get_descriptions

struct GranuleFilePointerRef : public native::FDBBGFilePointerV2 {
	GranuleFilePointerRef(const GranuleFilePointerRef&) = delete;
	BytesRef filename() const noexcept { return BytesRef(filename_ptr, filename_length); }
};

struct GranuleMutationRef : public native::FDBBGMutation {
	ByteString param1() const noexcept { return ByteString(param1_ptr, param1_length); }
	ByteString param2() const noexcept { return ByteString(param2_ptr, param2_length); }
};

struct GranuleDescriptionRef : native::FDBBGFileDescriptionV2 {
	GranuleDescriptionRef(const GranuleDescriptionRef&) = delete;
	const KeyRangeRef& keyRange() const noexcept { return (KeyRangeRef&)key_range; }
	KeyRef beginKey() const noexcept { return KeyRef(key_range.begin_key, key_range.begin_key_length); }
	KeyRef endKey() const noexcept { return KeyRef(key_range.end_key, key_range.end_key_length); }
	GranuleFilePointerRef* snapshotFile() const noexcept { return (GranuleFilePointerRef*)snapshot_file_pointer; }
	VectorRef<GranuleFilePointerRef*> deltaFiles() const noexcept {
		return VectorRef<GranuleFilePointerRef*>((GranuleFilePointerRef**)delta_files, delta_file_count);
	}
	VectorRef<GranuleMutationRef*> memoryMutations() const noexcept {
		return VectorRef<GranuleMutationRef*>((GranuleMutationRef**)memory_mutations, memory_mutation_count);
	}
};

struct GranuleFilePointerRefV1 : public native::FDBBGFilePointerV1 {
	GranuleFilePointerRefV1(const GranuleFilePointerRefV1&) = delete;
	BytesRef filename() const noexcept { return BytesRef(filename_ptr, filename_length); }
};

struct GranuleDescriptionRefV1 : native::FDBBGFileDescriptionV1 {
	GranuleDescriptionRefV1(const GranuleDescriptionRefV1&) = delete;
	const KeyRangeRef& keyRange() const noexcept { return (KeyRangeRef&)key_range; }
	KeyRef beginKey() const noexcept { return KeyRef(key_range.begin_key, key_range.begin_key_length); }
	KeyRef endKey() const noexcept { return KeyRef(key_range.end_key, key_range.end_key_length); }
	GranuleFilePointerRefV1* snapshotFile() const noexcept {
		return snapshot_present ? (GranuleFilePointerRefV1*)&snapshot_file_pointer : nullptr;
	}
	VectorRef<GranuleFilePointerRefV1> deltaFiles() const noexcept {
		return VectorRef<GranuleFilePointerRefV1>((GranuleFilePointerRefV1*)delta_files, delta_file_count);
	}
	VectorRef<GranuleMutationRef> memoryMutations() const noexcept {
		return VectorRef<GranuleMutationRef>((GranuleMutationRef*)memory_mutations, memory_mutation_count);
	}
};

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

// get charstring view from optional bytestring: e.g. std::optional<std::basic_string{_view}<uint8_t>>
template <template <class...> class StringLike, class Char>
CharsRef toCharsRef(const std::optional<StringLike<Char>>& s) noexcept {
	static_assert(sizeof(Char) == 1);
	if (s) {
		return CharsRef(reinterpret_cast<char const*>(s.value().data()), s.value().size());
	} else {
		return CharsRef("[not set]");
	}
}

[[maybe_unused]] constexpr const bool OverflowCheck = false;

inline int intSize(BytesRef b) {
	if constexpr (OverflowCheck) {
		if (b.size() > static_cast<size_t>(std::numeric_limits<int>::max()))
			throw std::overflow_error("byte strlen goes beyond int bounds");
	}
	return static_cast<int>(b.size());
}

template <template <class...> class StringLike, class Char>
StringLike<Char> strinc(const StringLike<Char>& s) {
	int index = -1;
	for (index = s.size() - 1; index >= 0; index--)
		if (static_cast<uint8_t>(s[index]) != 255)
			break;

	// Must not be called with a string that is empty or only consists of '\xff' bytes.
	assert(index >= 0);

	auto ret = s.substr(0, index + 1);
	ret.back()++;
	return ret;
}

class Error {
public:
	using CodeType = native::fdb_error_t;

	Error() noexcept : err(0) {}

	explicit Error(CodeType err) noexcept : err(err) {}

	char const* what() const noexcept { return native::fdb_get_error(err); }

	explicit operator bool() const noexcept { return err != 0; }

	bool is(CodeType other) const noexcept { return err == other; }

	CodeType code() const noexcept { return err; }

	bool retryable() const noexcept { return native::fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err) != 0; }

	bool maybeCommitted() const noexcept {
		return native::fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, err) != 0;
	}

	bool retryableNotCommitted() const noexcept {
		return native::fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, err) != 0;
	}

	static Error success() { return Error(); }

private:
	CodeType err;
};

[[noreturn]] inline void throwError(std::string_view preamble, Error err) {
	auto msg = std::string(preamble);
	msg.append(err.what());
	throw std::runtime_error(msg);
}

template <class DataT, class WrapperT>
class Result {
public:
	using DataType = DataT;
	using WrapperType = WrapperT;
	Result(DataType* r) {
		if (r) {
			resRef = std::shared_ptr<DataType>(r, destroy);
		}
	}
	Result() noexcept : Result(nullptr) {}
	Result(const Result&) noexcept = default;
	Result(Result&&) noexcept = default;
	Result& operator=(const Result&) noexcept = default;
	Result& operator=(Result&&) noexcept = default;

	bool valid() const noexcept { return resRef != nullptr; }

	explicit operator bool() const noexcept { return valid(); }

	DataType* data() const {
		assert(valid());
		return resRef.get();
	}

	native::FDBResult* getPtr() const { return (native::FDBResult*)resRef.get(); }

	static WrapperType create(DataType* r) {
		WrapperType res;
		((Result<DataType, WrapperType>&)res) = Result<DataType, WrapperType>(r);
		return res;
	}

private:
	std::shared_ptr<DataType> resRef;
	static void destroy(DataType* res) { fdb_result_destroy((native::FDBResult*)res); }
};

class ReadBlobGranulesDescriptionResult
  : public Result<native::FDBReadBGDescriptionResult, ReadBlobGranulesDescriptionResult> {
public:
	VectorRef<GranuleDescriptionRef*> descs() const noexcept {
		return VectorRef<GranuleDescriptionRef*>((GranuleDescriptionRef**)data()->desc_arr, data()->desc_count);
	}
};

using ReadBlobGranulesDescriptionResultV1 = VectorRef<GranuleDescriptionRefV1>;

class ReadRangeResult : public Result<native::FDBReadRangeResult, ReadRangeResult> {
public:
	using KeyValueRefArray = std::tuple<KeyValueRef const*, int, bool>;

	[[nodiscard]] Error getKeyValueArrayNothrow(KeyValueRefArray& out) const noexcept {
		auto out_more_native = native::fdb_bool_t{};
		auto& [out_kv, out_count, out_more] = out;
		auto err_raw = native::fdb_result_get_keyvalue_array(
		    getPtr(), reinterpret_cast<const native::FDBKeyValue**>(&out_kv), &out_count, &out_more_native);
		out_more = out_more_native != 0;
		return Error(err_raw);
	}

	KeyValueRefArray getKeyValueArray() const {
		auto ret = KeyValueRefArray{};
		if (auto err = getKeyValueArrayNothrow(ret))
			throwError("ERROR: result_get_keyvalue_array(): ", err);
		return ret;
	}
};

class ReadBGMutationsResult : public Result<native::FDBReadBGMutationsResult, ReadBGMutationsResult> {
public:
	[[nodiscard]] Error getGranuleMutationArrayNothrow(VectorRef<GranuleMutationRef>& out) const noexcept {
		const native::FDBBGMutation* out_mutations = nullptr;
		int out_count = 0;
		auto err_raw = native::fdb_result_get_bg_mutations_array(getPtr(), &out_mutations, &out_count);
		if (!err_raw) {
			out = VectorRef<GranuleMutationRef>((GranuleMutationRef*)out_mutations, out_count);
		}
		return Error(err_raw);
	}

	VectorRef<GranuleMutationRef> getGranuleMutationArray() const {
		VectorRef<GranuleMutationRef> ret;
		if (auto err = getGranuleMutationArrayNothrow(ret))
			throwError("ERROR: result_get_keyvalue_array(): ", err);
		return ret;
	}
};

/* Traits of value types held by ready futures.
   Holds type and value extraction function. */
namespace future_var {
struct None {
	struct Type {};
	static Error extract(native::FDBFuture*, Type&) noexcept { return Error(0); }
};
struct Bool {
	using Type = native::fdb_bool_t;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto err = native::fdb_future_get_bool(f, &out);
		return Error(err);
	}
};
struct Int64 {
	using Type = int64_t;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		return Error(native::fdb_future_get_int64(f, &out));
	}
};
struct Uint64 {
	using Type = uint64_t;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		return Error(native::fdb_future_get_uint64(f, &out));
	}
};
struct Double {
	using Type = double;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		return Error(native::fdb_future_get_double(f, &out));
	}
};
struct KeyRef {
	using Type = fdb::KeyRef;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		uint8_t const* out_key = nullptr;
		int out_key_length = 0;
		auto err = Error(native::fdb_future_get_key(f, &out_key, &out_key_length));
		out = fdb::KeyRef(out_key, out_key_length);
		return Error(err);
	}
};
struct ValueRef {
	using Type = std::optional<fdb::ValueRef>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto out_present = native::fdb_bool_t{};
		uint8_t const* out_value = nullptr;
		int out_value_length = 0;
		auto err = native::fdb_future_get_value(f, &out_present, &out_value, &out_value_length);
		out = out_present != 0 ? std::make_optional(fdb::ValueRef(out_value, out_value_length)) : std::nullopt;
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

struct KeyValueRefArray {
	using Type = std::tuple<KeyValueRef const*, int, bool>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_kv, out_count, out_more] = out;
		auto out_more_native = native::fdb_bool_t{};
		auto err = native::fdb_future_get_keyvalue_array(
		    f, reinterpret_cast<const native::FDBKeyValue**>(&out_kv), &out_count, &out_more_native);
		out_more = (out_more_native != 0);
		return Error(err);
	}
};

struct KeyRangeRefArray {
	using Type = std::tuple<KeyRangeRef const*, int>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_ranges, out_count] = out;
		auto err = native::fdb_future_get_keyrange_array(
		    f, reinterpret_cast<const native::FDBKeyRange**>(&out_ranges), &out_count);
		return Error(err);
	}
};

struct MappedKeyValueRefArray {
	using Type = std::tuple<MappedKeyValueRef const*, int, bool>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto out_more_native = native::fdb_bool_t{};
		auto& [out_ranges, out_count, out_more] = out;
		auto err = native::fdb_future_get_mappedkeyvalue_array(
		    f, reinterpret_cast<const native::FDBMappedKeyValue**>(&out_ranges), &out_count, &out_more_native);
		out_more = out_more_native != 0;
		return Error(err);
	}
};

struct GranuleSummaryRef : native::FDBGranuleSummary {
	fdb::KeyRef beginKey() const noexcept {
		return fdb::KeyRef(native::FDBGranuleSummary::key_range.begin_key,
		                   native::FDBGranuleSummary::key_range.begin_key_length);
	}
	fdb::KeyRef endKey() const noexcept {
		return fdb::KeyRef(native::FDBGranuleSummary::key_range.end_key,
		                   native::FDBGranuleSummary::key_range.end_key_length);
	}
};

struct GranuleSummaryRefArray {
	using Type = std::tuple<GranuleSummaryRef const*, int>;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		auto& [out_summaries, out_count] = out;
		auto err = native::fdb_future_get_granule_summary_array(
		    f, reinterpret_cast<const native::FDBGranuleSummary**>(&out_summaries), &out_count);
		return Error(err);
	}
};

struct ReadBlobGranulesDescriptionResultV1 {
	using Type = fdb::ReadBlobGranulesDescriptionResultV1;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		native::FDBBGFileDescriptionV1* descs;
		int desc_cnt;
		auto err = native::fdb_future_readbg_get_descriptions_v1(f, &descs, &desc_cnt);
		out = Type((GranuleDescriptionRefV1*)descs, desc_cnt);
		return Error(err);
	}
};

template <class ResType>
struct Result {
	using Type = ResType;
	static Error extract(native::FDBFuture* f, Type& out) noexcept {
		typename ResType::DataType* res;
		auto err = native::fdb_future_get_result(f, (native::FDBResult**)&res);
		if (!err) {
			out = ResType::create(res);
		}
		return Error(err);
	}
};

using ReadBlobGranulesDescriptionResult = Result<fdb::ReadBlobGranulesDescriptionResult>;

} // namespace future_var

inline int maxApiVersion() {
	return native::fdb_get_max_api_version();
}

namespace network {

[[nodiscard]] inline Error setOptionNothrow(FDBNetworkOption option, BytesRef str) noexcept {
	return Error(native::fdb_network_set_option(option, str.data(), intSize(str)));
}

[[nodiscard]] inline Error setOptionNothrow(FDBNetworkOption option, CharsRef str) noexcept {
	return setOptionNothrow(option, toBytesRef(str));
}

[[nodiscard]] inline Error setOptionNothrow(FDBNetworkOption option, int64_t value) noexcept {
	return Error(native::fdb_network_set_option(
	    option, reinterpret_cast<const uint8_t*>(&value), static_cast<int>(sizeof(value))));
}

[[nodiscard]] inline Error setOptionNothrow(FDBNetworkOption option) noexcept {
	return setOptionNothrow(option, "");
}

inline void setOption(FDBNetworkOption option, BytesRef str) {
	if (auto err = setOptionNothrow(option, str)) {
		throwError(fmt::format("ERROR: fdb_network_set_option({}): ",
		                       static_cast<std::underlying_type_t<FDBNetworkOption>>(option)),
		           err);
	}
}

inline void setOption(FDBNetworkOption option, CharsRef str) {
	setOption(option, toBytesRef(str));
}

inline void setOption(FDBNetworkOption option, int64_t value) {
	if (auto err = setOptionNothrow(option, value)) {
		throwError(fmt::format("ERROR: fdb_network_set_option({}, {}): ",
		                       static_cast<std::underlying_type_t<FDBNetworkOption>>(option),
		                       value),
		           err);
	}
}

inline void setOption(FDBNetworkOption option) {
	setOption(option, "");
}

[[nodiscard]] inline Error setupNothrow() noexcept {
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

class Future {
protected:
	friend class Transaction;
	friend class Database;
	friend std::hash<Future>;
	std::shared_ptr<native::FDBFuture> f;

	Future(native::FDBFuture* future) {
		if (future)
			f = std::shared_ptr<native::FDBFuture>(future, &native::fdb_future_destroy);
	}

	native::FDBFuture* nativeHandle() const noexcept { return f.get(); }

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
	Future(Future&&) noexcept = default;
	Future& operator=(const Future&) noexcept = default;
	Future& operator=(Future&&) noexcept = default;

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
	void releaseMemory() noexcept { native::fdb_future_release_memory(f.get()); }

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
	[[nodiscard]] Error getNothrow(typename VarTraits::Type& var) const noexcept {
		assert(valid());
		return VarTraits::extract(f.get(), var);
	}

	template <class UserFunc>
	void then(UserFunc&& fn) {
		then<Future>(std::forward<UserFunc>(fn));
	}

	bool operator==(const Future& other) const { return nativeHandle() == other.nativeHandle(); }
	bool operator!=(const Future& other) const { return !(*this == other); }
};

template <typename VarTraits>
class TypedFuture : public Future {
	friend class Future;
	friend class Transaction;
	friend class Tenant;
	using SelfType = TypedFuture<VarTraits>;
	using Future::Future;
	// hide type-unsafe inherited functions
	using Future::get;
	using Future::getNothrow;
	using Future::then;
	TypedFuture(Future&& f) noexcept : Future(f) {}
	TypedFuture(const Future& f) noexcept : Future(f) {}

public:
	using ContainedType = typename VarTraits::Type;

	Future eraseType() const noexcept { return static_cast<Future const&>(*this); }

	ContainedType get() const { return get<VarTraits>(); }

	[[nodiscard]] Error getNothrow(ContainedType& out) const noexcept { return getNothrow<VarTraits>(out); }

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
	friend class Tenant;
	std::shared_ptr<native::FDBTransaction> tr;

	explicit Transaction(native::FDBTransaction* tr_raw) {
		if (tr_raw)
			tr = std::shared_ptr<native::FDBTransaction>(tr_raw, &native::fdb_transaction_destroy);
	}

public:
	Transaction() noexcept : Transaction(nullptr) {}
	Transaction(const Transaction&) noexcept = default;
	Transaction& operator=(const Transaction&) noexcept = default;

	void atomic_store(Transaction other) { std::atomic_store(&tr, other.tr); }

	Transaction atomic_load() {
		Transaction retVal;
		retVal.tr = std::atomic_load(&tr);
		return retVal;
	}

	bool valid() const noexcept { return tr != nullptr; }

	explicit operator bool() const noexcept { return valid(); }

	[[nodiscard]] Error setOptionNothrow(FDBTransactionOption option, int64_t value) noexcept {
		return Error(native::fdb_transaction_set_option(
		    tr.get(), option, reinterpret_cast<const uint8_t*>(&value), static_cast<int>(sizeof(value))));
	}

	[[nodiscard]] Error setOptionNothrow(FDBTransactionOption option, BytesRef str) noexcept {
		return Error(native::fdb_transaction_set_option(tr.get(), option, str.data(), intSize(str)));
	}

	[[nodiscard]] Error setOptionNothrow(FDBTransactionOption option, CharsRef str) noexcept {
		return setOptionNothrow(option, toBytesRef(str));
	}

	[[nodiscard]] Error setOptionNothrow(FDBTransactionOption option) noexcept { return setOptionNothrow(option, ""); }

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

	void setOption(FDBTransactionOption option, CharsRef str) { setOption(option, toBytesRef(str)); }

	void setOption(FDBTransactionOption option) { setOption(option, ""); }

	TypedFuture<future_var::Int64> getReadVersion() { return native::fdb_transaction_get_read_version(tr.get()); }

	void setReadVersion(int64_t version) { native::fdb_transaction_set_read_version(tr.get(), version); }

	[[nodiscard]] Error getCommittedVersionNothrow(int64_t& out) {
		return Error(native::fdb_transaction_get_committed_version(tr.get(), &out));
	}

	int64_t getCommittedVersion() {
		auto out = int64_t{};
		if (auto err = getCommittedVersionNothrow(out)) {
			throwError("get_committed_version: ", err);
		}
		return out;
	}

	TypedFuture<future_var::KeyRef> getVersionstamp() { return native::fdb_transaction_get_versionstamp(tr.get()); }

	TypedFuture<future_var::KeyRef> getKey(KeySelector sel, bool snapshot) {
		return native::fdb_transaction_get_key(tr.get(), sel.key, sel.keyLength, sel.orEqual, sel.offset, snapshot);
	}

	TypedFuture<future_var::ValueRef> get(KeyRef key, bool snapshot) {
		return native::fdb_transaction_get(tr.get(), key.data(), intSize(key), snapshot);
	}

	// Usage: tx.getRange(key_select::firstGreaterOrEqual(firstKey), key_select::lastLessThan(lastKey), ...)
	// gets key-value pairs in key range [begin, end)
	TypedFuture<future_var::KeyValueRefArray> getRange(KeySelector first,
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

	TypedFuture<future_var::MappedKeyValueRefArray> getMappedRange(KeySelector first,
	                                                               KeySelector last,
	                                                               KeyRef mapperName,
	                                                               int limit,
	                                                               int targetBytes,
	                                                               FDBStreamingMode mode,
	                                                               int iteration,
	                                                               int matchIndex,
	                                                               bool snapshot,
	                                                               bool reverse) {
		return native::fdb_transaction_get_mapped_range(tr.get(),
		                                                first.key,
		                                                first.keyLength,
		                                                first.orEqual,
		                                                first.offset,
		                                                last.key,
		                                                last.keyLength,
		                                                last.orEqual,
		                                                last.offset,
		                                                mapperName.data(),
		                                                intSize(mapperName),
		                                                limit,
		                                                targetBytes,
		                                                mode,
		                                                iteration,
		                                                matchIndex,
		                                                snapshot,
		                                                reverse);
	}

	TypedFuture<future_var::StringArray> getAddressForKey(KeyRef key) {
		return native::fdb_transaction_get_addresses_for_key(tr.get(), key.data(), key.size());
	}

	TypedFuture<future_var::KeyRangeRefArray> getBlobGranuleRanges(KeyRef begin, KeyRef end, int rangeLimit) {
		return native::fdb_transaction_get_blob_granule_ranges(
		    tr.get(), begin.data(), intSize(begin), end.data(), intSize(end), rangeLimit);
	}

	ReadRangeResult readBlobGranules(KeyRef begin,
	                                 KeyRef end,
	                                 int64_t begin_version,
	                                 int64_t read_version,
	                                 native::FDBReadBlobGranuleContext context) {
		return ReadRangeResult::create((native::FDBReadRangeResult*)native::fdb_transaction_read_blob_granules(
		    tr.get(), begin.data(), intSize(begin), end.data(), intSize(end), begin_version, read_version, context));
	}

	TypedFuture<future_var::GranuleSummaryRefArray> summarizeBlobGranules(KeyRef begin,
	                                                                      KeyRef end,
	                                                                      int64_t summaryVersion,
	                                                                      int rangeLimit) {
		return native::fdb_transaction_summarize_blob_granules(
		    tr.get(), begin.data(), intSize(begin), end.data(), intSize(end), summaryVersion, rangeLimit);
	}

	TypedFuture<future_var::None> watch(KeyRef key) {
		return native::fdb_transaction_watch(tr.get(), key.data(), intSize(key));
	}

	TypedFuture<future_var::ReadBlobGranulesDescriptionResult> readBlobGranulesDescription(KeyRef begin,
	                                                                                       KeyRef end,
	                                                                                       int64_t beginVersion,
	                                                                                       int64_t readVersion) {
		return native::fdb_transaction_read_blob_granules_description_v2(
		    tr.get(), begin.data(), intSize(begin), end.data(), intSize(end), beginVersion, readVersion);
	}

	TypedFuture<future_var::ReadBlobGranulesDescriptionResultV1> readBlobGranulesDescriptionV1(
	    KeyRef begin,
	    KeyRef end,
	    int64_t beginVersion,
	    int64_t readVersion,
	    int64_t* readVersionOut) {
		return native::fdb_transaction_read_blob_granules_description_v1(tr.get(),
		                                                                 begin.data(),
		                                                                 intSize(begin),
		                                                                 end.data(),
		                                                                 intSize(end),
		                                                                 beginVersion,
		                                                                 readVersion,
		                                                                 readVersionOut);
	}

	ReadRangeResult parseSnapshotFile(BytesRef fileData,
	                                  native::FDBBGTenantPrefix const* tenantPrefix,
	                                  native::FDBBGEncryptionCtxV2 const* encryptionCtx) {
		return ReadRangeResult::create((native::FDBReadRangeResult*)native::fdb_readbg_parse_snapshot_file_v2(
		    fileData.data(), intSize(fileData), tenantPrefix, encryptionCtx));
	}

	ReadRangeResult parseSnapshotFileV1(BytesRef fileData,
	                                    native::FDBBGTenantPrefix const* tenantPrefix,
	                                    native::FDBBGEncryptionCtxV1 const* encryptionCtx) {
		return ReadRangeResult::create((native::FDBReadRangeResult*)native::fdb_readbg_parse_snapshot_file_v1(
		    fileData.data(), intSize(fileData), tenantPrefix, encryptionCtx));
	}

	ReadBGMutationsResult parseDeltaFile(BytesRef fileData,
	                                     native::FDBBGTenantPrefix const* tenantPrefix,
	                                     native::FDBBGEncryptionCtxV2 const* encryptionCtx) {
		return ReadBGMutationsResult::create((native::FDBReadBGMutationsResult*)native::fdb_readbg_parse_delta_file_v2(
		    fileData.data(), intSize(fileData), tenantPrefix, encryptionCtx));
	}

	ReadBGMutationsResult parseDeltaFileV1(BytesRef fileData,
	                                       native::FDBBGTenantPrefix const* tenantPrefix,
	                                       native::FDBBGEncryptionCtxV1 const* encryptionCtx) {
		return ReadBGMutationsResult::create((native::FDBReadBGMutationsResult*)native::fdb_readbg_parse_delta_file_v1(
		    fileData.data(), intSize(fileData), tenantPrefix, encryptionCtx));
	}

	TypedFuture<future_var::None> commit() { return native::fdb_transaction_commit(tr.get()); }

	TypedFuture<future_var::None> onError(Error err) { return native::fdb_transaction_on_error(tr.get(), err.code()); }

	TypedFuture<future_var::Int64> getApproximateSize() const {
		return native::fdb_transaction_get_approximate_size(tr.get());
	}

	TypedFuture<future_var::Int64> getTotalCost() const { return native::fdb_transaction_get_total_cost(tr.get()); }

	TypedFuture<future_var::Double> getTagThrottledDuration() const {
		return native::fdb_transaction_get_tag_throttled_duration(tr.get());
	}

	void reset() { return native::fdb_transaction_reset(tr.get()); }

	void cancel() { return native::fdb_transaction_cancel(tr.get()); }

	void set(KeyRef key, ValueRef value) {
		native::fdb_transaction_set(tr.get(), key.data(), intSize(key), value.data(), intSize(value));
	}

	void atomicOp(KeyRef key, ValueRef param, FDBMutationType operationType) {
		native::fdb_transaction_atomic_op(
		    tr.get(), key.data(), intSize(key), param.data(), intSize(param), operationType);
	}

	void clear(KeyRef key) { native::fdb_transaction_clear(tr.get(), key.data(), intSize(key)); }

	void clearRange(KeyRef begin, KeyRef end) {
		native::fdb_transaction_clear_range(tr.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	void addConflictRange(KeyRef begin, KeyRef end, FDBConflictRangeType rangeType) {
		if (auto err = Error(native::fdb_transaction_add_conflict_range(
		        tr.get(), begin.data(), intSize(begin), end.data(), intSize(end), rangeType))) {
			throwError("fdb_transaction_add_conflict_range returned error: ", err);
		}
	}

	void addReadConflictRange(KeyRef begin, KeyRef end) { addConflictRange(begin, end, FDB_CONFLICT_RANGE_TYPE_READ); }

	void addWriteConflictRange(KeyRef begin, KeyRef end) {
		addConflictRange(begin, end, FDB_CONFLICT_RANGE_TYPE_WRITE);
	}
};

// Handle this as an abstract class instead of interface to preserve lifetime of fdb objects owned by Tenant and
// Database.
class IDatabaseOps {
public:
	virtual ~IDatabaseOps() = default;

	virtual Transaction createTransaction() = 0;

	virtual TypedFuture<future_var::Bool> blobbifyRange(KeyRef begin, KeyRef end) = 0;
	virtual TypedFuture<future_var::Bool> blobbifyRangeBlocking(KeyRef begin, KeyRef end) = 0;
	virtual TypedFuture<future_var::Bool> unblobbifyRange(KeyRef begin, KeyRef end) = 0;
	virtual TypedFuture<future_var::KeyRangeRefArray> listBlobbifiedRanges(KeyRef begin,
	                                                                       KeyRef end,
	                                                                       int rangeLimit) = 0;
	virtual TypedFuture<future_var::Int64> verifyBlobRange(KeyRef begin, KeyRef end, int64_t version) = 0;
	virtual TypedFuture<future_var::Bool> flushBlobRange(KeyRef begin, KeyRef end, bool compact, int64_t version) = 0;
	virtual TypedFuture<future_var::KeyRef> purgeBlobGranules(KeyRef begin,
	                                                          KeyRef end,
	                                                          int64_t version,
	                                                          bool force) = 0;
	virtual TypedFuture<future_var::None> waitPurgeGranulesComplete(KeyRef purgeKey) = 0;
};

class Tenant final : public IDatabaseOps {
	friend class Database;
	std::shared_ptr<native::FDBTenant> tenant;

	explicit Tenant(native::FDBTenant* tenant_raw) {
		if (tenant_raw)
			tenant = std::shared_ptr<native::FDBTenant>(tenant_raw, &native::fdb_tenant_destroy);
	}

public:
	// This should only be mutated by API versioning
	static inline CharsRef tenantManagementMapPrefix = "\xff\xff/management/tenant/map/";

	Tenant(const Tenant&) noexcept = default;
	Tenant& operator=(const Tenant&) noexcept = default;
	Tenant() noexcept : tenant(nullptr) {}

	void atomic_store(Tenant other) { std::atomic_store(&tenant, other.tenant); }

	Tenant atomic_load() {
		Tenant retVal;
		retVal.tenant = std::atomic_load(&tenant);
		return retVal;
	}

	static void createTenant(Transaction tr, BytesRef name) {
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES, BytesRef());
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_LOCK_AWARE, BytesRef());
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_RAW_ACCESS, BytesRef());
		tr.set(toBytesRef(fmt::format("{}{}", tenantManagementMapPrefix, toCharsRef(name))), BytesRef());
	}

	static void deleteTenant(Transaction tr, BytesRef name) {
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES, BytesRef());
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_RAW_ACCESS, BytesRef());
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_LOCK_AWARE, BytesRef());
		tr.clear(toBytesRef(fmt::format("{}{}", tenantManagementMapPrefix, toCharsRef(name))));
	}

	static TypedFuture<future_var::ValueRef> getTenant(Transaction tr, BytesRef name) {
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_READ_SYSTEM_KEYS, BytesRef());
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_LOCK_AWARE, BytesRef());
		tr.setOption(FDBTransactionOption::FDB_TR_OPTION_RAW_ACCESS, BytesRef());
		return tr.get(toBytesRef(fmt::format("{}{}", tenantManagementMapPrefix, toCharsRef(name))), false);
	}

	Transaction createTransaction() override {
		auto tx_native = static_cast<native::FDBTransaction*>(nullptr);
		auto err = Error(native::fdb_tenant_create_transaction(tenant.get(), &tx_native));
		if (err)
			throwError("Failed to create transaction: ", err);
		return Transaction(tx_native);
	}

	TypedFuture<future_var::Bool> blobbifyRange(KeyRef begin, KeyRef end) override {
		if (!tenant)
			throw std::runtime_error("blobbifyRange() from null tenant");
		return native::fdb_tenant_blobbify_range(tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	TypedFuture<future_var::Bool> blobbifyRangeBlocking(KeyRef begin, KeyRef end) override {
		if (!tenant)
			throw std::runtime_error("blobbifyRangeBlocking() from null tenant");
		return native::fdb_tenant_blobbify_range_blocking(
		    tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	TypedFuture<future_var::Bool> unblobbifyRange(KeyRef begin, KeyRef end) override {
		if (!tenant)
			throw std::runtime_error("unblobbifyRange() from null tenant");
		return native::fdb_tenant_unblobbify_range(
		    tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	TypedFuture<future_var::KeyRangeRefArray> listBlobbifiedRanges(KeyRef begin, KeyRef end, int rangeLimit) override {
		if (!tenant)
			throw std::runtime_error("listBlobbifiedRanges() from null tenant");
		return native::fdb_tenant_list_blobbified_ranges(
		    tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end), rangeLimit);
	}

	TypedFuture<future_var::Int64> verifyBlobRange(KeyRef begin, KeyRef end, int64_t version) override {
		if (!tenant)
			throw std::runtime_error("verifyBlobRange() from null tenant");
		return native::fdb_tenant_verify_blob_range(
		    tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end), version);
	}

	TypedFuture<future_var::Bool> flushBlobRange(KeyRef begin, KeyRef end, bool compact, int64_t version) override {
		if (!tenant)
			throw std::runtime_error("flushBlobRange() from null tenant");
		return native::fdb_tenant_flush_blob_range(
		    tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end), compact, version);
	}

	TypedFuture<future_var::Int64> getId() {
		if (!tenant)
			throw std::runtime_error("getId() from null tenant");
		return native::fdb_tenant_get_id(tenant.get());
	}

	TypedFuture<future_var::KeyRef> purgeBlobGranules(KeyRef begin, KeyRef end, int64_t version, bool force) override {
		if (!tenant)
			throw std::runtime_error("purgeBlobGranules() from null tenant");
		native::fdb_bool_t forceBool = force;
		return native::fdb_tenant_purge_blob_granules(
		    tenant.get(), begin.data(), intSize(begin), end.data(), intSize(end), version, forceBool);
	}

	TypedFuture<future_var::None> waitPurgeGranulesComplete(KeyRef purgeKey) override {
		if (!tenant)
			throw std::runtime_error("waitPurgeGranulesComplete() from null tenant");
		return native::fdb_tenant_wait_purge_granules_complete(tenant.get(), purgeKey.data(), intSize(purgeKey));
	}
};

class Database : public IDatabaseOps {
	friend class Tenant;
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

	bool valid() const noexcept { return db != nullptr; }

	void atomic_store(Database other) { std::atomic_store(&db, other.db); }

	Database atomic_load() {
		Database retVal;
		retVal.db = std::atomic_load(&db);
		return retVal;
	}

	[[nodiscard]] Error setOptionNothrow(FDBDatabaseOption option, int64_t value) noexcept {
		return Error(native::fdb_database_set_option(
		    db.get(), option, reinterpret_cast<const uint8_t*>(&value), static_cast<int>(sizeof(value))));
	}

	[[nodiscard]] Error setOptionNothrow(FDBDatabaseOption option, BytesRef str) noexcept {
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

	Tenant openTenant(BytesRef name) {
		if (!db)
			throw std::runtime_error("openTenant from null database");
		auto tenant_native = static_cast<native::FDBTenant*>(nullptr);
		if (auto err = Error(native::fdb_database_open_tenant(db.get(), name.data(), name.size(), &tenant_native))) {
			throwError(fmt::format("Failed to open tenant with name '{}': ", toCharsRef(name)), err);
		}
		return Tenant(tenant_native);
	}

	Transaction createTransaction() override {
		if (!db)
			throw std::runtime_error("create_transaction from null database");
		auto tx_native = static_cast<native::FDBTransaction*>(nullptr);
		auto err = Error(native::fdb_database_create_transaction(db.get(), &tx_native));
		if (err)
			throwError("Failed to create transaction: ", err);
		return Transaction(tx_native);
	}

	double getMainThreadBusyness() const noexcept { return native::fdb_database_get_main_thread_busyness(db.get()); }

	TypedFuture<future_var::KeyRangeRefArray> listBlobbifiedRanges(KeyRef begin, KeyRef end, int rangeLimit) override {
		if (!db)
			throw std::runtime_error("listBlobbifiedRanges from null database");
		return native::fdb_database_list_blobbified_ranges(
		    db.get(), begin.data(), intSize(begin), end.data(), intSize(end), rangeLimit);
	}

	TypedFuture<future_var::Int64> verifyBlobRange(KeyRef begin, KeyRef end, int64_t version) override {
		if (!db)
			throw std::runtime_error("verifyBlobRange from null database");
		return native::fdb_database_verify_blob_range(
		    db.get(), begin.data(), intSize(begin), end.data(), intSize(end), version);
	}

	TypedFuture<future_var::Bool> flushBlobRange(KeyRef begin, KeyRef end, bool compact, int64_t version) override {
		if (!db)
			throw std::runtime_error("flushBlobRange from null database");
		return native::fdb_database_flush_blob_range(
		    db.get(), begin.data(), intSize(begin), end.data(), intSize(end), compact, version);
	}

	TypedFuture<future_var::Bool> blobbifyRange(KeyRef begin, KeyRef end) override {
		if (!db)
			throw std::runtime_error("blobbifyRange from null database");
		return native::fdb_database_blobbify_range(db.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	TypedFuture<future_var::Bool> blobbifyRangeBlocking(KeyRef begin, KeyRef end) override {
		if (!db)
			throw std::runtime_error("blobbifyRangeBlocking from null database");
		return native::fdb_database_blobbify_range_blocking(
		    db.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	TypedFuture<future_var::Bool> unblobbifyRange(KeyRef begin, KeyRef end) override {
		if (!db)
			throw std::runtime_error("unblobbifyRange from null database");
		return native::fdb_database_unblobbify_range(db.get(), begin.data(), intSize(begin), end.data(), intSize(end));
	}

	TypedFuture<future_var::KeyRef> purgeBlobGranules(KeyRef begin, KeyRef end, int64_t version, bool force) override {
		if (!db)
			throw std::runtime_error("purgeBlobGranules from null database");
		native::fdb_bool_t forceBool = force;
		return native::fdb_database_purge_blob_granules(
		    db.get(), begin.data(), intSize(begin), end.data(), intSize(end), version, forceBool);
	}

	TypedFuture<future_var::None> waitPurgeGranulesComplete(KeyRef purgeKey) override {
		if (!db)
			throw std::runtime_error("purgeBlobGranules from null database");
		return native::fdb_database_wait_purge_granules_complete(db.get(), purgeKey.data(), intSize(purgeKey));
	}

	TypedFuture<future_var::KeyRef> getClientStatus() { return native::fdb_database_get_client_status(db.get()); }

	TypedFuture<future_var::None> createSnapshot(ValueRef uid, ValueRef snapCommand) {
		if (!db) {
			throw std::runtime_error("createSnapshot from null database");
		}
		return native::fdb_database_create_snapshot(
		    db.get(), uid.data(), intSize(uid), snapCommand.data(), intSize(snapCommand));
	}

	TypedFuture<future_var::None> forceRecoveryWithDataLoss(ValueRef dcid) {
		if (!db) {
			throw std::runtime_error("forceRecoveryWithDataLoss from null database");
		}
		return native::fdb_database_force_recovery_with_data_loss(db.get(), dcid.data(), intSize(dcid));
	}

	TypedFuture<future_var::Int64> rebootWorker(ValueRef address, bool check, int duration) {
		if (!db) {
			throw std::runtime_error("rebootWorker from null database");
		}
		return native::fdb_database_reboot_worker(db.get(), address.data(), intSize(address), check, duration);
	}

	TypedFuture<future_var::Uint64> getServerProtocol(uint64_t expected_version) const {
		if (!db) {
			throw std::runtime_error("getServerProtocol from null database");
		}
		return native::fdb_database_get_server_protocol(db.get(), expected_version);
	}
};

[[nodiscard]] inline Error selectApiVersionNothrow(int version) {
	if (version < FDB_API_VERSION_TENANT_API_RELEASED) {
		Tenant::tenantManagementMapPrefix = "\xff\xff/management/tenant_map/";
	}
	return Error(native::fdb_select_api_version(version));
}

inline void selectApiVersion(int version) {
	if (auto err = selectApiVersionNothrow(version)) {
		throwError(fmt::format("ERROR: fdb_select_api_version({}): ", version), err);
	}
}

[[nodiscard]] inline Error selectApiVersionCappedNothrow(int version) {
	if (version < FDB_API_VERSION_TENANT_API_RELEASED) {
		Tenant::tenantManagementMapPrefix = "\xff\xff/management/tenant_map/";
	}
	return Error(
	    native::fdb_select_api_version_impl(version, std::min(native::fdb_get_max_api_version(), FDB_API_VERSION)));
}

inline void selectApiVersionCapped(int version) {
	if (auto err = selectApiVersionCappedNothrow(version)) {
		throwError(fmt::format("ERROR: fdb_select_api_version_capped({}): ", version), err);
	}
}

} // namespace fdb

template <>
struct std::hash<fdb::Future> {
	size_t operator()(const fdb::Future& f) const { return std::hash<fdb::native::FDBFuture*>{}(f.nativeHandle()); }
};

// Trivially construct a BytesRef out of a string literal
inline fdb::BytesRef operator""_br(const char* str, size_t len) {
	return fdb::BytesRef(reinterpret_cast<const uint8_t*>(str), len);
}

#endif /*FDB_API_HPP*/
