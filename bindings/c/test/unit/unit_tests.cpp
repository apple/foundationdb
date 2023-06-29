/*
 * unit_tests.cpp
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

// Unit tests for the FoundationDB C API.

#include "fdb_c_options.g.h"
#define FDB_USE_LATEST_API_VERSION
#include <assert.h>
#include <string.h>

#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>
#include <random>
#include <chrono>

#define DOCTEST_CONFIG_IMPLEMENT
#include <rapidjson/document.h>
#include "doctest.h"
#include "fdbclient/Tracing.h"
#include "fdbclient/Tuple.h"

#include "flow/config.h"
#include "flow/DeterministicRandom.h"
#include "flow/IRandom.h"

#include "test/fdb_api.hpp"

void fdbCheck(const fdb::Error& err) {
	if (err) {
		std::cerr << err.what() << std::endl;
		std::abort();
	}
}

fdb::Error waitFuture(fdb::Future& f) {
	fdbCheck(f.blockUntilReady());
	return f.error();
}

static fdb::Database db;
static std::string prefix;
static std::string clusterFilePath = "";

std::string key(const std::string& key) {
	return prefix + key;
}

// Given a string s, returns the "lowest" string greater than any string that
// starts with s. Taken from
// https://github.com/apple/foundationdb/blob/e7d72f458c6a985fdfa677ae021f357d6f49945b/flow/flow.cpp#L223.
std::string strinc_str(const std::string& s) {
	int index = -1;
	for (index = s.size() - 1; index >= 0; --index) {
		if ((uint8_t)s[index] != 255) {
			break;
		}
	}

	assert(index >= 0);

	std::string r = s.substr(0, index + 1);
	char* p = r.data();
	p[r.size() - 1]++;
	return r;
}

TEST_CASE("strinc_str") {
	CHECK(strinc_str("a").compare("b") == 0);
	CHECK(strinc_str("y").compare("z") == 0);
	CHECK(strinc_str("!").compare("\"") == 0);
	CHECK(strinc_str("*").compare("+") == 0);
	CHECK(strinc_str("fdb").compare("fdc") == 0);
	CHECK(strinc_str("foundation database 6").compare("foundation database 7") == 0);

	char terminated[] = { 'a', 'b', '\xff' };
	CHECK(strinc_str(std::string(terminated, 3)).compare("ac") == 0);
}

// Helper function to add `prefix` to all keys in the given map. Returns a new
// map.
std::map<std::string, std::string> create_data(std::map<std::string, std::string>&& map) {
	std::map<std::string, std::string> out;
	for (const auto& [key, val] : map) {
		out[prefix + key] = val;
	}
	return out;
}

// Clears all data in the database, then inserts the given key value pairs.
void insert_data(fdb::Database db, const std::map<std::string, std::string>& data) {
	auto tr = db.createTransaction();
	auto end_key = strinc_str(prefix);
	while (1) {
		tr.clearRange(fdb::toBytesRef(prefix), fdb::toBytesRef(end_key));
		for (const auto& [key, val] : data) {
			tr.set(fdb::toBytesRef(key), fdb::toBytesRef(val));
		}

		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}
}

// Get the value associated with `key_name` from the database. Accepts a list
// of transaction options to apply (values for options not supported). Returns
// an optional which will be populated with the result if one was found.
std::optional<std::string> getValue(fdb::KeyRef key, bool snapshot, std::vector<FDBTransactionOption> options) {
	auto tr = db.createTransaction();
	while (1) {
		for (auto& option : options) {
			tr.setOption(option);
		}
		auto f1 = tr.get(key, snapshot);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		return value.has_value() ? std::make_optional(std::string(value->begin(), value->end())) : std::nullopt;
	}
}

struct GetMappedRangeResult {
	struct MappedKV {
		MappedKV(const std::string& key,
		         const std::string& value,
		         const std::string& begin,
		         const std::string& end,
		         const std::vector<std::pair<std::string, std::string>>& range_results)
		  : key(key), value(value), begin(begin), end(end), range_results(range_results) {}

		std::string key;
		std::string value;
		std::string begin;
		std::string end;
		std::vector<std::pair<std::string, std::string>> range_results;
	};
	std::vector<MappedKV> mkvs;
	// True if values remain in the key range requested.
	bool more;
	// Set to a non-zero value if an error occurred during the transaction.
	fdb::Error err;
};

static inline std::string extractString(fdb::native::FDBKey key) {
	return std::string((const char*)key.key, key.key_length);
}

GetMappedRangeResult getMappedRange(fdb::Transaction& tr,
                                    fdb::KeySelector begin,
                                    fdb::KeySelector end,
                                    fdb::KeyRef mapperName,
                                    int limit,
                                    int target_bytes,
                                    FDBStreamingMode mode,
                                    int iteration,
                                    int matchIndex,
                                    bool snapshot,
                                    bool reverse) {
	auto f1 =
	    tr.getMappedRange(begin, end, mapperName, limit, target_bytes, mode, iteration, matchIndex, snapshot, reverse);
	auto err = waitFuture(f1);
	if (err) {
		return GetMappedRangeResult{ {}, false, err };
	}

	auto [outMkv, outCount, outMore] = f1.get();

	GetMappedRangeResult result;
	result.more = (outMore != 0);
	result.err = fdb::Error::success();

	//	std::cout << "out_count:" << out_count << " out_more:" << out_more << " out_mkv:" << (void*)out_mkv <<
	// std::endl;

	for (int i = 0; i < outCount; ++i) {
		fdb::native::FDBMappedKeyValue mkv = outMkv[i];
		auto key = extractString(mkv.key);
		auto value = extractString(mkv.value);
		auto begin = extractString(mkv.getRange.begin.key);
		auto end = extractString(mkv.getRange.end.key);
		//		std::cout << "key:" << key << " value:" << value << " begin:" << begin << " end:" << end << std::endl;

		std::vector<std::pair<std::string, std::string>> range_results;
		for (int i = 0; i < mkv.getRange.m_size; ++i) {
			const auto& kv = mkv.getRange.data[i];
			std::string k((const char*)kv.key, kv.key_length);
			std::string v((const char*)kv.value, kv.value_length);
			range_results.emplace_back(k, v);
			// std::cout << "[" << i << "]" << k << " -> " << v << std::endl;
		}
		result.mkvs.emplace_back(key, value, begin, end, range_results);
	}
	return result;
}

// Clears all data in the database.
void clear_data(fdb::Database db) {
	insert_data(db, {});
}

struct FdbEvent {
	void wait() {
		std::unique_lock<std::mutex> l(mutex);
		cv.wait(l, [this]() { return this->complete; });
	}
	void set() {
		std::unique_lock<std::mutex> l(mutex);
		complete = true;
		cv.notify_all();
	}

private:
	std::mutex mutex;
	std::condition_variable cv;
	bool complete = false;
};

TEST_CASE("fdb_future_set_callback") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, /*snapshot*/ true);

		struct Context {
			FdbEvent event;
		};
		Context context;
		f1.then([&context](fdb::Future f) { context.event.set(); });

		auto err = waitFuture(f1);
		context.event.wait(); // Wait until callback is called

		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		break;
	}
}

TEST_CASE("fdb_future_cancel after future completion") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		// Should have no effect
		f1.cancel();

		std::optional<fdb::ValueRef> val;
		fdbCheck(f1.getNothrow(val));
		break;
	}
}

TEST_CASE("fdb_future_is_ready") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(f1.ready());
		break;
	}
}

TEST_CASE("fdb_future_release_memory") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		// "After [fdb_future_release_memory] has been called the same number of
		// times as fdb_future_get_*(), further calls to fdb_future_get_*() will
		// return a future_released error".
		std::optional<fdb::ValueRef> value;
		fdbCheck(f1.getNothrow(value));
		fdbCheck(f1.getNothrow(value));

		f1.releaseMemory();
		fdbCheck(f1.getNothrow(value));
		f1.releaseMemory();
		f1.releaseMemory();
		err = f1.getNothrow(value);
		CHECK(err.code() == 1102); // future_released
		break;
	}
}

TEST_CASE("fdb_future_get_int64") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getReadVersion();

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		int64_t rv = f1.get();
		CHECK(rv > 0);
		break;
	}
}

TEST_CASE("fdb_future_get_key") {
	insert_data(db, create_data({ { "a", "1" }, { "baz", "2" }, { "bar", "3" } }));

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getKey(fdb::key_select::firstGreaterThan(fdb::toBytesRef(key("a"))),
		                    /* snapshot */ false);
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto dbKey = f1.get();
		CHECK(fdb::toCharsRef(dbKey) == prefix + "bar");
		break;
	}
}

TEST_CASE("fdb_future_get_value") {
	insert_data(db, create_data({ { "foo", "bar" } }));

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get(fdb::toBytesRef(key("foo")), /* snapshot */ false);
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto dbValue = f1.get();
		CHECK(fdb::toCharsRef(dbValue) == "bar");
		break;
	}
}

TEST_CASE("fdb_future_get_string_array") {
	insert_data(db, create_data({ { "foo", "bar" } }));

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getAddressForKey(fdb::toBytesRef(key("foo")));

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		auto strings = std::get<0>(output);
		auto count = std::get<1>(output);

		CHECK(count > 0);
		for (int i = 0; i < count; i++) {
			CHECK(strlen(strings[i]) > 0);
		}
		break;
	}
}

TEST_CASE("fdb_future_get_keyvalue_array") {
	auto data = create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	insert_data(db, data);

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(key("a"))),
		                      fdb::key_select::lastLessOrEqual(fdb::toBytesRef(key("c")), 1),
		                      /* limit */ 0,
		                      /* target_bytes */ 0,
		                      /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                      /* iteration */ 0,
		                      /* snapshot */ false,
		                      /* reverse */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		auto out_kv = std::get<0>(output);
		auto out_count = std::get<1>(output);
		auto out_more = std::get<2>(output);
		CHECK(out_count > 0);
		CHECK(out_count <= 3);
		if (out_count < 3) {
			CHECK(out_more);
		}

		for (int i = 0; i < out_count; ++i) {
			auto kv = *out_kv++;
			auto key = std::string(kv.key().begin(), kv.key().end());
		      	auto value = std::string(kv.value().begin(), kv.value().end());	
			CHECK(data[key].compare(value) == 0);
		}
		break;
	}
}

TEST_CASE("cannot read system key") {
	auto tr = db.createTransaction();
	auto f1 = tr.get("\xff/coordinators"_br, /* snapshot */ false);

	auto err = waitFuture(f1);
	CHECK(err.code() == 2004); // key_outside_legal_range
}

TEST_CASE("read system key") {
	auto value = getValue("\xff/coordinators"_br, /* snapshot */ false, { FDB_TR_OPTION_READ_SYSTEM_KEYS });
	REQUIRE(value.has_value());
}

TEST_CASE("cannot write system key") {
	auto tr = db.createTransaction();

	tr.set("\xff\x02"_br, "bar"_br);

	auto f1 = tr.commit();
	auto err = waitFuture(f1);
	CHECK(err.code() == 2004); // key_outside_legal_range
}

TEST_CASE("write system key") {
	auto tr = db.createTransaction();
	auto syskey = "\xff\x02"_br;

	while (1) {
		tr.setOption(FDB_TR_OPTION_ACCESS_SYSTEM_KEYS);
		tr.set(syskey, "bar"_br);
		auto f1 = tr.commit();

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(syskey, /* snapshot */ false, { FDB_TR_OPTION_READ_SYSTEM_KEYS });
	REQUIRE(value.has_value());
	CHECK(value->compare("bar") == 0);
}

TEST_CASE("fdb_transaction read_your_writes") {
	auto tr = db.createTransaction();
	clear_data(db);

	while (1) {
		tr.set("foo"_br, "bar"_br);
		auto f1 = tr.get("foo"_br, /*snapshot*/ false);

		// Read before committing, should read the initial write.
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto val = f1.get();
		CHECK(val.has_value());
		CHECK(fdb::toCharsRef(val).compare("bar") == 0);
		break;
	}
}

TEST_CASE("fdb_transaction_set_option read_your_writes_disable") {
	clear_data(db);

	auto tr = db.createTransaction();
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		tr.set("foo"_br, "bar"_br);
		auto f1 = tr.get("foo"_br, /*snapshot*/ false);

		// Read before committing, shouldn't read the initial write because
		// read_your_writes is disabled.
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto val = f1.get();
		CHECK(!val.has_value());
		break;
	}
}

TEST_CASE("fdb_transaction_set_option snapshot_read_your_writes_enable") {
	clear_data(db);

	auto tr = db.createTransaction();
	while (1) {
		// Enable read your writes for snapshot reads.
		tr.setOption(FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE);
		tr.set("foo"_br, "bar"_br);
		auto f1 = tr.get("foo"_br, /*snapshot*/ true);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto val = f1.get();
		CHECK(val.has_value());
		CHECK(fdb::toCharsRef(val).compare("bar") == 0);
		break;
	}
}

TEST_CASE("fdb_transaction_set_option snapshot_read_your_writes_disable") {
	clear_data(db);

	auto tr = db.createTransaction();
	while (1) {
		// Disable read your writes for snapshot reads.
		tr.setOption(FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE);
		tr.set("foo"_br, "bar"_br);
		auto f1 = tr.get("foo"_br, /*snapshot*/ true);
		auto f2 = tr.get("foo"_br, /*snapshot*/ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f3 = tr.onError(err);
			fdbCheck(waitFuture(f3));
			continue;
		}

		auto val = f1.get();
		CHECK(!val.has_value());

		//// Non-snapshot reads should still read writes in the transaction.
		err = waitFuture(f2);
		if (err) {
			auto f3 = tr.onError(err);
			fdbCheck(waitFuture(f3));
			continue;
		}

		val = f2.get();
		CHECK(val.has_value());
		CHECK(fdb::toCharsRef(val).compare("bar") == 0);
		break;
	}
}

TEST_CASE("fdb_transaction_set_option timeout") {
	auto tr = db.createTransaction();
	// Set smallest possible timeout, retry until a timeout occurs.
	int64_t timeout = 1;
	tr.setOption(FDB_TR_OPTION_TIMEOUT, timeout);

	fdb::Error err;
	while (!err) {
		auto f1 = tr.get("foo"_br, /* snapshot */ false);
		err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			err = waitFuture(f2);
		}
	}
	CHECK(err.code() == 1031); // transaction_timed_out
}

TEST_CASE("FDB_DB_OPTION_TRANSACTION_TIMEOUT") {
	// Set smallest possible timeout, retry until a timeout occurs.
	int64_t timeout = 1;
	db.setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, timeout);

	auto tr = db.createTransaction();
	fdb::Error err;
	while (!err) {
		auto f1 = tr.get("foo"_br, /* snapshot */ false);
		err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			err = waitFuture(f2);
		}
	}
	CHECK(err.code() == 1031); // transaction_timed_out

	// Reset transaction timeout (disable timeout).
	timeout = 0;
	db.setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, timeout);
}

TEST_CASE("fdb_transaction_set_option size_limit too small") {
	auto tr = db.createTransaction();

	// Size limit must be at least 32 to be valid, so test a smaller size.
	int64_t size_limit = 31;
	tr.setOption(FDB_TR_OPTION_SIZE_LIMIT, size_limit);
	tr.set("foo"_br, "bar"_br);
	auto f1 = tr.commit();
	CHECK(waitFuture(f1).code() == 2006); // invalid_option_value
}

TEST_CASE("fdb_transaction_set_option size_limit too large") {
	auto tr = db.createTransaction();

	// Size limit must be less than or equal to 10,000,000.
	int64_t size_limit = 10000001;
	tr.setOption(FDB_TR_OPTION_SIZE_LIMIT, size_limit);
	tr.set("foo"_br, "bar"_br);
	auto f1 = tr.commit();
	CHECK(waitFuture(f1).code() == 2006); // invalid_option_value
}

TEST_CASE("fdb_transaction_set_option size_limit") {
	auto tr = db.createTransaction();

	int64_t size_limit = 32;
	tr.setOption(FDB_TR_OPTION_SIZE_LIMIT, size_limit);
	tr.set("foo"_br, "foundation database is amazing"_br);
	auto f1 = tr.commit();
	CHECK(waitFuture(f1).code() == 2101);
}
//
// Setting the transaction size limit as a database option causes issues when
// outside the bounds of acceptable values. TODO: Needs investigating...
// TEST_CASE("FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT too small") {
//   // Size limit must be at least 32 to be valid, so test a smaller size.
//   int64_t size_limit = 31;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
//
//   fdb::Transaction tr(db);
//   tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
//   fdb::EmptyFuture f1 = tr.commit();
//
//   CHECK(wait_future(f1) == 2006); // invalid_option_value
//
//   // Set size limit back to default.
//   size_limit = 10000000;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
// }

// TEST_CASE("FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT too large") {
//   // Size limit must be less than or equal to 10,000,000.
//   int64_t size_limit = 10000001;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
//
//   fdb::Transaction tr(db);
//   tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
//   fdb::EmptyFuture f1 = tr.commit();
//
//   CHECK(wait_future(f1) == 2006); // invalid_option_value
//
//   // Set size limit back to default.
//   size_limit = 10000000;
//   fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
//                                     (const uint8_t *)&size_limit,
//                                     sizeof(size_limit)));
// }

TEST_CASE("FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT") {
	int64_t size_limit = 32;
	db.setOption(FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT, size_limit);

	auto tr = db.createTransaction();
	tr.set("foo"_br, "foundation database is amazing"_br);
	auto f1 = tr.commit();

	CHECK(waitFuture(f1).code() == 2101); // transaction_too_large

	// Set size limit back to default.
	size_limit = 10000000;
	db.setOption(FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT, size_limit);
}

TEST_CASE("fdb_transaction_set_read_version old_version") {
	auto tr = db.createTransaction();

	tr.setReadVersion(1);
	auto f1 = tr.get("foo"_br, /*snapshot*/ true);

	auto err = waitFuture(f1);
	CHECK(err.code() == 1007); // transaction_too_old
}

TEST_CASE("fdb_transaction_set_read_version future_version") {
	auto tr = db.createTransaction();

	tr.setReadVersion(1UL << 62);
	auto f1 = tr.get("foo"_br, /*snapshot*/ true);

	auto err = waitFuture(f1);
	CHECK(err.code() == 1009); // future_version
}

const std::string EMPTY = Tuple().pack().toString();
const KeyRef RECORD = "RECORD"_sr;
const KeyRef INDEX = "INDEX"_sr;
static Key primaryKey(const int i) {
	return Key(format("primary-key-of-record-%08d", i));
}
static Key indexKey(const int i) {
	return Key(format("index-key-of-record-%08d", i));
}
static Value dataOfRecord(const int i) {
	return Value(format("data-of-record-%08d", i));
}
static std::string indexEntryKey(const int i) {
	return Tuple::makeTuple(prefix, INDEX, indexKey(i), primaryKey(i)).pack().toString();
}
static std::string recordKey(const int i, const int split) {
	return Tuple::makeTuple(prefix, RECORD, primaryKey(i), split).pack().toString();
}
static std::string recordValue(const int i, const int split) {
	return Tuple::makeTuple(dataOfRecord(i), split).pack().toString();
}

const static int SPLIT_SIZE = 3;
std::map<std::string, std::string> fillInRecords(int n) {
	// Note: The user requested `prefix` should be added as the first element of the tuple that forms the key, rather
	// than the prefix of the key. So we don't use key() or create_data() in this test.
	std::map<std::string, std::string> data;
	for (int i = 0; i < n; i++) {
		data[indexEntryKey(i)] = EMPTY;
		for (int split = 0; split < SPLIT_SIZE; split++) {
			data[recordKey(i, split)] = recordValue(i, split);
		}
	}
	insert_data(db, data);
	return data;
}

GetMappedRangeResult getMappedIndexEntries(int beginId,
                                           int endId,
                                           fdb::Transaction& tr,
                                           std::string mapper,
                                           int matchIndex) {
	std::string indexEntryKeyBegin = indexEntryKey(beginId);
	std::string indexEntryKeyEnd = indexEntryKey(endId);

	return getMappedRange(tr,
	                      fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKeyBegin)),
	                      fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKeyEnd)),
	                      fdb::toBytesRef(mapper),
	                      /* limit */ 0,
	                      /* target_bytes */ 0,
	                      /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
	                      /* iteration */ 0,
	                      /* matchIndex */ matchIndex,
	                      /* snapshot */ false,
	                      /* reverse */ 0);
}

GetMappedRangeResult getMappedIndexEntries(int beginId,
                                           int endId,
                                           fdb::Transaction& tr,
                                           int matchIndex,
                                           bool allMissing) {
	std::string mapper =
	    Tuple::makeTuple(prefix, RECORD, (allMissing ? "{K[2]}"_sr : "{K[3]}"_sr), "{...}"_sr).pack().toString();
	return getMappedIndexEntries(beginId, endId, tr, mapper, matchIndex);
}

TEST_CASE("versionstamp_unit_test") {
	// a random 12 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12"_sr;
	TupleVersionstamp vs(str), vs2(str);
	ASSERT(vs == vs2);
	ASSERT(vs.begin() != vs2.begin());

	int64_t version = vs.getVersion();
	int64_t version2 = vs2.getVersion();
	int64_t versionExpected = ((int64_t)0x01 << 56) + ((int64_t)0x02 << 48) + ((int64_t)0x03 << 40) +
	                          ((int64_t)0x04 << 32) + (0x05 << 24) + (0x06 << 16) + (0x07 << 8) + 0x08;
	ASSERT(version == versionExpected);
	ASSERT(version2 == versionExpected);

	int16_t batch = vs.getBatchNumber();
	int16_t batch2 = vs2.getBatchNumber();
	int16_t batchExpected = (0x09 << 8) + 0x10;
	ASSERT(batch == batchExpected);
	ASSERT(batch2 == batchExpected);

	int16_t user = vs.getUserVersion();
	int16_t user2 = vs2.getUserVersion();
	int16_t userExpected = (0x11 << 8) + 0x12;
	ASSERT(user == userExpected);
	ASSERT(user2 == userExpected);

	ASSERT(vs.size() == VERSIONSTAMP_TUPLE_SIZE);
	ASSERT(vs2.size() == VERSIONSTAMP_TUPLE_SIZE);
}

TEST_CASE("tuple_support_versionstamp") {
	// a random 12 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12"_sr;
	TupleVersionstamp vs(str);
	const Tuple t = Tuple::makeTuple(prefix, RECORD, vs, "{K[3]}"_sr, "{...}"_sr);
	ASSERT(t.getVersionstamp(2) == vs);

	// verify the round-way pack-unpack path for a Tuple containing a versionstamp
	StringRef result1 = t.pack();
	Tuple t2 = Tuple::unpack(result1);
	StringRef result2 = t2.pack();
	ASSERT(t2.getVersionstamp(2) == vs);
	ASSERT(result1.toString() == result2.toString());
}

TEST_CASE("tuple_fail_to_append_truncated_versionstamp") {
	// a truncated 11 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11"_sr;
	try {
		TupleVersionstamp truncatedVersionstamp(str);
	} catch (Error& e) {
		return;
	}
	UNREACHABLE();
}

TEST_CASE("tuple_fail_to_append_longer_versionstamp") {
	// a longer than expected 13 bytes long StringRef as a versionstamp
	StringRef str = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11"_sr;
	try {
		TupleVersionstamp longerVersionstamp(str);
	} catch (Error& e) {
		return;
	}
	UNREACHABLE();
}

TEST_CASE("fdb_transaction_getMappedRange") {
	const int TOTAL_RECORDS = 20;
	fillInRecords(TOTAL_RECORDS);

	auto tr = db.createTransaction();
	// RYW should be enabled.
	while (1) {
		int beginId = 1;
		int endId = 19;
		const double r = deterministicRandom()->random01();
		int matchIndex = MATCH_INDEX_ALL;
		if (r < 0.25) {
			matchIndex = MATCH_INDEX_NONE;
		} else if (r < 0.5) {
			matchIndex = MATCH_INDEX_MATCHED_ONLY;
		} else if (r < 0.75) {
			matchIndex = MATCH_INDEX_UNMATCHED_ONLY;
		}
		auto result = getMappedIndexEntries(beginId, endId, tr, matchIndex, false);

		if (result.err) {
			auto f1 = tr.onError(result.err);
			fdbCheck(waitFuture(f1));
			continue;
		}

		int expectSize = endId - beginId;
		CHECK(result.mkvs.size() == expectSize);
		CHECK(!result.more);

		int id = beginId;
		for (int i = 0; i < expectSize; i++, id++) {
			const auto& mkv = result.mkvs[i];
			if (matchIndex == MATCH_INDEX_ALL || i == 0 || i == expectSize - 1) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_MATCHED_ONLY) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_UNMATCHED_ONLY) {
				CHECK(EMPTY.compare(mkv.key) == 0);
			} else {
				CHECK(EMPTY.compare(mkv.key) == 0);
			}
			CHECK(EMPTY.compare(mkv.value) == 0);
			CHECK(mkv.range_results.size() == SPLIT_SIZE);
			for (int split = 0; split < SPLIT_SIZE; split++) {
				auto& kv = mkv.range_results[split];
				CHECK(recordKey(id, split).compare(kv.first) == 0);
				CHECK(recordValue(id, split).compare(kv.second) == 0);
			}
		}
		break;
	}
}

TEST_CASE("fdb_transaction_getMappedRange_missing_all_secondary") {
	const int TOTAL_RECORDS = 20;
	fillInRecords(TOTAL_RECORDS);

	auto tr = db.createTransaction();
	// RYW should be enabled.
	while (1) {
		int beginId = 1;
		int endId = 19;
		const double r = deterministicRandom()->random01();
		int matchIndex = MATCH_INDEX_ALL;
		if (r < 0.25) {
			matchIndex = MATCH_INDEX_NONE;
		} else if (r < 0.5) {
			matchIndex = MATCH_INDEX_MATCHED_ONLY;
		} else if (r < 0.75) {
			matchIndex = MATCH_INDEX_UNMATCHED_ONLY;
		}
		auto result = getMappedIndexEntries(beginId, endId, tr, matchIndex, true);

		if (result.err) {
			auto f1 = tr.onError(result.err);
			fdbCheck(waitFuture(f1));
			continue;
		}

		int expectSize = endId - beginId;
		CHECK(result.mkvs.size() == expectSize);
		CHECK(!result.more);

		int id = beginId;
		for (int i = 0; i < expectSize; i++, id++) {
			const auto& mkv = result.mkvs[i];
			if (matchIndex == MATCH_INDEX_ALL || i == 0 || i == expectSize - 1) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_MATCHED_ONLY) {
				CHECK(EMPTY.compare(mkv.key) == 0);
			} else if (matchIndex == MATCH_INDEX_UNMATCHED_ONLY) {
				CHECK(indexEntryKey(id).compare(mkv.key) == 0);
			} else {
				CHECK(EMPTY.compare(mkv.key) == 0);
			}
			CHECK(EMPTY.compare(mkv.value) == 0);
		}
		break;
	}
}

TEST_CASE("fdb_transaction_getMappedRange_restricted_to_serializable") {
	std::string mapper = Tuple::makeTuple(prefix, RECORD, "{K[3]}"_sr).pack().toString();
	auto tr = db.createTransaction();
	auto result = getMappedRange(tr,
	                             fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKey(0))),
	                             fdb::key_select::firstGreaterThan(fdb::toBytesRef(indexEntryKey(1))),
	                             fdb::toBytesRef(mapper),
	                             /* limit */ 0,
	                             /* target_bytes */ 0,
	                             /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
	                             /* iteration */ 0,
	                             /* matchIndex */ MATCH_INDEX_ALL,
	                             /* snapshot */ true, // Set snapshot to true
	                             /* reverse */ 0);
	ASSERT(result.err.code() == error_code_unsupported_operation);
}

TEST_CASE("fdb_transaction_getMappedRange_restricted_to_ryw_enable") {
	std::string mapper = Tuple::makeTuple(prefix, RECORD, "{K[3]}"_sr).pack().toString();
	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE); // Not disable RYW
	auto result = getMappedRange(tr,
	                             fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(indexEntryKey(0))),
	                             fdb::key_select::firstGreaterThan(fdb::toBytesRef(indexEntryKey(1))),
	                             fdb::toBytesRef(mapper),
	                             /* limit */ 0,
	                             /* target_bytes */ 0,
	                             /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
	                             /* iteration */ 0,
	                             /* matchIndex */ MATCH_INDEX_ALL,
	                             /* snapshot */ false,
	                             /* reverse */ 0);
	ASSERT(result.err.code() == error_code_unsupported_operation);
}

void assertNotTuple(std::string str) {
	try {
		Tuple::unpack(str);
	} catch (Error& e) {
		return;
	}
	UNREACHABLE();
}

TEST_CASE("fdb_transaction_getMappedRange_fail_on_mapper_not_tuple") {
	// A string that cannot be parsed as tuple.
	//
	///"\x15:\x152\x15E\x15\x09\x15\x02\x02MySimpleRecord$repeater-version\x00\x15\x013\x00\x00\x00\x00\x1aU\x90\xba\x00\x00\x00\x02\x15\x04"
	// should fail at \x35
	//
	std::string mapper = {
		'\x15', ':',    '\x15', '2', '\x15', 'E',    '\x15', '\t',   '\x15', '\x02', '\x02', 'M',
		'y',    'S',    'i',    'm', 'p',    'l',    'e',    'R',    'e',    'c',    'o',    'r',
		'd',    '$',    'r',    'e', 'p',    'e',    'a',    't',    'e',    'r',    '-',    'v',
		'e',    'r',    's',    'i', 'o',    'n',    '\x00', '\x15', '\x01', '\x35', '\x00', '\x00',
		'\x00', '\x00', '\x1a', 'U', '\x90', '\xba', '\x00', '\x00', '\x00', '\x02', '\x15', '\x04'
	};
	assertNotTuple(mapper);
	auto tr = db.createTransaction();
	auto result = getMappedIndexEntries(1, 3, tr, mapper, MATCH_INDEX_ALL);
	ASSERT(result.err.code() == error_code_mapper_not_tuple);
}

TEST_CASE("fdb_transaction_get_range reverse") {
	auto data = create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	insert_data(db, data);

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(key("a"))),
		                      fdb::key_select::lastLessOrEqual(fdb::toBytesRef(key("d")), 1),
		                      /* limit */ 0,
		                      /* target_bytes */ 0,
		                      /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                      /* iteration */ 0,
		                      /* snapshot */ false,
		                      /* reverse */ true);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		auto out_kv = std::get<0>(output);
		auto out_count = std::get<1>(output);
		auto out_more = std::get<2>(output);
		CHECK(out_count > 0);
		CHECK(out_count <= 4);
		if (out_count < 4) {
			CHECK(out_more);
		}

		// Read data in reverse order.
		auto it = data.rbegin();
		for (int i = 0; i < out_count; i++) {
			auto kv = *out_kv++;
			auto key = fdb::toCharsRef(kv.key());
		      	auto value = fdb::toCharsRef(kv.value());	

			CHECK(key.compare(it->first) == 0);
			CHECK(value.compare(it->second) == 0);
			++it;
		}
		break;
	}
}

TEST_CASE("fdb_transaction_get_range limit") {
	auto data = create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	insert_data(db, data);

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(key("a"))),
		                      fdb::key_select::lastLessOrEqual(fdb::toBytesRef(key("d")), 1),
		                      /* limit */ 2,
		                      /* target_bytes */ 0,
		                      /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                      /* iteration */ 0,
		                      /* snapshot */ false,
		                      /* reverse */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		auto out_kv = std::get<0>(output);
		auto out_count = std::get<1>(output);
		auto out_more = std::get<2>(output);
		CHECK(out_count > 0);
		CHECK(out_count <= 2);
		if (out_count < 4) {
			CHECK(out_more);
		}

		for (int i = 0; i < out_count; i++) {
			auto kv = *out_kv++;
			auto key = std::string(kv.key().begin(), kv.key().end());
		      	auto value = std::string(kv.value().begin(), kv.value().end());	

			CHECK(data[key].compare(value) == 0);
		}
		break;
	}
}

TEST_CASE("fdb_transaction_get_range FDB_STREAMING_MODE_EXACT") {
	auto data = create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
	insert_data(db, data);

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(key("a"))),
		                      fdb::key_select::lastLessOrEqual(fdb::toBytesRef(key("d")), 1),
		                      /* limit */ 3,
		                      /* target_bytes */ 0,
		                      /* FDBStreamingMode */ FDB_STREAMING_MODE_EXACT,
		                      /* iteration */ 0,
		                      /* snapshot */ false,
		                      /* reverse */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		auto out_kv = std::get<0>(output);
		auto out_count = std::get<1>(output);
		auto out_more = std::get<2>(output);

		CHECK(out_count == 3);
		CHECK(out_more);

		for (int i = 0; i < out_count; i++) {
			auto kv = *out_kv++;
			auto key = std::string(kv.key().begin(), kv.key().end());
		      	auto value = std::string(kv.value().begin(), kv.value().end());	
			CHECK(data[key].compare(value) == 0);
		}
		break;
	}
}

TEST_CASE("fdb_transaction_clear") {
	insert_data(db, create_data({ { "foo", "bar" } }));

	auto tr = db.createTransaction();
	while (1) {
		tr.clear(fdb::toBytesRef(key("foo")));
		auto f1 = tr.commit();

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(!value.has_value());
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_ADD") {
	insert_data(db, create_data({ { "foo", "\x00" } }));

	auto tr = db.createTransaction();
	int8_t param = 1;
	int potentialCommitCount = 0;
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")),
		            fdb::BytesRef(reinterpret_cast<const uint8_t*>(&param), sizeof(param)),
		            FDB_MUTATION_TYPE_ADD);
		if (potentialCommitCount + 1 == 256) {
			// Trying to commit again might overflow the one unsigned byte we're looking at
			break;
		}
		++potentialCommitCount;
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			if (err.retryableNotCommitted()) {
				--potentialCommitCount;
			}
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(uint8_t(value->data()[0]) > 0);
	CHECK(uint8_t(value->data()[0]) <= potentialCommitCount);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BIT_AND") {
	// Test bitwise and on values of same length:
	//   db key = foo
	//   db value = 'a' == 97
	//   param = 'b' == 98
	//
	//   'a' == 97 == 0b01100001
	// & 'b' == 98 == 0b01100010
	//   -----------------------
	//                0b01100000 == 96 == '`'
	//
	// Test bitwise and on extended database value:
	//   db key = bar
	//   db value = 'c' == 99
	//   param = "ad"
	//
	//   'c' == 99 == 0b0110001100000000 (zero extended on right to match length of param)
	// & "ad"   ==    0b0110000101100100
	//   -------------------------------
	//                0b0110000100000000 == 'a' followed by null (0)
	//
	// Test bitwise and on truncated database value:
	//   db key = baz
	//   db value = "abc"
	//   param = 'e' == 101
	//
	//   "abc"  ->  0b01100001 (truncated to "a" to match length of param)
	// & 'e' == 101 0b01100101
	//   ---------------------
	//              0b01100001 == 97 == 'a'
	//
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "c" }, { "baz", "abc" } }));

	auto tr = db.createTransaction();
	char param[] = { 'a', 'd' };
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_BIT_AND);
		tr.atomicOp(fdb::toBytesRef(key("bar")),
		            fdb::BytesRef(reinterpret_cast<const uint8_t*>(param), 2),
		            FDB_MUTATION_TYPE_BIT_AND);
		tr.atomicOp(fdb::toBytesRef(key("baz")), "e"_br, FDB_MUTATION_TYPE_BIT_AND);

		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(value->data()[0] == 96);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 2);
	CHECK(value->data()[0] == 97);
	CHECK(value->data()[1] == 0);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(value->data()[0] == 97);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BIT_OR") {
	// Test bitwise or on values of same length:
	//   db key = foo
	//   db value = 'a' == 97
	//   param = 'b' == 98
	//
	//   'a' == 97 == 0b01100001
	// | 'b' == 98 == 0b01100010
	//   -----------------------
	//                0b01100011 == 99 == 'c'
	//
	// Test bitwise or on extended database value:
	//   db key = bar
	//   db value = 'b' == 98
	//   param = "ad"
	//
	//   'b' == 98 -> 0b0110001000000000 (zero extended on right to match length of param)
	// | "ad"   ==    0b0110000101100100
	//   -------------------------------
	//                0b0110001101100100 == "cd"
	//
	// Test bitwise or on truncated database value:
	//   db key = baz
	//   db value = "abc"
	//   param = 'd' == 100
	//
	//   "abc"  ->  0b01100001 (truncated to "a" to match length of param)
	// | 'd' == 100 0b01100100
	//   ---------------------
	//              0b01100101 == 101 == 'e'
	//
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));

	auto tr = db.createTransaction();
	char param[] = { 'a', 'd' };
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_BIT_OR);
		tr.atomicOp(fdb::toBytesRef(key("bar")),
		            fdb::BytesRef(reinterpret_cast<const uint8_t*>(param), 2),
		            FDB_MUTATION_TYPE_BIT_OR);
		tr.atomicOp(fdb::toBytesRef(key("baz")), "d"_br, FDB_MUTATION_TYPE_BIT_OR);

		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(value->data()[0] == 99);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("cd") == 0);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(value->data()[0] == 101);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BIT_XOR") {
	// Test bitwise xor on values of same length:
	//   db key = foo
	//   db value = 'a' == 97
	//   param = 'b' == 98
	//
	//   'a' == 97 == 0b01100001
	// ^ 'b' == 98 == 0b01100010
	//   -----------------------
	//                0b00000011 == 0x3
	//
	// Test bitwise xor on extended database value:
	//   db key = bar
	//   db value = 'b' == 98
	//   param = "ad"
	//
	//   'b' == 98 -> 0b0110001000000000 (zero extended on right to match length of param)
	// ^ "ad"   ==    0b0110000101100100
	//   -------------------------------
	//                0b0000001101100100 == 0x3 followed by 0x64
	//
	// Test bitwise xor on truncated database value:
	//   db key = baz
	//   db value = "abc"
	//   param = 'd' == 100
	//
	//   "abc"  ->  0b01100001 (truncated to "a" to match length of param)
	// ^ 'd' == 100 0b01100100
	//   ---------------------
	//              0b00000101 == 0x5
	//
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));

	auto tr = db.createTransaction();
	char param[] = { 'a', 'd' };
	int potentialCommitCount = 0;
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_BIT_XOR);
		tr.atomicOp(fdb::toBytesRef(key("bar")),
		            fdb::BytesRef(reinterpret_cast<const uint8_t*>(param), 2),
		            FDB_MUTATION_TYPE_BIT_XOR);
		tr.atomicOp(fdb::toBytesRef(key("baz")), "d"_br, FDB_MUTATION_TYPE_BIT_XOR);

		++potentialCommitCount;

		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			if (err.retryableNotCommitted()) {
				--potentialCommitCount;
			}
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	if (potentialCommitCount != 1) {
		MESSAGE("Transaction may not have committed exactly once. Suppressing assertions");
		return;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(value->data()[0] == 0x3);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 2);
	CHECK(value->data()[0] == 0x3);
	CHECK(value->data()[1] == 0x64);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 1);
	CHECK(value->data()[0] == 0x5);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_COMPARE_AND_CLEAR") {
	// Atomically remove a key-value pair from the database based on a value
	// comparison.
	insert_data(db, create_data({ { "foo", "bar" }, { "fdb", "foundation" } }));

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "bar"_br, FDB_MUTATION_TYPE_COMPARE_AND_CLEAR);
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	CHECK(!value.has_value());

	value = getValue(fdb::toBytesRef(key("fdb")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("foundation") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_APPEND_IF_FITS") {
	// Atomically append a value to an existing key-value pair, or insert the
	// key-value pair if an existing key-value pair doesn't exist.
	insert_data(db, create_data({ { "foo", "f" } }));

	auto tr = db.createTransaction();
	int potentialCommitCount = 0;
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "db"_br, FDB_MUTATION_TYPE_APPEND_IF_FITS);
		tr.atomicOp(fdb::toBytesRef(key("bar")), "foundation"_br, FDB_MUTATION_TYPE_APPEND_IF_FITS);
		++potentialCommitCount;

		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			if (err.retryableNotCommitted()) {
				--potentialCommitCount;
			}
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value_foo = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value_foo.has_value());

	auto value_bar = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value_bar.has_value());

	if (potentialCommitCount != 1) {
		MESSAGE("Transaction may not have committed exactly once. Suppressing assertions");
	} else {
		CHECK(value_foo.value() == "fdb");
		CHECK(value_bar.value() == "foundation");
	}
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_MAX") {
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_MAX);
		// Value in database will be extended with zeros to match length of param.
		tr.atomicOp(fdb::toBytesRef(key("bar")), "aa"_br, FDB_MUTATION_TYPE_MAX);
		// Value in database will be truncated to match length of param.
		tr.atomicOp(fdb::toBytesRef(key("baz")), "b"_br, FDB_MUTATION_TYPE_MAX);
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("b") == 0);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("aa") == 0);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("c") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_MIN") {
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_MIN);
		// Value in database will be extended with zeros to match length of param.
		tr.atomicOp(fdb::toBytesRef(key("bar")), "aa"_br, FDB_MUTATION_TYPE_MIN);
		// Value in database will be truncated to match length of param.
		tr.atomicOp(fdb::toBytesRef(key("baz")), "b"_br, FDB_MUTATION_TYPE_MIN);
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("a") == 0);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->size() == 2);
	CHECK(value->data()[0] == 'b');
	CHECK(value->data()[1] == 0);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("b") == 0);
}
//
TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BYTE_MAX") {
	// The difference with FDB_MUTATION_TYPE_MAX is that strings will not be
	// extended/truncated so lengths match.
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_BYTE_MAX);
		tr.atomicOp(fdb::toBytesRef(key("bar")), "cc"_br, FDB_MUTATION_TYPE_BYTE_MAX);
		tr.atomicOp(fdb::toBytesRef(key("baz")), "b"_br, FDB_MUTATION_TYPE_BYTE_MAX);
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("b") == 0);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("cc") == 0);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("cba") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BYTE_MIN") {
	// The difference with FDB_MUTATION_TYPE_MIN is that strings will not be
	// extended/truncated so lengths match.
	insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")), "b"_br, FDB_MUTATION_TYPE_BYTE_MIN);
		tr.atomicOp(fdb::toBytesRef(key("bar")), "aa"_br, FDB_MUTATION_TYPE_BYTE_MIN);
		tr.atomicOp(fdb::toBytesRef(key("baz")), "b"_br, FDB_MUTATION_TYPE_BYTE_MIN);
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
		break;
	}

	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("a") == 0);

	value = getValue(fdb::toBytesRef(key("bar")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("aa") == 0);

	value = getValue(fdb::toBytesRef(key("baz")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("abc") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY") {
	int offset = prefix.size() + 3;
	const char* p = reinterpret_cast<const char*>(&offset);
	char keybuf[] = {
		'f', 'o', 'o', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', p[0], p[1], p[2], p[3]
	};
	std::string key = prefix + std::string(keybuf, 17);
	std::string versionstamp("");

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key), "bar"_br, FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
		auto f1 = tr.getVersionstamp();
		auto f2 = tr.commit();

		auto err = waitFuture(f2);
		if (err) {
			auto f3 = tr.onError(err);
			fdbCheck(waitFuture(f3));
			continue;
		}

		fdbCheck(waitFuture(f1));
		auto key = f1.get();
		versionstamp = std::string(key.begin(), key.end());
		break;
	}

	REQUIRE(versionstamp.size() > 0);
	std::string dbKey(prefix + "foo" + versionstamp);
	auto value = getValue(fdb::toBytesRef(dbKey), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("bar") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE") {
	// Don't care about prefixing value like we did the key.
	char valbuf[] = { 'b', 'a', 'r', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', 3, 0, 0, 0 };
	std::string versionstamp("");

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::toBytesRef(key("foo")),
		            fdb::BytesRef(reinterpret_cast<const uint8_t*>(valbuf), 17),
		            FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);
		auto f1 = tr.getVersionstamp();
		auto f2 = tr.commit();

		auto err = waitFuture(f2);
		if (err) {
			auto f3 = tr.onError(err);
			fdbCheck(waitFuture(f3));
			continue;
		}

		fdbCheck(waitFuture(f1));
		auto key = f1.get();
		versionstamp = std::string(key.begin(), key.end());
		break;
	}

	REQUIRE(versionstamp.size() > 0);
	auto value = getValue(fdb::toBytesRef(key("foo")), /* snapshot */ false, {});
	REQUIRE(value.has_value());
	CHECK(value->compare("bar" + versionstamp) == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY invalid index") {
	// Only 9 bytes available starting at index 4 (ten bytes needed), should
	// return an error.
	char keybuf[] = { 'f', 'o', 'o', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', 4, 0, 0, 0 };

	auto tr = db.createTransaction();
	while (1) {
		tr.atomicOp(fdb::BytesRef(reinterpret_cast<const uint8_t*>(keybuf), 17),
		            "bar"_br,
		            FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);
		auto f1 = tr.commit();
		CHECK(waitFuture(f1).code() != 0); // type of error not specified
		break;
	}
}

TEST_CASE("fdb_transaction_get_committed_version read_only") {
	// Read-only transaction should have a committed version of -1.
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, /*snapshot*/ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		int64_t out_version;
		fdbCheck(tr.getCommittedVersionNothrow(out_version));
		CHECK(out_version == -1);
		break;
	}
}

TEST_CASE("fdb_transaction_get_committed_version") {
	auto tr = db.createTransaction();
	while (1) {
		tr.set(fdb::toBytesRef(key("foo")), "bar"_br);
		auto f1 = tr.commit();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		int64_t out_version;
		fdbCheck(tr.getCommittedVersionNothrow(out_version));
		CHECK(out_version >= 0);
		break;
	}
}

TEST_CASE("fdb_transaction_get_tag_throttled_duration") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, /*snapshot*/ false);
		auto err = waitFuture(f1);
		if (err) {
			auto fOnError = tr.onError(err);
			fdbCheck(waitFuture(fOnError));
			continue;
		}
		auto f2 = tr.getTagThrottledDuration();
		err = waitFuture(f2);
		if (err) {
			auto fOnError = tr.onError(err);
			fdbCheck(waitFuture(fOnError));
			continue;
		}
		double tagThrottledDuration = f2.get();
		CHECK(tagThrottledDuration >= 0.0);
		break;
	}
}

TEST_CASE("fdb_transaction_get_total_cost") {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("foo"_br, /*snapshot*/ false);
		auto err = waitFuture(f1);
		if (err) {
			auto fOnError = tr.onError(err);
			fdbCheck(waitFuture(fOnError));
			continue;
		}
		auto f2 = tr.getTotalCost();
		err = waitFuture(f2);
		if (err) {
			auto fOnError = tr.onError(err);
			fdbCheck(waitFuture(fOnError));
			continue;
		}
		int64_t cost = f2.get();
		CHECK(cost > 0);
		break;
	}
}

TEST_CASE("fdb_transaction_get_approximate_size") {
	auto tr = db.createTransaction();
	while (1) {
		tr.set(fdb::toBytesRef(key("foo")), "bar"_br);
		auto f1 = tr.getApproximateSize();
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		int64_t size = f1.get();
		CHECK(size >= 3);
		break;
	}
}

TEST_CASE("fdb_database_get_server_protocol") {
	// We don't really have any expectations other than "don't crash" here
	auto protocolFuture = db.getServerProtocol(0);
	fdbCheck(waitFuture(protocolFuture));
	uint64_t out = protocolFuture.get();

	// Passing in an expected version that's different than the cluster version
	protocolFuture = db.getServerProtocol(0x0FDB00A200090000LL);
	fdbCheck(waitFuture(protocolFuture));
	fdbCheck(protocolFuture.getNothrow(out));
}

TEST_CASE("fdb_transaction_watch read_your_writes_disable") {
	// Watches created on a transaction with the option READ_YOUR_WRITES_DISABLE
	// should return a watches_disabled error.
	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
	auto f1 = tr.watch(fdb::toBytesRef(key("foo")));
	CHECK(waitFuture(f1).code() == 1034); // watches_disabled
}

TEST_CASE("fdb_transaction_watch reset") {
	// Resetting (or destroying) an uncommitted transaction should cause watches
	// created by the transaction to fail with a transaction_cancelled error.
	auto tr = db.createTransaction();
	auto f1 = tr.watch(fdb::toBytesRef(key("foo")));
	tr.reset();
	CHECK(waitFuture(f1).code() == 1025); // transaction_cancelled
}

TEST_CASE("fdb_transaction_watch max watches") {
	int64_t max_watches = 3;
	db.setOption(FDB_DB_OPTION_MAX_WATCHES, max_watches);

	auto event = std::make_shared<FdbEvent>();

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.watch(fdb::toBytesRef(key("a")));
		auto f2 = tr.watch(fdb::toBytesRef(key("b")));
		auto f3 = tr.watch(fdb::toBytesRef(key("c")));
		auto f4 = tr.watch(fdb::toBytesRef(key("d")));
		auto f5 = tr.commit();

		auto err = waitFuture(f5);
		if (err) {
			auto f6 = tr.onError(err);
			fdbCheck(waitFuture(f6));
			continue;
		}

		// Callbacks will be triggered with operation_cancelled errors once the
		// too_many_watches error fires, as the other futures will go out of scope
		// and be cleaned up. The future which too_many_watches occurs on is
		// nondeterministic, so each future is checked.
		f1.then([&event](fdb::Future f) {
			auto err = waitFuture(f);
			if (err.code() != /*operation_cancelled*/ 1101 && !err.retryable()) {
				CHECK(err.code() == 1032); // too_many_watches
			}
			auto param = new std::shared_ptr<FdbEvent>(event);
			auto* e = static_cast<std::shared_ptr<FdbEvent>*>(param);
			(*e)->set();
			delete e;
		});

		f2.then([&event](fdb::Future f) {
			auto err = waitFuture(f);
			if (err.code() != /*operation_cancelled*/ 1101 && !err.retryable()) {
				CHECK(err.code() == 1032); // too_many_watches
			}
			auto param = new std::shared_ptr<FdbEvent>(event);
			auto* e = static_cast<std::shared_ptr<FdbEvent>*>(param);
			(*e)->set();
			delete e;
		});
		f3.then([&event](fdb::Future f) {
			auto err = waitFuture(f);
			if (err.code() != /*operation_cancelled*/ 1101 && !err.retryable()) {
				CHECK(err.code() == 1032); // too_many_watches
			}
			auto param = new std::shared_ptr<FdbEvent>(event);
			auto* e = static_cast<std::shared_ptr<FdbEvent>*>(param);
			(*e)->set();
			delete e;
		});
		f4.then([&event](fdb::Future f) {
			auto err = waitFuture(f);
			if (err.code() != /*operation_cancelled*/ 1101 && !err.retryable()) {
				CHECK(err.code() == 1032); // too_many_watches
			}
			auto param = new std::shared_ptr<FdbEvent>(event);
			auto* e = static_cast<std::shared_ptr<FdbEvent>*>(param);
			(*e)->set();
			delete e;
		});
		event->wait();
		break;
	}

	// Reset available number of watches.
	max_watches = 10000;
	db.setOption(FDB_DB_OPTION_MAX_WATCHES, max_watches);
}

TEST_CASE("fdb_transaction_watch") {
	insert_data(db, create_data({ { "foo", "foo" } }));

	struct Context {
		FdbEvent event;
	};
	Context context;

	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.watch(fdb::toBytesRef(key("foo")));
		auto f2 = tr.commit();

		auto err = waitFuture(f2);
		if (err) {
			auto f3 = tr.onError(err);
			fdbCheck(waitFuture(f3));
			continue;
		}

		f1.then([&context](fdb::Future f) { context.event.set(); });

		// Update value for key "foo" to trigger the watch.
		insert_data(db, create_data({ { "foo", "bar" } }));
		context.event.wait();
		break;
	}
}

TEST_CASE("fdb_transaction_cancel") {
	// Cannot use transaction after cancelling it...
	auto tr = db.createTransaction();
	tr.cancel();
	auto f1 = tr.get("foo"_br, /* snapshot */ false);
	CHECK(waitFuture(f1).code() == 1025); // transaction_cancelled

	// ... until the transaction has been reset.
	tr.reset();
	auto f2 = tr.get("foo"_br, /* snapshot */ false);
	CHECK(waitFuture(f2).code() != 1025); // transaction_cancelled
}

TEST_CASE("fdb_transaction_add_conflict_range") {
	bool success = false;

	bool retry = true;
	while (retry) {
		auto tr = db.createTransaction();
		while (1) {
			auto f1 = tr.getReadVersion();
			auto err = waitFuture(f1);
			if (err) {
				auto f2 = tr.onError(err);
				fdbCheck(waitFuture(f2));
				continue;
			}
			break;
		}

		auto tr2 = db.createTransaction();
		while (1) {
			tr2.addConflictRange(
			    fdb::toBytesRef(key("a")), fdb::toBytesRef(strinc_str(key("a"))), FDB_CONFLICT_RANGE_TYPE_WRITE);
			auto f1 = tr2.commit();
			auto err = waitFuture(f1);
			if (err) {
				auto f2 = tr2.onError(err);
				fdbCheck(waitFuture(f2));
				continue;
			}
			break;
		}

		while (1) {
			tr.addConflictRange(
			    fdb::toBytesRef(key("a")), fdb::toBytesRef(strinc_str(key("a"))), FDB_CONFLICT_RANGE_TYPE_READ);
			tr.addConflictRange(
			    fdb::toBytesRef(key("a")), fdb::toBytesRef(strinc_str(key("a"))), FDB_CONFLICT_RANGE_TYPE_WRITE);
			auto f1 = tr.commit();
			auto err = waitFuture(f1);
			if (err.code() == 1020) { // not_committed
				// Test should pass if transactions conflict.
				success = true;
				retry = false;
			} else if (err) {
				auto f2 = tr.onError(err);
				fdbCheck(waitFuture(f2));
				retry = true;
			} else {
				// If the transaction succeeded, something went wrong.
				CHECK(false);
				retry = false;
			}
			break;
		}
	}

	// Double check that failure was achieved and the loop wasn't just broken out
	// of.
	CHECK(success);
}

TEST_CASE("invalid trace parent") {
	auto tr = db.createTransaction();
	std::string invalidTraceParent = "invalid";
	auto err = tr.setOptionNothrow(FDB_TR_OPTION_TRACE_PARENT, invalidTraceParent);
	if (err.code() != 2006) { // invalid_option_value
		auto f1 = tr.commit();
		ASSERT_EQ(waitFuture(f1).code(), 2006);
	}
}

TEST_CASE("trace parent") {
	auto tr = db.createTransaction();
	SpanContext context(
	    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUInt64(), TraceFlags::unsampled);

	std::string traceParent = context.toString();

	tr.setOption(FDB_TR_OPTION_TRACE_PARENT, traceParent);
	while (1) {
		auto f1 = tr.get("\xff\xff/tracing/transaction_id"_br, /* snapshot */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		REQUIRE(output.has_value());
		UID transactionID = UID::fromString(std::string(output->begin(), output->end()));
		ASSERT_EQ(transactionID, context.traceID);
		break;
	}
}

TEST_CASE("special-key-space valid transaction ID") {
	auto value = getValue("\xff\xff/tracing/transaction_id"_br, /* snapshot */ false, {});
	REQUIRE(value.has_value());
	UID transactionID = UID::fromString(value.value());
	CHECK(transactionID.first() > 0);
	CHECK(transactionID.second() > 0);
}

TEST_CASE("special-key-space custom transaction ID") {
	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
	while (1) {
		UID randomTransactionID = UID(deterministicRandom()->randomUInt64(), deterministicRandom()->randomUInt64());
		tr.set("\xff\xff/tracing/transaction_id"_br, fdb::toBytesRef(randomTransactionID.toString()));
		auto f1 = tr.get("\xff\xff/tracing/transaction_id"_br, /* snapshot */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		REQUIRE(value.has_value());
		UID transactionID = UID::fromString(std::string(value->begin(), value->end()));
		CHECK(transactionID == randomTransactionID);
		break;
	}
}

TEST_CASE("special-key-space set transaction ID after write") {
	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
	while (1) {
		tr.set(fdb::toBytesRef(key("foo")), fdb::toBytesRef(key("bar")));
		tr.set("\xff\xff/tracing/transaction_id"_br, "0"_br);
		auto f1 = tr.get("\xff\xff/tracing/transaction_id"_br, /* snapshot */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		REQUIRE(value.has_value());
		UID transactionID = UID::fromString(std::string(value->begin(), value->end()));
		CHECK(transactionID.first() > 0);
		CHECK(transactionID.second() > 0);
		break;
	}
}

TEST_CASE("special-key-space disable tracing") {
	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
	while (1) {
		tr.set("\xff\xff/tracing/token"_br, "false"_br);
		auto f1 = tr.get("\xff\xff/tracing/token"_br, /* snapshot */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		REQUIRE(value.has_value());
		uint64_t token = std::stoul(std::string(value->begin(), value->end()));
		CHECK(token == 0);
		break;
	}
}

TEST_CASE("special-key-space tracing get range") {
	std::string tracingBegin = "\xff\xff/tracing/";
	std::string tracingEnd = "\xff\xff/tracing0";

	auto tr = db.createTransaction();
	tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
	while (1) {
		auto f1 = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(tracingBegin)),
		                      fdb::key_select::lastLessThan(fdb::toBytesRef(tracingEnd), 1),
		                      /* limit */ 0,
		                      /* target_bytes */ 0,
		                      /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                      /* iteration */ 0,
		                      /* snapshot */ false,
		                      /* reverse */ false);

		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f1.get();
		auto out_kv = std::get<0>(output);
		auto out_count = std::get<1>(output);
		auto out_more = std::get<2>(output);

		CHECK(!out_more);
		CHECK(out_count == 2);

		auto tracingBeginTidKey = fdb::toCharsRef(out_kv[1].key());
		CHECK(tracingBeginTidKey == tracingBegin + "transaction_id");
		auto tracingBeginTidVal = std::string(out_kv[1].value().begin(), out_kv[1].value().end());
		UID transactionID = UID::fromString(tracingBeginTidVal);
		CHECK(transactionID.first() > 0);
		CHECK(transactionID.second() > 0);
		break;
	}
}

std::string get_valid_status_json() {
	auto tr = db.createTransaction();
	while (1) {
		auto f1 = tr.get("\xff\xff/status/json"_br, false);
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto value = f1.get();
		assert(value.has_value());
		std::string statusJsonStr(value->begin(), value->end());
		rapidjson::Document statusJson;
		statusJson.Parse(statusJsonStr.c_str());
		// make sure it is available
		bool available = statusJson["client"]["database_status"]["available"].GetBool();
		if (!available)
			continue; // cannot reach to the cluster, retry
		return statusJsonStr;
	}
}

TEST_CASE("fdb_database_reboot_worker") {
#ifdef USE_TSAN
	MESSAGE(
	    "fdb_database_reboot_worker disabled for tsan, since fdbmonitor doesn't seem to restart the killed process");
	return;
#endif
	std::string status_json = get_valid_status_json();
	rapidjson::Document statusJson;
	statusJson.Parse(status_json.c_str());
	CHECK(statusJson.HasMember("cluster"));
	CHECK(statusJson["cluster"].HasMember("generation"));
	int old_generation = statusJson["cluster"]["generation"].GetInt();
	CHECK(statusJson["cluster"].HasMember("processes"));
	// Make sure we only have one process in the cluster
	// Thus, rebooting the worker ensures a recovery
	// Configuration changes may break the contract here
	CHECK(statusJson["cluster"]["processes"].MemberCount() == 1);
	auto processPtr = statusJson["cluster"]["processes"].MemberBegin();
	CHECK(processPtr->value.HasMember("address"));
	std::string networkAddress = processPtr->value["address"].GetString();
	while (1) {
		auto f = db.rebootWorker(fdb::toBytesRef(networkAddress), false, 0);
		fdbCheck(waitFuture(f));
		int64_t successful = f.get();
		if (successful)
			break; // retry rebooting until success
	}
	status_json = get_valid_status_json();
	statusJson.Parse(status_json.c_str());
	CHECK(statusJson.HasMember("cluster"));
	CHECK(statusJson["cluster"].HasMember("generation"));
	int new_generation = statusJson["cluster"]["generation"].GetInt();
	// The generation number should increase after the recovery
	CHECK(new_generation > old_generation);
}

TEST_CASE("fdb_database_force_recovery_with_data_loss") {
	// This command cannot be tested completely in the current unit test configuration
	// For now, we simply call the function to make sure it exist
	// Background:
	// It is also only usable when usable_regions=2, so it requires a fearless configuration
	// In particular, you have two data centers, and the storage servers in one region are allowed to fall behind (async
	// replication) Normally, you would not want to recover to that set of storage servers unless there are tlogs which
	// can let those storage servers catch up However, if all the tlogs are dead and you still want to be able to
	// recover your database even if that means losing recently committed mutation, that's the time this function works

	std::string dcid = "test_id";
	while (1) {
		auto f = db.forceRecoveryWithDataLoss(fdb::toBytesRef(dcid));
		fdbCheck(waitFuture(f));
		break;
	}
}

std::string random_hex_string(size_t length) {
	const char charset[] = "0123456789"
	                       "ABCDEF"
	                       "abcdef";
	// construct a random generator engine from a time-based seed:
	std::default_random_engine generator(time(nullptr));
	std::uniform_int_distribution<int> distribution(0, strlen(charset) - 1);
	auto randchar = [&charset, &generator, &distribution]() -> char { return charset[distribution(generator)]; };
	std::string str(length, 0);
	std::generate_n(str.begin(), length, randchar);
	return str;
}

TEST_CASE("fdb_database_create_snapshot") {
	std::string snapshot_command = "test";
	std::string uid = "invalid_uid";
	bool retry = false;
	while (1) {
		auto f = db.createSnapshot(fdb::toBytesRef(uid), fdb::toBytesRef(snapshot_command));
		auto err = waitFuture(f);
		if (err.code() == 2509) { // expected error code
			CHECK(!retry);
			uid = random_hex_string(32);
			retry = true;
		} else if (err.code() == 2505) {
			CHECK(retry);
			break;
		} else {
			// Otherwise, something went wrong.
			CHECK(false);
		}
	}
}

TEST_CASE("fdb_error_predicate") {
	CHECK(fdb::Error(1007).retryable()); // transaction_too_old
	CHECK(fdb::Error(1020).retryable()); // not_committed
	CHECK(fdb::Error(1038).retryable()); // database_locked

	CHECK(!fdb::Error(1036).retryable()); // accessed_unreadable
	CHECK(!fdb::Error(2000).retryable()); // client_invalid_operation
	CHECK(!fdb::Error(2004).retryable()); // key_outside_legal_range
	CHECK(!fdb::Error(2005).retryable()); // inverted_range
	CHECK(!fdb::Error(2006).retryable()); // invalid_option_value
	CHECK(!fdb::Error(2007).retryable()); // invalid_option
	CHECK(!fdb::Error(2011).retryable()); // version_invalid
	CHECK(!fdb::Error(2020).retryable()); // transaction_invalid_version
	CHECK(!fdb::Error(2023).retryable()); // transaction_read_only
	CHECK(!fdb::Error(2100).retryable()); // incompatible_protocol_version
	CHECK(!fdb::Error(2101).retryable()); // transaction_too_large
	CHECK(!fdb::Error(2102).retryable()); // key_too_large
	CHECK(!fdb::Error(2103).retryable()); // value_too_large
	CHECK(!fdb::Error(2108).retryable()); // unsupported_operation
	CHECK(!fdb::Error(2200).retryable()); // api_version_unset
	CHECK(!fdb::Error(4000).retryable()); // unknown_error
	CHECK(!fdb::Error(4001).retryable()); // internal_error

	CHECK(fdb::Error(1021).maybeCommitted()); // commit_unknown_result

	CHECK(!fdb::Error(1000).maybeCommitted()); // operation_failed
	CHECK(!fdb::Error(1004).maybeCommitted()); // timed_out
	CHECK(!fdb::Error(1025).maybeCommitted()); // transaction_cancelled
	CHECK(!fdb::Error(1038).maybeCommitted()); // database_locked
	CHECK(!fdb::Error(1101).maybeCommitted()); // operation_cancelled
	CHECK(!fdb::Error(2002).maybeCommitted()); // commit_read_incomplete

	CHECK(fdb::Error(1007).retryableNotCommitted()); // transaction_too_old
	CHECK(fdb::Error(1020).retryableNotCommitted()); // not_committed
	CHECK(fdb::Error(1038).retryableNotCommitted()); // database_locked

	CHECK(!fdb::Error(1021).retryableNotCommitted()); // commit_unknown_result
	CHECK(!fdb::Error(1025).retryableNotCommitted()); // transaction_cancelled
	CHECK(!fdb::Error(1031).retryableNotCommitted()); // transaction_timed_out
	CHECK(!fdb::Error(1040).retryableNotCommitted()); // proxy_memory_limit_exceeded
}

TEST_CASE("block_from_callback") {
	auto tr = db.createTransaction();
	auto f1 = tr.get("foo"_br, /*snapshot*/ true);
	struct Context {
		FdbEvent event;
		fdb::Transaction* tr;
	};
	Context context;
	context.tr = &tr;
	f1.then([&context](fdb::Future f) {
		auto f2 = context.tr->get("bar"_br, /*snapshot*/ true);
		auto err = f2.blockUntilReady();
		if (err) {
			CHECK(err.code() == /*blocked_from_network_thread*/ 2026);
		}
		context.event.set();
	});
	context.event.wait();
}

// monitors network busyness for 2 sec (40 readings)
TEST_CASE("monitor_network_busyness") {
	bool containsGreaterZero = false;
	for (int i = 0; i < 40; i++) {
		double busyness = db.getMainThreadBusyness();
		// make sure the busyness is between 0 and 1
		CHECK(busyness >= 0);
		CHECK(busyness <= 1);
		if (busyness > 0) {
			containsGreaterZero = true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}

	// assert that at least one of the busyness readings was greater than 0
	CHECK(containsGreaterZero);
}

// Commit a transaction and confirm it has not been reset
TEST_CASE("commit_does_not_reset") {
	auto tr = db.createTransaction();
	auto tr2 = db.createTransaction();

	// Commit two transactions, one that will fail with conflict and the other
	// that will succeed. Ensure both transactions are not reset at the end.
	while (1) {
		auto tr1GrvFuture = tr.getReadVersion();
		auto err = waitFuture(tr1GrvFuture);
		if (err) {
			auto tr1OnErrorFuture = tr.onError(err);
			fdbCheck(waitFuture(tr1OnErrorFuture));
			continue;
		}

		int64_t tr1StartVersion = tr1GrvFuture.get();

		auto tr2GrvFuture = tr2.getReadVersion();
		err = waitFuture(tr2GrvFuture);
		if (err) {
			auto tr2OnErrorFuture = tr2.onError(err);
			fdbCheck(waitFuture(tr2OnErrorFuture));
			continue;
		}

		int64_t tr2StartVersion = tr2GrvFuture.get();

		tr.set(fdb::toBytesRef(key("foo")), "bar"_br);
		auto tr1CommitFuture = tr.commit();
		err = waitFuture(tr1CommitFuture);
		if (err) {
			auto tr1OnErrorFuture = tr.onError(err);
			fdbCheck(waitFuture(tr1OnErrorFuture));
			continue;
		}

		tr2.addConflictRange(
		    fdb::toBytesRef(key("foo")), fdb::toBytesRef(strinc_str(key("foo"))), FDB_CONFLICT_RANGE_TYPE_READ);
		tr2.set(fdb::toBytesRef(key("foo")), "bar"_br);
		auto tr2CommitFuture = tr2.commit();
		err = waitFuture(tr2CommitFuture);
		CHECK(err.code() == 1020); // not_committed

		auto tr1GrvFuture2 = tr.getReadVersion();
		err = waitFuture(tr1GrvFuture2);
		if (err) {
			auto tr1OnErrorFuture = tr.onError(err);
			fdbCheck(waitFuture(tr1OnErrorFuture));
			continue;
		}

		int64_t tr1EndVersion = tr1GrvFuture2.get();

		auto tr2GrvFuture2 = tr2.getReadVersion();
		err = waitFuture(tr2GrvFuture2);
		if (err) {
			auto tr2OnErrorFuture = tr2.onError(err);
			fdbCheck(waitFuture(tr2OnErrorFuture));
			continue;
		}

		int64_t tr2EndVersion = tr2GrvFuture2.get();

		// If we reset the transaction, then the read version will change
		CHECK(tr1StartVersion == tr1EndVersion);
		CHECK(tr2StartVersion == tr2EndVersion);
		break;
	}
}

TEST_CASE("Fast alloc thread cleanup") {
	// Try to cause an OOM if thread cleanup doesn't work
	for (int i = 0; i < 50000; ++i) {
		auto thread = std::thread([]() {
			auto tr = db.createTransaction();
			for (int s = 0; s < 11; ++s) {
				tr.set(fdb::toBytesRef(key("foo")), fdb::toBytesRef(std::string(8 << s, '\x00')));
			}
		});
		thread.join();
	}
}

TEST_CASE("Tenant create, access, and delete") {
	std::string tenantName = "tenant";
	std::string testKey = "foo";
	std::string testValue = "bar";

	auto tr = db.createTransaction();
	while (1) {
		tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr.set(fdb::toBytesRef("\xff\xff/management/tenant/map/" + tenantName), ""_br);
		auto commitFuture = tr.commit();
		auto err = waitFuture(commitFuture);
		if (err) {
			auto f = tr.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr.reset();
		break;
	}

	while (1) {
		std::string begin = "\xff\xff/management/tenant/map/";
		std::string end = "\xff\xff/management/tenant/map0";

		tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
		auto f = tr.getRange(fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(begin)),
		                     fdb::key_select::firstGreaterOrEqual(fdb::toBytesRef(end)),
		                     /* limit */ 0,
		                     /* target_bytes */ 0,
		                     /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
		                     /* iteration */ 0,
		                     /* snapshot */ false,
		                     /* reverse */ false);

		auto err = waitFuture(f);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f.get();
		auto outKv = std::get<0>(output);
		auto outCount = std::get<1>(output);

		CHECK(outCount == 1);
		CHECK(StringRef(outKv[0].key().begin(), outKv[0].key().size()) == StringRef(tenantName).withPrefix(begin));

		tr.reset();
		break;
	}

	auto tenant = db.openTenant(fdb::toBytesRef(tenantName));
	auto tr2 = tenant.createTransaction();

	while (1) {
		tr2.set(fdb::toBytesRef(testKey), fdb::toBytesRef(testValue));
		auto commitFuture = tr2.commit();
		auto err = waitFuture(commitFuture);
		if (err) {
			auto f = tr2.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr2.reset();
		break;
	}

	while (1) {
		auto f1 = tr2.get(fdb::toBytesRef(testKey), false);
		auto err = waitFuture(f1);
		if (err) {
			auto f2 = tr2.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto val = f1.get();
		CHECK(val.has_value());
		CHECK(testValue == std::string(val->begin(), val->end()));

		tr2.clear(fdb::toBytesRef(testKey));
		auto commitFuture = tr2.commit();
		err = waitFuture(commitFuture);
		if (err) {
			auto f = tr2.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr2.reset();
		break;
	}

	while (1) {
		tr.setOption(FDB_TR_OPTION_SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr.clear(fdb::toBytesRef("\xff\xff/management/tenant/map/" + tenantName));
		auto commitFuture = tr.commit();
		auto err = waitFuture(commitFuture);
		if (err) {
			auto f = tr.onError(err);
			fdbCheck(waitFuture(f));
			continue;
		}
		tr.reset();
		break;
	}

	while (1) {
		auto f1 = tr2.get(fdb::toBytesRef(testKey), false);
		auto err = waitFuture(f1);
		if (err.code() == error_code_tenant_not_found) {
			tr2.reset();
			break;
		}
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}
	}
}

int64_t granule_start_load_fail(const char* filename,
                                int filenameLength,
                                int64_t offset,
                                int64_t length,
                                int64_t fullFileLength,
                                void* userContext) {
	CHECK(false);
	return -1;
}

uint8_t* granule_get_load_fail(int64_t loadId, void* userContext) {
	CHECK(false);
	return nullptr;
}

void granule_free_load_fail(int64_t loadId, void* userContext) {
	CHECK(false);
}

TEST_CASE("Blob Granule Functions") {
	auto confValue =
	    getValue("\xff/conf/blob_granules_enabled"_br, /* snapshot */ false, { FDB_TR_OPTION_READ_SYSTEM_KEYS });
	if (!confValue.has_value() || confValue.value() != "1") {
		// std::cout << "skipping blob granule test" << std::endl;
		return;
	}

	// write some data
	insert_data(db, create_data({ { "bg1", "a" }, { "bg2", "b" }, { "bg3", "c" } }));

	// because wiring up files is non-trivial, just test the calls complete with the expected no_materialize error
	fdb::native::FDBReadBlobGranuleContext granuleContext;
	granuleContext.userContext = nullptr;
	granuleContext.start_load_f = &granule_start_load_fail;
	granuleContext.get_load_f = &granule_get_load_fail;
	granuleContext.free_load_f = &granule_free_load_fail;
	granuleContext.debugNoMaterialize = true;
	granuleContext.granuleParallelism = 1;

	auto tr = db.createTransaction();
	int64_t originalReadVersion = -1;

	// test no materialize gets error but completes, save read version
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		// -2 is latest version
		auto r = tr.readBlobGranules(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 0, -2, granuleContext);
		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);

		// If read done, save read version. Should have already used read version so this shouldn't error
		auto grvFuture = tr.getReadVersion();
		auto grvErr = waitFuture(grvFuture);
		CHECK(!grvErr);
		CHECK(!grvFuture.getNothrow(originalReadVersion));
		CHECK(originalReadVersion > 0);

		tr.reset();
		break;
	}

	// test with begin version > 0
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		// -2 is latest version, read version should be >= originalReadVersion
		auto r = tr.readBlobGranules(
		    fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), originalReadVersion, -2, granuleContext);

		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);
		tr.reset();
		break;
	}

	// test with prior read version completes after delay larger than normal MVC window
	// TODO: should we not do this?
	std::this_thread::sleep_for(std::chrono::milliseconds(6000));
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		auto r = tr.readBlobGranules(
		    fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 0, originalReadVersion, granuleContext);

		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);
		tr.reset();
		break;
	}

	// test ranges

	while (1) {
		auto f = tr.getBlobGranuleRanges(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 1000);
		auto err = waitFuture(f);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto output = f.get();
		auto outKr = std::get<0>(output);
		auto outCount = std::get<1>(output);

		CHECK(fdb::toCharsRef(outKr[0].beginKey()) <= key("bg"));
		CHECK(fdb::toCharsRef(outKr[outCount - 1].endKey()) >= key("bh"));

		CHECK(outCount >= 1);
		// check key ranges are in order
		for (int i = 0; i < outCount; i++) {
			// key range start < end
			CHECK(fdb::toCharsRef(outKr[i].beginKey()) < fdb::toCharsRef(outKr[i].endKey()));
		}
		// Ranges themselves are sorted and contiguous
		for (int i = 0; i < outCount - 1; i++) {
			CHECK(fdb::toCharsRef(outKr[i].endKey()) == fdb::toCharsRef(outKr[i + 1].beginKey()));
		}

		tr.reset();
		break;
	}

	// do a purge + wait at that version to purge everything before originalReadVersion

	auto purgeKeyFuture =
	    db.purgeBlobGranules(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), originalReadVersion, false);
	fdbCheck(waitFuture(purgeKeyFuture));

	fdb::KeyRef purgeKey = purgeKeyFuture.get();

	auto waitPurgeFuture = db.waitPurgeGranulesComplete(purgeKey);
	fdbCheck(waitFuture(waitPurgeFuture));

	// re-read again at the purge version to make sure it is still valid
	while (1) {
		tr.setOption(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE);
		auto r = tr.readBlobGranules(
		    fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), 0, originalReadVersion, granuleContext);
		fdb::future_var::KeyValueRefArray::Type out;
		auto err = r.getKeyValueArrayNothrow(out);
		if (err && err.code() != 2037 /* blob_granule_not_materialized */) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		CHECK(err.code() == 2037 /* blob_granule_not_materialized */);
		tr.reset();
		break;
	}

	// check granule summary
	while (1) {
		auto f =
		    tr.summarizeBlobGranules(fdb::toBytesRef(key("bg")), fdb::toBytesRef(key("bh")), originalReadVersion, 100);
		auto err = waitFuture(f);
		if (err) {
			auto f2 = tr.onError(err);
			fdbCheck(waitFuture(f2));
			continue;
		}

		auto out = f.get();
		auto out_summaries = std::get<0>(out);
		auto out_count = std::get<1>(out);

		CHECK(out_count >= 1);
		CHECK(out_count <= 100);

		// check that ranges cover requested range
		CHECK(fdb::toCharsRef(out_summaries[0].beginKey()) <= key("bg"));
		CHECK(fdb::toCharsRef(out_summaries[out_count - 1].endKey()) >= key("bh"));

		// check key ranges are in order
		for (int i = 0; i < out_count; i++) {
			// key range start < end
			CHECK(fdb::toCharsRef(out_summaries[i].beginKey()) < fdb::toCharsRef(out_summaries[i].endKey()));
			//// sanity check versions and sizes
			CHECK(out_summaries[i].snapshot_version <= originalReadVersion);
			CHECK(out_summaries[i].delta_version <= originalReadVersion);
			CHECK(out_summaries[i].snapshot_version <= out_summaries[i].delta_version);
			CHECK(out_summaries[i].snapshot_size > 0);
			CHECK(out_summaries[i].delta_size >= 0);
		}

		// Ranges themselves are sorted and contiguous
		for (int i = 0; i < out_count - 1; i++) {
			CHECK(fdb::toCharsRef(out_summaries[i].endKey()) == fdb::toCharsRef(out_summaries[i + 1].beginKey()));
		}

		tr.reset();
		break;
	}
}

int main(int argc, char** argv) {
	if (argc < 3) {
		std::cout << "Unit tests for the FoundationDB C API.\n"
		          << "Usage: fdb_c_unit_tests /path/to/cluster_file key_prefix [externalClient] [doctest args]"
		          << std::endl;
		return 1;
	}
	fdb::selectApiVersion(FDB_API_VERSION);
	if (argc >= 4) {
		std::string externalClientLibrary = argv[3];
		if (externalClientLibrary.substr(0, 2) != "--") {
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_DISABLE_LOCAL_CLIENT);
			fdb::network::setOption(FDBNetworkOption::FDB_NET_OPTION_EXTERNAL_CLIENT_LIBRARY,
			                                        externalClientLibrary);
		}
	}

	/* fdb_check(fdb_network_set_option( */
	/*     FDBNetworkOption::FDB_NET_OPTION_CLIENT_BUGGIFY_ENABLE, reinterpret_cast<const uint8_t*>(""), 0)); */

	doctest::Context context;
	context.applyCommandLine(argc, argv);

	fdb::network::setup();
	std::thread network_thread{ [] { fdbCheck(fdb::network::run()); } };

	db = fdb::Database(argv[1]);
	clusterFilePath = std::string(argv[1]);
	prefix = argv[2];
	int res = context.run();

	if (context.shouldExit()) {
		fdbCheck(fdb::network::stop());
		network_thread.join();
		return res;
	}
	fdbCheck(fdb::network::stop());
	network_thread.join();

	return res;
}
