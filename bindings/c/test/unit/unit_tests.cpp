/*
 * unit_tests.cpp
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

// Unit tests for the FoundationDB C API.

#define FDB_API_VERSION 620
#include <foundationdb/fdb_c.h>
#include <assert.h>
#include <string.h>

#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <vector>

#define DOCTEST_CONFIG_IMPLEMENT
#define DOCTEST_CONFIG_NO_EXCEPTIONS_BUT_WITH_ALL_ASSERTS
#define DOCTEST_CONFIG_NO_POSIX_SIGNALS
#include "doctest.h"

#include "fdb_api.hpp"

// Macros to replace bare key with prefixed key.
#define KEY(key) (const uint8_t *)(prefix + key).c_str()
#define KEYSIZE(key) (prefix + key).size()

void fdb_check(fdb_error_t e) {
  if (e) {
    std::cerr << fdb_get_error(e) << std::endl;
    std::abort();
  }
}

FDBDatabase *fdb_open_database(const char *clusterFile) {
  FDBDatabase *db;
  fdb_check(fdb_create_database(clusterFile, &db));
  return db;
}

static FDBDatabase *db = nullptr;
static std::string prefix;

// Blocks until the given future is ready, returning an error code if there was
// an issue.
fdb_error_t wait_future(fdb::Future &f) {
  fdb_check(f.block_until_ready());
  return f.get_error();
}

// Given a string s, returns the "lowest" string greater than any string that
// starts with s. Taken from
// https://github.com/apple/foundationdb/blob/e7d72f458c6a985fdfa677ae021f357d6f49945b/flow/flow.cpp#L223.
std::string strinc(const std::string &s) {
  int index = -1;
  for (index = s.size() - 1; index >= 0; --index) {
    if ((uint8_t)s[index] != 255) {
      break;
    }
  }

  assert(index >= 0);

  std::string r = s.substr(0, index + 1);
  char *p = r.data();
  p[r.size() - 1]++;
  return r;
}

TEST_CASE("strinc") {
  CHECK(strinc("a").compare("b") == 0);
  CHECK(strinc("y").compare("z") == 0);
  CHECK(strinc("!").compare("\"") == 0);
  CHECK(strinc("*").compare("+") == 0);
  CHECK(strinc("fdb").compare("fdc") == 0);
  CHECK(strinc("foundation database 6").compare("foundation database 7") == 0);

  char terminated[] = {'a', 'b', '\xff'};
  CHECK(strinc(std::string(terminated, 3)).compare("ac") == 0);
}

// Helper function to add `prefix` to all keys in the given map. Returns a new
// map.
std::map<std::string, std::string>
create_data(std::map<std::string, std::string> &&map) {
  std::map<std::string, std::string> out;
  for (const auto & [ key, val ] : map) {
    out[prefix + key] = val;
  }
  return out;
}

// Clears all data in the database, then inserts the given key value pairs.
void insert_data(FDBDatabase *db,
                 const std::map<std::string, std::string> &data) {
  fdb::Transaction tr(db);
  auto end_key = strinc(prefix);
  while (1) {
    tr.clear_range((const uint8_t *)prefix.c_str(), prefix.size(),
                   (const uint8_t *)end_key.c_str(), end_key.size());
    for (const auto & [ key, val ] : data) {
      tr.set((const uint8_t *)key.c_str(), key.size(),
             (const uint8_t *)val.c_str(), val.size());
    }

    fdb::EmptyFuture f1 = tr.commit();
    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }
}

// Get the value associated with `key_name` from the database. Accepts a list
// of transaction options to apply (values for options not supported). Returns
// an optional which will be populated with the result if one was found.
std::optional<std::string>
get_value(const uint8_t *key_name, int key_name_length, fdb_bool_t snapshot,
          std::vector<FDBTransactionOption> options) {
  fdb::Transaction tr(db);
  while (1) {
    for (auto &option : options) {
      fdb_check(tr.set_option(option, nullptr, 0));
    }
    fdb::ValueFuture f1 = tr.get(key_name, key_name_length, snapshot);

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int out_present;
    char *val;
    int vallen;
    fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));
    return out_present ? std::make_optional(std::string(val, vallen)) : std::nullopt;
  }
}

struct GetRangeResult {
  // List of key-value pairs in the range read.
  std::vector<std::pair<std::string, std::string>> kvs;
  // True if values remain in the key range requested.
  bool more;
};

// Helper function to get a range of kv pairs. Returns a GetRangeResult struct
// containing the results of the range read.
GetRangeResult
get_range(fdb::Transaction& tr, const uint8_t* begin_key_name,
          int begin_key_name_length, fdb_bool_t begin_or_equal,
          int begin_offset, const uint8_t* end_key_name,
          int end_key_name_length, fdb_bool_t end_or_equal, int end_offset,
          int limit, int target_bytes, FDBStreamingMode mode,
          int iteration, fdb_bool_t snapshot, fdb_bool_t reverse) {
  while (1) {
    fdb::KeyValueArrayFuture f1 = tr.get_range(
        begin_key_name, begin_key_name_length, begin_or_equal, begin_offset,
        end_key_name, end_key_name_length, end_or_equal, end_offset, limit,
        target_bytes, mode, iteration, snapshot, reverse);

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    const FDBKeyValue *out_kv;
    int out_count;
    fdb_bool_t out_more;
    fdb_check(f1.get(&out_kv, &out_count, &out_more));

    std::vector<std::pair<std::string, std::string>> results;
    for (int i = 0; i < out_count; ++i) {
      std::string key((const char *)out_kv[i].key, out_kv[i].key_length);
      std::string value((const char *)out_kv[i].value, out_kv[i].value_length);
      results.push_back(std::make_pair(key, value));
    }
    return GetRangeResult{results, out_more != 0};
  }
}

// Clears all data in the database.
void clear_data(FDBDatabase *db) {
  insert_data(db, {});
}

struct Event {
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
  fdb::Transaction tr(db);
  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ true);
  struct Context {
    Event event;
  };
  Context context;
  fdb_check(f1.set_callback(+[](FDBFuture *f1, void *param) {
                              auto *context =
                                  static_cast<Context *>(param);
                              context->event.set();
                            },
                            &context));
  fdb_check(f1.block_until_ready());
  context.event.wait();
}

TEST_CASE("fdb_future_cancel after future completion") {
  fdb::Transaction tr(db);

  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, false);
  fdb_check(f1.block_until_ready());
  // Should have no effect
  f1.cancel();

  int out_present;
  char *val;
  int vallen;
  fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));
}

TEST_CASE("fdb_future_is_ready") {
  fdb::Transaction tr(db);
  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, false);

  CHECK(!f1.is_ready());
  fdb_check(f1.block_until_ready());
  CHECK(f1.is_ready());
}

TEST_CASE("fdb_future_release_memory") {
  fdb::Transaction tr(db);

  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, false);
  fdb_check(f1.block_until_ready());

  int out_present;
  char *val;
  int vallen;
  fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));

  f1.release_memory();
  fdb_error_t err = f1.get(&out_present,
                                 (const uint8_t **)&val, &vallen);
  CHECK(err == 1102); // future_released
}

TEST_CASE("fdb_future_get_int64") {
  fdb::Transaction tr(db);
  while (1) {
    fdb::Int64Future f1 = tr.get_read_version();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int64_t rv;
    fdb_check(f1.get(&rv));
    CHECK(rv > 0);
    break;
  }
}

TEST_CASE("fdb_future_get_key") {
  insert_data(db,
              create_data({ { "a", "1" }, { "baz", "2" }, { "bar", "3" } }));

  fdb::Transaction tr(db);
  while (1) {
    fdb::KeyFuture f1 = tr.get_key(
        FDB_KEYSEL_FIRST_GREATER_THAN(KEY("a"), KEYSIZE("a")),
        /* snapshot */ false);

    const uint8_t *key;
    int keylen;
    CHECK(f1.get(&key, &keylen));

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    fdb_check(f1.get(&key, &keylen));

    std::string dbKey((const char *)key, keylen);
    CHECK(dbKey.compare(prefix + "bar") == 0);
    break;
  }
}

TEST_CASE("fdb_future_get_value") {
  insert_data(db, create_data({ { "foo", "bar" } }));

  fdb::Transaction tr(db);
  while (1) {
    fdb::ValueFuture f1 = tr.get(KEY("foo"), KEYSIZE("foo"),
                                 /* snapshot */ false);

    int out_present;
    char *val;
    int vallen;
    CHECK(f1.get(&out_present, (const uint8_t **)&val, &vallen));

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));

    CHECK(out_present);
    std::string dbValue(val, vallen);
    CHECK(dbValue.compare("bar") == 0);
    break;
  }
}

TEST_CASE("fdb_future_get_string_array") {
  insert_data(db, create_data({ { "foo", "bar" } }));

  fdb::Transaction tr(db);
  while (1) {
    fdb::StringArrayFuture f1 = tr.get_addresses_for_key(KEY("foo"),
                                                         KEYSIZE("foo"));

    const char **strings;
    int count;
    CHECK(f1.get(&strings, &count));

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    fdb_check(f1.get(&strings, &count));

    CHECK(count > 0);
    break;
  }
}

TEST_CASE("fdb_future_get_keyvalue_array") {
  std::map<std::string, std::string> data =
      create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
  insert_data(db, data);

  fdb::Transaction tr(db);
  while (1) {
    fdb::KeyValueArrayFuture f1 = tr.get_range(
        FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(KEY("a"), KEYSIZE("a")),
        FDB_KEYSEL_LAST_LESS_OR_EQUAL(KEY("c"), KEYSIZE("c")) + 1,
        /* limit */ 0, /* target_bytes */ 0,
        /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL, /* iteration */ 0,
        /* snapshot */ false, /* reverse */ 0);

    FDBKeyValue const *out_kv;
    int out_count;
    int out_more;
    CHECK(f1.get(&out_kv, &out_count, &out_more));

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    fdb_check(f1.get(&out_kv, &out_count, &out_more));

    CHECK(out_count > 0);
    CHECK(out_count <= 3);
    if (out_count < 3) {
      CHECK(out_more);
    }

    for (int i = 0; i < out_count; ++i) {
      FDBKeyValue kv = *out_kv++;

      std::string key((const char *)kv.key, kv.key_length);
      std::string value((const char *)kv.value, kv.value_length);

      CHECK(data[key].compare(value) == 0);
    }
    break;
  }
}

TEST_CASE("cannot read system key") {
  fdb::Transaction tr(db);

  std::string syskey("\xff/coordinators");
  fdb::ValueFuture f1 = tr.get((const uint8_t *)syskey.c_str(), syskey.size(),
                               /* snapshot */ false);

  fdb_error_t err = wait_future(f1);
  CHECK(err == 2004); // key_outside_legal_range
}

TEST_CASE("read system key") {
  fdb::Transaction tr(db);

  std::string syskey("\xff/coordinators");
  auto value = get_value((const uint8_t *)syskey.c_str(), syskey.size(),
                         /* snapshot */ false,
                         { FDB_TR_OPTION_READ_SYSTEM_KEYS });
  REQUIRE(value.has_value());
}

TEST_CASE("cannot write system key") {
  fdb::Transaction tr(db);

  std::string syskey("\xff\x02");
  tr.set((const uint8_t *)syskey.c_str(), syskey.size(),
         (const uint8_t *)"bar", 3);

  fdb::EmptyFuture f1 = tr.commit();
  fdb_error_t err = wait_future(f1);
  CHECK(err == 2004); // key_outside_legal_range
}

TEST_CASE("write system key") {
  fdb::Transaction tr(db);

  std::string syskey("\xff\x02");
  fdb_check(tr.set_option(FDB_TR_OPTION_ACCESS_SYSTEM_KEYS, nullptr, 0));
  tr.set((const uint8_t *)syskey.c_str(), syskey.size(),
         (const uint8_t *)"bar", 3);

  while (1) {
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value((const uint8_t *)syskey.c_str(), syskey.size(),
                         /* snapshot */ false,
                         { FDB_TR_OPTION_READ_SYSTEM_KEYS });
  REQUIRE(value.has_value());
  CHECK(value->compare("bar") == 0);
}

TEST_CASE("fdb_transaction read_your_writes") {
  fdb::Transaction tr(db);
  clear_data(db);

  while (1) {
    tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ false);

    // Read before committing, should read the initial write.
    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int out_present;
    char *val;
    int vallen;
    fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));

    CHECK(out_present);
    std::string value(val, vallen);
    CHECK(value.compare("bar") == 0);
    break;
  }
}

TEST_CASE("fdb_transaction_set_option read_your_writes_disable") {
  clear_data(db);

  fdb::Transaction tr(db);
  while (1) {
    fdb_check(tr.set_option(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0));
    tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ false);

    // Read before committing, shouldn't read the initial write because
    // read_your_writes is disabled.
    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int out_present;
    char *val;
    int vallen;
    fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));

    CHECK(!out_present);
    break;
  }
}

TEST_CASE("fdb_transaction_set_option snapshot_read_your_writes_enable") {
  clear_data(db);

  fdb::Transaction tr(db);
  while (1) {
    // Enable read your writes for snapshot reads.
    fdb_check(tr.set_option(FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE, nullptr, 0));
    tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ true);

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int out_present;
    char *val;
    int vallen;
    fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));

    CHECK(out_present);
    std::string value(val, vallen);
    CHECK(value.compare("bar") == 0);
    break;
  }
}

TEST_CASE("fdb_transaction_set_option snapshot_read_your_writes_disable") {
  clear_data(db);

  fdb::Transaction tr(db);
  while (1) {
    // Disable read your writes for snapshot reads.
    fdb_check(tr.set_option(FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE, nullptr, 0));
    tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ true);
    fdb::ValueFuture f2 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ false);

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f3 = tr.on_error(err);
      fdb_check(wait_future(f3));
      continue;
    }

    int out_present;
    char *val;
    int vallen;
    fdb_check(f1.get(&out_present, (const uint8_t **)&val, &vallen));

    CHECK(!out_present);

    // Non-snapshot reads should still read writes in the transaction.
    err = wait_future(f2);
    if (err) {
      fdb::EmptyFuture f3 = tr.on_error(err);
      fdb_check(wait_future(f3));
      continue;
    }
    fdb_check(f2.get(&out_present, (const uint8_t **)&val, &vallen));

    CHECK(out_present);
    std::string value(val, vallen);
    CHECK(value.compare("bar") == 0);
    break;
  }
}

TEST_CASE("fdb_transaction_set_option timeout") {
  fdb::Transaction tr(db);
  // Set smallest possible timeout, retry until a timeout occurs.
  int64_t timeout = 1;
  fdb_check(tr.set_option(FDB_TR_OPTION_TIMEOUT, (const uint8_t *)&timeout,
                          sizeof(timeout)));

  fdb_error_t err = 0;
  while (!err) {
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /* snapshot */ false);
    err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      err = wait_future(f2);
    }
  }
  CHECK(err == 1031); // transaction_timed_out
}

TEST_CASE("FDB_DB_OPTION_TRANSACTION_TIMEOUT") {
  // Set smallest possible timeout, retry until a timeout occurs.
  int64_t timeout = 1;
  fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_TIMEOUT,
                                    (const uint8_t *)&timeout,
                                    sizeof(timeout)));

  fdb::Transaction tr(db);
  fdb_error_t err = 0;
  while (!err) {
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /* snapshot */ false);
    err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      err = wait_future(f2);
    }
  }
  CHECK(err == 1031); // transaction_timed_out

  // Reset transaction timeout (disable timeout).
  timeout = 0;
  fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_TIMEOUT,
                                    (const uint8_t *)&timeout,
                                    sizeof(timeout)));
}

TEST_CASE("fdb_transaction_set_option size_limit too small") {
  fdb::Transaction tr(db);

  // Size limit must be at least 32 to be valid, so test a smaller size.
  int64_t size_limit = 31;
  fdb_check(tr.set_option(FDB_TR_OPTION_SIZE_LIMIT,
                          (const uint8_t *)&size_limit, sizeof(size_limit)));
  tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
  fdb::EmptyFuture f1 = tr.commit();

  CHECK(wait_future(f1) == 2006); // invalid_option_value
}

TEST_CASE("fdb_transaction_set_option size_limit too large") {
  fdb::Transaction tr(db);

  // Size limit must be less than or equal to 10,000,000.
  int64_t size_limit = 10000001;
  fdb_check(tr.set_option(FDB_TR_OPTION_SIZE_LIMIT,
                          (const uint8_t *)&size_limit, sizeof(size_limit)));
  tr.set((const uint8_t *)"foo", 3, (const uint8_t *)"bar", 3);
  fdb::EmptyFuture f1 = tr.commit();

  CHECK(wait_future(f1) == 2006); // invalid_option_value
}

TEST_CASE("fdb_transaction_set_option size_limit") {
  fdb::Transaction tr(db);

  int64_t size_limit = 32;
  fdb_check(tr.set_option(FDB_TR_OPTION_SIZE_LIMIT,
                          (const uint8_t *)&size_limit, sizeof(size_limit)));
  tr.set((const uint8_t *)"foo", 3,
         (const uint8_t *)"foundation database is amazing", 30);
  fdb::EmptyFuture f1 = tr.commit();

  CHECK(wait_future(f1) == 2101); // transaction_too_large
}

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
  fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
                                    (const uint8_t *)&size_limit,
                                    sizeof(size_limit)));

  fdb::Transaction tr(db);
  tr.set((const uint8_t *)"foo", 3,
         (const uint8_t *)"foundation database is amazing", 30);
  fdb::EmptyFuture f1 = tr.commit();

  CHECK(wait_future(f1) == 2101); // transaction_too_large

  // Set size limit back to default.
  size_limit = 10000000;
  fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_TRANSACTION_SIZE_LIMIT,
                                    (const uint8_t *)&size_limit,
                                    sizeof(size_limit)));
}

TEST_CASE("fdb_transaction_set_read_version old_version") {
  fdb::Transaction tr(db);

  tr.set_read_version(1);
  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ true);

  fdb_error_t err = wait_future(f1);
  CHECK(err == 1007); // transaction_too_old
}

TEST_CASE("fdb_transaction_set_read_version future_version") {
  fdb::Transaction tr(db);

  tr.set_read_version(1UL << 62);
  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ true);

  fdb_error_t err = wait_future(f1);
  CHECK(err == 1009); // future_version
}

TEST_CASE("fdb_transaction_get_range reverse") {
  std::map<std::string, std::string> data =
      create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
  insert_data(db, data);

  fdb::Transaction tr(db);
  auto result = get_range(
      tr, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(KEY("a"), KEYSIZE("a")),
      FDB_KEYSEL_LAST_LESS_OR_EQUAL(KEY("d"), KEYSIZE("d")) + 1, /* limit */ 0,
      /* target_bytes */ 0, /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL,
      /* iteration */ 0, /* snapshot */ false, /* reverse */ 1);

  CHECK(result.kvs.size() > 0);
  CHECK(result.kvs.size() <= 4);
  if (result.kvs.size() < 4) {
    CHECK(result.more);
  }

  // Read data in reverse order, keeping in mind that out_count might be
  // smaller than requested.
  auto it = data.rbegin();
  std::advance(it, data.size() - result.kvs.size());
  for (auto results_it = result.kvs.begin(); it != data.rend(); ++it) {
    std::string data_key = it->first;
    std::string data_value = it->second;

    auto [key, value] = *results_it++;

    CHECK(data_key.compare(key) == 0);
    CHECK(data[data_key].compare(value) == 0);
  }
}

// Flaky (or broken)
// TEST_CASE("fdb_transaction_get_range limit") {
//   std::map<std::string, std::string> data =
//       create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
//   insert_data(db, data);
// 
//   fdb::Transaction tr(db);
// 
//   FDBKeyValue const *out_kv;
//   int out_count;
//   int out_more;
//   get_range(tr, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(KEY("a"), KEYSIZE("a")),
//       FDB_KEYSEL_LAST_LESS_OR_EQUAL(KEY("d"), KEYSIZE("d")) + 1,
//       /* limit */ 2, /* target_bytes */ 0,
//       /* FDBStreamingMode */ FDB_STREAMING_MODE_WANT_ALL, /* iteration */ 0,
//       /* snapshot */ false, /* reverse */ 0, &out_kv, &out_count, &out_more);
// 
//   CHECK(out_count > 0);
//   CHECK(out_count <= 2);
//   if (out_count < 4) {
//     CHECK(out_more);
//   }
// 
//   for (int i = 0; i < out_count; ++i) {
//     FDBKeyValue kv = *out_kv++;
// 
//     std::string key((const char *)kv.key, kv.key_length);
//     std::string value((const char *)kv.value, kv.value_length);
// 
//     CHECK(data[key].compare(value) == 0);
//   }
// }

TEST_CASE("fdb_transaction_get_range FDB_STREAMING_MODE_EXACT") {
  std::map<std::string, std::string> data =
      create_data({ { "a", "1" }, { "b", "2" }, { "c", "3" }, { "d", "4" } });
  insert_data(db, data);

  fdb::Transaction tr(db);

  auto result = get_range(
      tr, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(KEY("a"), KEYSIZE("a")),
      FDB_KEYSEL_LAST_LESS_OR_EQUAL(KEY("d"), KEYSIZE("d")) + 1, /* limit */ 3,
      /* target_bytes */ 0, /* FDBStreamingMode */ FDB_STREAMING_MODE_EXACT,
      /* iteration */ 0, /* snapshot */ false, /* reverse */ 0);

  CHECK(result.kvs.size() == 3);
  CHECK(result.more);

  for (int i = 0; i < result.kvs.size(); ++i) {
    auto [key, value] = result.kvs[i];
    CHECK(data[key].compare(value) == 0);
  }
}

TEST_CASE("fdb_transaction_clear") {
  insert_data(db, create_data({ { "foo", "bar" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.clear(KEY("foo"), KEYSIZE("foo"));

    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(!value.has_value());
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_ADD") {
  insert_data(db, create_data({ { "foo", "a" } }));

  fdb::Transaction tr(db);
  int8_t param = 1;
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)&param,
                 sizeof(param), FDB_MUTATION_TYPE_ADD);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 1);
  CHECK(value->data()[0] == 'b'); // incrementing 'a' results in 'b'
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

  fdb::Transaction tr(db);
  char param[] = { 'a', 'd' };
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BIT_AND);
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)param, 2,
                 FDB_MUTATION_TYPE_BIT_AND);
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"e", 1,
                 FDB_MUTATION_TYPE_BIT_AND);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 1);
  CHECK(value->data()[0] == 96);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 2);
  CHECK(value->data()[0] == 97);
  CHECK(value->data()[1] == 0);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
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

  fdb::Transaction tr(db);
  char param[] = { 'a', 'd' };
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BIT_OR);
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)param, 2,
                 FDB_MUTATION_TYPE_BIT_OR);
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"d", 1,
                 FDB_MUTATION_TYPE_BIT_OR);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 1);
  CHECK(value->data()[0] == 99);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("cd") == 0);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
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

  fdb::Transaction tr(db);
  char param[] = { 'a', 'd' };
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BIT_XOR);
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)param, 2,
                 FDB_MUTATION_TYPE_BIT_XOR);
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"d", 1,
                 FDB_MUTATION_TYPE_BIT_XOR);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 1);
  CHECK(value->data()[0] == 0x3);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 2);
  CHECK(value->data()[0] == 0x3);
  CHECK(value->data()[1] == 0x64);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 1);
  CHECK(value->data()[0] == 0x5);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_COMPARE_AND_CLEAR") {
  // Atomically remove a key-value pair from the database based on a value
  // comparison.
  insert_data(db, create_data({ { "foo", "bar" }, { "fdb", "foundation" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"bar", 3,
                 FDB_MUTATION_TYPE_COMPARE_AND_CLEAR);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  CHECK(!value.has_value());

  value = get_value(KEY("fdb"), KEYSIZE("fdb"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("foundation") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_APPEND_IF_FITS") {
  // Atomically append a value to an existing key-value pair, or insert the
  // key-value pair if an existing key-value pair doesn't exist.
  insert_data(db, create_data({ { "foo", "f" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"db", 2,
                 FDB_MUTATION_TYPE_APPEND_IF_FITS);
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)"foundation", 10,
                 FDB_MUTATION_TYPE_APPEND_IF_FITS);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("fdb") == 0);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("foundation") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_MAX") {
  insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_MAX);
    // Value in database will be extended with zeros to match length of param.
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)"aa", 2,
                 FDB_MUTATION_TYPE_MAX);
    // Value in database will be truncated to match length of param.
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_MAX);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("b") == 0);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("aa") == 0);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("c") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_MIN") {
  insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_MIN);
    // Value in database will be extended with zeros to match length of param.
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)"aa", 2,
                 FDB_MUTATION_TYPE_MIN);
    // Value in database will be truncated to match length of param.
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_MIN);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("a") == 0);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->size() == 2);
  CHECK(value->data()[0] == 'b');
  CHECK(value->data()[1] == 0);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("b") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BYTE_MAX") {
  // The difference with FDB_MUTATION_TYPE_MAX is that strings will not be
  // extended/truncated so lengths match.
  insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "cba" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BYTE_MAX);
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)"cc", 2,
                 FDB_MUTATION_TYPE_BYTE_MAX);
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BYTE_MAX);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("b") == 0);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("cc") == 0);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("cba") == 0);
}

TEST_CASE("fdb_transaction_atomic_op FDB_MUTATION_TYPE_BYTE_MIN") {
  // The difference with FDB_MUTATION_TYPE_MIN is that strings will not be
  // extended/truncated so lengths match.
  insert_data(db, create_data({ { "foo", "a" }, { "bar", "b" }, { "baz", "abc" } }));

  fdb::Transaction tr(db);
  while (1) {
    tr.atomic_op(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BYTE_MIN);
    tr.atomic_op(KEY("bar"), KEYSIZE("bar"), (const uint8_t *)"aa", 2,
                 FDB_MUTATION_TYPE_BYTE_MIN);
    tr.atomic_op(KEY("baz"), KEYSIZE("baz"), (const uint8_t *)"b", 1,
                 FDB_MUTATION_TYPE_BYTE_MIN);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }
    break;
  }

  auto value = get_value(KEY("foo"), KEYSIZE("foo"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("a") == 0);

  value = get_value(KEY("bar"), KEYSIZE("bar"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("aa") == 0);

  value = get_value(KEY("baz"), KEYSIZE("baz"), /* snapshot */ false, {});
  REQUIRE(value.has_value());
  CHECK(value->compare("abc") == 0);
}

TEST_CASE("fdb_transaction_get_committed_version read_only") {
  // Read-only transaction should have a committed version of -1.
  fdb::Transaction tr(db);
  while (1) {
    fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ false);

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int64_t out_version;
    fdb_check(tr.get_committed_version(&out_version));
    CHECK(out_version == -1);
    break;
  }
}

TEST_CASE("fdb_transaction_get_committed_version") {
  fdb::Transaction tr(db);
  while (1) {
    tr.set(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"bar", 3);
    fdb::EmptyFuture f1 = tr.commit();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int64_t out_version;
    fdb_check(tr.get_committed_version(&out_version));
    CHECK(out_version >= 0);
    break;
  }
}

TEST_CASE("fdb_transaction_get_approximate_size") {
  fdb::Transaction tr(db);
  while (1) {
    tr.set(KEY("foo"), KEYSIZE("foo"), (const uint8_t *)"bar", 3);
    fdb::Int64Future f1 = tr.get_approximate_size();

    fdb_error_t err = wait_future(f1);
    if (err) {
      fdb::EmptyFuture f2 = tr.on_error(err);
      fdb_check(wait_future(f2));
      continue;
    }

    int64_t size;
    fdb_check(f1.get(&size));
    CHECK(size >= 3);
    break;
  }
}

TEST_CASE("fdb_transaction_watch read_your_writes_disable") {
  // Watches created on a transaction with the option READ_YOUR_WRITES_DISABLE
  // should return a watches_disabled error.
  fdb::Transaction tr(db);
  fdb_check(tr.set_option(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, nullptr, 0));
  fdb::EmptyFuture f1 = tr.watch(KEY("foo"), KEYSIZE("foo"));

  CHECK(wait_future(f1) == 1034); // watches_disabled
}

TEST_CASE("fdb_transaction_watch reset") {
  // Resetting (or destroying) an uncommitted transaction should cause watches
  // created by the transaction to fail with a transaction_cancelled error.
  fdb::Transaction tr(db);
  fdb::EmptyFuture f1 = tr.watch(KEY("foo"), KEYSIZE("foo"));
  tr.reset();
  CHECK(wait_future(f1) == 1025); // transaction_cancelled
}

TEST_CASE("fdb_transaction_watch max watches") {
  int64_t max_watches = 3;
  fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_MAX_WATCHES,
                                    (const uint8_t *)&max_watches, 8));

  fdb::Transaction tr(db);
  fdb::EmptyFuture f1 = tr.watch(KEY("a"), KEYSIZE("a"));
  fdb::EmptyFuture f2 = tr.watch(KEY("b"), KEYSIZE("b"));
  fdb::EmptyFuture f3 = tr.watch(KEY("c"), KEYSIZE("c"));
  fdb::EmptyFuture f4 = tr.watch(KEY("d"), KEYSIZE("d"));
  fdb::EmptyFuture f5 = tr.commit();

  CHECK(wait_future(f1) == 1032); // too_many_watches

  // Reset available number of watches.
  max_watches = 10000;
  fdb_check(fdb_database_set_option(db, FDB_DB_OPTION_MAX_WATCHES,
                                    (const uint8_t *)&max_watches, 8));
}

TEST_CASE("fdb_transaction_watch") {
  fdb::Transaction tr(db);

  struct Context {
    Event event;
  };
  Context context;

  while (1) {
    fdb::EmptyFuture f1 = tr.watch(KEY("foo"), KEYSIZE("foo"));
    fdb::EmptyFuture f2 = tr.commit();

    fdb_error_t err = wait_future(f2);
    if (err) {
      fdb::EmptyFuture f3 = tr.on_error(err);
      fdb_check(wait_future(f3));
      continue;
    }

    fdb_check(f1.set_callback(+[](FDBFuture *f1, void *param) {
                                auto *context =
                                    static_cast<Context *>(param);
                                context->event.set();
                              },
                              &context));
    break;
  }

  // Insert data for key "foo" to trigger the watch.
  insert_data(db, create_data({ { "foo", "bar" } }));
  context.event.wait();
}

TEST_CASE("fdb_transaction_cancel") {
  // Cannot use transaction after cancelling it...
  fdb::Transaction tr(db);
  tr.cancel();
  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /* snapshot */ false);
  CHECK(wait_future(f1) == 1025); // transaction_cancelled

  // ... until the transaction has been reset.
  tr.reset();
  fdb::ValueFuture f2 = tr.get((const uint8_t *)"foo", 3, /* snapshot */ false);
  fdb_check(wait_future(f2));
}

TEST_CASE("block_from_callback") {
  fdb::Transaction tr(db);
  fdb::ValueFuture f1 = tr.get((const uint8_t *)"foo", 3, /*snapshot*/ true);
  struct Context {
    Event event;
    fdb::Transaction *tr;
  };
  Context context;
  context.tr = &tr;
  fdb_check(f1.set_callback(
      +[](FDBFuture *f1, void *param) {
            auto *context = static_cast<Context *>(param);
            fdb::ValueFuture f2 = context->tr->get((const uint8_t *)"bar", 3,
                                                /*snapshot*/ true);
            fdb_error_t error = f2.block_until_ready();
            if (error) {
              CHECK(error == /*blocked_from_network_thread*/ 2025);
            }
            context->event.set();
          },
      &context));
  context.event.wait();
}

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cout << "Unit tests for the FoundationDB C API.\n"
              << "Usage: fdb_c_unit_tests /path/to/cluster_file key_prefix"
              << std::endl;
    return 1;
  }

  doctest::Context context;

  fdb_check(fdb_select_api_version(620));
  fdb_check(fdb_setup_network());
  std::thread network_thread{ &fdb_run_network };

  db = fdb_open_database(argv[1]);
  prefix = argv[2];
  int res = context.run();
  fdb_database_destroy(db);

  if (context.shouldExit()) {
    fdb_check(fdb_stop_network());
    network_thread.join();
    return res;
  }
  fdb_check(fdb_stop_network());
  network_thread.join();

  return res;
}
