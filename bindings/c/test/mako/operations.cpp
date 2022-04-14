/*
 * operations.cpp
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

#include "blob_granules.hpp"
#include "operations.hpp"
#include "mako.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include "fdbclient/zipf.h"
#include <array>

extern thread_local mako::Logger logr;

namespace mako {

OpIterator getOpBegin(Arguments const& args) noexcept {
	for (auto op = 0; op < MAX_OP; op++) {
		if (isAbstractOp(op) || args.txnspec.ops[op][OP_COUNT] == 0)
			continue;
		return OpIterator{ op, 0, 0 };
	}
	return OpEnd;
}

OpIterator getOpNext(Arguments const& args, OpIterator current) noexcept {
	if (OpEnd == current)
		return OpEnd;
	auto [op, count, step] = current;
	assert(op < MAX_OP && !isAbstractOp(op));
	if (opTable[op].steps() > step + 1)
		return OpIterator{ op, count, step + 1 };
	count++;
	for (; op < MAX_OP; op++, count = 0) {
		if (isAbstractOp(op) || args.txnspec.ops[op][OP_COUNT] <= count)
			continue;
		return OpIterator{ op, count, 0 };
	}
	return OpEnd;
}

using namespace fdb;

inline int nextKey(Arguments const& args) {
	if (args.zipf)
		return zipfian_next();
	return urand(0, args.rows - 1);
}

char const* getOpName(int ops_code) {
	if (ops_code >= 0 && ops_code < MAX_OP)
		return opTable[ops_code].name().data();
	return "";
}

const std::array<Operation, MAX_OP> opTable{
	{ { "GRV",
	    { { StepKind::READ,
	        [](Transaction tx, Arguments const&, ByteString&, ByteString&, ByteString&) {
	            return tx.getReadVersion().eraseType();
	        },
	        [](Future f, Transaction, Arguments const&, ByteString&, ByteString&, ByteString&) {
	            if (f && !f.error()) {
		            f.get<future_var::Int64>();
	            }
	        } } },
	    false },
	  { "GET",
	    { { StepKind::READ,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	            const auto num = nextKey(args);
	            genKey(key, KEY_PREFIX, args, num);
	            return tx.get(key, false /*snapshot*/).eraseType();
	        },
	        [](Future f, Transaction, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	            if (f && !f.error()) {
		            f.get<future_var::Value>();
	            }
	        } } },
	    false },
	  { "GETRANGE",
	    { { StepKind::READ,
	        [](Transaction tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	            const auto num_begin = nextKey(args);
	            genKey(begin, KEY_PREFIX, args, num_begin);
	            auto num_end = num_begin + args.txnspec.ops[OP_GETRANGE][OP_RANGE] - 1;
	            if (num_end > args.rows - 1)
		            num_end = args.rows - 1;
	            genKey(end, KEY_PREFIX, args, num_end);
	            return tx
	                .getRange<key_select::Inclusive, key_select::Inclusive>(begin,
	                                                                        end,
	                                                                        0 /*limit*/,
	                                                                        0 /*target_bytes*/,
	                                                                        args.streaming_mode,
	                                                                        0 /*iteration*/,
	                                                                        false /*snapshot*/,
	                                                                        args.txnspec.ops[OP_GETRANGE][OP_REVERSE])
	                .eraseType();
	        },
	        [](Future f, Transaction, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	            if (f && !f.error()) {
		            f.get<future_var::KeyValueArray>();
	            }
	        } } },
	    false },
	  { "SGET",
	    { { StepKind::READ,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	            const auto num = nextKey(args);
	            genKey(key, KEY_PREFIX, args, num);
	            return tx.get(key, true /*snapshot*/).eraseType();
	        },
	        [](Future f, Transaction, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	            if (f && !f.error()) {
		            f.get<future_var::Value>();
	            }
	        } } },
	    false },
	  { "SGETRANGE",
	    { {

	        StepKind::READ,
	        [](Transaction tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	            const auto num_begin = nextKey(args);
	            genKey(begin, KEY_PREFIX, args, num_begin);
	            auto num_end = num_begin + args.txnspec.ops[OP_SGETRANGE][OP_RANGE] - 1;
	            if (num_end > args.rows - 1)
		            num_end = args.rows - 1;
	            genKey(end, KEY_PREFIX, args, num_end);
	            return tx
	                .getRange<key_select::Inclusive, key_select::Inclusive>(begin,
	                                                                        end,
	                                                                        0 /*limit*/,
	                                                                        0 /*target_bytes*/,
	                                                                        args.streaming_mode,
	                                                                        0 /*iteration*/,
	                                                                        true /*snapshot*/,
	                                                                        args.txnspec.ops[OP_SGETRANGE][OP_REVERSE])
	                .eraseType();
	        },
	        [](Future f, Transaction, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	            if (f && !f.error()) {
		            f.get<future_var::KeyValueArray>();
	            }
	        } } },
	    false },
	  { "UPDATE",
	    { { StepKind::READ,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	            const auto num = nextKey(args);
	            genKey(key, KEY_PREFIX, args, num);
	            return tx.get(key, false /*snapshot*/).eraseType();
	        },
	        [](Future f, Transaction, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	            if (f && !f.error()) {
		            f.get<future_var::Value>();
	            }
	        } },
	      { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	            randomString(value, args.value_length);
	            tx.set(key, value);
	            return Future();
	        } } },
	    true },
	  { "INSERT",
	    { { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	            genKeyPrefix(key, KEY_PREFIX, args);
	            // concat([padding], key_prefix, random_string): reasonably unique
	            randomString<false /*clear-before-append*/>(key, args.key_length - static_cast<int>(key.size()));
	            randomString(value, args.value_length);
	            tx.set(key, value);
	            return Future();
	        } } },
	    true },
	  { "INSERTRANGE",
	    { { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	            genKeyPrefix(key, KEY_PREFIX, args);
	            const auto prefix_len = static_cast<int>(key.size());
	            const auto range = args.txnspec.ops[OP_INSERTRANGE][OP_RANGE];
	            assert(range > 0);
	            const auto range_digits = digits(range);
	            assert(args.key_length - prefix_len >= range_digits);
	            const auto rand_len = args.key_length - prefix_len - range_digits;
	            // concat([padding], prefix, random_string, range_digits)
	            randomString<false /*clear-before-append*/>(key, rand_len);
	            randomString(value, args.value_length);
	            for (auto i = 0; i < range; i++) {
		            fmt::format_to(std::back_inserter(key), "{0:0{1}d}", i, range_digits);
		            tx.set(key, value);
		            key.resize(key.size() - static_cast<size_t>(range_digits));
	            }
	            return Future();
	        } } },
	    true },
	  { "OVERWRITE",
	    { { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	            genKey(key, KEY_PREFIX, args, nextKey(args));
	            randomString(value, args.value_length);
	            tx.set(key, value);
	            return Future();
	        } } },
	    true },
	  { "CLEAR",
	    { { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	            genKey(key, KEY_PREFIX, args, nextKey(args));
	            tx.clear(key);
	            return Future();
	        } } },
	    true },
	  { "SETCLEAR",
	    { { StepKind::COMMIT,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	            genKeyPrefix(key, KEY_PREFIX, args);
	            const auto prefix_len = static_cast<int>(key.size());
	            randomString<false /*append-after-clear*/>(key, args.key_length - prefix_len);
	            randomString(value, args.value_length);
	            tx.set(key, value);
	            return tx.commit().eraseType();
	        } },
	      { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	            tx.reset(); // assuming commit from step 0 worked.
	            tx.clear(key); // key should forward unchanged from step 0
	            return Future();
	        } } },
	    true },
	  { "CLEARRANGE",
	    { { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	            const auto num_begin = nextKey(args);
	            genKey(begin, KEY_PREFIX, args, num_begin);
	            const auto range = args.txnspec.ops[OP_CLEARRANGE][OP_RANGE];
	            assert(range > 0);
	            genKey(end, KEY_PREFIX, args, std::min(args.rows - 1, num_begin + range - 1));
	            tx.clearRange(begin, end);
	            return Future();
	        } } },
	    true },
	  { "SETCLEARRANGE",
	    { { StepKind::COMMIT,
	        [](Transaction tx, Arguments const& args, ByteString& key_begin, ByteString& key, ByteString& value) {
	            genKeyPrefix(key, KEY_PREFIX, args);
	            const auto prefix_len = static_cast<int>(key.size());
	            const auto range = args.txnspec.ops[OP_SETCLEARRANGE][OP_RANGE];
	            assert(range > 0);
	            const auto range_digits = digits(range);
	            assert(args.key_length - prefix_len >= range_digits);
	            const auto rand_len = args.key_length - prefix_len - range_digits;
	            // concat([padding], prefix, random_string, range_digits)
	            randomString<false /*clear-before-append*/>(key, rand_len);
	            randomString(value, args.value_length);
	            for (auto i = 0; i <= range; i++) {
		            fmt::format_to(std::back_inserter(key), "{0:0{1}d}", i, range_digits);
		            if (i == range)
			            break; // preserve "exclusive last"
		            // preserve first key for step 1
		            if (i == 0)
			            key_begin = key;
		            tx.set(key, value);
		            // preserve last key for step 1
		            key.resize(key.size() - static_cast<size_t>(range_digits));
	            }
	            return tx.commit().eraseType();
	        } },
	      { StepKind::IMM,
	        [](Transaction tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	            tx.reset();
	            tx.clearRange(begin, end);
	            return Future();
	        } } },
	    true },
	  { "COMMIT", { { StepKind::NONE, nullptr } }, false },
	  { "TRANSACTION", { { StepKind::NONE, nullptr } }, false },
	  { "TASK", { { StepKind::NONE, nullptr } }, false },
	  { "READBLOBGRANULE",
	    { { StepKind::ON_ERROR,
	        [](Transaction tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	            const auto num_begin = nextKey(args);
	            genKey(begin, KEY_PREFIX, args, num_begin);
	            const auto range = args.txnspec.ops[OP_READ_BG][OP_RANGE];
	            assert(range > 0);
	            genKey(end, KEY_PREFIX, args, std::min(args.rows - 1, num_begin + range - 1));
	            auto err = Error{};

	            err = tx.setOptionNothrow(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, BytesRef());
	            if (err) {
		            // Issuing read/writes before disabling RYW results in error.
		            // Possible malformed workload?
		            // As workloads execute in sequence, retrying would likely repeat this error.
		            fmt::print(stderr, "ERROR: TR_OPTION_READ_YOUR_WRITES_DISABLE: {}", err.what());
		            return Future();
	            }

	            // Allocate a separate context per call to avoid multiple threads accessing
	            auto user_context = blob_granules::local_file::UserContext(args.bg_file_path);

	            auto api_context = blob_granules::local_file::createApiContext(user_context, args.bg_materialize_files);

	            auto r = tx.readBlobGranules(begin,
	                                         end,
	                                         0 /* beginVersion*/,
	                                         -2, /* endVersion. -2 (latestVersion) is use txn read version */
	                                         api_context);

	            user_context.clear();

	            auto out = Result::KeyValueArray{};
	            err = r.getKeyValueArrayNothrow(out);
	            if (!err || err.is(2037 /*blob_granule_not_materialized*/))
		            return Future();
	            const auto level = (err.is(1020 /*not_committed*/) || err.is(1021 /*commit_unknown_result*/) ||
	                                err.is(1213 /*tag_throttled*/))
	                                   ? VERBOSE_WARN
	                                   : VERBOSE_NONE;
	            logr.printWithLogLevel(level, "ERROR", "get_keyvalue_array() after readBlobGranules(): {}", err.what());
	            return tx.onError(err).eraseType();
	        } } },
	    false } }
};

} // namespace mako
