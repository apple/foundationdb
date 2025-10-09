/*
 * operations.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "operations.hpp"
#include "mako.hpp"
#include "logger.hpp"
#include "utils.hpp"
#include <array>

extern thread_local mako::Logger logr;

namespace mako {

using namespace fdb;

const std::array<Operation, MAX_OP> opTable{ {
	{ "GRV",
	  { { StepKind::READ,
	      [](Transaction& tx, Arguments const&, ByteString&, ByteString&, ByteString&) {
	          return tx.getReadVersion().eraseType();
	      },
	      [](Future& f, Transaction&, Arguments const&, ByteString&, ByteString&, ByteString&) {
	          if (f && !f.error()) {
		          f.get<future_var::Int64>();
	          }
	      } } },
	  1,
	  false },
	{ "GET",
	  { { StepKind::READ,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	          return tx.get(key, false /*snapshot*/).eraseType();
	      },
	      [](Future& f, Transaction&, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	          if (f && !f.error()) {
		          f.get<future_var::ValueRef>();
	          }
	      } } },
	  1,
	  false },
	{ "GETRANGE",
	  { { StepKind::READ,
	      [](Transaction& tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	          return tx
	              .getRange(key_select::firstGreaterOrEqual(begin),
	                        key_select::lastLessOrEqual(end, 1),
	                        0 /*limit*/,
	                        0 /*target_bytes*/,
	                        args.streaming_mode,
	                        0 /*iteration*/,
	                        false /*snapshot*/,
	                        args.txnspec.ops[OP_GETRANGE][OP_REVERSE])
	              .eraseType();
	      },
	      [](Future& f, Transaction&, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	          if (f && !f.error()) {
		          f.get<future_var::KeyValueRefArray>();
	          }
	      } } },
	  1,
	  false },
	{ "SGET",
	  { { StepKind::READ,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	          return tx.get(key, true /*snapshot*/).eraseType();
	      },
	      [](Future& f, Transaction&, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	          if (f && !f.error()) {
		          f.get<future_var::ValueRef>();
	          }
	      } } },
	  1,
	  false },
	{ "SGETRANGE",
	  { {

	      StepKind::READ,
	      [](Transaction& tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	          return tx
	              .getRange(key_select::firstGreaterOrEqual(begin),
	                        key_select::lastLessOrEqual(end, 1),
	                        0 /*limit*/,
	                        0 /*target_bytes*/,
	                        args.streaming_mode,
	                        0 /*iteration*/,
	                        true /*snapshot*/,
	                        args.txnspec.ops[OP_GETRANGE][OP_REVERSE])
	              .eraseType();
	      },
	      [](Future& f, Transaction&, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	          if (f && !f.error()) {
		          f.get<future_var::KeyValueRefArray>();
	          }
	      } } },
	  1,
	  false },
	{ "UPDATE",
	  { { StepKind::READ,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	          return tx.get(key, false /*snapshot*/).eraseType();
	      },
	      [](Future& f, Transaction&, Arguments const&, ByteString&, ByteString&, ByteString& val) {
	          if (f && !f.error()) {
		          f.get<future_var::ValueRef>();
	          }
	      } },
	    { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	          randomString(value.data(), args.value_length);
	          tx.set(key, value);
	          return Future();
	      } } },
	  2,
	  true },
	{ "INSERT",
	  { { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	          // key[0..args.key_length] := concat(key_prefix, random_string)
	          randomString(key.data() + intSize(KEY_PREFIX), args.key_length - intSize(KEY_PREFIX));
	          randomString(value.data(), args.value_length);
	          tx.set(key, value);
	          return Future();
	      } } },
	  1,
	  true },
	{ "INSERTRANGE",
	  { { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	          randomString(value.data(), args.value_length);

	          // key[0..args.key_length] := concat(prefix, random_string, num[0..range_digits])
	          const auto range = args.txnspec.ops[OP_INSERTRANGE][OP_RANGE];
	          assert(range > 0);
	          const auto range_digits = digits(range);
	          const auto random_len = args.key_length - intSize(KEY_PREFIX) - range_digits;
	          randomString(&key[intSize(KEY_PREFIX)], random_len);
	          for (auto i = 0; i < range; i++) {
		          numericWithFill(&key[args.key_length - range_digits], range_digits, i);
		          tx.set(key, value);
	          }
	          return Future();
	      } } },
	  1,
	  true },
	{ "OVERWRITE",
	  { { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	          randomString(value.data(), args.value_length);
	          tx.set(key, value);
	          return Future();
	      } } },
	  1,
	  true },
	{ "CLEAR",
	  { { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	          tx.clear(key);
	          return Future();
	      } } },
	  1,
	  true },
	{ "SETCLEAR",
	  { { StepKind::COMMIT,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString& value) {
	          randomString(&key[KEY_PREFIX.size()], args.key_length - intSize(KEY_PREFIX));
	          randomString(value.data(), args.value_length);
	          tx.set(key, value);
	          return tx.commit().eraseType();
	      } },
	    { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& key, ByteString&, ByteString&) {
	          tx.clear(key); // key should forward unchanged from step 0
	          return Future();
	      } } },
	  2,
	  true },
	{ "CLEARRANGE",
	  { { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	          tx.clearRange(begin, end);
	          return Future();
	      } } },
	  1,
	  true },
	{ "SETCLEARRANGE",
	  { { StepKind::COMMIT,
	      [](Transaction& tx, Arguments const& args, ByteString& key_begin, ByteString& key, ByteString& value) {
	          randomString(value.data(), args.value_length);

	          // key[0..args.key_length] := concat(prefix, random_string, num[0..range_digits])
	          const auto range = args.txnspec.ops[OP_SETCLEARRANGE][OP_RANGE];
	          assert(range > 0);
	          const auto range_digits = digits(range);
	          const auto random_len = args.key_length - intSize(KEY_PREFIX) - range_digits;
	          randomString(&key[KEY_PREFIX.size()], random_len);
	          for (auto i = 0; i < range; i++) {
		          numericWithFill(&key[args.key_length - range_digits], range_digits, i);
		          tx.set(key, value);
		          if (i == 0)
			          key_begin.assign(key);
	          }
	          return tx.commit().eraseType();
	      } },
	    { StepKind::IMM,
	      [](Transaction& tx, Arguments const& args, ByteString& begin, ByteString& end, ByteString&) {
	          tx.clearRange(begin, end);
	          return Future();
	      } } },
	  2,
	  true },
	{ "COMMIT", { { StepKind::NONE, nullptr } }, 0, false },
	{ "TRANSACTION", { { StepKind::NONE, nullptr } }, 0, false },
} };

} // namespace mako
