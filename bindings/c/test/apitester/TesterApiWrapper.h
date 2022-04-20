/*
 * TesterApiWrapper.h
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

#pragma once

#ifndef APITESTER_API_WRAPPER_H
#define APITESTER_API_WRAPPER_H

#include <string_view>
#include <optional>
#include <memory>
#include <unordered_map>

#define FDB_API_VERSION 720
#include "bindings/c/foundationdb/fdb_c.h"

#undef ERROR
#define ERROR(name, number, description) enum { error_code_##name = number };

#include "flow/error_definitions.h"

#include "TesterUtil.h"

namespace FdbApiTester {

// Wrapper parent class to manage memory of an FDBFuture pointer. Cleans up
// FDBFuture when this instance goes out of scope.
class Future {
public:
	Future() = default;
	Future(FDBFuture* f);

	FDBFuture* fdbFuture() { return future_.get(); };

	fdb_error_t getError() const;
	explicit operator bool() const { return future_ != nullptr; };
	void reset();
	void cancel();

protected:
	std::shared_ptr<FDBFuture> future_;
};

class ValueFuture : public Future {
public:
	ValueFuture() = default;
	ValueFuture(FDBFuture* f) : Future(f) {}
	std::optional<std::string> getValue() const;
};

class KeyRangesFuture : public Future {
public:
	KeyRangesFuture() = default;
	KeyRangesFuture(FDBFuture* f) : Future(f) {}
	std::vector<KeyValue> getKeyRanges() const;
};

class Result {
public:
	Result() = default;
	Result(FDBResult* r);

	FDBResult* fdbResult() { return result_.get(); };

	fdb_error_t getError() const { return error_; }

	explicit operator bool() const { return result_ != nullptr; };

	fdb_error_t error_ = error_code_client_invalid_operation; // have to call getX function to set this

protected:
	std::shared_ptr<FDBResult> result_;
};

class KeyValuesResult : public Result {
public:
	KeyValuesResult() = default;
	KeyValuesResult(FDBResult* f) : Result(f) {}
	std::vector<KeyValue> getKeyValues(bool* more_out);
};

class Transaction {
public:
	Transaction() = default;
	Transaction(FDBTransaction* tx);
	ValueFuture get(std::string_view key, fdb_bool_t snapshot);
	void set(std::string_view key, std::string_view value);
	void clear(std::string_view key);
	void clearRange(std::string_view begin, std::string_view end);
	Future commit();
	void cancel();
	Future onError(fdb_error_t err);
	void reset();
	fdb_error_t setOption(FDBTransactionOption option);

	KeyValuesResult readBlobGranules(std::string_view begin, std::string_view end, const std::string& basePath);
	KeyRangesFuture getBlobGranuleRanges(std::string_view begin, std::string_view end);

private:
	std::shared_ptr<FDBTransaction> tx_;
};

class FdbApi {
public:
	static fdb_error_t setOption(FDBNetworkOption option, std::string_view value);
	static fdb_error_t setOption(FDBNetworkOption option, int64_t value);
	static fdb_error_t setOption(FDBNetworkOption option);
};

} // namespace FdbApiTester

#endif