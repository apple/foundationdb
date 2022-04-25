/*
 * TesterApiWrapper.cpp
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
#include "TesterApiWrapper.h"
#include <cstdint>
#include <fmt/format.h>
#include <fstream>

namespace FdbApiTester {

namespace {

void fdb_check(fdb_error_t e) {
	if (e) {
		fmt::print(stderr, "Unexpected error: {}\n", fdb_get_error(e));
		std::abort();
	}
}

} // namespace

Future::Future(FDBFuture* f) : future_(f, fdb_future_destroy) {}

void Future::reset() {
	future_.reset();
}

void Future::cancel() {
	ASSERT(future_);
	fdb_future_cancel(future_.get());
}

fdb_error_t Future::getError() const {
	ASSERT(future_);
	return fdb_future_get_error(future_.get());
}

std::optional<std::string> ValueFuture::getValue() const {
	ASSERT(future_);
	int out_present;
	const std::uint8_t* val;
	int vallen;
	fdb_check(fdb_future_get_value(future_.get(), &out_present, &val, &vallen));
	return out_present ? std::make_optional(std::string((const char*)val, vallen)) : std::nullopt;
}

std::vector<KeyValue> KeyRangesFuture::getKeyRanges() const {
	ASSERT(future_);

	int count;
	const FDBKeyRange* ranges;

	fdb_check(fdb_future_get_keyrange_array(future_.get(), &ranges, &count));
	std::vector<KeyValue> result;
	result.reserve(count);
	for (int i = 0; i < count; i++) {
		FDBKeyRange kr = *ranges++;
		KeyValue rkv;
		rkv.key = std::string((const char*)kr.begin_key, kr.begin_key_length);
		rkv.value = std::string((const char*)kr.end_key, kr.end_key_length);
		result.push_back(rkv);
	}

	return result;
}

Result::Result(FDBResult* r) : result_(r, fdb_result_destroy) {}

std::vector<KeyValue> KeyValuesResult::getKeyValues(bool* more_out) {
	ASSERT(result_);

	int count;
	const FDBKeyValue* kvs;
	int more;

	std::vector<KeyValue> result;

	error_ = fdb_result_get_keyvalue_array(result_.get(), &kvs, &count, &more);

	if (error_ != error_code_success) {
		return result;
	}

	result.reserve(count);
	for (int i = 0; i < count; i++) {
		FDBKeyValue kv = *kvs++;
		KeyValue rkv;
		rkv.key = std::string((const char*)kv.key, kv.key_length);
		rkv.value = std::string((const char*)kv.value, kv.value_length);
		result.push_back(rkv);
	}
	*more_out = more;

	return result;
}

// Given an FDBDatabase, initializes a new transaction.
Transaction::Transaction(FDBTransaction* tx) : tx_(tx, fdb_transaction_destroy) {}

ValueFuture Transaction::get(std::string_view key, fdb_bool_t snapshot) {
	ASSERT(tx_);
	return ValueFuture(fdb_transaction_get(tx_.get(), (const uint8_t*)key.data(), key.size(), snapshot));
}

void Transaction::set(std::string_view key, std::string_view value) {
	ASSERT(tx_);
	fdb_transaction_set(tx_.get(), (const uint8_t*)key.data(), key.size(), (const uint8_t*)value.data(), value.size());
}

void Transaction::clear(std::string_view key) {
	ASSERT(tx_);
	fdb_transaction_clear(tx_.get(), (const uint8_t*)key.data(), key.size());
}

void Transaction::clearRange(std::string_view begin, std::string_view end) {
	ASSERT(tx_);
	fdb_transaction_clear_range(
	    tx_.get(), (const uint8_t*)begin.data(), begin.size(), (const uint8_t*)end.data(), end.size());
}

Future Transaction::commit() {
	ASSERT(tx_);
	return Future(fdb_transaction_commit(tx_.get()));
}

void Transaction::cancel() {
	ASSERT(tx_);
	fdb_transaction_cancel(tx_.get());
}

Future Transaction::onError(fdb_error_t err) {
	ASSERT(tx_);
	return Future(fdb_transaction_on_error(tx_.get(), err));
}

void Transaction::reset() {
	ASSERT(tx_);
	fdb_transaction_reset(tx_.get());
}

fdb_error_t Transaction::setOption(FDBTransactionOption option) {
	ASSERT(tx_);
	return fdb_transaction_set_option(tx_.get(), option, reinterpret_cast<const uint8_t*>(""), 0);
}

class TesterGranuleContext {
public:
	std::unordered_map<int64_t, uint8_t*> loadsInProgress;
	int64_t nextId = 0;
	std::string basePath;

	~TesterGranuleContext() {
		// if there was an error or not all loads finished, delete data
		for (auto& it : loadsInProgress) {
			uint8_t* dataToFree = it.second;
			delete dataToFree;
		}
	}
};

static int64_t granule_start_load(const char* filename,
                                  int filenameLength,
                                  int64_t offset,
                                  int64_t length,
                                  int64_t fullFileLength,
                                  void* context) {

	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	int64_t loadId = ctx->nextId++;

	uint8_t* buffer = new uint8_t[length];
	std::ifstream fin(ctx->basePath + std::string(filename, filenameLength), std::ios::in | std::ios::binary);
	fin.seekg(offset);
	fin.read((char*)buffer, length);

	ctx->loadsInProgress.insert({ loadId, buffer });

	return loadId;
}

static uint8_t* granule_get_load(int64_t loadId, void* context) {
	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	return ctx->loadsInProgress.at(loadId);
}

static void granule_free_load(int64_t loadId, void* context) {
	TesterGranuleContext* ctx = (TesterGranuleContext*)context;
	auto it = ctx->loadsInProgress.find(loadId);
	uint8_t* dataToFree = it->second;
	delete dataToFree;

	ctx->loadsInProgress.erase(it);
}

KeyValuesResult Transaction::readBlobGranules(std::string_view begin,
                                              std::string_view end,
                                              const std::string& basePath) {
	ASSERT(tx_);

	TesterGranuleContext testerContext;
	testerContext.basePath = basePath;

	FDBReadBlobGranuleContext granuleContext;
	granuleContext.userContext = &testerContext;
	granuleContext.debugNoMaterialize = false;
	granuleContext.granuleParallelism = 1;
	granuleContext.start_load_f = &granule_start_load;
	granuleContext.get_load_f = &granule_get_load;
	granuleContext.free_load_f = &granule_free_load;

	return KeyValuesResult(fdb_transaction_read_blob_granules(tx_.get(),
	                                                          (const uint8_t*)begin.data(),
	                                                          begin.size(),
	                                                          (const uint8_t*)end.data(),
	                                                          end.size(),
	                                                          0 /* beginVersion */,
	                                                          -2 /* latest read version */,
	                                                          granuleContext));
}

KeyRangesFuture Transaction::getBlobGranuleRanges(std::string_view begin, std::string_view end) {
	ASSERT(tx_);
	return KeyRangesFuture(fdb_transaction_get_blob_granule_ranges(
	    tx_.get(), (const uint8_t*)begin.data(), begin.size(), (const uint8_t*)end.data(), end.size()));
}

fdb_error_t FdbApi::setOption(FDBNetworkOption option, std::string_view value) {
	return fdb_network_set_option(option, reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

fdb_error_t FdbApi::setOption(FDBNetworkOption option, int64_t value) {
	return fdb_network_set_option(option, reinterpret_cast<const uint8_t*>(&value), sizeof(value));
}

fdb_error_t FdbApi::setOption(FDBNetworkOption option) {
	return fdb_network_set_option(option, reinterpret_cast<const uint8_t*>(""), 0);
}

} // namespace FdbApiTester