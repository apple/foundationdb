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
#include "TesterUtil.h"
#include <cstdint>
#include <fmt/format.h>

namespace FdbApiTester {

namespace {

void fdb_check(fdb_error_t e) {
	if (e) {
		fmt::print(stderr, "Unexpected error: %s\n", fdb_get_error(e));
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