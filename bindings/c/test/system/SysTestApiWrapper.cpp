/*
 * SysTestApiWrapper.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "SysTestApiWrapper.h"
#include <cstdint>
#include <iostream>

namespace FDBSystemTester {

namespace {

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cerr << fdb_get_error(e) << std::endl;
		std::abort();
	}
}

} // namespace

Future::~Future() {
	if (future_) {
		fdb_future_destroy(future_);
	}
}

void Future::reset() {
	if (future_) {
		fdb_future_destroy(future_);
		future_ = nullptr;
	}
}

fdb_error_t Future::getError() {
	return fdb_future_get_error(future_);
}

std::optional<std::string_view> ValueFuture::getValue() {
	int out_present;
	const std::uint8_t* val;
	int vallen;
	fdb_check(fdb_future_get_value(future_, &out_present, &val, &vallen));
	return out_present ? std::make_optional(std::string((const char*)val, vallen)) : std::nullopt;
}

// Given an FDBDatabase, initializes a new transaction.
Transaction::Transaction(FDBTransaction* tx) : tx_(tx) {}

ValueFuture Transaction::get(std::string_view key, fdb_bool_t snapshot) {
	return ValueFuture(fdb_transaction_get(tx_, (const uint8_t*)key.data(), key.size(), snapshot));
}

void Transaction::set(std::string_view key, std::string_view value) {
	fdb_transaction_set(tx_, (const uint8_t*)key.data(), key.size(), (const uint8_t*)value.data(), value.size());
}

EmptyFuture Transaction::commit() {
	return EmptyFuture(fdb_transaction_commit(tx_));
}

EmptyFuture Transaction::onError(fdb_error_t err) {
	return EmptyFuture(fdb_transaction_on_error(tx_, err));
}

Transaction::~Transaction() {
	fdb_transaction_destroy(tx_);
}

} // namespace FDBSystemTester