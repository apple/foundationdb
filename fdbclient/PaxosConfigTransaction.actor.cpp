/*
 * PaxosConfigTransaction.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/PaxosConfigTransaction.h"
#include "flow/actorcompiler.h" // must be last include

class PaxosConfigTransactionImpl {};

Future<Version> PaxosConfigTransaction::getReadVersion() {
	// TODO: Implement
	return ::invalidVersion;
}

Optional<Version> PaxosConfigTransaction::getCachedReadVersion() const {
	// TODO: Implement
	return ::invalidVersion;
}

Future<Optional<Value>> PaxosConfigTransaction::get(Key const& key, Snapshot snapshot) {
	// TODO: Implement
	return Optional<Value>{};
}

Future<Standalone<RangeResultRef>> PaxosConfigTransaction::getRange(KeySelector const& begin,
                                                                    KeySelector const& end,
                                                                    int limit,
                                                                    Snapshot snapshot,
                                                                    Reverse reverse) {
	// TODO: Implement
	ASSERT(false);
	return Standalone<RangeResultRef>{};
}

Future<Standalone<RangeResultRef>> PaxosConfigTransaction::getRange(KeySelector begin,
                                                                    KeySelector end,
                                                                    GetRangeLimits limits,
                                                                    Snapshot snapshot,
                                                                    Reverse reverse) {
	// TODO: Implement
	ASSERT(false);
	return Standalone<RangeResultRef>{};
}

void PaxosConfigTransaction::set(KeyRef const& key, ValueRef const& value) {
	// TODO: Implememnt
	ASSERT(false);
}

void PaxosConfigTransaction::clear(KeyRef const& key) {
	// TODO: Implememnt
	ASSERT(false);
}

Future<Void> PaxosConfigTransaction::commit() {
	// TODO: Implememnt
	ASSERT(false);
	return Void();
}

Version PaxosConfigTransaction::getCommittedVersion() const {
	// TODO: Implement
	ASSERT(false);
	return ::invalidVersion;
}

int64_t PaxosConfigTransaction::getApproximateSize() const {
	// TODO: Implement
	ASSERT(false);
	return 0;
}

void PaxosConfigTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	// TODO: Implement
	ASSERT(false);
}

Future<Void> PaxosConfigTransaction::onError(Error const& e) {
	// TODO: Implement
	ASSERT(false);
	return Void();
}

void PaxosConfigTransaction::cancel() {
	// TODO: Implement
	ASSERT(false);
}

void PaxosConfigTransaction::reset() {
	// TODO: Implement
	ASSERT(false);
}

void PaxosConfigTransaction::fullReset() {
	// TODO: Implement
	ASSERT(false);
}

void PaxosConfigTransaction::debugTransaction(UID dID) {
	// TODO: Implement
	ASSERT(false);
}

void PaxosConfigTransaction::checkDeferredError() const {
	// TODO: Implement
	ASSERT(false);
}

PaxosConfigTransaction::PaxosConfigTransaction(Database const& cx) {
	// TODO: Implement
	ASSERT(false);
}

PaxosConfigTransaction::~PaxosConfigTransaction() = default;
