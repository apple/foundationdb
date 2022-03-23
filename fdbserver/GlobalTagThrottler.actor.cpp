/*
 * TagThrottler.h
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

#include "fdbserver/TagThrottler.h"

class GlobalTagThrottlerImpl {
	Database db;
	UID id;

public:
	GlobalTagThrottlerImpl(Database db, UID id) : db(db), id(id) {}
	Future<Void> monitorThrottlingChanges() { return Void(); }
	void addRequests(TransactionTag tag, int count) {}
	uint64_t getThrottledTagChangeId() const { return 0; }
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() { return {}; }
	int64_t autoThrottleCount() const { return 0; }
	uint32_t busyReadTagCount() const { return 0; }
	uint32_t busyWriteTagCount() const { return 0; }
	int64_t manualThrottleCount() const { return 0; }
	bool isAutoThrottlingEnabled() const { return 0; }
	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const& ss) { return Void(); }
};

GlobalTagThrottler::GlobalTagThrottler(Database db, UID id) : impl(PImpl<GlobalTagThrottlerImpl>::create(db, id)) {}

GlobalTagThrottler::~GlobalTagThrottler() = default;

Future<Void> GlobalTagThrottler::monitorThrottlingChanges() {
	return impl->monitorThrottlingChanges();
}
void GlobalTagThrottler::addRequests(TransactionTag tag, int count) {
	return impl->addRequests(tag, count);
}
uint64_t GlobalTagThrottler::getThrottledTagChangeId() const {
	return impl->getThrottledTagChangeId();
}
PrioritizedTransactionTagMap<ClientTagThrottleLimits> GlobalTagThrottler::getClientRates() {
	return impl->getClientRates();
}
int64_t GlobalTagThrottler::autoThrottleCount() const {
	return impl->autoThrottleCount();
}
uint32_t GlobalTagThrottler::busyReadTagCount() const {
	return impl->busyReadTagCount();
}
uint32_t GlobalTagThrottler::busyWriteTagCount() const {
	return impl->busyWriteTagCount();
}
int64_t GlobalTagThrottler::manualThrottleCount() const {
	return impl->manualThrottleCount();
}
bool GlobalTagThrottler::isAutoThrottlingEnabled() const {
	return impl->isAutoThrottlingEnabled();
}
Future<Void> GlobalTagThrottler::tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
	return impl->tryUpdateAutoThrottling(ss);
}
