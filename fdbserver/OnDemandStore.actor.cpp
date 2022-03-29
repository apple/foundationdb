/*
 * OnDemandStore.actor.cpp
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

#include "fdbserver/OnDemandStore.h"
#include "flow/actorcompiler.h" // must be last include

ACTOR static Future<Void> onErr(Future<Future<Void>> e) {
	Future<Void> f = wait(e);
	wait(f);
	return Void();
}

void OnDemandStore::open() {
	platform::createDirectory(folder);
	store = keyValueStoreMemory(joinPath(folder, prefix), myID, 500e6);
	err.send(store->getError());
}

OnDemandStore::OnDemandStore(std::string const& folder, UID myID, std::string const& prefix)
  : folder(folder), myID(myID), store(nullptr), prefix(prefix) {}

OnDemandStore::~OnDemandStore() {
	close();
}

IKeyValueStore* OnDemandStore::get() {
	if (!store) {
		open();
	}
	return store;
}

bool OnDemandStore::exists() const {
	return store || fileExists(joinPath(folder, prefix + "0.fdq")) || fileExists(joinPath(folder, prefix + "1.fdq")) ||
	       fileExists(joinPath(folder, prefix + ".fdb"));
}

IKeyValueStore* OnDemandStore::operator->() {
	return get();
}

Future<Void> OnDemandStore::getError() const {
	return onErr(err.getFuture());
}

Future<Void> OnDemandStore::onClosed() const {
	return store->onClosed();
}

void OnDemandStore::dispose() {
	if (store) {
		store->dispose();
		store = nullptr;
	}
}
void OnDemandStore::close() {
	if (store) {
		store->close();
		store = nullptr;
	}
}
