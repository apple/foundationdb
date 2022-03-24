/*
 * ISingleThreadTransaction.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ISingleThreadTransaction.h"
#include "fdbclient/PaxosConfigTransaction.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SimpleConfigTransaction.h"

ISingleThreadTransaction* ISingleThreadTransaction::allocateOnForeignThread(Type type) {
	if (type == Type::RYW) {
		auto tr = new ReadYourWritesTransaction;
		return tr;
	} else if (type == Type::SIMPLE_CONFIG) {
		auto tr = new SimpleConfigTransaction;
		return tr;
	} else if (type == Type::PAXOS_CONFIG) {
		auto tr = new PaxosConfigTransaction;
		return tr;
	}
	ASSERT(false);
	return nullptr;
}

Reference<ISingleThreadTransaction> ISingleThreadTransaction::create(Type type, Database const& cx) {
	Reference<ISingleThreadTransaction> result;
	if (type == Type::RYW) {
		result = makeReference<ReadYourWritesTransaction>();
	} else if (type == Type::SIMPLE_CONFIG) {
		result = makeReference<SimpleConfigTransaction>();
	} else {
		result = makeReference<PaxosConfigTransaction>();
	}
	result->construct(cx);
	return result;
}

Reference<ISingleThreadTransaction> ISingleThreadTransaction::create(Type type,
                                                                     Database const& cx,
                                                                     TenantName const& tenant) {
	Reference<ISingleThreadTransaction> result;
	if (type == Type::RYW) {
		result = makeReference<ReadYourWritesTransaction>();
	} else if (type == Type::SIMPLE_CONFIG) {
		result = makeReference<SimpleConfigTransaction>();
	} else {
		result = makeReference<PaxosConfigTransaction>();
	}
	result->construct(cx, tenant);
	return result;
}
