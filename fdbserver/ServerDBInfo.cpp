/*
 * ServerDBInfo.cpp
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

#include "fdbserver/ServerDBInfoRef.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/genericactors.actor.h"

void ServerDBInfoRef::addref() {
	reinterpret_cast<AsyncVar<ServerDBInfo>*>(impl)->addref();
}

void ServerDBInfoRef::delref() {
	reinterpret_cast<AsyncVar<ServerDBInfo>*>(impl)->delref();
}

ServerDBInfoRef::ServerDBInfoRef(void* impl) : impl(impl) {
	addref();
}
ServerDBInfoRef::ServerDBInfoRef(const ServerDBInfoRef& other) : impl(other.impl) {
	addref();
}
ServerDBInfoRef::ServerDBInfoRef(ServerDBInfoRef&& other) : impl(other.impl) {
	other.impl = nullptr;
}

ServerDBInfoRef::~ServerDBInfoRef() {
	delref();
}

ServerDBInfoRef& ServerDBInfoRef::operator=(const ServerDBInfoRef& other) {
	delref();
	impl = other.impl;
	addref();
	return *this;
}
ServerDBInfoRef& ServerDBInfoRef::operator=(ServerDBInfoRef&& other) {
	delref();
	impl = other.impl;
	other.impl = nullptr;
	return *this;
}

Reference<AsyncVar<ServerDBInfo>> ServerDBInfo::fromReference(const ServerDBInfoRef& ref) {
	return Reference<AsyncVar<ServerDBInfo>>::addRef(reinterpret_cast<AsyncVar<ServerDBInfo>*>(ref.impl));
}

ServerDBInfoRef ServerDBInfo::toReference(const Reference<AsyncVar<ServerDBInfo>>& ref) {
	return ServerDBInfoRef{ ref.getPtr() };
}
