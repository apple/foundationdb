/*
 * workloads.cpp
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

#include "workloads.h"

FDBWorkloadFactoryImpl::~FDBWorkloadFactoryImpl() {}

std::map<std::string, IFDBWorkloadFactory*>& FDBWorkloadFactoryImpl::factories() {
	static std::map<std::string, IFDBWorkloadFactory*> _factories;
	return _factories;
}

std::shared_ptr<FDBWorkload> FDBWorkloadFactoryImpl::create(const std::string& name) {
	auto res = factories().find(name);
	if (res == factories().end()) {
		return nullptr;
	}
	return res->second->create();
}

FDBWorkloadFactory* workloadFactory(FDBLogger*) {
	static FDBWorkloadFactoryImpl impl;
	return &impl;
}
