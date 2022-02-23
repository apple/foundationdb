/*
 * SysTestCorrectnessWorkload.cpp
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
#include "SysTestWorkload.h"
#include <memory>
#include <optional>
#include <iostream>

namespace FDBSystemTester {

namespace {

class UpdateTxActor : public TransactionActorBase {
public:
	ValueFuture fGet;

	void start() override {
		fGet = tx()->get(dbKey("foo"), false);
		ctx()->continueAfter(fGet, [this]() { this->step1(); });
	}

	void step1() {
		std::optional<std::string_view> optStr = fGet.getValue();
		tx()->set(dbKey("foo"), optStr.value_or("bar"));
		commit();
	}

	void reset() override { fGet.reset(); }
};

} // namespace

class ApiCorrectnessWorkload : public WorkloadBase {
public:
	ApiCorrectnessWorkload() : numTxLeft(10) {}

	void start() override {
		schedule([this]() { nextTransaction(); });
	}

private:
	void nextTransaction() {
		if (numTxLeft > 0) {
			numTxLeft--;
			UpdateTxActor* tx = new UpdateTxActor();
			execTransaction(tx, [this, tx]() { transactionDone(tx); });
			std::cout << numTxLeft << " transactions left" << std::endl;
		} else {
			std::cout << "Last transaction completed" << std::endl;
		}
	}

	void transactionDone(UpdateTxActor* tx) {
		delete tx;
		nextTransaction();
	}

	int numTxLeft;
};

std::unique_ptr<IWorkload> createApiCorrectnessWorkload() {
	return std::make_unique<ApiCorrectnessWorkload>();
}

} // namespace FDBSystemTester