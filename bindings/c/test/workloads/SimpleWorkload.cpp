/*
 * workloads.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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
#define FDB_API_VERSION 610
#include "foundationdb/fdb_c.h"

namespace {

struct SimpleWorkload : FDBWorkload {
	static const std::string name;
	FDBWorkloadContext* context = nullptr;
	unsigned long numTuples;
	unsigned long numActors;
	unsigned long insertsPerTx;

	struct PopulateActor {
		enum class State {};

		SimpleWorkload& self;
		FDBDatabase* db;
		unsigned long from, to;
		FDBTransaction* tx = nullptr;
		fdb_error_t error = 0;
		State state;

		PopulateActor(SimpleWorkload& self, FDBDatabase* db, unsigned long from, unsigned long to)
		  : self(self), db(db) {
			error = fdb_database_create_transaction(db, &tx);
			if (error) {
				return;
			}
			body();
		}

		~PopulateActor() {
			if (tx) {
				fdb_transaction_destroy(tx);
			}
		}

		void body() {
		}

		static void callback(FDBFuture* future, void* cb) {
			// auto self = reinterpret_cast<PopulateActor*>(cb);
		}
	};

	std::string description() const override { return name; }
	bool init(FDBWorkloadContext* context) override {
		this->context = context;
		numTuples = context->getOption("numTuples", 100000ul);
		numActors = context->getOption("numActors", 100ul);
		insertsPerTx = context->getOption("insertsPerTx", 100ul);
		return true;
	}
	void setup(FDBDatabase* db, GenericPromise<bool> done) override {}
	void start(FDBDatabase* db, GenericPromise<bool> done) override {}
	void check(FDBDatabase* db, GenericPromise<bool> done) override {}
	void getMetrics(std::vector<FDBPerfMetric>& out) const override {}
};

const std::string SimpleWorkload::name = "SimpleWorkload";

} // namespace

FDBWorkloadFactoryT<SimpleWorkload> simpleWorkload(SimpleWorkload::name);
