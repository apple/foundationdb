/*
 * SimpleWorkload.cpp
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

#define FDB_USE_LATEST_API_VERSION
#include "foundationdb/fdb_c.h"
#undef DLLEXPORT
#include "workloads.h"

#include <unordered_map>
#include <functional>
#include <random>
#include <iostream>

namespace {

struct SimpleWorkload final : FDBWorkload {
	static const std::string name;
	static const std::string KEY_PREFIX;
	std::mt19937 random;
	bool success = true;
	FDBWorkloadContext* context = nullptr;
	unsigned long numTuples, numActors, insertsPerTx, opsPerTx;
	double runFor;

	// stats
	std::vector<double> gets, txs, retries;

	template <class Actor>
	struct ActorBase {
		using Callback = std::function<void(Actor*)>;
		Callback done;
		SimpleWorkload& self;
		FDBDatabase* db;
		fdb_error_t error = 0;
		FDBFuture* currentFuture = nullptr;
		int numWaiters = 0;

		ActorBase(const Callback& done, SimpleWorkload& self, FDBDatabase* db) : done(done), self(self), db(db) {}

		Actor* super() { return static_cast<Actor*>(this); }

		const Actor* super() const { return static_cast<const Actor*>(this); }

		template <class State>
		void wait(FDBFuture* future, State state) {
			if (++numWaiters != 1) {
				std::cerr << "More than one wait in one actor" << std::endl;
				std::terminate();
			}
			super()->state = state;
			currentFuture = future;
			if (fdb_future_is_ready(future)) {
				callback(future, this);
			} else {
				auto err = fdb_future_set_callback(future, &ActorBase<Actor>::callback, this);
				if (err) {
					auto self = static_cast<Actor*>(this);
					self->callbacks[self->state].onError(err);
					fdb_future_destroy(future);
				}
			}
		}

		static void callback(FDBFuture* future, void* data) {
			auto self = reinterpret_cast<Actor*>(data);
			--self->numWaiters;
			auto err = fdb_future_get_error(future);
			if (err) {
				self->callbacks[self->state].onError(fdb_future_get_error(future));
			} else {
				self->callbacks[self->state].onSuccess(future);
			}
			fdb_future_destroy(future);
		}
	};

	struct ActorCallback {
		std::function<void(FDBFuture*)> onSuccess;
		std::function<void(fdb_error_t)> onError;
	};

	struct PopulateActor : ActorBase<PopulateActor> {
		enum class State { Commit, Retry };
		State state;
		FDBTransaction* tx = nullptr;

		unsigned long from, to, lastTx = 0;
		std::unordered_map<State, ActorCallback> callbacks;

		PopulateActor(const Callback& promise,
		              SimpleWorkload& self,
		              FDBDatabase* db,
		              unsigned long from,
		              unsigned long to)
		  : ActorBase(promise, self, db), from(from), to(to) {
			error = fdb_database_create_transaction(db, &tx);
			if (error) {
				done(this);
			}
			setCallbacks();
		}

		~PopulateActor() {
			if (tx) {
				fdb_transaction_destroy(tx);
			}
		}

		void run() {
			if (error || from >= to) {
				done(this);
				return;
			}
			lastTx = 0;
			unsigned ops = 0;
			for (; from < to && ops < self.insertsPerTx; ++ops, ++from) {
				std::string value = std::to_string(from);
				std::string key = KEY_PREFIX + value;
				fdb_transaction_set(tx,
				                    reinterpret_cast<const uint8_t*>(key.c_str()),
				                    key.size(),
				                    reinterpret_cast<const uint8_t*>(value.c_str()),
				                    value.size());
			}
			lastTx = ops;
			auto commit_future = fdb_transaction_commit(tx);
			wait(commit_future, State::Commit);
		}

		void setCallbacks() {
			callbacks[State::Commit] = {
				[this](FDBFuture* future) {
				    fdb_transaction_reset(tx);
				    self.context->trace(FDBSeverity::Debug, "TXComplete", { { "NumInserts", std::to_string(lastTx) } });
				    lastTx = 0;
				    run();
				},
				[this](fdb_error_t error) { wait(fdb_transaction_on_error(tx, error), State::Retry); }
			};
			callbacks[State::Retry] = { [this](FDBFuture* future) {
				                           from -= lastTx;
				                           fdb_transaction_reset(tx);
				                           run();
				                       },
				                        [this](fdb_error_t error) {
				                            self.context->trace(FDBSeverity::Error,
				                                                "AssertionFailure",
				                                                { { "Reason", "tx.onError failed" },
				                                                  { "Error", std::string(fdb_get_error(error)) } });
				                            self.success = false;
				                            done(this);
				                        } };
		}
	};

	struct ClientActor : ActorBase<ClientActor> {
		enum class State { Get, Commit, Retry };
		State state;
		std::unordered_map<State, ActorCallback> callbacks;
		unsigned long ops = 0;
		std::uniform_int_distribution<decltype(SimpleWorkload::numTuples)> random;
		FDBTransaction* tx = nullptr;

		unsigned numCommits = 0;
		unsigned numRetries = 0;
		unsigned numGets = 0;
		double startTime;

		ClientActor(const Callback& promise, SimpleWorkload& self, FDBDatabase* db)
		  : ActorBase(promise, self, db), random(0, self.numTuples - 1), startTime(self.context->now()) {
			error = fdb_database_create_transaction(db, &tx);
			if (error) {
				done(this);
			}
			setCallbacks();
		}

		~ClientActor() {
			if (tx) {
				fdb_transaction_destroy(tx);
			}
		}

		void run() { get(); }

		void get() {
			if (self.context->now() > startTime + self.runFor) {
				done(this);
				return;
			}
			auto key = KEY_PREFIX + std::to_string(random(self.random));
			auto f = fdb_transaction_get(tx, reinterpret_cast<const uint8_t*>(key.c_str()), key.size(), false);
			wait(f, State::Get);
		}

		void commit() {
			if (self.context->now() > startTime + self.runFor) {
				done(this);
				return;
			}
			wait(fdb_transaction_commit(tx), State::Commit);
		}

		void setCallbacks() {
			callbacks[State::Get] = { [this](FDBFuture* future) {
				                         ++numGets;
				                         if (++ops >= self.opsPerTx) {
					                         commit();
				                         } else {
					                         get();
				                         }
				                     },
				                      [this](fdb_error_t error) {
				                          wait(fdb_transaction_on_error(tx, error), State::Retry);
				                      } };
			callbacks[State::Retry] = { [this](FDBFuture* future) {
				                           ops = 0;
				                           fdb_transaction_reset(tx);
				                           ++numRetries;
				                           get();
				                       },
				                        [this](fdb_error_t) {
				                            self.context->trace(FDBSeverity::Error,
				                                                "AssertionFailure",
				                                                { { "Reason", "tx.onError failed" },
				                                                  { "Error", std::string(fdb_get_error(error)) } });
				                            self.success = false;
				                            done(this);
				                        } };
			callbacks[State::Commit] = { [this](FDBFuture* future) {
				                            ++numCommits;
				                            ops = 0;
				                            fdb_transaction_reset(tx);
				                            get();
				                        },
				                         [this](fdb_error_t) {
				                             wait(fdb_transaction_on_error(tx, error), State::Retry);
				                         } };
		}
	};

	// std::string description() const override { return name; }
	bool init(FDBWorkloadContext* context) override {
		this->context = context;
		context->trace(FDBSeverity::Info, "SimpleWorkloadInit", {});
		random = decltype(random)(context->rnd());
		numTuples = context->getOption("numTuples", 100000ul);
		numActors = context->getOption("numActors", 100ul);
		insertsPerTx = context->getOption("insertsPerTx", 100ul);
		opsPerTx = context->getOption("opsPerTx", 100ul);
		runFor = context->getOption("runFor", 10.0);
		auto err = fdb_select_api_version(FDB_API_VERSION);
		if (err) {
			context->trace(
			    FDBSeverity::Info, "SelectAPIVersionFailed", { { "Error", std::string(fdb_get_error(err)) } });
		}
		return true;
	}
	void setup(FDBDatabase* db, GenericPromise<bool> done) override {
		if (this->context->clientId() == 0) {
			done.send(true);
			return;
		}
		struct Populator {
			std::vector<PopulateActor*> actors;
			GenericPromise<bool> promise;
			bool success = true;

			void operator()(PopulateActor* done) {
				if (done->error) {
					success = false;
				}
				for (int i = 0; i < actors.size(); ++i) {
					if (actors[i] == done) {
						actors[i] = actors.back();
						delete done;
						actors.pop_back();
					}
				}
				if (actors.empty()) {
					promise.send(success);
					delete this;
				}
			}
		};
		decltype(numTuples) from = 0;
		auto p = new Populator{ {}, std::move(done) };
		for (decltype(numActors) i = 0; i < numActors; ++i) {
			decltype(from) to = from + (numTuples / numActors);
			if (i == numActors - 1) {
				to = numTuples;
			}
			auto actor = new PopulateActor([p](PopulateActor* self) { (*p)(self); }, *this, db, from, to);
			p->actors.emplace_back(actor);
			from = to;
		}
		for (auto actor : p->actors) {
			actor->run();
		}
	}
	void start(FDBDatabase* db, GenericPromise<bool> done) override {
		if (!success) {
			done.send(false);
		}
		struct ClientRunner {
			std::vector<ClientActor*> actors;
			GenericPromise<bool> done;
			SimpleWorkload* self;

			void operator()(ClientActor* actor) {
				double now = self->context->now();
				for (int i = 0; i < actors.size(); ++i) {
					if (actors[i] == actor) {
						actors[i] = actors.back();
						actors.pop_back();
					}
				}
				double s = now - actor->startTime;
				if (s > 0.01) {
					self->gets.emplace_back(double(actor->numGets) / s);
					self->txs.emplace_back(double(actor->numCommits) / s);
					self->retries.emplace_back(double(actor->numRetries) / s);
				}
				delete actor;
				if (actors.empty()) {
					done.send(self->success);
					delete this;
				}
			}
		};
		auto runner = new ClientRunner{ {}, std::move(done), this };
		for (decltype(numActors) i = 0; i < numActors; ++i) {
			auto actor = new ClientActor([runner](ClientActor* self) { (*runner)(self); }, *this, db);
			runner->actors.push_back(actor);
		}
		for (auto actor : runner->actors) {
			actor->run();
		}
	}
	void check(FDBDatabase* db, GenericPromise<bool> done) override { done.send(success); }

	template <class Vec>
	double accumulateMetric(const Vec& v) const {
		double res = 0.0;
		for (auto val : v) {
			res += val;
		}
		return res / double(v.size());
	}

	void getMetrics(std::vector<FDBPerfMetric>& out) const override {
		out.emplace_back(FDBPerfMetric{ "Get/s", accumulateMetric(gets), true });
		out.emplace_back(FDBPerfMetric{ "Tx/s", accumulateMetric(txs), true });
		out.emplace_back(FDBPerfMetric{ "Retries/s", accumulateMetric(retries), true });
	}
};

const std::string SimpleWorkload::name = "SimpleWorkload";
const std::string SimpleWorkload::KEY_PREFIX = "csimple/";

} // namespace

FDBWorkloadFactoryT<SimpleWorkload> simpleWorkload(SimpleWorkload::name);
