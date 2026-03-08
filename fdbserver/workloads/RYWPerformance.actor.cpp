/*
 * RYWPerformance.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RYWPerformanceWorkload : TestWorkload {
	static constexpr auto NAME = "RYWPerformance";

	int keyBytes, nodes, ranges;
	RYWPerformanceWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nodes = getOption(options, "nodes"_sr, 10000);
		ranges = getOption(options, "ranges"_sr, 10);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0)
			return _setup(cx, this);
		return Void();
	}

	Future<Void> _setup(Database cx, RYWPerformanceWorkload* self) {
		Transaction tr(cx);

		loop {
			Error err;
			try {
				for (int i = 0; i < self->nodes; i++)
					tr.set(self->keyForIndex(i), "bar"_sr);

				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx, this);
		return Void();
	}

	static Future<Void> fillCache(ReadYourWritesTransaction* tr, RYWPerformanceWorkload* self, int type) {
		int i{ 0 };
		if (type == 0) {
			for (i = 0; i < self->nodes; i++) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 1) {
			std::vector<Future<Optional<Value>>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->get(self->keyForIndex(i)));
			}
			co_await waitForAll(gets);
		} else if (type == 2) {
			std::vector<Future<Optional<Value>>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->get(self->keyForIndex(i)));
			}
			co_await waitForAll(gets);
			for (i = 0; i < self->nodes; i++) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 3) {
			std::vector<Future<Optional<Value>>> gets;
			for (i = 0; i < self->nodes; i += 2) {
				gets.push_back(tr->get(self->keyForIndex(i)));
			}
			co_await waitForAll(gets);
			for (i = 1; i < self->nodes; i += 2) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 4) {
			co_await success(
			    tr->getRange(KeyRangeRef(self->keyForIndex(0), self->keyForIndex(self->nodes)), self->nodes));
		} else if (type == 5) {
			co_await success(
			    tr->getRange(KeyRangeRef(self->keyForIndex(0), self->keyForIndex(self->nodes)), self->nodes));
			for (i = 0; i < self->nodes; i++) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 6) {
			co_await success(
			    tr->getRange(KeyRangeRef(self->keyForIndex(0), self->keyForIndex(self->nodes)), self->nodes));
			for (i = 0; i < self->nodes; i += 2) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 7) {
			co_await success(
			    tr->getRange(KeyRangeRef(self->keyForIndex(0), self->keyForIndex(self->nodes)), self->nodes));
			for (i = 0; i < self->nodes; i++) {
				tr->clear(self->keyForIndex(i));
			}
		} else if (type == 8) {
			co_await success(
			    tr->getRange(KeyRangeRef(self->keyForIndex(0), self->keyForIndex(self->nodes)), self->nodes));
			for (i = 0; i < self->nodes; i += 2) {
				tr->clear(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 1)));
			}
		} else if (type == 9) {
			std::vector<Future<RangeResult>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->getRange(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 2)), self->nodes));
			}
			co_await waitForAll(gets);
		} else if (type == 10) {
			std::vector<Future<RangeResult>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->getRange(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 2)), self->nodes));
			}
			co_await waitForAll(gets);
			for (i = 0; i < self->nodes; i++) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 11) {
			std::vector<Future<RangeResult>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->getRange(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 2)), self->nodes));
			}
			co_await waitForAll(gets);
			for (i = 0; i < self->nodes; i += 2) {
				tr->set(self->keyForIndex(i), "foo"_sr);
			}
		} else if (type == 12) {
			std::vector<Future<RangeResult>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->getRange(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 2)), self->nodes));
			}
			co_await waitForAll(gets);
			for (i = 0; i < self->nodes; i++) {
				tr->clear(self->keyForIndex(i));
			}
		} else if (type == 13) {
			std::vector<Future<RangeResult>> gets;
			for (i = 0; i < self->nodes; i++) {
				gets.push_back(tr->getRange(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 2)), self->nodes));
			}
			co_await waitForAll(gets);
			for (i = 0; i < self->nodes; i += 2) {
				tr->clear(KeyRangeRef(self->keyForIndex(i), self->keyForIndex(i + 1)));
			}
		}
	}

	static Future<Void> test_get_single(Database cx, RYWPerformanceWorkload* self, int cacheType) {
		int i{ 0 };
		ReadYourWritesTransaction tr(cx);

		loop {
			Error err;
			try {
				co_await self->fillCache(&tr, self, cacheType);

				double startTime = timer();

				for (i = 0; i < self->nodes; i++) {
					co_await success(tr.get(self->keyForIndex(self->nodes / 2)));
				}

				fprintf(stderr, "%f", self->nodes / (timer() - startTime));

				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> test_get_many_sequential(Database cx, RYWPerformanceWorkload* self, int cacheType) {
		int i{ 0 };
		ReadYourWritesTransaction tr(cx);

		loop {
			Error err;
			try {
				co_await self->fillCache(&tr, self, cacheType);

				double startTime = timer();

				for (i = 0; i < self->nodes; i++) {
					co_await success(tr.get(self->keyForIndex(i)));
				}

				fprintf(stderr, "%f", self->nodes / (timer() - startTime));

				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> test_get_range_basic(Database cx, RYWPerformanceWorkload* self, int cacheType) {
		int i{ 0 };
		ReadYourWritesTransaction tr(cx);

		loop {
			Error err;
			try {
				co_await self->fillCache(&tr, self, cacheType);

				double startTime = timer();

				for (i = 0; i < self->ranges; i++) {
					co_await success(
					    tr.getRange(KeyRangeRef(self->keyForIndex(0), self->keyForIndex(self->nodes)), self->nodes));
				}

				fprintf(stderr, "%f", self->ranges / (timer() - startTime));

				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> test_interleaved_sets_gets(Database cx, RYWPerformanceWorkload* self, int cacheType) {
		int i{ 0 };
		ReadYourWritesTransaction tr(cx);

		loop {
			Error err;
			try {
				co_await self->fillCache(&tr, self, cacheType);

				tr.set(self->keyForIndex(self->nodes / 2), self->keyForIndex(self->nodes));

				double startTime = timer();

				for (i = 0; i < self->nodes; i++) {
					co_await success(tr.get(self->keyForIndex(self->nodes / 2)));
					tr.set(self->keyForIndex(self->nodes / 2), self->keyForIndex(i));
				}

				fprintf(stderr, "%f", self->nodes / (timer() - startTime));

				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> _start(Database cx, RYWPerformanceWorkload* self) {
		int i{ 0 };
		fprintf(stderr, "test_get_single, ");
		for (i = 0; i < 14; i++) {
			co_await self->test_get_single(cx, self, i);
			if (i == 13)
				fprintf(stderr, "\n");
			else
				fprintf(stderr, ", ");
		}
		fprintf(stderr, "test_get_many_sequential, ");
		for (i = 0; i < 14; i++) {
			co_await self->test_get_many_sequential(cx, self, i);
			if (i == 13)
				fprintf(stderr, "\n");
			else
				fprintf(stderr, ", ");
		}
		fprintf(stderr, "test_get_range_basic, ");
		for (i = 4; i < 14; i++) {
			co_await self->test_get_range_basic(cx, self, i);
			if (i == 13)
				fprintf(stderr, "\n");
			else
				fprintf(stderr, ", ");
		}
		fprintf(stderr, "test_interleaved_sets_gets, ");
		for (i = 0; i < 14; i++) {
			co_await self->test_interleaved_sets_gets(cx, self, i);
			if (i == 13)
				fprintf(stderr, "\n");
			else
				fprintf(stderr, ", ");
		}
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Key keyForIndex(uint64_t index) {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodes;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}
};

WorkloadFactory<RYWPerformanceWorkload> RYWPerformanceWorkloadFactory;
