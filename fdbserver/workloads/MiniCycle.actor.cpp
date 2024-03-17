/*
 * MiniCycle.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include <cstring>

#include "flow/actorcompiler.h" // This must be the last #include.

struct MiniCycleWorkload : TestWorkload {
	static constexpr auto NAME = "MiniCycle";

	int actorCount, nodeCount;
	double testDuration, transactionsPerSecond, minExpectedTransactionsPerSecond, traceParentProbability;
	Key keyPrefix;

	FlowLock checkLock;
	PerfIntCounter transactions, retries, tooOldRetries, commitFailedRetries;
	PerfDoubleCounter totalLatency;

	MiniCycleWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), tooOldRetries("Retries.too_old"),
	    commitFailedRetries("Retries.commit_failed"), totalLatency("Latency") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, transactionsPerSecond / 5);
		nodeCount = getOption(options, "nodeCount"_sr, transactionsPerSecond * clientCount);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, ""_sr).toString());
		traceParentProbability = getOption(options, "traceParentProbability "_sr, 0.01);
		minExpectedTransactionsPerSecond = transactionsPerSecond * getOption(options, "expectedRate"_sr, 0.7);
		ASSERT(cycleSize(clientId) >= 3); // The workload assumes that each cycle has at least 3 distinct keys
	}

	Future<Void> setup(Database const& cx) override {
		return bulkSetup(cx->clone(),
		                 this,
		                 cycleSize(clientId),
		                 Promise<double>(),
		                 false,
		                 0.0,
		                 1e12,
		                 std::vector<uint64_t>(),
		                 Promise<std::vector<std::pair<uint64_t, double>>>(),
		                 0,
		                 0.1,
		                 beginKey(clientId),
		                 endKey(clientId));
	}

	Future<Void> start(Database const& cx) override { return Void(); }
	Future<bool> check(Database const& cx) override { return _check(cx->clone(), this); }

	ACTOR Future<bool> _check(Database cx, MiniCycleWorkload* self) {
		state bool ok = true;
		bool ret1 = wait(self->_checkCycle(cx->clone(), self, ok));
		ASSERT(ret1);
		state std::vector<Future<Void>> cycleClients;
		for (int c = 0; c < self->clientCount; c++)
			cycleClients.push_back(
			    timeout(self->cycleClient(cx->clone(), self, self->actorCount / self->transactionsPerSecond),
			            self->testDuration,
			            Void()));

		state Future<Void> end = delay(self->testDuration);
		loop {
			choose {
				when(bool ret = wait(self->_checkCycle(cx->clone(), self, ok))) {
					ok = ret && ok;
					if (!ok)
						return false;
				}
				when(wait(end)) {
					break;
				}
			}
		}

		// Check for errors in the cycle clients
		int errors = 0;
		for (int c = 0; c < cycleClients.size(); c++)
			errors += cycleClients[c].isError();
		if (errors || !ok)
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were check or cycle client errors.");
		cycleClients.clear();

		printf("Beginning full cycle check...");
		bool ret2 = wait(self->_checkCycle(cx->clone(), self, ok));
		return ret2;
	}

	ACTOR Future<bool> _checkCycle(Database cx, MiniCycleWorkload* self, bool ok) {
		state std::vector<Future<bool>> checkClients;
		for (int c = 0; c < self->clientCount; c++)
			checkClients.push_back(self->cycleCheckClient(cx->clone(), self, ok));
		bool ret = wait(allTrue(checkClients));

		// Check for errors in the cycle clients
		int errors = 0;
		for (int c = 0; c < checkClients.size(); c++)
			errors += checkClients[c].isError();
		if (errors)
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were checker errors.");
		return ret;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.push_back(tooOldRetries.getMetric());
		m.push_back(commitFailedRetries.getMetric());
		m.emplace_back("Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), Averaged::True);
		m.emplace_back("Read rows/simsec (approx)", transactions.getValue() * 3 / testDuration, Averaged::False);
		m.emplace_back("Write rows/simsec (approx)", transactions.getValue() * 4 / testDuration, Averaged::False);
	}

	Key keyForIndex(int n) { return key(n); }
	Key key(int n) { return doubleToTestKey(n, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }
	int fromValue(const ValueRef& v) { return testKeyToDouble(v, keyPrefix); }

	// cycleSize returns the length of each mini-cycle besides the last,
	// which is cycleSize + remainder nodes in length
	int cycleSize(int clientId) {
		// The remaining keys should go in the last cycle
		int rem = (clientId == clientCount - 1) ? nodeCount % clientCount : 0;
		return nodeCount / clientCount + rem;
	}
	// cycleOffset returns the node number at which clientId's mini-cycle begins
	int cycleOffset(int clientId) { return clientId * cycleSize(0); }
	int beginKey(int clientId) { return cycleOffset(clientId); }
	int endKey(int clientId) { return cycleSize(clientId) + cycleOffset(clientId); }

	Standalone<KeyValueRef> operator()(int n) {
		const uint64_t val = (n + 1) % endKey(clientId) ? n + 1 : beginKey(clientId);
		return KeyValueRef(key(n), value(val));
	}

	void badRead(const char* name, int r, Transaction& tr) {
		TraceEvent(SevError, "MiniCycleBadRead")
		    .detail(name, r)
		    .detail("Key", printable(key(r)))
		    .detail("Version", tr.getReadVersion().get())
		    .detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken);
	}

	ACTOR Future<Void> cycleClient(Database cx, MiniCycleWorkload* self, double delay) {
		state double lastTime = now();
		try {
			loop {
				wait(poisson(&lastTime, delay));

				state double tstart = now();
				state int r =
				    deterministicRandom()->randomInt(self->beginKey(self->clientId), self->endKey(self->clientId));
				state Transaction tr(cx);
				if (deterministicRandom()->random01() >= self->traceParentProbability) {
					state Span span("MiniCycleClient"_loc);
					TraceEvent("MiniCycleTracingTransaction", span.context.traceID).log();
					tr.setOption(FDBTransactionOptions::SPAN_PARENT,
					             BinaryWriter::toValue(span.context, Unversioned()));
				}
				while (true) {
					try {
						// Reverse next and next^2 node
						Optional<Value> v = wait(tr.get(self->key(r)));
						if (!v.present())
							self->badRead("KeyR", r, tr);
						state int r2 = self->fromValue(v.get());
						Optional<Value> v2 = wait(tr.get(self->key(r2)));
						if (!v2.present())
							self->badRead("KeyR2", r2, tr);
						state int r3 = self->fromValue(v2.get());
						Optional<Value> v3 = wait(tr.get(self->key(r3)));
						if (!v3.present())
							self->badRead("KeyR3", r3, tr);
						int r4 = self->fromValue(v3.get());

						tr.clear(self->key(r)); //< Shouldn't have an effect, but will break with wrong ordering
						tr.set(self->key(r), self->value(r3));
						tr.set(self->key(r2), self->value(r4));
						tr.set(self->key(r3), self->value(r2));
						// TraceEvent("CyclicTest").detail("Key", self->key(r).toString()).detail("Value", self->value(r3).toString());
						// TraceEvent("CyclicTest").detail("Key", self->key(r2).toString()).detail("Value", self->value(r4).toString());
						// TraceEvent("CyclicTest").detail("Key", self->key(r3).toString()).detail("Value", self->value(r2).toString());

						wait(tr.commit());
						// TraceEvent("MiniCycleCommit");
						break;
					} catch (Error& e) {
						if (e.code() == error_code_transaction_too_old)
							++self->tooOldRetries;
						else if (e.code() == error_code_not_committed)
							++self->commitFailedRetries;
						wait(tr.onError(e));
					}
					++self->retries;
				}
				++self->transactions;
				self->totalLatency += now() - tstart;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "MiniCycleClient").error(e);
			throw;
		}
	}

	void logTestData(const VectorRef<KeyValueRef>& data) {
		TraceEvent("TestFailureDetail")
		    .detail("NodeCount", nodeCount)
		    .detail("ClientCount", clientCount)
		    .detail("ClientId", beginKey(clientId))
		    .detail("CycleBegin", beginKey(clientId))
		    .detail("CycleSize", cycleSize(clientId));
		int index = 0;
		for (auto& entry : data) {
			TraceEvent("CurrentDataEntry")
			    .detail("Index", index)
			    .detail("ValueDecoded", fromValue(entry.value))
			    .detail("Key", entry.key.toString())
			    .detail("Value", entry.value.toString());
			index++;
		}
	}

	bool cycleCheckData(const VectorRef<KeyValueRef>& data, Version v, int clientID) {
		if (data.size() != cycleSize(clientId)) {
			logTestData(data);
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "Node count changed")
			    .detail("Before", cycleSize(clientId))
			    .detail("After", data.size())
			    .detail("Version", v)
			    .detail("KeyPrefix", keyPrefix.printable());
			TraceEvent(SevError, "TestFailureInfo")
			    .detail("DataSize", data.size())
			    .detail("CycleSize", cycleSize(clientId))
			    .detail("Workload", description());
			return false;
		}
		int i = beginKey(clientId);
		int iPrev = beginKey(clientId);
		double d;
		int c;
		for (c = 0; c < cycleSize(clientId); c++) {
			if (c && !(i - beginKey(clientId))) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "MiniCycle got shorter")
				    .detail("Before", cycleSize(clientId))
				    .detail("After", c)
				    .detail("KeyPrefix", keyPrefix.printable());
				logTestData(data);
				return false;
			}
			if (data[i - beginKey(clientId)].key != key(i)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Key changed")
				    .detail("KeyPrefix", keyPrefix.printable());
				logTestData(data);
				return false;
			}

			d = testKeyToDouble(data[i - beginKey(clientId)].value, keyPrefix);
			iPrev = i;
			i = (int)d;
			if (i != d || i < beginKey(clientId) || i >= endKey(clientId)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Invalid value")
				    .detail("KeyPrefix", keyPrefix.printable());
				logTestData(data);
				return false;
			}
		}
		if (i - beginKey(clientId) != 0) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "MiniCycle got longer")
			    .detail("KeyPrefix", keyPrefix.printable())
			    .detail("Key", key(i))
			    .detail("Value", data[i - beginKey(clientId)].value)
			    .detail("Iteration", c)
			    .detail("CycleSize", cycleSize(clientId))
			    .detail("Int", i)
			    .detail("Double", d)
			    .detail("ValuePrev", data[iPrev - beginKey(clientId)].value)
			    .detail("KeyPrev", data[iPrev - beginKey(clientId)].key);
			logTestData(data);
			return false;
		}
		return true;
	}
	ACTOR Future<bool> cycleCheckClient(Database cx, MiniCycleWorkload* self, bool ok) {
		if (self->transactions.getMetric().value() < self->testDuration * self->minExpectedTransactionsPerSecond) {
			TraceEvent(SevWarnAlways, "TestFailure")
			    .detail("Reason", "Rate below desired rate")
			    .detail("File", __FILE__)
			    .detail(
			        "Details",
			        format("%.2f",
			               self->transactions.getMetric().value() / (self->transactionsPerSecond * self->testDuration)))
			    .detail("TransactionsAchieved", self->transactions.getMetric().value())
			    .detail("MinTransactionsExpected", self->testDuration * self->minExpectedTransactionsPerSecond)
			    .detail("TransactionGoal", self->transactionsPerSecond * self->testDuration);
			ok = false;
		}

		// One client checks the validity of the cycle at a time
		wait(self->checkLock.take());
		state FlowLock::Releaser releaser(self->checkLock);

		state Transaction tr(cx);
		state int retryCount = 0;
		loop {
			try {
				state Version v = wait(tr.getReadVersion());
				RangeResult data = wait(
				    tr.getRange(firstGreaterOrEqual(doubleToTestKey(self->beginKey(self->clientId), self->keyPrefix)),
				                firstGreaterOrEqual(doubleToTestKey(self->endKey(self->clientId), self->keyPrefix)),
				                self->cycleSize(self->clientId) + 1));
				ok = self->cycleCheckData(data, v, self->clientId) && ok;
				break;
			} catch (Error& e) {
				retryCount++;
				TraceEvent(retryCount > 20 ? SevWarnAlways : SevWarn, "MiniCycleCheckError").error(e);
				wait(tr.onError(e));
			}
		}
		return ok;
	}
};

WorkloadFactory<MiniCycleWorkload> MiniCycleWorkloadFactory;
