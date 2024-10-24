/*
 * FileSystem.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct FileSystemWorkload : TestWorkload {
	static constexpr auto NAME = "FileSystem";
	int actorCount, writeActorCount, fileCount, pathMinChars, pathCharRange, serverCount, userIDCount;
	double testDuration, transactionsPerSecond, deletedFilesRatio;
	bool discardEdgeMeasurements, performingWrites, loggingQueries;
	std::string operationName;

	std::vector<Future<Void>> clients;
	PerfIntCounter queries, writes;
	DDSketch<double> latencies;
	DDSketch<double> writeLatencies;

	class FileSystemOp {
	public:
		virtual Future<Optional<Version>> run(FileSystemWorkload* self, Transaction* tr) = 0;
		virtual const char* name() = 0;
		virtual ~FileSystemOp() {}
	};

	FileSystemWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), queries("Queries"), writes("Latency"), latencies(), writeLatencies() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0) / clientCount;
		double allowedLatency = getOption(options, "allowedLatency"_sr, 0.250);
		actorCount = transactionsPerSecond * allowedLatency;
		fileCount = getOption(options, "fileCount"_sr, 100000);
		pathMinChars = std::max(getOption(options, "pathMinChars"_sr, 32), 8);
		pathCharRange = std::max(getOption(options, "pathMaxChars"_sr, 128), pathMinChars) - pathMinChars;
		discardEdgeMeasurements = getOption(options, "discardEdgeMeasurements"_sr, true);
		deletedFilesRatio = getOption(options, "deletedFilesRatio"_sr, 0.01);
		serverCount = getOption(options, "serverCount"_sr, 32);
		userIDCount = getOption(options, "userIDCount"_sr, std::max(100, fileCount / 3000));
		operationName = getOption(options, "operationName"_sr, "modificationQuery"_sr).toString();
		performingWrites = getOption(options, "performingWrites"_sr, false);
		writeActorCount = getOption(options, "writeActorCount"_sr, 4);
		loggingQueries = getOption(options, "loggingQueries"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return nodeSetup(cx, this); }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = testDuration * (discardEdgeMeasurements ? 0.75 : 1.0);
		m.emplace_back("Measured Duration", duration, Averaged::True);
		m.emplace_back("Transactions/sec", queries.getValue() / duration, Averaged::False);
		m.emplace_back("Writes/sec", writes.getValue() / duration, Averaged::False);
		m.emplace_back("Mean Latency (ms)", 1000 * latencies.mean(), Averaged::True);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * latencies.median(), Averaged::True);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * latencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% Latency (ms, averaged)", 1000 * latencies.percentile(0.98), Averaged::True);
		m.emplace_back("Median Write Latency (ms, averaged)", 1000 * writeLatencies.median(), Averaged::True);
	}

	Key keyForFileID(uint64_t id) { return StringRef(format("/files/id/%016llx", id)); }

	void initializeFile(Transaction* tr, FileSystemWorkload* self, uint64_t id) {
		Key key = self->keyForFileID(id);

		int pathLen = self->pathMinChars + deterministicRandom()->randomInt(0, self->pathCharRange);
		std::string path = "";
		for (int i = 0; i < pathLen; i += 4)
			path +=
			    format(format("%%0%dx", std::min(pathLen - i, 4)).c_str(), deterministicRandom()->randomInt(0, 0xFFFF));
		uint64_t userID = deterministicRandom()->randomInt(0, self->userIDCount);
		int serverID = deterministicRandom()->randomInt(0, self->serverCount);
		bool deleted = deterministicRandom()->random01() < self->deletedFilesRatio;
		double time = now();

		tr->set(key, path);
		std::string keyStr(key.toString());
		tr->set(keyStr + "/size", format("%d", deterministicRandom()->randomInt(0, std::numeric_limits<int>::max())));
		tr->set(keyStr + "/server", format("%d", deterministicRandom()->randomInt(0, self->serverCount)));
		tr->set(keyStr + "/deleted", deleted ? "1"_sr : "0"_sr);
		tr->set(keyStr + "/server", format("%d", serverID));
		tr->set(keyStr + "/created", doubleToTestKey(time));
		tr->set(keyStr + "/lastupdated", doubleToTestKey(time));
		tr->set(keyStr + "/userid", format("%016llx", userID));

		if (deleted)
			tr->set(format("/files/server/%08x/deleted/%016llx", serverID, id), doubleToTestKey(time));
		tr->set(format("/files/user/%016llx/updated/%016llx/%016llx", userID, *(uint64_t*)&time, id), path);
		tr->set(format("/files/user/%016llx/path/", userID) + path, format("%016llx", id));
		// This index was not specified in the original test: it removes duplicated paths
		tr->set("/files/path/" + path, format("%016llx", id));
	}

	ACTOR Future<Void> setupRange(Database cx, FileSystemWorkload* self, int begin, int end) {
		state Transaction tr(cx);
		while (true) {
			try {
				Optional<Value> f = wait(tr.get(self->keyForFileID(begin)));
				if (f.present())
					break; // The transaction already completed!

				for (int n = begin; n < end; n++)
					self->initializeFile(&tr, self, n);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> nodeSetup(Database cx, FileSystemWorkload* self) {
		state int i;
		state std::vector<int> order;
		state int nodesToSetUp = self->fileCount / self->clientCount + 1;
		state int startingNode = nodesToSetUp * self->clientId;
		state int batchCount = 5;
		for (int o = 0; o <= nodesToSetUp / batchCount; o++)
			order.push_back(o * batchCount);
		deterministicRandom()->randomShuffle(order);
		for (i = 0; i < order.size();) {
			std::vector<Future<Void>> fs;
			for (int j = 0; j < 100 && i < order.size(); j++) {
				fs.push_back(self->setupRange(
				    cx,
				    self,
				    startingNode + order[i],
				    std::min(startingNode + order[i] + batchCount, nodesToSetUp * (self->clientId + 1))));
				i++;
			}
			wait(waitForAll(fs));
		}
		TraceEvent("FileSetupOK")
		    .detail("ClientIdx", self->clientId)
		    .detail("ClientCount", self->clientCount)
		    .detail("StartingFile", startingNode)
		    .detail("FilesToSetUp", nodesToSetUp);
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, FileSystemWorkload* self) {
		state FileSystemOp* operation;
		if (self->operationName == "deletionQuery")
			operation = new ServerDeletionCountQuery();
		else
			operation = new RecentModificationQuery();
		wait(timeout(self->operationClient(cx, self, operation, 0.01), 1.0, Void()));
		self->queries.clear();
		self->writes.clear();

		if (self->performingWrites) {
			for (int c = 0; c < self->writeActorCount; c++) {
				self->clients.push_back(timeout(self->writeClient(cx, self), self->testDuration, Void()));
			}
		}
		for (int c = 0; c < self->actorCount; c++) {
			self->clients.push_back(
			    timeout(self->operationClient(cx, self, operation, self->actorCount / self->transactionsPerSecond),
			            self->testDuration,
			            Void()));
		}
		wait(waitForAll(self->clients));

		wait(delay(0.01)); // Make sure the deletion happens after actor cancellation
		delete operation;
		return Void();
	}

	bool shouldRecord(double clientBegin) {
		double n = now();
		return !discardEdgeMeasurements ||
		       (n > (clientBegin + testDuration * 0.125) && n < (clientBegin + testDuration * 0.875));
	}

	ACTOR Future<Void> operationClient(Database cx, FileSystemWorkload* self, FileSystemOp* operation, double delay) {
		state double clientBegin = now();
		state double lastTime = now();
		loop {
			wait(poisson(&lastTime, delay));
			state double tstart = now();
			state Transaction tr(cx);
			loop {
				try {
					Optional<Version> ver = wait(operation->run(self, &tr));
					if (ver.present())
						break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			if (self->shouldRecord(clientBegin)) {
				++self->queries;
				double latency = now() - tstart;
				self->latencies.addSample(latency);
			}
		}
	}

	static int testKeyToInt(const KeyRef& p) {
		int x = 0;
		sscanf(p.toString().c_str(), "%d", &x);
		return x;
	}

	ACTOR Future<Void> writeClient(Database cx, FileSystemWorkload* self) {
		state double clientBegin = now();
		loop {
			state int fileID = deterministicRandom()->randomInt(0, self->fileCount);
			state bool isDeleting = deterministicRandom()->random01() < 0.25;
			state int size = isDeleting ? 0 : deterministicRandom()->randomInt(0, std::numeric_limits<int>::max());
			state std::string keyStr = self->keyForFileID(fileID).toString();
			state double tstart = now();
			state Transaction tr(cx);
			loop {
				try {
					state double time = now();
					if (isDeleting) {
						state Optional<Value> deleted = wait(tr.get(StringRef(keyStr + "/deleted")));
						ASSERT(deleted.present());
						Optional<Value> serverStr = wait(tr.get(StringRef(keyStr + "/server")));
						ASSERT(serverStr.present());
						int serverID = testKeyToInt(serverStr.get());
						if (deleted.get().toString() == "1") {
							tr.set(keyStr + "/deleted", "0"_sr);
							tr.clear(format("/files/server/%08x/deleted/%016llx", serverID, fileID));
						} else {
							tr.set(keyStr + "/deleted", "1"_sr);
							tr.set(format("/files/server/%08x/deleted/%016llx", serverID, fileID),
							       doubleToTestKey(time));
						}
					} else {
						tr.set(keyStr + "/size", format("%d", size));
					}
					tr.set(keyStr + "/lastupdated", doubleToTestKey(time));
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			if (self->shouldRecord(clientBegin)) {
				++self->writes;
				self->writeLatencies.addSample(now() - tstart);
			}
		}
	}

	ACTOR Future<Optional<Version>> modificationQuery(FileSystemWorkload* self, Transaction* tr) {
		state uint64_t userID = deterministicRandom()->randomInt(0, self->userIDCount);
		state std::string base = format("/files/user/%016llx", userID);
		if (self->loggingQueries)
			TraceEvent("UserQuery").detail("UserID", userID).detail("PathBase", base);
		Key keyEnd(base + "/updated0");
		RangeResult val = wait(tr->getRange(firstGreaterOrEqual(keyEnd) - 10, firstGreaterOrEqual(keyEnd), 10));
		Key keyBegin(base + "/updated/");
		for (int i = val.size() - 1; i >= 0; i--) {
			if (val[i].key.startsWith(keyBegin) && self->loggingQueries) {
				TraceEvent("UserQueryResults")
				    .detail("UserID", userID)
				    .detail("PathBase", base)
				    .detail("LastModified", printable(val[i].key.substr(54)));
				break;
			}
		}
		return Optional<Version>(Version(0));
	}

	ACTOR Future<Optional<Version>> deletionQuery(FileSystemWorkload* self, Transaction* tr) {
		state uint64_t serverID = deterministicRandom()->randomInt(0, self->serverCount);
		state std::string base = format("/files/server/%08x/deleted", serverID);
		if (self->loggingQueries)
			TraceEvent("DeletionQuery").detail("ServerID", serverID).detail("PathBase", base);
		state Key keyBegin(base + "/");
		state Key keyEnd(base + "0");
		state KeySelectorRef begin = firstGreaterThan(keyBegin);
		state KeySelectorRef end = firstGreaterOrEqual(keyEnd);
		state int transferred = 1000;
		state int transferSize = 1000;
		state uint64_t deletedFiles = 0;
		while (transferred == transferSize) {
			RangeResult val = wait(tr->getRange(begin, end, transferSize));
			transferred = val.size();
			deletedFiles += transferred;
			begin = begin + transferred;
		}
		if (self->loggingQueries) {
			TraceEvent("DeletionQueryResults")
			    .detail("ServerID", serverID)
			    .detail("PathBase", base)
			    .detail("DeletedFiles", deletedFiles);
		}
		return Optional<Version>(Version(0));
	}

	class RecentModificationQuery : public FileSystemOp {
		Future<Optional<Version>> run(FileSystemWorkload* self, Transaction* tr) override {
			return self->modificationQuery(self, tr);
		}
		const char* name() override { return "RecentUserModifications"; }
	};

	class ServerDeletionCountQuery : public FileSystemOp {
		Future<Optional<Version>> run(FileSystemWorkload* self, Transaction* tr) override {
			return self->deletionQuery(self, tr);
		}
		const char* name() override { return "ServerDeletions"; }
	};
};

WorkloadFactory<FileSystemWorkload> FileSystemWorkloadFactory;
