/*
 * AsyncFileRead.actor.cpp
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
#include <utility>
#include <vector>

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "flow/SystemMonitor.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbserver/workloads/AsyncFile.actor.h"
#include "flow/DeterministicRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static const double ROLL_TIME = 5.0;

struct IOLog {
	struct ProcessLog {
		double startTime;
		double lastTime;
		double sumSq, sum, max, count;

		bool logLatency; // when false, log times and compute elapsed, else, interpret the log'd time as a latency

		ProcessLog() : logLatency(false){};

		virtual void reset() {
			startTime = now();
			lastTime = startTime;
			sumSq = 0.0;
			sum = 0.0;
			max = 0.0;
			count = 0.0;
		}

		void log(double time) {
			count++;
			auto l = logLatency ? time : (time - lastTime); // see logLatency comment above
			sum += l;
			sumSq += l * l;
			max = std::max<double>(max, l);
			lastTime = time;
		}

		void dumpMetrics(std::string name) {
			double elapsed = now() - startTime;
			TraceEvent("ProcessLog")
			    .detail("Name", name)
			    .detail("Hz", count / elapsed)
			    .detail("Latency", sumSq / elapsed / 2.0)
			    .detail("AvgLatency", sum / count)
			    .detail("MaxLatency", max)
			    .detail("StartTime", startTime)
			    .detail("Elapsed", elapsed);
		}
	};

	double lastRoll;

	ProcessLog issue, completion, duration;
	ProcessLog issueR, completionR, durationR;
	ProcessLog issueW, completionW, durationW;

	std::vector<std::pair<std::string, ProcessLog*>> logs;

	IOLog() {
		logs.emplace_back("issue", &issue);
		logs.emplace_back("completion", &completion);
		logs.emplace_back("duration", &duration);
		logs.emplace_back("issueR", &issueR);
		logs.emplace_back("completionR", &completionR);
		logs.emplace_back("durationR", &durationR);
		logs.emplace_back("issueW", &issueW);
		logs.emplace_back("completionW", &completionW);
		logs.emplace_back("durationW", &durationW);

		duration.logLatency = true;
		durationR.logLatency = true;
		durationW.logLatency = true;

		lastRoll = now();
		for (int i = 0; i < logs.size(); i++)
			logs[i].second->reset();
	}

	~IOLog() { roll(); }

	void roll() {
		for (int i = 0; i < logs.size(); i++) {
			logs[i].second->dumpMetrics(logs[i].first);
			logs[i].second->reset();
		}
	}

	void checkRoll(double time) {
		if (time - lastRoll > ROLL_TIME) {
			roll();
			lastRoll = time;
		}
	}

	void logIOIssue(bool isWrite, double issueTime) {
		issue.log(issueTime);
		if (isWrite)
			issueW.log(issueTime);
		else
			issueR.log(issueTime);

		checkRoll(issueTime);
	}

	void logIOCompletion(bool isWrite, double start, double end) {
		completion.log(end);
		if (isWrite)
			completionW.log(end);
		else
			completionR.log(end);

		auto elapsed = end - start;
		duration.log(elapsed);
		if (isWrite)
			durationW.log(elapsed);
		else
			durationR.log(elapsed);

		checkRoll(end);
	}
};

struct AsyncFileReadWorkload : public AsyncFileWorkload {
	// Buffers used to store what is being read or written
	std::vector<Reference<AsyncFileBuffer>> readBuffers;

	// The futures for the asynchronous read operations
	std::vector<Future<int>> readFutures;

	// Number of reads to perform in parallel.  Read tests are performed only if this is greater than zero
	int numParallelReads;

	// The number of bytes read in each call of read
	int readSize;

	// Whether or not I/O should be performed sequentially
	bool sequential;

	bool randomData; // if true, randomize the data in writes

	bool unbatched; // If true, issue reads continuously instead of waiting for all numParallelReads reads to complete
	double writeFraction;
	double fixedRate;

	double averageCpuUtilization;
	PerfIntCounter bytesRead;

	IOLog* ioLog;
	RandomByteGenerator rbg;

	AsyncFileReadWorkload(WorkloadContext const& wcx) : AsyncFileWorkload(wcx), bytesRead("Bytes Read"), ioLog(0) {
		// Only run on one client
		numParallelReads = getOption(options, LiteralStringRef("numParallelReads"), 0);
		readSize = getOption(options, LiteralStringRef("readSize"), _PAGE_SIZE);
		fileSize =
		    getOption(options, LiteralStringRef("fileSize"), (int64_t)0); // 0 = use existing, else, change file size
		unbatched = getOption(options, LiteralStringRef("unbatched"), false);
		sequential = getOption(options, LiteralStringRef("sequential"), true);
		writeFraction = getOption(options, LiteralStringRef("writeFraction"), 0.0);
		randomData = getOption(options, LiteralStringRef("randomData"), true);
		fixedRate = getOption(options, LiteralStringRef("fixedRate"), 0.0);
	}

	~AsyncFileReadWorkload() override {}

	std::string description() const override { return "AsyncFileRead"; }

	Future<Void> setup(Database const& cx) override {
		if (enabled)
			return _setup(this);

		return Void();
	}

	ACTOR Future<Void> _setup(AsyncFileReadWorkload* self) {
		// Allow only 4K aligned reads if doing unbuffered IO
		if (self->unbufferedIO && self->readSize % AsyncFileWorkload::_PAGE_SIZE != 0)
			self->readSize = std::max(AsyncFileWorkload::_PAGE_SIZE,
			                          self->readSize - self->readSize % AsyncFileWorkload::_PAGE_SIZE);

		// Allocate the read buffers
		for (int i = 0; i < self->numParallelReads; i++)
			self->readBuffers.push_back(self->allocateBuffer(self->readSize));

		wait(self->openFile(
		    self, IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0666, self->fileSize, self->fileSize != 0));

		int64_t fileSize = wait(self->fileHandle->file->size());
		self->fileSize = fileSize;

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (enabled)
			return _start(this);

		return Void();
	}

	ACTOR Future<Void> _start(AsyncFileReadWorkload* self) {
		state StatisticsState statState;
		customSystemMonitor("AsyncFile Metrics", &statState);

		wait(timeout(self->runReadTest(self), self->testDuration, Void()));

		SystemStatistics stats = customSystemMonitor("AsyncFile Metrics", &statState);
		self->averageCpuUtilization = stats.processCPUSeconds / stats.elapsed;

		// Try to let the IO operations finish so we can clean up after them
		wait(timeout(waitForAll(self->readFutures), 10, Void()));

		return Void();
	}

	ACTOR static Future<Void> readLoop(AsyncFileReadWorkload* self, int bufferIndex, double fixedRate) {
		state bool writeFlag = false;
		state double begin = 0.0;
		state double lastTime = now();
		loop {
			if (fixedRate)
				wait(poisson(&lastTime, 1.0 / fixedRate));

			// state Future<Void> d = delay( 1/25. * (.75 + 0.5*deterministicRandom()->random01()) );
			int64_t offset;
			if (self->unbufferedIO)
				offset = (int64_t)(deterministicRandom()->random01() * (self->fileSize - 1) /
				                   AsyncFileWorkload::_PAGE_SIZE) *
				         AsyncFileWorkload::_PAGE_SIZE;
			else
				offset = (int64_t)(deterministicRandom()->random01() * (self->fileSize - 1));

			writeFlag = deterministicRandom()->random01() < self->writeFraction;
			if (writeFlag)
				self->rbg.writeRandomBytesToBuffer((char*)self->readBuffers[bufferIndex]->buffer, self->readSize);

			auto r =
			    writeFlag
			        ? tag(self->fileHandle->file->write(self->readBuffers[bufferIndex]->buffer, self->readSize, offset),
			              self->readSize)
			        : self->fileHandle->file->read(self->readBuffers[bufferIndex]->buffer, self->readSize, offset);
			begin = now();
			if (self->ioLog)
				self->ioLog->logIOIssue(writeFlag, begin);
			wait(success(uncancellable(holdWhile(self->fileHandle, holdWhile(self->readBuffers[bufferIndex], r)))));
			if (self->ioLog)
				self->ioLog->logIOCompletion(writeFlag, begin, now());
			self->bytesRead += self->readSize;
			// wait(d);
		}
	}

	ACTOR Future<Void> runReadTest(AsyncFileReadWorkload* self) {
		if (self->unbatched) {
			self->ioLog = new IOLog();

			std::vector<Future<Void>> readers;

			readers.reserve(self->numParallelReads);
			for (int i = 0; i < self->numParallelReads; i++)
				readers.push_back(readLoop(self, i, self->fixedRate / self->numParallelReads));
			wait(waitForAll(readers));

			delete self->ioLog;
			return Void();
		}

		state int64_t offset = self->fileSize;
		loop {
			// Read consecutive chunks of the file using different actors
			for (int i = 0; i < self->numParallelReads; i++) {
				if (self->sequential) {
					offset += self->readSize;

					// If the file is exhausted, start over at the beginning
					if (offset >= self->fileSize)
						offset = 0;
				} else if (self->unbufferedIO)
					offset = (int64_t)(deterministicRandom()->random01() * (self->fileSize - 1) /
					                   AsyncFileWorkload::_PAGE_SIZE) *
					         AsyncFileWorkload::_PAGE_SIZE;
				else
					offset = (int64_t)(deterministicRandom()->random01() * (self->fileSize - 1));

				// Perform the read.  Don't allow it to be cancelled (because the underlying IO may not be cancellable)
				// and don't allow objects that the read uses to be deleted
				self->readFutures.push_back(uncancellable(holdWhile(
				    self->fileHandle,
				    holdWhile(self->readBuffers[i],
				              self->fileHandle->file->read(self->readBuffers[i]->buffer, self->readSize, offset)))));
			}

			wait(waitForAll(self->readFutures));
			self->bytesRead += self->readSize * self->numParallelReads;

			self->readFutures.clear();

			wait(delay(0));
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (enabled) {
			m.emplace_back("Bytes read/sec", bytesRead.getValue() / testDuration, Averaged::False);
			m.emplace_back("Average CPU Utilization (Percentage)", averageCpuUtilization * 100, Averaged::False);
		}
	}
};

WorkloadFactory<AsyncFileReadWorkload> AsyncFileReadWorkloadFactory("AsyncFileRead");
