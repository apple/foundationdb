/*
 * AsyncFileDetachableTests.cpp
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

#include "fdbrpc/AsyncFileNonDurable.h"
#include "fdbrpc/SimulatorMachineInfo.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/UnitTest.h"

namespace {

class DetachableTestFile final : public IAsyncFile, public ReferenceCounted<DetachableTestFile> {
public:
	explicit DetachableTestFile(Promise<Void> destroyed) : destroyed(destroyed) {}
	~DetachableTestFile() override { destroyed.send(Void()); }
	void addref() override { ReferenceCounted<DetachableTestFile>::addref(); }
	void delref() override { ReferenceCounted<DetachableTestFile>::delref(); }
	Future<int> read(void*, int, int64_t) override { return unsupported_operation(); }
	Future<Void> write(void const*, int, int64_t) override { return unsupported_operation(); }
	Future<Void> truncate(int64_t) override { return unsupported_operation(); }
	Future<Void> sync() override { return unsupported_operation(); }
	Future<int64_t> size() const override { return unsupported_operation(); }
	int64_t debugFD() const override { return -1; }
	std::string getFilename() const override { return "detachable-test-file"; }

private:
	Promise<Void> destroyed;
};

class DetachableTestProcesses {
public:
	DetachableTestProcesses()
	  : controller(g_simulator->getCurrentProcess()), controllerPriority(g_network->getCurrentTask()),
	    caller(createProcess("DetachableTestCaller")), producer(createProcess("DetachableTestProducer")) {}
	~DetachableTestProcesses() {
		g_simulator->destroyProcess(producer);
		g_simulator->destroyProcess(caller);
	}
	ISimulator::ProcessInfo* getCaller() const { return caller; }
	ISimulator::ProcessInfo* getProducer() const { return producer; }
	Future<Void> onController() const { return g_simulator->onProcess(controller, controllerPriority); }
	void shutdown(ISimulator::ProcessInfo* process) const {
		ASSERT(process == caller || process == producer);
		// Publish shutdown synchronously so tests control its order against queued completions.
		process->shutdownSignal.send(ISimulator::KillType::RebootProcess);
	}

private:
	ISimulator::ProcessInfo* createProcess(const char* name) {
		auto* process = g_simulator->newProcess(name,
		                                        controller->address.ip,
		                                        controller->machine->getRandomPort(),
		                                        false,
		                                        1,
		                                        controller->locality,
		                                        controller->metadata,
		                                        "",
		                                        "",
		                                        controller->protocolVersion,
		                                        false);
		process->excludeFromRestarts = true;
		return process;
	}

	ISimulator::ProcessInfo* controller;
	TaskPriority controllerPriority;
	ISimulator::ProcessInfo* caller;
	ISimulator::ProcessInfo* producer;
};

struct OpenObservation {
	Reference<IAsyncFile> file;
	Optional<Error> error;
	ISimulator::ProcessInfo* process;
	TaskPriority priority;
};

Future<OpenObservation> observeOpen(Future<Reference<IAsyncFile>> opened, DetachableTestProcesses* processes) {
	OpenObservation observation;
	try {
		observation.file = co_await opened;
	} catch (Error& e) {
		observation.error = e;
	}
	// Observe delivery before another wait can replace its process or priority.
	observation.process = g_simulator->getCurrentProcess();
	observation.priority = g_network->getCurrentTask();
	opened = Future<Reference<IAsyncFile>>();
	co_await processes->onController();
	co_return observation;
}

constexpr TaskPriority callerPriority = TaskPriority::DiskRead;
constexpr TaskPriority producerPriority = TaskPriority::DefaultYield;

} // namespace

TEST_CASE("/fdbrpc/AsyncFileDetachable/openContextAndShutdown") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Void> destroyed;
	auto raw = makeReference<DetachableTestFile>(destroyed);
	Promise<Reference<IAsyncFile>> input;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(input.getFuture());
	bool wasPending = !opened.isReady();
	auto observed = observeOpen(opened, &processes);
	co_await g_simulator->onProcess(processes.getProducer(), producerPriority);
	input.send(raw);
	co_await processes.onController();
	auto result = co_await observed;
	ASSERT(wasPending);
	ASSERT(!result.error.present());
	ASSERT(result.process == processes.getCaller());
	ASSERT(result.priority == callerPriority);
	Reference<IAsyncFile> file = result.file;
	result.file.clear();
	observed = Future<OpenObservation>();
	opened = Future<Reference<IAsyncFile>>();
	input = Promise<Reference<IAsyncFile>>();
	raw.clear();
	ASSERT(!destroyed.getFuture().isReady());

	processes.shutdown(processes.getProducer());
	ASSERT(processes.getProducer()->onShutdown().isReady());
	ASSERT(!destroyed.getFuture().isReady());
	ASSERT(file->getFilename() == "detachable-test-file");
	processes.shutdown(processes.getCaller());
	ASSERT(processes.getCaller()->onShutdown().isReady());
	ASSERT(destroyed.getFuture().isReady());
	try {
		(void)file->getFilename();
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_io_error);
		ASSERT(e.isInjectedFault());
	}
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openSameProcessPriority") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Void> destroyed;
	auto raw = makeReference<DetachableTestFile>(destroyed);
	Promise<Reference<IAsyncFile>> input;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(input.getFuture());
	bool wasPending = !opened.isReady();
	auto observed = observeOpen(opened, &processes);
	co_await g_simulator->onProcess(processes.getCaller(), producerPriority);
	input.send(raw);
	co_await processes.onController();
	auto result = co_await observed;
	ASSERT(wasPending);
	ASSERT(!result.error.present());
	ASSERT(result.process == processes.getCaller());
	ASSERT(result.priority == callerPriority);
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openErrorContext") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	for (Error error : { file_not_found(), actor_cancelled() }) {
		Promise<Reference<IAsyncFile>> input;
		co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
		auto opened = AsyncFileDetachable::open(input.getFuture());
		bool wasPending = !opened.isReady();
		auto observed = observeOpen(opened, &processes);
		co_await g_simulator->onProcess(processes.getProducer(), producerPriority);
		input.sendError(error);
		co_await processes.onController();
		auto result = co_await observed;
		ASSERT(wasPending);
		ASSERT(result.error.present());
		ASSERT_EQ(result.error.get().code(), error.code());
		ASSERT(result.process == processes.getCaller());
		ASSERT(result.priority == callerPriority);
	}
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openShutdown") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Reference<IAsyncFile>> input;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(input.getFuture());
	bool wasPending = !opened.isReady();
	auto observed = observeOpen(opened, &processes);
	co_await g_simulator->onProcess(processes.getProducer(), producerPriority);
	processes.shutdown(processes.getCaller());
	co_await processes.onController();
	auto result = co_await observed;
	ASSERT(wasPending);
	ASSERT(result.error.present());
	ASSERT_EQ(result.error.get().code(), error_code_io_error);
	ASSERT(result.error.get().isInjectedFault());
	observed = Future<OpenObservation>();
	opened = Future<Reference<IAsyncFile>>();
	ASSERT_EQ(input.getFutureReferenceCount(), 0);
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/shutdownBeforeReturn") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Void> destroyed;
	auto raw = makeReference<DetachableTestFile>(destroyed);
	Promise<Reference<IAsyncFile>> input;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(input.getFuture());
	bool wasPending = !opened.isReady();
	auto observed = observeOpen(opened, &processes);
	co_await g_simulator->onProcess(processes.getProducer(), producerPriority);
	input.send(raw);
	// The caller has not run its queued completion when shutdown becomes ready.
	processes.shutdown(processes.getCaller());
	co_await processes.onController();
	auto result = co_await observed;
	ASSERT(wasPending);
	ASSERT(result.error.present());
	ASSERT_EQ(result.error.get().code(), error_code_io_error);
	ASSERT(result.error.get().isInjectedFault());
	ASSERT(!result.file);
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openCancellation") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Reference<IAsyncFile>> input;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(input.getFuture());
	bool wasPending = !opened.isReady();
	co_await g_simulator->onProcess(processes.getProducer(), producerPriority);
	opened.cancel();
	bool cancelledImmediately =
	    opened.isReady() && opened.isError() && opened.getError().code() == error_code_actor_cancelled;
	co_await processes.onController();
	ASSERT(wasPending);
	ASSERT(cancelledImmediately);
	input.sendError(file_not_found());
	ASSERT(opened.isReady() && opened.isError());
	ASSERT_EQ(opened.getError().code(), error_code_actor_cancelled);
	opened = Future<Reference<IAsyncFile>>();
	ASSERT_EQ(input.getFutureReferenceCount(), 0);
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openAlreadyReady") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Void> destroyed;
	auto raw = makeReference<DetachableTestFile>(destroyed);
	Future<Reference<IAsyncFile>> readyFile{ Reference<IAsyncFile>(raw) };
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(readyFile);
	bool succeededImmediately = opened.isReady() && !opened.isError();
	bool successContextUnchanged =
	    g_simulator->getCurrentProcess() == processes.getCaller() && g_network->getCurrentTask() == callerPriority;
	processes.shutdown(processes.getCaller());
	auto tied = AsyncFileDetachable::open(readyFile);
	bool shutdownImmediately = tied.isReady() && tied.isError() && tied.getError().code() == error_code_io_error &&
	                           tied.getError().isInjectedFault();
	bool shutdownContextUnchanged =
	    g_simulator->getCurrentProcess() == processes.getCaller() && g_network->getCurrentTask() == callerPriority;
	co_await processes.onController();
	ASSERT(succeededImmediately);
	ASSERT(successContextUnchanged);
	ASSERT(shutdownImmediately);
	ASSERT(shutdownContextUnchanged);
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/cancelBeforeReturn") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcesses processes;
	Promise<Void> destroyed;
	auto raw = makeReference<DetachableTestFile>(destroyed);
	Promise<Reference<IAsyncFile>> input;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	auto opened = AsyncFileDetachable::open(input.getFuture());
	bool wasPending = !opened.isReady();
	co_await g_simulator->onProcess(processes.getProducer(), producerPriority);
	input.send(raw);
	bool awaitingReturn = !opened.isReady();
	opened.cancel();
	bool cancelledImmediately =
	    opened.isReady() && opened.isError() && opened.getError().code() == error_code_actor_cancelled;
	co_await g_simulator->onProcess(processes.getCaller(), callerPriority);
	co_await processes.onController();
	ASSERT(wasPending);
	ASSERT(awaitingReturn);
	ASSERT(cancelledImmediately);
	ASSERT(opened.isReady() && opened.isError());
	ASSERT_EQ(opened.getError().code(), error_code_actor_cancelled);
	opened = Future<Reference<IAsyncFile>>();
	ASSERT_EQ(input.getFutureReferenceCount(), 0);
	input = Promise<Reference<IAsyncFile>>();
	raw.clear();
	ASSERT(destroyed.getFuture().isReady());
}

void forceLinkAsyncFileDetachableTests() {}
