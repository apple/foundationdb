/*
 * AsyncFileNonDurable.cpp
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

#include "flow/CoroUtils.h"
#include "flow/UnitTest.h"

std::map<std::string, Future<Void>> AsyncFileNonDurable::filesBeingDeleted;

Future<Void> waitShutdownSignal() {
	return success(g_simulator->getCurrentProcess()->shutdownSignal.getFuture());
}

Future<Void> sendOnProcess(ISimulator::ProcessInfo* process, Promise<Void> promise, TaskPriority taskID) {
	co_await g_simulator->onProcess(process, taskID);
	promise.send(Void());
}

Future<Void> sendErrorOnProcess(ISimulator::ProcessInfo* process, Promise<Void> promise, Error e, TaskPriority taskID) {
	co_await g_simulator->onProcess(process, taskID);
	promise.sendError(e);
}

Future<Void> AsyncFileDetachable::doShutdown(AsyncFileDetachable* self) {
	co_await g_simulator->getCurrentProcess()->shutdownSignal.getFuture();
	self->file = Reference<IAsyncFile>();
}

Future<Reference<IAsyncFile>> AsyncFileDetachable::open(Future<Reference<IAsyncFile>> wrappedFile) {
	auto* process = g_simulator->getCurrentProcess();
	TaskPriority task = g_network->getCurrentTask();
	auto shutdown = process->shutdownSignal.getFuture();
	auto result = co_await race(shutdown, ready(wrappedFile));
	if (result.index() == 0) {
		throw io_error().asInjectedFault();
	}
	// Pending opens are shared within a machine. Restore the caller before delivering the result
	// or binding the detachable file to a process's shutdown signal.
	if (g_simulator->getCurrentProcess() != process || g_network->getCurrentTask() != task) {
		auto resumed = co_await race(shutdown, g_simulator->onProcess(process, task));
		if (resumed.index() == 0) {
			throw io_error().asInjectedFault();
		}
	}
	co_return makeReference<AsyncFileDetachable>(wrappedFile.get());
}

Future<int> AsyncFileDetachable::read(void* data, int length, int64_t offset) {
	if (!file.getPtr() || g_simulator->getCurrentProcess()->shutdownSignal.getFuture().isReady())
		return io_error().asInjectedFault();
	return sendErrorOnShutdown(file->read(data, length, offset), assertOnReadWriteCancel);
}

Future<Void> AsyncFileDetachable::write(void const* data, int length, int64_t offset) {
	if (!file.getPtr() || g_simulator->getCurrentProcess()->shutdownSignal.getFuture().isReady())
		return io_error().asInjectedFault();
	return sendErrorOnShutdown(file->write(data, length, offset), assertOnReadWriteCancel);
}

Future<Void> AsyncFileDetachable::truncate(int64_t size) {
	if (!file.getPtr() || g_simulator->getCurrentProcess()->shutdownSignal.getFuture().isReady())
		return io_error().asInjectedFault();
	return sendErrorOnShutdown(file->truncate(size));
}

Future<Void> AsyncFileDetachable::sync() {
	if (!file.getPtr() || g_simulator->getCurrentProcess()->shutdownSignal.getFuture().isReady())
		return io_error().asInjectedFault();
	return sendErrorOnShutdown(file->sync());
}

Future<int64_t> AsyncFileDetachable::size() const {
	if (!file.getPtr() || g_simulator->getCurrentProcess()->shutdownSignal.getFuture().isReady())
		return io_error().asInjectedFault();
	return sendErrorOnShutdown(file->size());
}

Future<Reference<IAsyncFile>> AsyncFileNonDurable::open(std::string filename,
                                                        std::string actualFilename,
                                                        Future<Reference<IAsyncFile>> wrappedFile,
                                                        Reference<DiskParameters> diskParameters,
                                                        bool aio) {
	ISimulator::ProcessInfo* currentProcess = g_simulator->getCurrentProcess();
	TaskPriority currentTaskID = g_network->getCurrentTask();
	Future<Void> shutdown = success(currentProcess->shutdownSignal.getFuture());

	//TraceEvent("AsyncFileNonDurableOpenBegin").detail("Filename", filename).detail("Addr", g_simulator->getCurrentProcess()->address);
	co_await g_simulator->onMachine(currentProcess);
	Error err;
	try {
		co_await (success(wrappedFile) || shutdown);

		if (shutdown.isReady())
			throw io_error().asInjectedFault();

		Reference<IAsyncFile> file = wrappedFile.get();

		// If we are in the process of deleting a file, we can't let someone else modify it at the same time.  We
		// therefore block the creation of new files until deletion is complete
		auto deletedFile = AsyncFileNonDurable::filesBeingDeleted.find(filename);
		if (deletedFile != AsyncFileNonDurable::filesBeingDeleted.end()) {
			//TraceEvent("AsyncFileNonDurableOpenWaitOnDelete1").detail("Filename", filename);
			co_await (deletedFile->second || shutdown);
			//TraceEvent("AsyncFileNonDurableOpenWaitOnDelete2").detail("Filename", filename);
			if (shutdown.isReady())
				throw io_error().asInjectedFault();
			co_await g_simulator->onProcess(currentProcess, currentTaskID);
		}

		Reference<AsyncFileNonDurable> nonDurableFile(
		    new AsyncFileNonDurable(filename, actualFilename, file, diskParameters, currentProcess->address, aio));

		// Causes the approximateSize member to be set
		Future<int64_t> sizeFuture = nonDurableFile->size();
		co_await (success(sizeFuture) || shutdown);

		if (shutdown.isReady())
			throw io_error().asInjectedFault();

		//TraceEvent("AsyncFileNonDurableOpenComplete").detail("Filename", filename);

		co_await g_simulator->onProcess(currentProcess, currentTaskID);
		co_return nonDurableFile;
	} catch (Error& e) {
		err = e;
	}

	std::string currentFilename =
	    (wrappedFile.isReady() && !wrappedFile.isError()) ? wrappedFile.get()->getFilename() : actualFilename;
	currentProcess->machine->openFiles.erase(currentFilename);
	//TraceEvent("AsyncFileNonDurableOpenError").errorUnsuppressed(e).detail("Filename", filename).detail("Address", currentProcess->address).detail("Addr", g_simulator->getCurrentProcess()->address);
	co_await g_simulator->onProcess(currentProcess, currentTaskID);
	throw err;
}

Future<int> AsyncFileNonDurable::read(AsyncFileNonDurable* self, void* data, int length, int64_t offset) {
	ISimulator::ProcessInfo* currentProcess = g_simulator->getCurrentProcess();
	TaskPriority currentTaskID = g_network->getCurrentTask();
	co_await g_simulator->onMachine(currentProcess);

	Error err;
	try {
		int rep = co_await self->onRead(self, data, length, offset);
		co_await g_simulator->onProcess(currentProcess, currentTaskID);
		co_return rep;
	} catch (Error& e) {
		err = e;
	}

	co_await g_simulator->onProcess(currentProcess, currentTaskID);
	throw err;
}

Future<Void> AsyncFileNonDurable::closeFile(AsyncFileNonDurable* self) {
	ISimulator::ProcessInfo* currentProcess = g_simulator->getCurrentProcess();

	g_simulator->getMachineByNetworkAddress(self->openedAddress)->deletingOrClosingFiles.insert(self->getFilename());

	co_await g_simulator->onMachine(currentProcess);
	// Make sure all writes have gone through.
	Promise<bool> startSyncPromise = self->startSyncPromise;
	self->startSyncPromise = Promise<bool>();
	startSyncPromise.send(true);

	std::vector<Future<Void>> outstandingModifications;

	for (auto itr = self->pendingModifications.ranges().begin(); itr != self->pendingModifications.ranges().end();
	     ++itr)
		if (itr->value().isValid() && !itr->value().isReady())
			outstandingModifications.push_back(itr->value());

	// Ignore errors here so that all modifications can finish
	co_await waitForAllReady(outstandingModifications);

	// Make sure we aren't in the process of killing the file
	if (self->killed.isSet())
		co_await self->killComplete.getFuture();

	// Remove this file from the filesBeingDeleted map so that new files can be created with this filename
	g_simulator->getMachineByNetworkAddress(self->openedAddress)->closingFiles.erase(self->getFilename());
	g_simulator->getMachineByNetworkAddress(self->openedAddress)->deletingOrClosingFiles.erase(self->getFilename());
	AsyncFileNonDurable::filesBeingDeleted.erase(self->filename);
	//TraceEvent("AsyncFileNonDurable_FinishDelete", self->id).detail("Filename", self->filename);

	delete self;
}

void AsyncFileNonDurable::removeOpenFile(std::string filename, AsyncFileNonDurable* file) {
	auto& openFiles = g_simulator->getCurrentProcess()->machine->openFiles;

	auto iter = openFiles.find(filename);

	// Various actions (e.g. simulated delete) can remove a file from openFiles prematurely, so it may already
	// be gone. Renamed files (from atomic write and create) will also be present under only one of the two
	// names.
	if (iter != openFiles.end()) {
		// even if the filename exists, it doesn't mean that it references the same file. It could be that the
		// file was renamed and later a file with the same name was opened.
		if (iter->second.getPtrIfReady().orDefault(nullptr) == file) {
			openFiles.erase(iter);
		}
	}
}

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

class DetachableTestProcess {
public:
	DetachableTestProcess() : controller(g_simulator->getCurrentProcess()), priority(g_network->getCurrentTask()) {
		caller = g_simulator->newProcess("DetachableTestCaller",
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
		caller->excludeFromRestarts = true;
	}
	~DetachableTestProcess() { g_simulator->destroyProcess(caller); }
	ISimulator::ProcessInfo* getCaller() const { return caller; }
	Future<Void> onController() const { return g_simulator->onProcess(controller, priority); }
	void shutdown() const { caller->shutdownSignal.send(ISimulator::KillType::RebootProcess); }

private:
	ISimulator::ProcessInfo* controller;
	TaskPriority priority;
	ISimulator::ProcessInfo* caller;
};

Future<Reference<IAsyncFile>> checkOpenContext(Future<Reference<IAsyncFile>> opened,
                                               DetachableTestProcess* process,
                                               Error expected) {
	auto result = co_await errorOr(opened);
	ASSERT(g_simulator->getCurrentProcess() == process->getCaller());
	ASSERT(g_network->getCurrentTask() == TaskPriority::DiskRead);
	ASSERT(result.isError() == expected.isValid());
	if (expected.isValid()) {
		ASSERT_EQ(result.getError().code(), expected.code());
	}
	// Release the input before leaving its completion stack, so it cannot retain the raw file.
	opened = Future<Reference<IAsyncFile>>();
	co_await process->onController();
	co_return result.isError() ? Reference<IAsyncFile>() : result.get();
}

} // namespace

TEST_CASE("/fdbrpc/AsyncFileDetachable/openContextAndShutdown") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	for (auto [sameProcess, error] : { std::pair{ false, Error() },
	                                   { true, Error() },
	                                   { false, file_not_found() },
	                                   { false, actor_cancelled() } }) {
		DetachableTestProcess process;
		Promise<Void> destroyed;
		auto raw = makeReference<DetachableTestFile>(destroyed);
		Promise<Reference<IAsyncFile>> input;
		co_await g_simulator->onProcess(process.getCaller(), TaskPriority::DiskRead);
		auto opened = AsyncFileDetachable::open(input.getFuture());
		ASSERT(!opened.isReady());
		auto checked = checkOpenContext(opened, &process, error);
		co_await (sameProcess ? g_simulator->onProcess(process.getCaller(), TaskPriority::DefaultYield)
		                      : process.onController());
		if (error.isValid()) {
			input.sendError(error);
		} else {
			input.send(raw);
		}
		co_await process.onController();
		auto file = co_await checked;
		ASSERT(bool(file) == !error.isValid());
		checked = Future<Reference<IAsyncFile>>();
		opened = Future<Reference<IAsyncFile>>();
		input = Promise<Reference<IAsyncFile>>();
		raw.clear();
		if (file) {
			ASSERT(!destroyed.getFuture().isReady());
			ASSERT(file->getFilename() == "detachable-test-file");
			process.shutdown();
			ASSERT(destroyed.getFuture().isReady());
			try {
				(void)file->getFilename();
				ASSERT(false);
			} catch (Error& e) {
				ASSERT_EQ(e.code(), error_code_io_error);
				ASSERT(e.isInjectedFault());
			}
		}
	}
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openInterrupted") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	for (bool cancel : { false, true }) {
		for (bool duringReturn : { false, true }) {
			DetachableTestProcess process;
			Promise<Void> destroyed;
			auto raw = makeReference<DetachableTestFile>(destroyed);
			Promise<Reference<IAsyncFile>> input;
			co_await g_simulator->onProcess(process.getCaller(), TaskPriority::DiskRead);
			auto opened = AsyncFileDetachable::open(input.getFuture());
			co_await process.onController();
			if (duringReturn) {
				input.send(raw);
			}
			ASSERT(!opened.isReady());
			if (cancel) {
				opened.cancel();
			} else {
				process.shutdown();
			}
			ASSERT(opened.isReady() && opened.isError());
			Error expected = opened.getError();
			ASSERT_EQ(expected.code(), cancel ? error_code_actor_cancelled : error_code_io_error);
			ASSERT(cancel || expected.isInjectedFault());
			if (!duringReturn) {
				input.send(raw);
			}
			// Drain a queued caller handoff and ensure it cannot replace the interruption.
			co_await g_simulator->onProcess(process.getCaller(), TaskPriority::DiskRead);
			co_await process.onController();
			ASSERT(opened.isError() && opened.getError().code() == expected.code());
			opened = Future<Reference<IAsyncFile>>();
			ASSERT_EQ(input.getFutureReferenceCount(), 0);
			input = Promise<Reference<IAsyncFile>>();
			raw.clear();
			ASSERT(destroyed.getFuture().isReady());
		}
	}
}

TEST_CASE("/fdbrpc/AsyncFileDetachable/openAlreadyReady") {
	if (!g_network->isSimulated()) {
		co_return;
	}
	DetachableTestProcess process;
	Promise<Void> destroyed;
	Future<Reference<IAsyncFile>> input{ makeReference<DetachableTestFile>(destroyed) };
	co_await g_simulator->onProcess(process.getCaller(), TaskPriority::DiskRead);
	auto opened = AsyncFileDetachable::open(input);
	ASSERT(opened.isReady() && !opened.isError());
	process.shutdown();
	auto tied = AsyncFileDetachable::open(input);
	ASSERT(tied.isReady() && tied.isError());
	ASSERT_EQ(tied.getError().code(), error_code_io_error);
	ASSERT(tied.getError().isInjectedFault());
	ASSERT(g_simulator->getCurrentProcess() == process.getCaller());
	ASSERT(g_network->getCurrentTask() == TaskPriority::DiskRead);
	co_await process.onController();
}
