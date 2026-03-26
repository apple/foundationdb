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
	co_await success(g_simulator->getCurrentProcess()->shutdownSignal.getFuture());
	self->file = Reference<IAsyncFile>();
}

Future<Reference<IAsyncFile>> AsyncFileDetachable::open(Future<Reference<IAsyncFile>> wrappedFile) {
	auto res = co_await race(g_simulator->getCurrentProcess()->shutdownSignal.getFuture(), wrappedFile);
	if (res.index() == 0) {
		throw io_error().asInjectedFault();
	} else if (res.index() == 1) {
		Reference<IAsyncFile> f = std::get<1>(std::move(res));
		co_return makeReference<AsyncFileDetachable>(f);
	}
	UNREACHABLE();
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
		std::map<std::string, Future<Void>>::iterator deletedFile =
		    AsyncFileNonDurable::filesBeingDeleted.find(filename);
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
