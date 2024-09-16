/*
 * AsyncFileNonDurable.actor.cpp
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

#include "fdbrpc/AsyncFileNonDurable.actor.h"

#include "fdbrpc/SimulatorMachineInfo.h"
#include "fdbrpc/SimulatorProcessInfo.h"

#include "flow/actorcompiler.h" // has to be last include

std::map<std::string, Future<Void>> AsyncFileNonDurable::filesBeingDeleted;

Future<Void> waitShutdownSignal() {
	return success(g_simulator->getCurrentProcess()->shutdownSignal.getFuture());
}

ACTOR Future<Void> sendOnProcess(ISimulator::ProcessInfo* process, Promise<Void> promise, TaskPriority taskID) {
	wait(g_simulator->onProcess(process, taskID));
	promise.send(Void());
	return Void();
}

ACTOR Future<Void> sendErrorOnProcess(ISimulator::ProcessInfo* process,
                                      Promise<Void> promise,
                                      Error e,
                                      TaskPriority taskID) {
	wait(g_simulator->onProcess(process, taskID));
	promise.sendError(e);
	return Void();
}

ACTOR Future<Void> AsyncFileDetachable::doShutdown(AsyncFileDetachable* self) {
	wait(success(g_simulator->getCurrentProcess()->shutdownSignal.getFuture()));
	self->file = Reference<IAsyncFile>();
	return Void();
}

ACTOR Future<Reference<IAsyncFile>> AsyncFileDetachable::open(Future<Reference<IAsyncFile>> wrappedFile) {
	choose {
		when(wait(success(g_simulator->getCurrentProcess()->shutdownSignal.getFuture()))) {
			throw io_error().asInjectedFault();
		}
		when(Reference<IAsyncFile> f = wait(wrappedFile)) {
			return makeReference<AsyncFileDetachable>(f);
		}
	}
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

ACTOR Future<Reference<IAsyncFile>> AsyncFileNonDurable::open(std::string filename,
                                                              std::string actualFilename,
                                                              Future<Reference<IAsyncFile>> wrappedFile,
                                                              Reference<DiskParameters> diskParameters,
                                                              bool aio) {
	state ISimulator::ProcessInfo* currentProcess = g_simulator->getCurrentProcess();
	state TaskPriority currentTaskID = g_network->getCurrentTask();
	state Future<Void> shutdown = success(currentProcess->shutdownSignal.getFuture());

	//TraceEvent("AsyncFileNonDurableOpenBegin").detail("Filename", filename).detail("Addr", g_simulator->getCurrentProcess()->address);
	wait(g_simulator->onMachine(currentProcess));
	try {
		wait(success(wrappedFile) || shutdown);

		if (shutdown.isReady())
			throw io_error().asInjectedFault();

		state Reference<IAsyncFile> file = wrappedFile.get();

		// If we are in the process of deleting a file, we can't let someone else modify it at the same time.  We
		// therefore block the creation of new files until deletion is complete
		state std::map<std::string, Future<Void>>::iterator deletedFile =
		    AsyncFileNonDurable::filesBeingDeleted.find(filename);
		if (deletedFile != AsyncFileNonDurable::filesBeingDeleted.end()) {
			//TraceEvent("AsyncFileNonDurableOpenWaitOnDelete1").detail("Filename", filename);
			wait(deletedFile->second || shutdown);
			//TraceEvent("AsyncFileNonDurableOpenWaitOnDelete2").detail("Filename", filename);
			if (shutdown.isReady())
				throw io_error().asInjectedFault();
			wait(g_simulator->onProcess(currentProcess, currentTaskID));
		}

		state Reference<AsyncFileNonDurable> nonDurableFile(
		    new AsyncFileNonDurable(filename, actualFilename, file, diskParameters, currentProcess->address, aio));

		// Causes the approximateSize member to be set
		state Future<int64_t> sizeFuture = nonDurableFile->size();
		wait(success(sizeFuture) || shutdown);

		if (shutdown.isReady())
			throw io_error().asInjectedFault();

		//TraceEvent("AsyncFileNonDurableOpenComplete").detail("Filename", filename);

		wait(g_simulator->onProcess(currentProcess, currentTaskID));

		return nonDurableFile;
	} catch (Error& e) {
		state Error err = e;
		std::string currentFilename =
		    (wrappedFile.isReady() && !wrappedFile.isError()) ? wrappedFile.get()->getFilename() : actualFilename;
		currentProcess->machine->openFiles.erase(currentFilename);
		//TraceEvent("AsyncFileNonDurableOpenError").errorUnsuppressed(e).detail("Filename", filename).detail("Address", currentProcess->address).detail("Addr", g_simulator->getCurrentProcess()->address);
		wait(g_simulator->onProcess(currentProcess, currentTaskID));
		throw err;
	}
}

ACTOR Future<int> AsyncFileNonDurable::read(AsyncFileNonDurable* self, void* data, int length, int64_t offset) {
	state ISimulator::ProcessInfo* currentProcess = g_simulator->getCurrentProcess();
	state TaskPriority currentTaskID = g_network->getCurrentTask();
	wait(g_simulator->onMachine(currentProcess));

	try {
		state int rep = wait(self->onRead(self, data, length, offset));
		wait(g_simulator->onProcess(currentProcess, currentTaskID));
		return rep;
	} catch (Error& e) {
		state Error err = e;
		wait(g_simulator->onProcess(currentProcess, currentTaskID));
		throw err;
	}
}

ACTOR Future<Void> AsyncFileNonDurable::closeFile(AsyncFileNonDurable* self) {
	state ISimulator::ProcessInfo* currentProcess = g_simulator->getCurrentProcess();
	state std::string filename = self->filename;

	g_simulator->getMachineByNetworkAddress(self->openedAddress)->deletingOrClosingFiles.insert(self->getFilename());

	wait(g_simulator->onMachine(currentProcess));
	try {
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
		wait(waitForAllReady(outstandingModifications));

		// Make sure we aren't in the process of killing the file
		if (self->killed.isSet())
			wait(self->killComplete.getFuture());

		// Remove this file from the filesBeingDeleted map so that new files can be created with this filename
		g_simulator->getMachineByNetworkAddress(self->openedAddress)->closingFiles.erase(self->getFilename());
		g_simulator->getMachineByNetworkAddress(self->openedAddress)->deletingOrClosingFiles.erase(self->getFilename());
		AsyncFileNonDurable::filesBeingDeleted.erase(self->filename);
		//TraceEvent("AsyncFileNonDurable_FinishDelete", self->id).detail("Filename", self->filename);

		delete self;
		return Void();
	} catch (Error& e) {
		state Error err = e;
		throw err;
	}
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
