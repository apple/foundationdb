/*
 * Profiler.actor.cpp
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

#include "flow/flow.h"
#include "flow/network.h"

#ifdef __linux__

#include <execinfo.h>
#include <signal.h>
#include <sys/time.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <link.h>

#include "flow/Platform.h"
#include "flow/actorcompiler.h" // This must be the last include.

extern volatile thread_local int profilingEnabled;

static uint64_t sys_gettid() {
	return syscall(__NR_gettid);
}

struct SignalClosure {
	void (*func)(int, siginfo_t*, void*, void*);
	void* userdata;

	SignalClosure(void (*func)(int, siginfo_t*, void*, void*), void* userdata) : func(func), userdata(userdata) {}

	static void signal_handler(int s, siginfo_t* si, void* ucontext) {
		// async signal safe!
		// This is intended to work as a SIGPROF handler for past and future versions of the flow profiler (when
		// multiple are running in a process!) So don't change what it does without really good reason
		SignalClosure* closure = (SignalClosure*)(si->si_value.sival_ptr);
		closure->func(s, si, ucontext, closure->userdata);
	}
};

struct SyncFileForSim : ReferenceCounted<SyncFileForSim> {
	FILE* f;
	SyncFileForSim(std::string const& filename) { f = fopen(filename.c_str(), "wb"); }

	bool isOpen() const { return f != nullptr; }

	int64_t debugFD() const { return (int64_t)f; }

	Future<int> read(void* data, int length, int64_t offset) {
		ASSERT(false);
		throw internal_error();
	}

	Future<Void> write(void const* data, int length, int64_t offset) {
		ASSERT(isOpen());
		fseek(f, offset, SEEK_SET);
		if (fwrite(data, 1, length, f) != length)
			throw io_error();
		return Void();
	}

	Future<Void> truncate(int64_t size) {
		ASSERT(size == 0);
		return Void();
	}

	Future<Void> flush() {
		ASSERT(isOpen());
		fflush(f);
		return Void();
	}

	Future<Void> sync() {
		ASSERT(false);
		throw internal_error();
	}

	Future<int64_t> size() const {
		ASSERT(false);
		throw internal_error();
	}
};

struct Profiler {
	struct OutputBuffer {
		std::vector<void*> output;

		OutputBuffer() { output.reserve(100000); }
		void clear() { output.clear(); }
		void push(void* ptr) { // async signal safe!
			if (output.size() < output.capacity())
				output.push_back(ptr);
		}
		Future<Void> writeTo(Reference<SyncFileForSim> file, int64_t& offset) {
			int64_t offs = offset;
			offset += sizeof(void*) * output.size();
			return file->write(&output[0], sizeof(void*) * output.size(), offs);
		}
	};

	enum { MAX_STACK_DEPTH = 256 };

	void* addresses[MAX_STACK_DEPTH];
	SignalClosure signalClosure;
	OutputBuffer* output_buffer;
	Future<Void> actor;
	sigset_t profilingSignals;
	static Profiler* active_profiler;
	BinaryWriter environmentInfoWriter;
	INetwork* network;
	timer_t periodicTimer;
	bool timerInitialized;

	Profiler(int period, std::string const& outfn, INetwork* network)
	  : signalClosure(signal_handler_for_closure, this), environmentInfoWriter(Unversioned()), network(network),
	    timerInitialized(false) {
		actor = profile(this, period, outfn);
	}

	~Profiler() {
		enableSignal(false);

		if (timerInitialized) {
			timer_delete(periodicTimer);
		}
	}

	void signal_handler() { // async signal safe!
		static std::atomic<bool> inSigHandler = false;
		if (inSigHandler.exchange(true)) {
			return;
		}
		if (profilingEnabled) {
			double t = timer();
			output_buffer->push(*(void**)&t);
			size_t n = platform::raw_backtrace(addresses, 256);
			for (int i = 0; i < n; i++)
				output_buffer->push(addresses[i]);
			output_buffer->push((void*)-1LL);
		}
		inSigHandler.store(false);
	}

	static void signal_handler_for_closure(int, siginfo_t* si, void*, void* self) { // async signal safe!
		((Profiler*)self)->signal_handler();
	}

	void enableSignal(bool enabled) { sigprocmask(enabled ? SIG_UNBLOCK : SIG_BLOCK, &profilingSignals, nullptr); }

	void phdr(struct dl_phdr_info* info) {
		environmentInfoWriter << int64_t(1) << info->dlpi_addr
		                      << StringRef((const uint8_t*)info->dlpi_name, strlen(info->dlpi_name));
		for (int s = 0; s < info->dlpi_phnum; s++) {
			auto const& h = info->dlpi_phdr[s];
			environmentInfoWriter << int64_t(2) << h.p_type << h.p_flags // Word (uint32_t)
			                      << h.p_offset // Off (uint64_t)
			                      << h.p_vaddr << h.p_paddr // Addr (uint64_t)
			                      << h.p_filesz << h.p_memsz << h.p_align; // XWord (uint64_t)
		}
	}

	static int phdr_callback(struct dl_phdr_info* info, size_t size, void* data) {
		((Profiler*)data)->phdr(info);
		return 0;
	}

	ACTOR static Future<Void> profile(Profiler* self, int period, std::string outfn) {
		// Open and truncate output file
		state Reference<SyncFileForSim> outFile = makeReference<SyncFileForSim>(outfn);
		if (!outFile->isOpen()) {
			TraceEvent(SevWarn, "FailedToOpenProfilingOutputFile").detail("Filename", outfn).GetLastError();
			return Void();
		}

		// According to folk wisdom, calling this once before setting up the signal handler makes
		// it async signal safe in practice :-/
		platform::raw_backtrace(self->addresses, MAX_STACK_DEPTH);

		// Write environment information header
		// At the moment this consists of the output of dl_iterate_phdr, the locations of
		// all shared objects loaded into this process (to help locate symbols) and the period in ns
		self->environmentInfoWriter << int64_t(0x101) << int64_t(period * 1000);
		dl_iterate_phdr(phdr_callback, self);
		self->environmentInfoWriter << int64_t(0);
		while (self->environmentInfoWriter.getLength() % sizeof(void*))
			self->environmentInfoWriter << uint8_t(0);

		self->output_buffer = new OutputBuffer;
		state OutputBuffer* otherBuffer = new OutputBuffer;

		// The profilingSignals signal set will be used by enableSignal
		sigemptyset(&self->profilingSignals);
		sigaddset(&self->profilingSignals, SIGPROF);

		// Set up profiling signal handler
		struct sigaction act;
		act.sa_sigaction = SignalClosure::signal_handler;
		sigemptyset(&act.sa_mask);
		act.sa_flags = SA_SIGINFO;
		sigaction(SIGPROF, &act, nullptr);

		// Set up periodic profiling timer
		int period_ns = period * 1000;
		itimerspec tv;
		tv.it_interval.tv_sec = 0;
		tv.it_interval.tv_nsec = period_ns;
		tv.it_value.tv_sec = 0;
		tv.it_value.tv_nsec = nondeterministicRandom()->randomInt(period_ns / 2, period_ns + 1);

		sigevent sev;
		sev.sigev_notify = SIGEV_THREAD_ID;
		sev.sigev_signo = SIGPROF;
		sev.sigev_value.sival_ptr = &(self->signalClosure);
		sev._sigev_un._tid = sys_gettid();
		if (timer_create(CLOCK_THREAD_CPUTIME_ID, &sev, &self->periodicTimer) != 0) {
			TraceEvent(SevWarn, "FailedToCreateProfilingTimer").GetLastError();
			return Void();
		}
		self->timerInitialized = true;
		if (timer_settime(self->periodicTimer, 0, &tv, nullptr) != 0) {
			TraceEvent(SevWarn, "FailedToSetProfilingTimer").GetLastError();
			return Void();
		}

		state int64_t outOffset = 0;
		wait(outFile->truncate(outOffset));

		wait(outFile->write(self->environmentInfoWriter.getData(), self->environmentInfoWriter.getLength(), outOffset));
		outOffset += self->environmentInfoWriter.getLength();

		loop {
			wait(self->network->delay(1.0, TaskPriority::Min) || self->network->delay(2.0, TaskPriority::Max));

			self->enableSignal(false);
			std::swap(self->output_buffer, otherBuffer);
			self->enableSignal(true);

			wait(otherBuffer->writeTo(outFile, outOffset));
			wait(outFile->flush());
			otherBuffer->clear();
		}
	}
};

// Outlives main
Profiler* Profiler::active_profiler = nullptr;

std::string findAndReplace(std::string const& fn, std::string const& symbol, std::string const& value) {
	auto i = fn.find(symbol);
	if (i == std::string::npos)
		return fn;
	return fn.substr(0, i) + value + fn.substr(i + symbol.size());
}

void startProfiling(INetwork* network,
                    Optional<int> maybePeriod /*= {}*/,
                    Optional<StringRef> maybeOutputFile /*= {}*/) {
	int period;
	if (maybePeriod.present()) {
		period = maybePeriod.get();
	} else {
		const char* periodEnv = getenv("FLOW_PROFILER_PERIOD");
		period = (periodEnv ? atoi(periodEnv) : 2000);
	}
	std::string outputFile;
	if (maybeOutputFile.present()) {
		outputFile = std::string((const char*)maybeOutputFile.get().begin(), maybeOutputFile.get().size());
	} else {
		const char* outfn = getenv("FLOW_PROFILER_OUTPUT");
		outputFile = (outfn ? outfn : "profile.bin");
	}
	outputFile = findAndReplace(
	    findAndReplace(
	        findAndReplace(outputFile, "%ADDRESS%", findAndReplace(network->getLocalAddress().toString(), ":", ".")),
	        "%PID%",
	        format("%d", getpid())),
	    "%TID%",
	    format("%llx", (long long)sys_gettid()));

	if (!Profiler::active_profiler)
		Profiler::active_profiler = new Profiler(period, outputFile, network);
}

void stopProfiling() {
	if (Profiler::active_profiler) {
		Profiler* p = Profiler::active_profiler;
		Profiler::active_profiler = nullptr;
		delete p;
	}
}

#else

void startProfiling(INetwork* network, Optional<int> period, Optional<StringRef> outputFile) {}
void stopProfiling() {}

#endif
