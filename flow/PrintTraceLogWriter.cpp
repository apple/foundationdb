/*
 * PrintTraceLogWriter.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/PrintTraceLogWriter.h"
#include "flow/Platform.h"
#include "flow/flow.h"
#include "flow/ThreadHelper.actor.h"

PrintTraceLogWriter::PrintTraceLogWriter(Stream stream) {
	if (stream == Stream::STDOUT) {
		outStream = stdout;
	} else if (stream == Stream::STDERR) {
		outStream = stderr;
	} else {
		ASSERT(false);
	}
}

void PrintTraceLogWriter::addref() {
	ReferenceCounted<PrintTraceLogWriter>::addref();
}

void PrintTraceLogWriter::delref() {
	ReferenceCounted<PrintTraceLogWriter>::delref();
}

void PrintTraceLogWriter::write(const std::string& str) {
	write(str.data(), str.size());
}

void PrintTraceLogWriter::write(const StringRef& str) {
	write(reinterpret_cast<const char*>(str.begin()), str.size());
}

void PrintTraceLogWriter::write(const char* str, int size) {
	fprintf(outStream, "%.*s\n", size, str)
}

void PrintTraceLogWriter::open(TraceLogWriterParams const& params) {}

void PrintTraceLogWriter::setNetworkAddress(NetworkAddress const& address) {}

void PrintTraceLogWriter::close() {}

void PrintTraceLogWriter::roll() {}

void PrintTraceLogWriter::sync() {}
