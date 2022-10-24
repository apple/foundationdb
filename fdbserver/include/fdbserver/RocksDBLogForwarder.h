/*
 * RocksDBLogForwarder.h
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

#ifndef __ROCKSDB_LOG_FORWARDER_H__
#define __ROCKSDB_LOG_FORWARDER_H__

#include <cstdarg>
#include <thread>

#include <rocksdb/env.h>

#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"

namespace details {

// Stores a RocksDB log line, transformed into Key/Value pairs
struct RocksDBLogRecord {
	double logReceiveTime;
	Severity severity;
	UID uid;
	std::thread::id threadID;
	std::vector<std::pair<std::string, std::string>> kvPairs;
};

// Stores RocksDB log lines for furthur consumption.
// *NOTE* This logger *MUST* run in a thread that is able to generate TraceEvents, e.g. in the event loop thread.
class RocksDBLogger {
	// The mutex that protects log records, as RocksDB is multi-threaded
	std::mutex recordsMutex;

	// Main thread ID. Only triggers TraceEvent when on main thread. In FDB only the main thread contains information
	// that could thread.
	const std::thread::id mainThreadId;

	// The log record
	std::vector<RocksDBLogRecord> records;

	// An ACTOR that logs the non-main thread data periodically
	Future<Void> periodicLogger;

public:
	// Constructor
	RocksDBLogger();

	// *Moves* the record to internal records
	void inject(RocksDBLogRecord&& record);

	// Consumes all the records
	void consume();
};

} // namespace details

class RocksDBLogForwarder : public rocksdb::Logger {
	// The ID of the RocksDB instance
	const UID id;

	// The cache that stores the logs from RocksDB
	details::RocksDBLogger records;

public:
	// Constructor
	// id is the UID of the logger
	// log_level specifies the log level
	explicit RocksDBLogForwarder(const UID& id,
	                             const rocksdb::InfoLogLevel log_level = rocksdb::InfoLogLevel::INFO_LEVEL);

	// Destructor
	virtual ~RocksDBLogForwarder();

	// Writes an entry to the log file
	virtual void Logv(const char* format, va_list ap);

	// Writes an entry to the log file, with a specificied log level
	virtual void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap);
};

#endif // __ROCKSDB_LOG_FORWARDER_H__
