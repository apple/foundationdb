/*
 * RocksDBLogForwarder.actor.cpp
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

#ifdef WITH_ROCKSDB
#include "fdbserver/RocksDBLogForwarder.h"

#include "flow/network.h"
#include "flow/Trace.h"
#include "fdbrpc/simulator.h"

#include "flow/actorcompiler.h" // This must be the last include file

using InfoLogLevel = rocksdb::InfoLogLevel;

namespace {

Severity getSeverityFromLogLevel(const InfoLogLevel& log_level) {
	switch (log_level) {
	case InfoLogLevel::DEBUG_LEVEL:
		return SevDebug;
	case InfoLogLevel::INFO_LEVEL:
		return SevInfo;
	case InfoLogLevel::WARN_LEVEL:
		return SevWarn;
	case InfoLogLevel::ERROR_LEVEL:
		return SevError;
	case InfoLogLevel::FATAL_LEVEL:
		return SevError;
	case InfoLogLevel::HEADER_LEVEL:
		return SevVerbose;
	case InfoLogLevel::NUM_INFO_LOG_LEVELS:
		ASSERT(false);
	}
	UNREACHABLE();
}

} // namespace

namespace details {

void logTraceEvent(const RocksDBLogRecord& record) {
	TraceEvent event = TraceEvent(record.severity, "RocksDBLogRecord", record.uid);
	event.detail("RocksDBLogTime", record.logReceiveTime);

	{
		std::stringstream ss;
		ss << record.threadID;
		event.detail("RocksDBThreadID", ss.str());
	}

	for (const auto& [k, v] : record.kvPairs) {
		event.detail(k.c_str(), v);
	}
}

ACTOR Future<Void> rocksDBPeriodicallyLogger(RocksDBLogger* pRecords) {
	loop choose {
		when(wait(delay(0.1))) {
			pRecords->consume();
		}
	}
}

RocksDBLogger::RocksDBLogger()
  : mainThreadId(std::this_thread::get_id()), periodicLogger(rocksDBPeriodicallyLogger(this)) {}

void RocksDBLogger::inject(RocksDBLogRecord&& record) {
	const std::thread::id threadId = std::this_thread::get_id();
	if (threadId == mainThreadId) {
		// In the main thread, it is *NOT* necessary to cache the record.
		logTraceEvent(record);

		consume();
	} else {
		const std::lock_guard<std::mutex> lockGuard(recordsMutex);

		logRecords.emplace_back();
		logRecords.back() = std::move(record);
	}
}

void RocksDBLogger::consume() {
	std::vector<RocksDBLogRecord> currentRecords;
	{
		const std::lock_guard<std::mutex> lockGuard(recordsMutex);
		currentRecords.swap(logRecords);
	}

	for (const auto& record : currentRecords) {
		logTraceEvent(record);
	}
}

} // namespace details

RocksDBLogForwarder::RocksDBLogForwarder(const UID& id_, const InfoLogLevel log_level)
  : rocksdb::Logger(log_level), id(id_), logger() {
	TraceEvent(SevInfo, "RocksDBLoggerStart", id);
}

RocksDBLogForwarder::~RocksDBLogForwarder() {
	TraceEvent(SevInfo, "RocksDBLoggerStop", id);
}

void RocksDBLogForwarder::Logv(const char* format, va_list ap) {
	Logv(InfoLogLevel::INFO_LEVEL, format, ap);
}

void RocksDBLogForwarder::Logv(const InfoLogLevel log_level, const char* format, va_list ap) {
	const std::thread::id threadID = std::this_thread::get_id();

	// FIXME: Restrict the RocksDB log level to warn in order to prevent almost all simulation test failure. This has to
	// be reconsidered.
	const Severity severity = std::min(getSeverityFromLogLevel(log_level), SevWarn);

	// TODO: Parse the log information into KV pairs
	// At this stage vsnprintf is used
	char buf[1024];
	vsnprintf(buf, 1024, format, ap);
	if (severity < SevError) {
		logger.inject(details::RocksDBLogRecord{ now(), severity, id, threadID, { { "Text", std::string(buf) } } });
	} else {
		logger.inject(details::RocksDBLogRecord{
		    now(),
		    severity,
		    id,
		    threadID,
		    { { "Text", std::string(buf) }, { "OriginalBacktrace", platform::get_backtrace() } } });
	}
}

#endif // WITH_ROCKSDB
