/*
 * Utils.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ptxn/test/Utils.h"

#include <iostream>
#include <iomanip>
#include <stdexcept>

#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/TestTLogPeek.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/String.h"

namespace ptxn::test {

UID randomUID() {
	return deterministicRandom()->randomUniqueID();
}

StorageTeamID getNewStorageTeamID() {
	return randomUID();
}

std::vector<StorageTeamID> generateRandomStorageTeamIDs(const int numStorageTeams) {
	std::vector<StorageTeamID> result(numStorageTeams);
	for (auto& item : result) {
		item = getNewStorageTeamID();
	}
	return result;
}

std::string getRandomAlnum(int lower, int upper) {
	return deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(lower, upper));
}

namespace print {

namespace {

template <typename T>
inline std::string formatKVPair(const std::string& key, const T& value) {
	return concatToString(std::setw(28), key, ": ", value);
}

template <typename T>
inline std::string formatKVPair(const std::string& key, const Optional<T>& optionalValue) {
	if (optionalValue.present()) {
		return formatKVPair(key, optionalValue.get());
	} else {
		return formatKVPair(key, "Optional value not present");
	}
}

inline std::string formatKVPair(const std::string& key, const MessageTransferModel& model) {
	switch (model) {
	case MessageTransferModel::StorageServerActivelyPull:
		return formatKVPair(key, "Storage Server pulls mutations from TLog");
	case MessageTransferModel::TLogActivelyPush:
		return formatKVPair(key, "TLog pushes mutations to Storage Server");
	default:
		throw internal_error_msg(
		    concatToString("Invalid MessageTransferModel value: ", static_cast<int>(model)).c_str());
	}
}

} // anonymous namespace

void print(const TLogCommitReply& reply) {
	std::cout << std::endl << ">>> TLogCommitReply" << std::endl;

	std::cout << formatKVPair("Version", reply.version) << std::endl;
}

void print(const TLogCommitRequest& request) {
	std::cout << std::endl << ">>> TLogCommitRequest" << std::endl;

	std::cout << formatKVPair("Span ID", request.spanID) << std::endl
	          << formatKVPair("Log Group ID", request.tLogGroupID) << std::endl
	          << formatKVPair("Debug ID", request.debugID) << std::endl
	          << formatKVPair("Message data length", request.messages.size()) << std::endl
	          << formatKVPair("Previous version", request.prevVersion) << std::endl
	          << formatKVPair("Version", request.version) << std::endl;
}

void print(const TLogPeekRequest& request) {
	std::cout << std::endl << ">>> TLogPeekRequest" << std::endl;

	std::cout << formatKVPair("Debug ID", request.debugID) << std::endl
	          << formatKVPair("Begin version", request.beginVersion) << std::endl
	          << formatKVPair("End version", request.endVersion) << std::endl
	          << formatKVPair("Team ID", request.storageTeamID) << std::endl
	          << formatKVPair("Log Group ID", request.tLogGroupID) << std::endl;
}

void print(const TLogPeekReply& reply) {
	std::cout << std::endl << ">> TLogPeekReply" << std::endl;

	std::cout << formatKVPair("Debug ID", reply.debugID) << std::endl
	          << formatKVPair("Serialized data length", reply.data.size()) << std::endl;
}

void print(const TestDriverOptions& option) {
	std::cout << std::endl << ">> ptxn/test//Driver.actor.cpp:DriverTestOptions:" << std::endl;

	std::cout << formatKVPair("numCommits", option.numCommits) << std::endl
	          << formatKVPair("numStorageTeams", option.numStorageTeams) << std::endl
	          << formatKVPair("numProxies", option.numProxies) << std::endl
	          << formatKVPair("numTLogs", option.numTLogs) << std::endl
	          << formatKVPair("numTLogGroups", option.numTLogGroups) << std::endl
	          << formatKVPair("numStorageServers", option.numStorageServers) << std::endl
	          << formatKVPair("numResolvers", option.numResolvers) << std::endl
	          << formatKVPair("skipCommitValidation", option.skipCommitValidation) << std::endl
	          << formatKVPair("Message Transfer Model", option.transferModel) << std::endl;
}

void print(const ptxn::test::TestTLogPeekOptions& option) {
	std::cout << std::endl << ">> ptxn/test//Driver.actor.cpp:DriverTestOptions:" << std::endl;

	std::cout << formatKVPair("Mutations", option.numMutations) << std::endl
	          << formatKVPair("Teams", option.numStorageTeams) << std::endl
	          << formatKVPair("Intial version", option.initialVersion) << std::endl;
}

void print(const CommitRecord& record) {
	std::cout << ">> ptxn/test/CommitUtils.h:CommitRecord:" << std::endl;
	for (const auto& [version, storageTeamIDMessageMap] : record.messages) {
		std::cout << "\n\tVersion: " << version << std::endl;
		for (const auto& [storageTeamID, subsequenceMessage] : storageTeamIDMessageMap) {
			std::cout << "\t\tStorage Team ID: " << storageTeamID.toString() << std::endl;
			for (const auto& [subsequence, message] : subsequenceMessage) {
				std::cout << "\t\t\t" << message << std::endl;
			}
		}
	}
}

void print(const TLogGroup& tLogGroup) {
	std::cout << ">> WorkInterface.actor.h:TLogGroup:"<<std::endl;
	for(const auto& [storageTeamID, tags] : tLogGroup.storageTeams) {
		std::cout << concatToString("\tStorage Team ID: ", storageTeamID, "\n");
		std::cout << "\tTags:" << std::endl;
		for(const auto& tag : tags) {
			std::cout << concatToString("\t\t", tag, "\n");
		}
	}
}

PrintTiming::DummyOStream operator<<(PrintTiming& printTiming, std::ostream& (*manip)(std::ostream&)) {
	printTiming << "" << manip;
	return PrintTiming::DummyOStream();
}

PrintTiming::DummyOStream operator<<(PrintTiming& printTiming, std::ios_base& (*manip)(std::ios_base&)) {
	printTiming << "" << manip;
	return PrintTiming::DummyOStream();
}

PrintTiming::DummyOStream&& operator<<(PrintTiming::DummyOStream&& stream, std::ostream& (*manip)(std::ostream&)) {
	std::cout << manip;
	return std::move(stream);
}

PrintTiming::DummyOStream&& operator<<(PrintTiming::DummyOStream&& stream, std::ios_base& (*manip)(std::ios_base&)) {
	std::cout << manip;
	return std::move(stream);
}

PrintTiming::PrintTiming(const std::string& functionName_)
  : functionName(functionName_), startTime(clock_t::now()), lastTagTime(clock_t::now()) {

	(*this) << "Start" << std::endl;
}

PrintTiming::~PrintTiming() {
	(*this) << "Terminate, duration = " << duration_t(clock_t::now() - startTime).count() << "s" << std::endl;
}

} // namespace print

} // namespace ptxn::test