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
	          << formatKVPair("Team ID", request.storageTeamID) << std::endl;
}

void print(const TLogPeekReply& reply) {
	std::cout << std::endl << ">> TLogPeekReply" << std::endl;

	std::cout << formatKVPair("Debug ID", reply.debugID) << std::endl
	          << formatKVPair("Serialized data length", reply.data.size()) << std::endl;
}

void print(const TestDriverOptions& option) {
	std::cout << std::endl << ">> ptxn/test/Driver.actor.cpp:DriverTestOptions:" << std::endl;

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
	std::cout << std::endl << ">> ptxn/test/Driver.actor.cpp:DriverTestOptions:" << std::endl;

	std::cout << formatKVPair("Versions", option.numVersions) << std::endl
	          << formatKVPair("Mutations per versions", option.numMutationsPerVersion) << std::endl
	          << formatKVPair("Teams", option.numStorageTeams) << std::endl
	          << formatKVPair("Intial version", option.initialVersion) << std::endl;
}

template <typename Container>
void printUIDs(const Container& uidContainer) {
	const int rowMaxItems = 3;
	int itemRowCount = 0;
	for (auto iter = std::begin(uidContainer); iter != std::end(uidContainer); ++iter) {
		if (itemRowCount == 0) {
			std::cout << "\t\t";
		}
		std::cout << iter->toString() << '\t';
		if (++itemRowCount == rowMaxItems) {
			std::cout << std::endl;
			itemRowCount = 0;
		}
	}
	if (itemRowCount != 0) {
		std::cout << std::endl;
	}
}

void printTLogs() {
	const auto& fixture = TestEnvironment::getTLogs();

	std::cout << std::endl << ">> ptxn/test/Driver.actor.cpp:TLogFixture:" << std::endl;
	for (const auto& tLogGroupLeader : fixture->tLogGroupLeaders) {
		std::cout << formatKVPair("tLogGroup", tLogGroupLeader.first.toString()) << '\t'
		          << formatKVPair("Leader UID", tLogGroupLeader.second->id()) << std::endl;
	}
	std::cout << std::endl;
}

void printTLogGroup() {
	const auto& fixture = TestEnvironment::getTLogGroup();

	std::cout << std::endl << ">> ptxn/test/Driver.actor.cpp:ptxnTLogFixture" << std::endl;
	std::cout << formatKVPair("numTLogGroups", fixture.getNumTLogGroups()) << std::endl;
	for (const auto& [tLogGroupID, storageTeamIDSet] : fixture.tLogGroupStorageTeamMapping) {
		std::cout << "\t" << formatKVPair("TLog Group", tLogGroupID) << "\t"
		          << formatKVPair("numStorageTeamIDs", storageTeamIDSet.size()) << std::endl;
		printUIDs(storageTeamIDSet);
	}
	std::cout << "\t"
	          << formatKVPair("Reverse StorageTeamID -> TLogGroupID", fixture.storageTeamTLogGroupMapping.size())
	          << std::endl;
	for (const auto& [storageTeamID, tLogGroupID] : fixture.storageTeamTLogGroupMapping) {
		std::cout << "\t\t" << formatKVPair(storageTeamID.toString(), tLogGroupID) << std::endl;
	}

	// Check if is TLogGroupWithPrivateMutationsFixture
	if (const auto* ptr = dynamic_cast<const details::TLogGroupWithPrivateMutationsFixture*>(&fixture)) {
		std::cout << formatKVPair("Team mutation storage team ID", ptr->privateMutationsStorageTeamID) << std::endl;
	}

	std::cout << std::endl;
}

void printCommitRecords() {
	const auto& record = TestEnvironment::getCommitRecords();
	std::cout << ">> ptxn/test/CommitUtils.h:CommitRecord:" << std::endl;
	for (const auto& [version, storageTeamIDMessageMap] : record.messages) {
		std::cout << "\n\tCommit version: " << std::setw(10) << version
		          << "\tStorage Team Version: " << record.commitVersionStorageTeamVersionMapper.at(version)
		          << std::endl;
		for (const auto& [storageTeamID, sms] : storageTeamIDMessageMap) {
			std::cout << "\t\t\t"
			          << "Storage Team ID = " << storageTeamID.toString() << std::endl;
			for (const auto& [subsequence, message] : sms) {
				std::cout << "\t\t\t\t" << std::setw(10) << subsequence << '\t' << message.toString() << std::endl;
			}
		}
	}
}

void print(const TLogGroup& tLogGroup) {
	std::cout << ">> WorkInterface.actor.h:TLogGroup: " << tLogGroup.logGroupId << std::endl;
	for (const auto& [storageTeamID, tags] : tLogGroup.storageTeams) {
		std::cout << concatToString("\tStorage Team ID: ", storageTeamID, "\n");
		std::cout << "\tTags:" << std::endl;
		for (const auto& tag : tags) {
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