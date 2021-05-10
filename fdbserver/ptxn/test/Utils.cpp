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
#include <string>
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

TeamID getNewTeamID() {
	return randomUID();
}

std::vector<TeamID> generateRandomTeamIDs(const int numTeams) {
	std::vector<TeamID> result(numTeams);
	for (auto& item : result) {
		item = getNewTeamID();
	}
	return result;
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
	          << formatKVPair("Team ID", request.teamID) << std::endl
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
	          << formatKVPair("Team ID", request.teamID) << std::endl;
}

void print(const TLogPeekReply& reply) {
	std::cout << std::endl << ">> TLogPeekReply" << std::endl;

	std::cout << formatKVPair("Debug ID", reply.debugID) << std::endl
	          << formatKVPair("Serialized data length", reply.data.size()) << std::endl;
}

void print(const TestDriverOptions& option) {
	std::cout << std::endl << ">> ptxn/test//Driver.actor.cpp:DriverTestOptions:" << std::endl;

	std::cout << formatKVPair("numCommits", option.numCommits) << std::endl
	          << formatKVPair("numTeams", option.numTeams) << std::endl
	          << formatKVPair("numProxies", option.numProxies) << std::endl
	          << formatKVPair("numTLogs", option.numTLogs) << std::endl
	          << formatKVPair("Message Transfer Model", option.transferModel) << std::endl;
}

void print(const CommitRecord& record) {
	std::cout << std::endl << ">> ptxn/test/Driver.h:CommitRecord;" << std::endl;

	std::cout << formatKVPair("Verson", record.version) << std::endl
	          << formatKVPair("teamID", record.teamID) << std::endl;

	std::cout << formatKVPair("Muations", record.mutations.size()) << std::endl;
	for (const auto& mutation : record.mutations) {
		std::cout << mutation.toString() << std::endl;
	}
}

void print(const ptxn::test::tLogPeek::TestTLogPeekOptions& option) {
	std::cout << std::endl << ">> ptxn/test//Driver.actor.cpp:DriverTestOptions:" << std::endl;

	std::cout << formatKVPair("Mutations", option.numMutations) << std::endl
	          << formatKVPair("Teams", option.numTeams) << std::endl
	          << formatKVPair("Intial version", option.initialVersion) << std::endl;
}

void printCommitRecord(const std::vector<CommitRecord>& records) {
	std::cout << "Commits from Proxy: \n\n";
	Version currentVersion = 0;
	for (const auto& record : records) {
		if (record.version != currentVersion) {
			std::cout << "\n\tVersion: " << record.version << "\n\n";
			currentVersion = record.version;
		}
		std::cout << "\t\tTeam ID: " << record.teamID.toString() << std::endl;
		for (const auto& mutation : record.mutations) {
			std::cout << "\t\t\t" << mutation.toString() << std::endl;
		}
	}
}

void printNotValidatedRecords(const std::vector<CommitRecord>& records) {
	std::cout << "Unvalidated commits: \n\n";
	for (const auto& record : records) {
		if (record.validation.validated())
			continue;
		std::cout << "\tVersion: " << record.version << "\tTeam ID: " << record.teamID.toString() << std::endl;
		for (const auto& mutation : record.mutations) {
			std::cout << "\t\t\t" << mutation.toString() << std::endl;
		}

		if (!record.validation.tLogValidated) {
			std::cout << "\tTLog has not validated the reception of this commit." << std::endl;
		}
		if (!record.validation.storageServerValidated) {
			std::cout << "\tStorageServer has not validated the reception of this commit." << std::endl;
		}
	}
}

} // namespace print

} // namespace ptxn::test