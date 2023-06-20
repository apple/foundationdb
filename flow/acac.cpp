#ifdef WITH_ACAC

#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options.hpp>

#include "flow/ActorContext.h"

std::unordered_map<UID, std::string> loadUIDActorMapping(const std::string& build_directory_path) {
	std::unordered_map<UID, std::string> identifierToActor;

	for (const auto& dirEntry : std::filesystem::recursive_directory_iterator(build_directory_path)) {
		if (!dirEntry.is_regular_file()) {
			continue;
		}
		if (dirEntry.path().extension() != ".uid") {
			continue;
		}
		std::ifstream ifs(dirEntry.path());
		for (;;) {
			std::string line;
			if (!std::getline(ifs, line)) {
				break;
			}

			std::stringstream ss;
			ss << line;

			uint64_t part1, part2;
			char pipeChar;
			std::string actorName;
			ss >> part1 >> pipeChar >> part2 >> pipeChar >> actorName;

			identifierToActor[UID(part1, part2)] = actorName;
		}
	}

	return identifierToActor;
}

void dumpActorContextTree(std::ostream& stream,
                          const DecodedActorContext& decoded,
                          const std::unordered_map<UID, std::string>& identifierToActor) {

	std::unordered_map<ActorID, std::vector<ActorID>> spawnInfo;
	for (const auto& [actorID, _1, parentID] : decoded.context) {
		spawnInfo[parentID].push_back(actorID);
	}

	std::unordered_map<ActorID, std::string> actorNames;
	for (const auto& [actorID, actorIdentifier, _1] : decoded.context) {
		actorNames[actorID] = identifierToActor.at(actorIdentifier);
	}

	// 2-space indentation
	constexpr int INDENT = 2;
	std::deque<std::pair<ActorID, int>> actorQueue;

	actorQueue.push_back({ INIT_ACTOR_ID, 0 });

	while (!actorQueue.empty()) {
		const auto [actorID, depth] = actorQueue.front();
		actorQueue.pop_front();
		if (spawnInfo.count(actorID)) {
			for (const auto childID : spawnInfo.at(actorID)) {
				actorQueue.push_front({ childID, depth + 1 });
			}
		}
		if (actorID == 0) {
			continue;
		}

		stream << std::string(INDENT * depth, ' ') << '(' << std::setw(12) << actorID << ") " << actorNames.at(actorID)
		       << (actorID == decoded.currentRunningActor ? "  <ACTIVE>" : "") << std::endl;
	}
}

void dumpActorContextStack(std::ostream& stream,
                           const DecodedActorContext& decoded,
                           const std::unordered_map<UID, std::string>& identifierToActor) {
	for (const auto& [actorID, actorIdentifier, spawnerActorID] : decoded.context) {
		const std::string& actorName = identifierToActor.at(actorIdentifier);
		stream << std::setw(12) << actorID << " " << actorName
		       << (actorID == decoded.currentRunningActor ? "  <ACTIVE>" : "") << std::endl;
	}
}

void decodeClass(std::ostream& stream,
                 const std::string& classIdentifier,
                 const std::unordered_map<UID, std::string>& identifierToActor) {
	UID uid = UID::fromString(classIdentifier);
	stream << classIdentifier << " -- " << identifierToActor.at(uid) << std::endl;
}

namespace bpo = boost::program_options;

int main(int argc, char* argv[]) {
	bpo::options_description desc("Options");
	desc.add_options()("help",
	                   "Print help message")("fdb-build-directory", bpo::value<std::string>(), "Build directory")(
	    "decode-class", bpo::value<std::string>(), "Decode a class key");

	bpo::variables_map varMap;
	bpo::store(bpo::parse_command_line(argc, argv, desc), varMap);

	bpo::notify(varMap);

	if (varMap.count("help")) {
		std::cerr << desc << std::endl;
		return 1;
	}

	std::string buildDirectory = ".";
	if (varMap.count("fdb-build-directory") != 0) {
		buildDirectory = varMap["fdb-build-directory"].as<std::string>();
	}

	const auto lib = loadUIDActorMapping(buildDirectory);
	if (varMap.count("decode-class") != 0) {
		decodeClass(std::cout, varMap["decode-class"].as<std::string>(), lib);
		return 0;
	}

	std::string encodedContext;
	while (std::cin) {
		std::string line;
		std::getline(std::cin, line);
		boost::trim(line);
		// libb64 will generate newline every 72 characters, which will be encoded as "\0xa"
		// in the TraceEvent log file.
		boost::replace_all(line, "\\x0a", "\n");
		encodedContext += line;
	}

	const auto decodedActorContext = decodeActorContext(encodedContext);
	switch (decodedActorContext.dumpType) {
	case ActorContextDumpType::FULL_CONTEXT:
		dumpActorContextTree(std::cout, decodedActorContext, lib);
		break;
	case ActorContextDumpType::CURRENT_STACK:
	case ActorContextDumpType::CURRENT_CALL_BACKTRACE:
		dumpActorContextStack(std::cout, decodedActorContext, lib);
		break;
	default:
		std::cerr << "Unexpected ActorContextDumpType: " << static_cast<uint8_t>(decodedActorContext.dumpType)
		          << std::endl;
		return -1;
	}
	return 0;
}

#else // WITH_ACAC

#include <iostream>

int main(int argcc, char* argv[]) {
	std::cerr << "FoundationDB is built without ACAC enabled" << std::endl;
	return -1;
}

#endif // WITH_ACAC
