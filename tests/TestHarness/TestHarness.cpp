/*
 * TestHarness.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "Events.h"
#include "fmt/core.h"
#include "fmt/ostream.h"
#include <algorithm>
#include <cassert>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iomanip>
#include <ios>
#include <iostream>
#include <iterator>
#include <locale>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <pwd.h>
#include <random>
#include <rapidxml.hpp>
#include <stdexcept>
#include <string_view>
#include <string>
#include <sstream>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <vector>
#include <set>
#include <map>
#include <exception>
#include <thread>
#include <regex>
#define __USE_POSIX
#include <limits.h>
#ifndef HOST_NAME_MAX
#if defined(_POSIX_HOST_NAME_MAX)
#define HOST_NAME_MAX _POSIX_HOST_NAME_MAX
#elif defined(MAXHOSTNAMELEN)
#define HOST_NAME_MAX MAXHOSTNAMELEN
#endif
#endif /* HOST_NAME_MAX */
#include <fmt/format.h>

using namespace std::literals;
using namespace magnesium;

extern "C" {

extern char** environ;
}

namespace {
std::mt19937_64 g_rnd;
constexpr double buggifyOnRatio = 0.8;
constexpr double unseedRatio = 0.05;
constexpr int killSeconds = 30 * 60;
constexpr int maxWarnings = 10;
constexpr int maxStderrBytes = 1000;
// TODO: support more than Linux?
constexpr const char* OS_NAME = "linux";
constexpr const char* BINARY = "fdbserver";
constexpr const char* PLUGIN = "FDBLibTLS.so";
const std::string ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

int usageMessage() {
	printf("Usage:\n");
	printf("  TestHarness run [temp/runDir] [fdbserver[.exe]] [TLSplugin] [testfolder] [summary.xml] <useValgrind> "
	       "<maxTries>\n");
	printf("  TestHarness summarize [trace.xml] [summary.xml] <valgrind-XML-file> <external-error> <traceToStdOut>\n");
	printf("  TestHarness replay [temp/runDir] [fdbserver[.exe]] [TLSplugin] [summary-in.xml] [summary-out.xml]\n");
	printf("  TestHarness auto [temp/runDir] [directory] [shareDir] <useValgrind> <maxTries>\n");
	printf("  TestHarness remote [queue folder] [root foundation folder] [duration in hours] [amount of tests] "
	       "[all/fast/<test_path>] [scope]\n");
	printf("  TestHarness extract-errors [summary-file] [error-summary-file]\n");
	printf("  TestHarness joshua-run <useValgrind> <maxTries>\n");
	printf("Version:  1.01\n");
	return 1;
}

Severity stringToSeverity(std::string const& str) {
	return static_cast<Severity>(std::stoi(str));
}

struct Env {
	static std::string getUserName() {
		auto uid = getuid();
		auto pw = getpwuid(uid);
		if (pw) {
			return pw->pw_name;
		}
		return "";
	}
};

} // namespace

namespace std {

std::string to_string(Severity s) {
	return to_string(static_cast<typename std::underlying_type<Severity>::type>(s));
}

} // namespace std

namespace {

enum class XType { Element, Text };

struct XNode {
	virtual ~XNode() {}

	virtual XNode* clone() const = 0;
	virtual XType type() const = 0;
	virtual void write(FILE*, std::string const&) const = 0;
};

struct XText : XNode {
	std::string text;

	XText(std::string const& str) : text(str) {}
	XText(std::string&& str) : text(std::move(str)) {}

	XNode* clone() const override { return new XText(text); }
	XType type() const override { return XType::Text; }

	void write(FILE* out, std::string const& indent) const override { fmt::print(out, "{}{}\n", indent, text); }
};

struct XElement : XNode {
	std::string name;
	XType type() const override { return XType::Element; }
	XElement(std::string const& name) : name(name) {}
	XElement(std::string const& name, std::vector<XNode*> const& children) : name(name), children(children) {}
	XElement(XElement const& other) : name(other.name), attributes(other.attributes) {
		children.reserve(other.children.size());
		for (auto p : other.children) {
			children.emplace_back(p->clone());
		}
	}
	XElement(XElement&& other)
	  : name(std::move(other.name)), attributes(std::move(other.attributes)), children(std::move(other.children)) {}
	~XElement() {
		for (auto child : children) {
			delete child;
		}
	}
	XElement& operator=(XElement const& other) {
		name = other.name;
		attributes = other.attributes;
		children.reserve(other.children.size());
		for (auto p : other.children) {
			children.emplace_back(p->clone());
		}
		return *this;
	}
	XElement& operator=(XElement&& other) {
		name = std::move(other.name);
		attributes = std::move(other.attributes);
		children = std::move(other.children);
		return *this;
	}
	std::string& operator[](std::string const& key) {
		for (auto& p : attributes) {
			if (p.first == key) {
				return p.second;
			}
		}
		attributes.emplace_back(key, "");
		return attributes.back().second;
	}

	XElement& addChildElement(std::string const& name) {
		auto res = new XElement{ name };
		children.push_back(res);
		return *res;
	}

	void addText(std::string const& t) { children.push_back(new XText{ t }); }

	template <class Iter>
	void append(Iter const& begin, Iter const& end) {
		for (auto iter = begin; iter != end; ++iter) {
			attributes.emplace_back(iter->first, iter->second);
		}
	}

	XNode* clone() const override { return new XElement(*this); }

	void write(FILE* out, std::string const& indent = "") const override {
		std::vector<std::string> attribs;
		attribs.reserve(attributes.size());
		std::transform(attributes.begin(), attributes.end(), std::back_inserter(attribs),
		               [](auto const& p) { return fmt::format("{}=\"{}\"", p.first, p.second); });
		fmt::print(out, "{}<{} {}{}\n", indent, name, fmt::join(attribs, " "), children.empty() ? "/>" : ">");
		auto nextIndent = fmt::format("  {}", indent);
		for (auto& child : children) {
			child->write(out, nextIndent);
		}
		if (children.empty()) {
			fmt::print(out, "</{}>\n", name);
		}
	}

	static void extractChildren(XElement& self, magnesium::Event const& ev) {
		self["Type"] = ev.type;
		self["Machine"] = ev.machine;
		self["ID"] = ev.id;
		self["Severity"] = std::to_string(ev.severity);
		self["Time"] = std::to_string(ev.time);
		for (auto const& p : ev.details) {
			self[p.first] = p.second;
		}
	}
	static void extractChildren(XElement& self, magnesium::TestPlan const& testPlan) {
		extractChildren(self, static_cast<magnesium::Event const&>(testPlan));
	}
	static void extractChildren(XElement& self, magnesium::Test const& test) {
		extractChildren(self, static_cast<magnesium::TestPlan const&>(test));
		self["TestUID"] = test.testUID;
		self["TestFile"] = test.testFile;
		self["SourceVersion"] = test.sourceVersion;
		self["OK"] = test.ok ? "1" : "0";
		self["RandomSeed"] = std::to_string(test.randomSeed);
		self["RandomUnseed"] = std::to_string(test.randomUnseed);
		self["SimElapsedTime"] = std::to_string(test.simElapsedTime);
		self["RealElapsedTime"] = std::to_string(test.realElapsedTime);
		self["Passed"] = std::to_string(test.passed);
		self["Failed"] = std::to_string(test.failed);
		self["PeakMemory"] = std::to_string(test.peakMemUsage);
		for (auto const& child : test.events) {
			auto res = new XElement(fromEvent(*child));
			self.children.push_back(res);
		}
	}

	static XElement fromEvent(magnesium::Event const& ev) {
		XElement res{ "Event" };
		extractChildren(res, ev);
		return res;
	}
	static XElement fromTestPlan(magnesium::TestPlan const& testPlan) {
		XElement res{ "TestPlan" };
		extractChildren(res, testPlan);
		return res;
	}
	static XElement fromTest(magnesium::Test const& test) {
		XElement res{ "Test" };
		extractChildren(res, test);
		return res;
	}

	std::vector<std::pair<std::string, std::string>> attributes;
	std::vector<XNode*> children;
};

std::string getMachineName() {
	static std::string machineName;
	if (machineName.empty()) {
		char hostName[HOST_NAME_MAX];
		gethostname(hostName, HOST_NAME_MAX);
		machineName = hostName;
	}
	return machineName;
}

struct FileNameMutex {
	std::string lockName;
	int fd = -1;

	explicit FileNameMutex(std::string const& filename)
	  : lockName(filename + ".lock"), fd(::open(lockName.c_str(), O_RDWR)) {
		if (fd < 0) {
			throw std::system_error{ errno, std::system_category() };
		}
	}

	void lock() {
		auto res = flock(fd, LOCK_EX);
		if (res) {
			throw std::system_error{ errno, std::system_category() };
		}
	}

	void unlock() {
		auto res = flock(fd, LOCK_UN);
		if (res) {
			throw std::system_error{ errno, std::system_category() };
		}
	}

	bool try_lock() {
		auto res = flock(fd, LOCK_NB);
		if (res != 0 && errno == EWOULDBLOCK) {
			return false;
		} else if (res == 0) {
			return true;
		} else {
			throw std::system_error{ errno, std::system_category() };
		}
	}

	~FileNameMutex() {
		if (fd >= 0) {
			close(fd);
			std::error_code err;
			std::filesystem::remove(lockName, err);
		}
	}
};

struct FileMutex {
	int fd;

	explicit FileMutex(int fd) : fd(fd) {}

	void lock() {
		auto res = flock(fd, LOCK_EX);
		if (res) {
			throw std::system_error{ errno, std::system_category() };
		}
	}
	void unlock() {
		auto res = flock(fd, LOCK_UN);
		if (res) {
			throw std::system_error{ errno, std::system_category() };
		}
	}
	bool try_lock() {
		auto res = flock(fd, LOCK_NB);
		if (res != 0 && errno == EWOULDBLOCK) {
			return false;
		} else if (res == 0) {
			return true;
		} else {
			throw std::system_error{ errno, std::system_category() };
		}
	}
};

std::string getRandomFilename() {
	int alphabetSize = ALPHABET.size();
	std::uniform_int_distribution<> dist{ 0, alphabetSize };
	std::string res;
	int len = std::uniform_int_distribution<>{ 5, 15 }(g_rnd);
	res.reserve(len);
	for (int i = 0; i < len; ++i) {
		res.push_back(ALPHABET[dist(g_rnd)]);
	}
	return res;
}

std::string newGuid() {
	std::uniform_int_distribution<uint32_t> dist32{};
	std::uniform_int_distribution<uint16_t> dist16{};
	std::uniform_int_distribution<uint64_t> dist48{ 0, (uint64_t{ 1 } << 49) - 1 };
	return fmt::format("{:8x}-{:4x}-{:4x}-{:4x}-{:12x}", dist32(g_rnd), dist16(g_rnd), dist16(g_rnd), dist16(g_rnd),
	                   dist48(g_rnd));
}

bool versionGreaterThanOrEqual(std::string const& ver1, std::string const& ver2) {
	auto tokens1 = split(ver1, '.');
	auto tokens2 = split(ver2, '.');
	if (tokens1.size() != tokens2.size() || tokens2.size() != 3) {
		throw std::invalid_argument("Invalid Version Format Version1: " + ver1 + " Version2: " + ver2);
	}
	std::vector<int> version1, version2;
	std::transform(tokens1.begin(), tokens1.end(), std::back_inserter(version1),
	               [](std::string_view arg) { return std::stoi(std::string{ arg }); });
	std::transform(tokens2.begin(), tokens2.end(), std::back_inserter(version2),
	               [](std::string_view arg) { return std::stoi(std::string{ arg }); });
	for (int i = 0; i < version1.size(); ++i) {
		if (ver1[i] < ver2[i]) return false;
	}
	return true;
}

void appendToSummary(std::string const& summaryFileName, XElement const& xout, bool traceToStdout = false) {
	if (traceToStdout) {
		xout.write(stdout);
		return;
	}
	CFile out{ summaryFileName, "a" };
	if (ftell(out.file) == 0) {
		fmt::print(out.file, "<Trace>\n");
		xout.write(out.file);
	}
}

void appendToFile(std::string const& fileName, std::string const& content) {
	CFile out{ fileName, "a" };
	FileMutex fMutex{ out.fd() };
	std::unique_lock<FileMutex> _{ fMutex };
	fprintf(out.file, content.c_str());
}

void appendXmlMessageToSummary(std::string const& summaryFileName, XElement& xout, bool traceToStdout = false,
                               std::optional<std::string> testFile = {}, std::optional<int> seed = {},
                               std::optional<bool> buggify = {}, std::optional<bool> determinismCheck = {},
                               std::optional<std::string> oldBinaryName = {}) {
	XElement test{ "Test", { new XElement{ xout } } };
	if (testFile.has_value()) {
		test["TestFile"] = testFile.value();
	}
	if (seed.has_value()) {
		test["RandomSeed"] = std::to_string(seed.value());
	}
	if (buggify.has_value()) {
		test["BuggifyEnabled"] = buggify.value() ? "1" : "0";
	}
	if (determinismCheck.has_value()) {
		test["DeterminismCheck"] = determinismCheck.value() ? "1" : "0";
	}
	if (oldBinaryName.has_value()) {
		test["OldBinary"] = oldBinaryName.value();
	}
	appendToSummary(summaryFileName, test, traceToStdout);
}

std::vector<std::string> parseValgrindOutput(std::string valgrindOutputFileName, bool traceToStdout) {
	if (!traceToStdout) {
		fmt::print("Reading vXML file: {}\n", valgrindOutputFileName);
	}
	CFile file{ valgrindOutputFileName, "r" };
	auto str = file.readWholeFile();
	rapidxml::xml_document<> doc;
	doc.parse<0>(str->ptr);
	rapidxml::xml_node<char>* val = doc.first_node();
	while (val != nullptr && (val->type() != rapidxml::node_element || val->name() != "valgrindoutput"sv)) {
		val = val->next_sibling();
	}
	if (val == nullptr) {
		throw std::runtime_error{ "Couldn't find valgrindoutput" };
	}
	std::vector<std::string> whats;
	for (val = val->first_node(); val; val = val->next_sibling()) {
		if (val->name() != "error"sv) {
			continue;
		}
		std::string kind;
		for (auto v = val->first_node(); v; v = v->next_sibling()) {
			if (v->type() == rapidxml::node_element && v->name() == "kind"sv) {
				for (auto t = v->first_node(); t; t = t->next_sibling()) {
					if (t->type() == rapidxml::node_data) {
						kind += std::string{ t->value() };
					}
				}
				break;
			}
		}
		if (stringStartsWith(kind, "Leak"sv)) {
			continue;
		}
		std::string what;
		for (auto v = val->first_node(); v; v = v->next_sibling()) {
			if (v->type() == rapidxml::node_element && v->name() == "what"sv) {
				for (auto t = v->first_node(); t; t = t->next_sibling()) {
					if (t->type() == rapidxml::node_data) {
						what += std::string{ t->value() };
					}
				}
				break;
			}
		}
		whats.push_back(what);
	}
	return whats;
}

int summarize(std::vector<std::filesystem::path> const& traceFiles, std::string const& summaryFileName,
              std::optional<std::string> const& errorFileName, std::optional<bool> killed,
              std::vector<std::string> const& outputErrors, std::optional<int> exitCode, std::optional<long> peakMemory,
              std::optional<std::string> const& uid, std::optional<std::string> const& valgrindOutputFileName,
              int expectedUnseed, int& unseed, bool& retryableError, bool logOnRetryableError, bool willRestart = false,
              bool restarted = false, std::string const& oldBinaryName = "", bool traceToStdout = false,
              std::string const& externalError = "") {
	unseed = -1;
	retryableError = false;
	std::vector<std::string> errorList;
	XElement xout{ "Test" };
	if (uid.has_value()) xout["TestUID"] = uid.value();
	bool ok = false;
	std::string testFile = "Unknown";
	int testsPassed = 0, testCount = -1, warnings = 0, errors = 0;
	bool testBeginFound = false, testEndFound = false, error = false;
	std::string firstRetryableError = "";
	auto stderrSeverity = static_cast<typename std::underlying_type<Severity>::type>(Severity::SevError);
	std::map<std::pair<std::string, Severity>, Severity> severityMap;
	std::map<std::pair<std::string, std::string>, bool> codeCoverageMap;
	for (auto& trace : traceFiles) {
		if (!traceToStdout) {
			printf("Summarizing %s\n", trace.c_str());
		}
		CFile file{ trace, "r" };
		auto events = XMLParser::parse(file);
		for (auto event : events) {
			auto& ev = *event;
			auto iter = severityMap.find(std::make_pair(ev.type, ev.severity));
			if (iter != severityMap.end()) {
				ev.severity = iter->second;
			}
			if (ev.severity >= Severity::SevWarnAlways && ev.details.hasKey("ErrorIsInjectedFault")) {
				ev.severity = Severity::SevWarn;
			}
			if (ev.type == "ProgramStart" && !testBeginFound &&
			    (!ev.details.hasKey("Simulated") || ev.details["Simulated"] != "1")) {
				xout["RandomSeed"] = ev.details["RandomSeed"];
				xout["SourceVersion"] = ev.details["SourceVersion"];
				xout["Time"] = ev.details["ActualTime"];
				xout["BuggifyEnabled"] = ev.details["BuggifyEnabled"];
				xout["DeterminismCheck"] = ev.details["DeterminismCheck"];
				xout["OldBinary"] = oldBinaryName;
				testBeginFound = true;
			}
			if (ev.type == "Simulation") {
				xout["TestFile"] = ev.details["TestFile"];
				testFile = "TestFile";
			}
			if (ev.type == "ActualRun") {
				xout["TestFile"] = ev.details["TestFile"];
			}
			if (ev.type == "ElapsedTime" && !testEndFound) {
				testEndFound = true;
				unseed = std::stoi(std::string{ ev.details["unseed"] });
				if (expectedUnseed != -1 && expectedUnseed != unseed) {
					auto iter = severityMap.find(std::make_pair(std::string{ "UnseedMismatch" }, Severity::SevError));
					if (iter != severityMap.end()) {
						ev.severity = Severity::SevError;
					} else if (iter->second >= Severity::SevWarnAlways) {
						auto& el = xout.addChildElement("UnseedMismatch");
						el["Unseed"] = unseed;
						el["ExpectedUnseed"] = expectedUnseed;
						el["Severity"] = std::to_string(iter->second);
						if (iter->second == Severity::SevError) {
							error = true;
							errorList.push_back("UnseedMismatch");
						}
					}
				}
				xout["SimElapsedTime"] = ev.details["SimTime"];
				xout["RealElapsedTime"] = ev.details["RealTime"];
				xout["RandomUnseed"] = ev.details["RandomUnseed"];
			}
			if (ev.severity == Severity::SevWarnAlways) {
				if (warnings < maxWarnings) {
					auto& el = xout.addChildElement(ev.type);
					el["Severity"] = std::to_string(ev.severity);
					el.append(ev.details.begin(), ev.details.end());
				}
				++warnings;
			}
			if (ev.severity >= Severity::SevError) {
				auto errorString = ev.formatTestError(true);
				if (errorString.find("platform_error") != std::string::npos) {
					if (!retryableError) {
						firstRetryableError = errorString;
					}
					retryableError = true;
				}
				if (errors < maxWarnings) {
					auto& el = xout.addChildElement(ev.type);
					el["Severity"] = std::to_string(ev.severity);
					el.append(ev.details.begin(), ev.details.end());
					errorList.push_back(errorString);
				}
				errors++;
				error = true;
			}
			if (ev.type == "CodeCoverage" && !willRestart) {
				bool covered = true;
				if (ev.details.hasKey("Covered")) {
					covered = std::stoi(std::string{ ev.details["Covered"] }) != 0;
				}
				auto key = std::make_pair(std::string{ ev.details["File"] }, std::string{ ev.details["Line"] });
				auto iter = codeCoverageMap.find(key);
				if (covered || iter != codeCoverageMap.end()) {
					iter->second = covered;
				}
			}
			if (ev.type == "FaultInjected" || (ev.type == "BuggifySection" && ev.details["Activated"] == "1")) {
				auto& el = xout.addChildElement(ev.type);
				el["File"] = ev.details["File"];
				el["Line"] = ev.details["Line"];
			}
			if (ev.type == "TestsExpectedToPass") {
				testCount = std::stoi(std::string{ ev.details["Count"] });
			}
			if (ev.type == "TestResults" && ev.details["Passed"] == "1") {
				++testsPassed;
			}
			if (ev.type == "RemapEventSeverity") {
				severityMap[std::make_pair(std::string{ ev.details["TargetEvent"] },
				                           stringToSeverity(std::string{ ev.details["OriginalSeverity"] }))] =
				    stringToSeverity(std::string{ ev.details["NewSeverity"] });
			}
			if (ev.type == "StderrSeverity") {
				stderrSeverity = std::stoi(std::string{ ev.details["NewSeverity"] });
			}
		}
	}
	if (externalError.size() > 0) {
		auto& el = xout.addChildElement(externalError);
		el["Severity"] = "40";
	}

	for (const auto& kv : codeCoverageMap) {
		auto& el = xout.addChildElement("CodeCoverage");
		el["File"] = kv.first.first;
		el["Line"] = kv.first.second;
		if (!kv.second) {
			el["Covered"] = "0";
		}
	}

	if (warnings > maxWarnings) {
		auto& el = xout.addChildElement("WarningLimitExceeded");
		el["Severity"] = "30";
		el["WarningCount"] = std::to_string(warnings);
	}
	if (errors > maxWarnings) {
		error = true;
		errorList.push_back("ErrorLimitExceeded");
		auto& el = xout.addChildElement("ErrorLimitExceeded");
		el["Severity"] = "40";
		el["WarningCount"] = std::to_string(errors);
	}

	if (killed) {
		if (!retryableError) {
			firstRetryableError = "ExternalTimeout";
		}

		retryableError = true;
		error = true;
		auto& el = xout.addChildElement("ExternalTimeout");
		el["Severity"] = "40";
	}

	if (!outputErrors.empty()) {
		int stderrBytes = 0;
		for (auto& err : outputErrors) {
			if (stderrSeverity == static_cast<int>(Severity::SevError)) {
				error = true;
			}

			int remainingBytes = maxStderrBytes - stderrBytes;
			if (remainingBytes > 0) {
				auto& el = xout.addChildElement("StdErrOutput");
				el["Severity"] = std::to_string(stderrSeverity);
				if (err.size() > remainingBytes) {
					el["Output"] = err.substr(0, remainingBytes) + "...";
				} else {
					el["Output"] = err;
				}
			}

			stderrBytes += err.size();
		}

		if (stderrBytes > maxStderrBytes) {
			auto& el = xout.addChildElement("StdErrOutputTruncated");
			el["Severity"] = std::to_string(stderrSeverity);
			el["BytesRemaining"] = stderrBytes - maxStderrBytes;
		}
	}

	if (exitCode.has_value() && exitCode.value() != 0) {
		error = true;
		auto& el = xout.addChildElement("ExitCode");
		el["Code"] = std::to_string(exitCode.value());
		el["Severity"] = "40";
		errorList.push_back(fmt::format("ExitCode 0x{0:x}", exitCode.value()));
	}

	if (!testEndFound && !willRestart) {
		// We didn't terminate the test, but it didn't reach the end?
		error = true;
		auto& el = xout.addChildElement("TestUnexpectedlyNotFinished");
		el["Severity"] = "40";
		errorList.push_back("TestUnexpectedlyNotFinished");
	}

	ok = testsPassed == testCount && testsPassed > 0 && !error;
	xout["Passed"] = std::to_string(testsPassed);
	xout["Failed"] = std::to_string(testCount - testsPassed);
	if (peakMemory.has_value() && peakMemory.value() > 0) {
		xout["PeakMemory"] = std::to_string(peakMemory.value());
	}

	if (valgrindOutputFileName.has_value() && valgrindOutputFileName.has_value()) {
		auto whats = parseValgrindOutput(valgrindOutputFileName.value(), traceToStdout);
		for (const auto& what : whats) {
			auto& el = xout.addChildElement("ValgrindError");
			el["Severity"] = "40";
			el["What"] = what;
			ok = false;
			error = true;
		}
	}

	if (retryableError && !logOnRetryableError) {
		xout = XElement{ "Test" };
		auto& el = xout.addChildElement("RetryingError");
		el["Severity"] = "30";
		el["What"] = firstRetryableError;
	} else {
		xout["OK"] = ok || willRestart ? "true" : "false";
	}

	appendToSummary(summaryFileName, xout, traceToStdout);

	if ((!retryableError || logOnRetryableError) && errorFileName.has_value() && (errorList.size() > 0 || !ok) &&
	    !willRestart) {
		if (!ok && errorList.size() == 0) {
			errorList.push_back("Failed with no explanation");
		}
		std::vector<std::string_view> uniqueErrors;
		std::unordered_set<std::string_view> knownErrors;
		knownErrors.reserve(uniqueErrors.size());
		for (const auto& err : errorList) {
			if (knownErrors.count(err) == 0) {
				knownErrors.insert(err);
				uniqueErrors.push_back(err);
			}
		}
		appendToFile(errorFileName.value(),
		             fmt::format("Test {0} failed with:\n\t{1}\n", testFile, fmt::join(uniqueErrors, "\n\t")));
	}
	if (!error) {
		return 0;
	} else {
		return 102;
	}

	return 0;
}

template <class T>
bool trueFunction(T) {
	return true;
}

template <class Fun>
void copyAll(std::filesystem::path const& source, std::filesystem::path const& target,
             Fun predicate = trueFunction<std::filesystem::path>) {
	if (!std::filesystem::exists(target)) {
		std::filesystem::create_directories(target);
	}
	for (auto& p : std::filesystem::directory_iterator(source)) {
		if (std::filesystem::is_directory(p)) {
			copyAll(p, target / p.path().filename(), predicate);
		} else if (predicate(p)) {
			std::filesystem::copy_file(p, target);
		}
	}
}

template <class Clock>
std::string formatDate(typename Clock::time_point const& when) {
	auto tt = Clock::to_time_t(when);
	auto lt = std::localtime(&tt);
	// std::localtime(Clock::to_time_t(now))
	std::stringstream ss;
	ss << std::put_time(lt, "%Y-%m-%d-%H-%M");
	return ss.str();
}

int remote(std::filesystem::path const& queue, std::filesystem::path const& fdbRoot, double const& addHours,
           int const& testCount, std::string const& testTypes, std::string const& userScope) {
	using Clock = std::chrono::system_clock;
	auto output = queue / "archive";
	auto now = Clock::now();
	auto date = formatDate<Clock>(now);
	if (std::filesystem::exists(output)) {
		std::filesystem::create_directory(output);
	}
	auto maxCount = 0;
	std::regex r{ fmt::format("{}-.*\\.xml", date) };
	for (auto f : std::filesystem::directory_iterator(queue)) {
		std::smatch match;
		auto fName = f.path().filename().string();
		if (f.is_regular_file() && std::regex_match(fName, match, r)) {
			auto str = f.path().string();
			auto count = std::stoi(split(str, '-')[5]);
			maxCount = std::max(count, maxCount);
		}
	}
	++maxCount;
	auto suffix = fmt::format("{}-{}-{}", Env::getUserName(), userScope, OS_NAME);
	r = std::regex{ fmt::format(".*{}\\.xml", suffix) };
	for (auto f : std::filesystem::directory_iterator(queue)) {
		std::smatch match;
		std::string fName = f.path().filename().string();
		if (f.is_regular_file() && std::regex_match(fName, match, r)) {
			std::filesystem::remove(f);
		}
	}
	auto label = fmt::format("{}-{}-{}", date, maxCount, suffix);
	auto testStaging = output / label;
	std::filesystem::create_directory(testStaging);
	{
		std::ofstream ofs{ testStaging / "errors.txt" };
		ofs << "";
		ofs.close();
	}
	auto release = fdbRoot / "bin";
	std::filesystem::copy_file(release / BINARY, testStaging / BINARY);
	std::filesystem::copy_file(fdbRoot / "tls-plugins" / PLUGIN, testStaging / PLUGIN);
	r = std::regex{ "coverage.*\\.xml" };
	for (auto f : std::filesystem::directory_iterator(release)) {
		auto fName = f.path().filename().string();
		std::smatch match;
		if (f.is_regular_file() && std::regex_match(fName, match, r)) {
			std::filesystem::copy_file(f, testStaging / fName);
		}
	}
	std::filesystem::create_directory(testStaging / "tests");
	auto copyAll = [&](const char* subfolder) {
		std::filesystem::copy(fdbRoot / "tests" / subfolder, testStaging / "tests",
		                      std::filesystem::copy_options::recursive);
	};
	if (testTypes == "fast" || testTypes == "all") {
		copyAll("fast");
	}
	if (testTypes == "restarting" || testTypes == "all") {
		copyAll("restarting");
	}
	if (testTypes == "all") {
		copyAll("slow");
		copyAll("rare");
	}
	if (testTypes != "fast" && testTypes != "all" && testTypes != "restarting") {
		std::filesystem::path file{ testTypes };
		std::filesystem::create_directory(testStaging / "tests" / file.parent_path().filename());
		std::filesystem::copy_file(file,
		                           testStaging / "tests" / file.parent_path().filename() / file.filename().string());
	}
	XElement e{ "TestDefinition" };
	{
		auto& duration = e.addChildElement("Duration");
		duration["Hours"] = std::to_string(addHours);
		auto& testCountEl = e.addChildElement("TestCount");
		testCountEl.addText(std::to_string(testCount));
	}
	{
		CFile f{ queue / (label + ".xml"), "w" };
		e.write(f.file);
	}
	std::filesystem::copy_file(queue / (label + ".xml"), testStaging / (label + ".xml"));
	fmt::print("{}\n", label);
	return 0;
}

void allFiles(std::vector<std::filesystem::path>& files, std::filesystem::path const& dir) {
	assert(std::filesystem::is_directory(dir));
	for (const auto& p : std::filesystem::directory_iterator(dir)) {
		if (std::filesystem::is_directory(p)) {
			allFiles(files, p);
		} else if (std::filesystem::is_regular_file(p)) {
			files.emplace_back(p);
		}
	}
}

void logTestPlan(std::string const& summaryFileName, std::string const& testFileName, int randomSeed, bool buggify,
                 bool testDeterminism, std::string const& uid, std::string const& oldBinary = "") {
	XElement xout{ "TestPlan" };
	xout["TestUID"] = uid;
	xout["RandomSeed"] = randomSeed;
	xout["TestFile"] = testFileName;
	xout["BuggifyEnabled"] = buggify ? "1" : "0";
	xout["DeterminismCheck"] = testDeterminism ? "1" : "0";
	xout["OldBinary"] = oldBinary;
	appendToSummary(summaryFileName, xout);
}

struct ExecArgs {
	std::vector<char*> args;
	ExecArgs& operator()(const std::string_view& str) {
		std::unique_ptr<char[]> ptr{ new char[str.size()] };
		memcpy(ptr.get(), str.data(), str.size());
		args.push_back(ptr.release());
		return *this;
	}

	~ExecArgs() {
		for (auto ptr : args) {
			delete[] ptr;
		}
	}
	auto data() { return args.data(); }
};

ExecArgs operator+(ExecArgs const& lhs, ExecArgs const& rhs) {
	ExecArgs res;
	res.args = lhs.args;
	res.args.reserve(res.args.size() + rhs.args.size());
	for (const auto& arg : rhs.args) {
		res.args.push_back(arg);
	}
	return res;
}

struct ThreadObserver {
	using Clock = std::chrono::system_clock;
	const pid_t pid;
	const int outfd, errfd;
	const bool useValgrind;
	const std::string::size_type maxErrorLength;
	std::atomic<bool> processExited = false;
	std::thread outThread, errThread, memoryObserver;
	const Clock::time_point begin = Clock::now();
	Clock::duration duration;

	bool killed = false;
	unsigned long memory = 0;
	std::vector<std::string> outputErrors;
	ThreadObserver(pid_t pid, int outfd, int errfd, bool useValgrind, std::string::size_type maxErrorLength = 100)
	  : pid(pid), outfd(outfd), errfd(errfd), useValgrind(useValgrind), maxErrorLength(maxErrorLength) {}

	template <class Fun>
	void handleIO(Fun const& fun) {
		const size_t bufferSize = 4098;
		std::unique_ptr<char[]> buffer;
		std::string currLine;
		for (;;) {
			auto cnt = ::read(outfd, buffer.get(), bufferSize);
			if (cnt < 0) {
				if (errno == EINTR) continue;
				throw std::runtime_error{ "Could not read buffer" };
			} else if (cnt == 0) {
				break;
			} else {
				for (size_t i = 0; i < cnt; ++i) {
					if (buffer[i] == '\n') {
						fun(currLine);
						currLine.clear();
					} else {
						currLine += buffer[i];
					}
				}
			}
		}
		if (!currLine.empty()) {
			fun(currLine);
		}
	}
	void handleStdOut() {
		handleIO([](std::string const&) {});
	}
	void handleStdErr() {
		handleIO([this](std::string const& line) {
			outputErrors.push_back(line.substr(0, std::min(maxErrorLength, line.size())));
		});
	}
	void observeMemory() {
		unsigned long pageSize = sysconf(_SC_PAGESIZE);
		std::filesystem::path statmp{ "/proc" };
		statmp /= std::to_string(pid);
		statmp /= "statm";
		while (!processExited.load()) {
			if (!std::filesystem::exists(statmp)) {
				// we're probably not on
				return;
			}
			std::ifstream in(statmp);
			unsigned long numPages = 0;
			in >> numPages;
			if (processExited.load()) {
				memory = std::max(memory, numPages * pageSize);
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}
	void processKiller() {
		auto killAfter = begin + std::chrono::seconds(useValgrind ? killSeconds * 20 : killSeconds);
		bool termSent = false, killSent = false;
		while (!processExited.load()) {
			if (Clock::now() > killAfter && !killSent) {
				::kill(pid, termSent ? SIGKILL : SIGTERM);
				killSent = termSent;
				termSent = true;
				killed = true;
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}
	void run() {
		outThread = std::thread{ [this]() { handleStdOut(); } };
		errThread = std::thread{ [this]() { handleStdErr(); } };
		memoryObserver = std::thread{ [this]() { observeMemory(); } };
	}
	void done() {
		processExited.store(true);
		duration = Clock::now() - begin;
		join();
	}
	void join() {
		outThread.join();
		errThread.join();
		memoryObserver.join();
	}
	~ThreadObserver() {
		close(outfd);
		close(errfd);
	}
};

int runTest(std::filesystem::path const& fdbserverName, std::filesystem::path const& tlsPluginFile,
            std::string const& summaryFileName, std::optional<std::string> const& errorFileName, int seed, bool buggify,
            std::string const& testFile, std::optional<std::filesystem::path> const& runDir, std::string const& uid,
            int expectedUnseed, int& unseed, bool& retryableError, bool logOnRetryableError, bool useValgrind,
            bool restarting = false, bool willRestart = false, std::string const& oldBinaryName = "",
            bool traceToStdout = false) {
	unseed = -1;
	retryableError = false;
	int ok = 0;

	auto tempPath = runDir.has_value() ? runDir.value() : std::filesystem::temp_directory_path() / uid;
	auto oldDir = std::filesystem::current_path();

	std::filesystem::create_directory(tempPath);
	std::filesystem::current_path(tempPath);

	if (restarting) {
		logTestPlan(summaryFileName, testFile, seed, buggify, expectedUnseed != -1, uid);
	}

	std::optional<std::string> valgrindOutputFile;
	ExecArgs args;
	std::string binaryName;
	if (willRestart && stringEndsWith(oldBinaryName, "alpha6")) {
		args("-Rs")("1000000000")("-r")("simulation")("-q")("-s")(std::to_string(seed))("-f")(testFile)("-b")(
		    buggify ? "on" : "off")(fmt::format("--tls_plugin={}", tlsPluginFile))("--crash");
	} else {
		args("-Rs")("1GB")("-r")("simulation")("-q")("-s")(std::to_string(seed))("-f")(testFile)("-b")(
		    buggify ? "on" : "off")(fmt::format("--tls_plugin={}", tlsPluginFile))("--crash");
	}
	if (restarting) {
		args("--restarting");
	}
	if (useValgrind && !willRestart) {
		ExecArgs valgrindArgs;
		valgrindOutputFile = fmt::format("valgrind-{}.xml", seed);
		binaryName = "valgrind";
		{
			auto p = std::getenv("FDB_VALGRIND_DBGPATH");
			if (p != nullptr && std::string_view(p) != "") {
				valgrindArgs(fmt::format("--extra-debuginfo-path={}", p));
			}
		}
		valgrindArgs("--xml=yes")(fmt::format("--xml-file={}", valgrindOutputFile.value()))("-q")(
		    fdbserverName.c_str());
		args = valgrindArgs + args;
	} else {
		binaryName = fdbserverName;
	}
	int errfd[2];
	int outfd[2];
	if (pipe(outfd) == -1) {
		assert(false);
	}
	if (pipe(errfd) == -1) {
		assert(false);
	}
	auto pid = ::fork();
	if (pid == 0) {
		while ((dup2(outfd[0], STDOUT_FILENO) == -1) && (errno == EINTR)) {
		}
		while ((dup2(errfd[0], STDERR_FILENO) == -1) && (errno == EINTR)) {
		}
		close(outfd[0]);
		close(outfd[1]);
		close(errfd[0]);
		close(errfd[1]);
		// child
		execve(fdbserverName.c_str(), args.data(), environ);
		assert(false);
	}
	close(outfd[1]);
	close(errfd[1]);
	ThreadObserver outHandler{ pid, outfd[0], outfd[1], useValgrind };
	outHandler.run();

	int statLoc;
	waitpid(pid, &statLoc, 0);
	int exitCode = WEXITSTATUS(statLoc);
	outHandler.done();
	std::regex r{ "^trace.*\\.xml" };
	std::vector<std::filesystem::path> traces;
	for (auto p : std::filesystem::directory_iterator(tempPath)) {
		if (!p.is_regular_file()) continue;
		std::smatch match;
		std::string fname = p.path().filename();
		if (std::regex_match(fname, match, r)) {
			traces.push_back(p);
		}
	}
	std::sort(traces.begin(), traces.end());
	if (traces.size() == 0) {
		XElement xout{ useValgrind ? "NoTraceFileGeneratedLibPos" : "NoTraceFileGenerated" };
		xout["Severity"] = "30";
		xout["Plugin"] = tlsPluginFile;
		xout["MachineName"] = getMachineName();
		appendXmlMessageToSummary(summaryFileName, xout, traceToStdout, testFile, seed, buggify, expectedUnseed != -1,
		                          oldBinaryName);
		ok = useValgrind ? 0 : 103;
	} else {
		auto result =
		    summarize(traces, summaryFileName, errorFileName, outHandler.killed, outHandler.outputErrors, exitCode,
		              outHandler.memory, uid, valgrindOutputFile, expectedUnseed, unseed, retryableError,
		              logOnRetryableError, willRestart, restarting, oldBinaryName, traceToStdout);
		if (result != 0) {
			ok = result;
		}
	}

	if (willRestart) {
		for (auto& trace : traces) {
			std::filesystem::remove(trace);
		}
	}

	if (!traceToStdout) {
		printf("Done (unseed %d)\n", unseed);
	}

	std::filesystem::current_path(oldDir);
	if (!willRestart) {
		std::filesystem::remove_all(tempPath);
	}
	return ok;
}

int run(std::filesystem::path const& fdbserverName, std::filesystem::path const& tlsPluginFile,
        std::filesystem::path const& testFolder, std::string const& summaryFileName,
        std::optional<std::string> const& errorFileName, std::optional<std::filesystem::path> const& runDir,
        std::optional<std::filesystem::path> const& oldBinaryFolder, bool useValgrind, int maxTries,
        bool traceToStdout = false, std::filesystem::path const& tlsPluginFile_5_1 = "") {
	uint64_t seed = std::uniform_int_distribution<uint64_t>{}(g_rnd);
	bool buggify = std::uniform_real_distribution<double>{ 0, 1 }(g_rnd) < buggifyOnRatio;
	std::optional<std::filesystem::path> testFile;
	std::filesystem::path testDir;
	std::string oldServerName;

	if (std::filesystem::exists(testFolder) && std::filesystem::is_directory(testFolder)) {
		int poolSize = 0;
		if (std::filesystem::exists(testFolder / "rare")) poolSize += 1;
		if (std::filesystem::exists(testFolder / "restarting")) poolSize += 1;
		if (std::filesystem::exists(testFolder / "slow")) poolSize += 5;
		if (std::filesystem::exists(testFolder / "fast")) poolSize += 14;
		if (poolSize == 0) {
			fprintf(stderr, "Passed folder %s didn't have a fast, slow, rare, or restarting sub-folder\n",
			        testFolder.c_str());
			return 1;
		}
		auto selection = std::uniform_int_distribution<>{ 0, poolSize }(g_rnd);
		int selectionWindow = 0;
		while (true) {
			if (std::filesystem::exists(testFolder / "rare")) selectionWindow += 1;
			if (selection < selectionWindow) {
				testDir = testFolder / "rare";
				break;
			}
			if (std::filesystem::exists(testFolder / "restarting")) selectionWindow += 1;
			if (selection < selectionWindow) {
				testDir = testFolder / "restarting";
				break;
			}
			if (std::filesystem::exists(testFolder / "slow")) selectionWindow += 5;
			if (selection < selectionWindow) {
				testDir = testFolder / "slow";
				break;
			}
			testDir = testFolder / "fast";
			break;
		}
		std::vector<std::filesystem::path> files;
		allFiles(files, testDir);
		if (testDir.filename() == "restarting") {
			std::set<std::string> uniqueFiles;
			for (auto& f : files) {
				auto pos = f.string().find_last_of('-');
				uniqueFiles.emplace(f.string().substr(0, pos));
			}
			auto iter = uniqueFiles.begin();
			std::advance(iter, std::uniform_int_distribution<size_t>(0, uniqueFiles.size())(g_rnd));
			testFile = *iter;
		} else {
			testFile = files[std::uniform_int_distribution<size_t>(0, files.size())(g_rnd)];
		}
	} else if (std::filesystem::is_directory(testFolder)) {
		testFile = testFolder;
	} else {
		fprintf(stderr, "Passed path (%s) was not a folder or file\n", testFolder.c_str());
		return 1;
	}
	int result = 0;
	bool unseedCheck = std::uniform_real_distribution<>{ 0, 1 }(g_rnd) < unseedRatio;
	for (int i = 0; i < maxTries; ++i) {
		bool logOnRetryableError = i == maxTries - 1;
		bool retryableError = false;

		if (testDir.filename() == "restarting") {
			int expectedUnseed = -1;
			int unseed;
			std::string uid = newGuid();
			auto oldServerNameTokens = split(oldServerName, '-');
			bool useNewPlugin = oldServerName == fdbserverName ||
			                    versionGreaterThanOrEqual(oldServerNameTokens[oldServerNameTokens.size() - 1], "5.2.0");
			result =
			    runTest(oldServerName, useNewPlugin ? tlsPluginFile : tlsPluginFile_5_1, summaryFileName, errorFileName,
			            seed, buggify, testFile.value().string() + "-1.txt", runDir, uid, expectedUnseed, unseed,
			            retryableError, logOnRetryableError, useValgrind, false, true, oldServerName, traceToStdout);
			if (result == 0) {
				result =
				    runTest(fdbserverName, tlsPluginFile, summaryFileName, errorFileName, seed + 1, buggify,
				            testFile.value().string() + "-2.txt", runDir, uid, expectedUnseed, unseed, retryableError,
				            logOnRetryableError, useValgrind, true, false, oldServerName, traceToStdout);
			}
		} else {
			int expectedUnseed = -1;
			if (!useValgrind && unseedCheck) {
				result = runTest(fdbserverName, tlsPluginFile, {}, {}, seed, buggify, testFile.value(), runDir,
				                 newGuid(), -1, expectedUnseed, retryableError, logOnRetryableError, false, false,
				                 false, "", traceToStdout);
			}

			if (!retryableError) {
				int unseed;
				result = runTest(fdbserverName, tlsPluginFile, summaryFileName, errorFileName, seed, buggify,
				                 testFile.value(), runDir, newGuid(), expectedUnseed, unseed, retryableError,
				                 logOnRetryableError, useValgrind, false, false, "", traceToStdout);
			}
		}

		if (!retryableError) {
			return result;
		}
	}
	return result;
}

int replay(std::string const& fdbserverName, std::filesystem::path const& tlsPluginFile,
           std::filesystem::path const& inputSummaryFileName, std::filesystem::path const& outputSummaryFileName,
           std::optional<std::filesystem::path> runDir) {
	std::ifstream in{ inputSummaryFileName };
	std::string line;
	size_t lineBufferLen = 1024;
	std::unique_ptr<char[]> lineBuffer{ new char[lineBufferLen] };
	while (std::getline(in, line)) {
		if (line.size() + 1 > lineBufferLen) {
			lineBufferLen = line.size() + 1;
			lineBuffer.reset(new char[lineBufferLen]);
			memcpy(lineBuffer.get(), line.c_str(), line.size() + 1);
		}
		rapidxml::xml_document<> doc;
		doc.parse<rapidxml::parse_non_destructive>(lineBuffer.get());
		if (doc.first_node()->name() == "TestPlan"sv) {
			int randomSeed = 0;
			bool buggifyEnabled = true;
			std::string testFile;
			std::string testUID;
			for (auto a = doc.first_node()->first_attribute(); a; a = a->next_attribute()) {
				if (a->name() == "RandomSeed"sv) {
					randomSeed = std::stoi(std::string{ a->value() });
				} else if (a->name() == "BuggifyEnabled"sv) {
					buggifyEnabled = a->value() != "0"sv;
				} else if (a->name() == "TestFile"sv) {
					testFile = std::string{ a->value() };
				} else if (a->name() == "TestUID"sv) {
					testUID = std::string{ a->value() };
				}
			}
			int unseed;
			bool retryableError;
			runTest(fdbserverName, tlsPluginFile, outputSummaryFileName, {}, randomSeed, buggifyEnabled, testFile,
			        runDir, testUID, -1, unseed, retryableError, true, false);
		}
	}
	return 0;
}

struct TestDescr {
	using Clock = std::chrono::system_clock;
	std::optional<Clock::time_point> testEnd;
	std::filesystem::path queueDirectory, inputDir, outputDir;
	std::optional<std::filesystem::path> runDir, oldBinaryDir;
	std::string label;
	int testCount;

	void readSpecFile() {
		double testDuration = 0.0;
		auto specFile = queueDirectory / fmt::format("{}.xml", label);
		CFile file{ specFile.c_str(), "r" };
		auto str = file.readWholeFile();
		rapidxml::xml_document<> doc;
		doc.parse<0>(str->ptr);
		for (ElementIterator iter{ doc.first_node() }; iter != ElementIterator{}; ++iter) {
			if (iter->type() == rapidxml::node_element && iter->name() == "TestDefinition"sv) {
				for (ElementIterator i{ iter->first_node() }; i != ElementIterator{}; ++i) {
					if (i->type() == rapidxml::node_element) {
						if (i->name() == "Duration"sv) {
							for (auto attr = i->first_attribute(); attr; attr = attr->next_attribute()) {
								if (attr->name() == "Hours"sv) {
									testDuration = std::stod(std::string{ attr->value() });
								}
							}
						} else if (i->name() == "TestCount"sv) {
							assert(i->first_node()->type() == rapidxml::node_data);
							testCount = std::stoi(trim_copy(std::string{ i->first_node()->value() }));
						}
					}
				}
				break;
			}
		}
		struct stat fileStats;
		if (fstat(file.fd(), &fileStats) != 0) {
			throw std::system_error{ errno, std::generic_category() };
		}
		timespec birthtime;
#if defined(__LINUX__)
		birthtime = fileStats.st_birthtime;
#elif defined(__APPLE__)
		birthtime = fileStats.st_birthtimespec;
#endif
		auto d = std::chrono::seconds{ birthtime.tv_sec } + std::chrono::nanoseconds{ birthtime.tv_nsec };
		Clock::time_point beginTime{ std::chrono::duration_cast<Clock::duration>(d) };
		testEnd = beginTime + std::chrono::duration_cast<Clock::duration>(
		                          std::chrono::duration<double, std::ratio<3600>>(testDuration));
	}

	TestDescr(std::filesystem::path const& queueDirectory, std::string const& label,
	          std::filesystem::path const& runDir, std::filesystem::path const& shareDir,
	          std::optional<std::filesystem::path> const& cacheDir)
	  : queueDirectory(queueDirectory), outputDir(queueDirectory / "archive"), runDir(runDir), label(label) {
		readSpecFile();
		if (!std::filesystem::exists(outputDir)) {
			std::filesystem::create_directory(outputDir);
		}
		if (cacheDir.has_value()) {
			inputDir = cacheDir.value() / "archive" / label;
			if (!std::filesystem::exists(cacheDir.value() / "archive")) {
				std::filesystem::create_directory(cacheDir.value() / "archive");
			}
			if (!std::filesystem::exists(inputDir)) {
				FileNameMutex mutex(inputDir);
				std::unique_lock<FileNameMutex> _(mutex);
				if (!std::filesystem::exists(inputDir)) {
					auto tmpDir = cacheDir.value() / "archive.part" / (label + "." + getRandomFilename() + ".part");
					std::filesystem::create_directory(tmpDir);
					copyAll(outputDir, tmpDir, [](std::filesystem::path const& file) {
						std::string filename = file.filename();
						return filename != "fdbserver.debug"sv && !stringStartsWith(filename, "summary-"sv) &&
						       filename != "errors.txt"sv;
					});
					std::filesystem::rename(tmpDir, inputDir);
				}
			}
		} else {
			inputDir = outputDir;
			oldBinaryDir = shareDir / "oldBinaries";
		}
	}

	static TestDescr getTest(std::filesystem::path const& parent, std::filesystem::path const& runDir,
	                         std::filesystem::path const& shareDir, std::filesystem::path const& cacheDir) {
		std::regex r{ fmt::format(".*{}\\.xml", OS_NAME) };
		for (;;) {
			std::vector<std::filesystem::path> testFiles;
			for (auto f : std::filesystem::directory_iterator(parent)) {
				std::smatch match;
				auto fName = f.path().filename().string();
				if (f.is_regular_file() && std::regex_match(fName, match, r)) {
					testFiles.push_back(f.path());
				}
			}
			if (testFiles.empty()) {
				auto testFile = testFiles[std::uniform_int_distribution<>{ 0, int(testFiles.size()) }(g_rnd)];
				TestDescr test{ parent, testFile.stem(), runDir, shareDir, cacheDir };
				if ((test.testCount < 0 || updateTestTotals(test.outputDir / "testCount", test.testCount)) &&
				    !test.done()) {
					return test;
				} else {
					test.finalize();
				}
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}

	static bool updateTestTotals(std::filesystem::path const& countFileName, int desiredTestCount) {
		FileNameMutex mutex{ countFileName };
		int currentCount = 0;
		try {
			CFile file{ countFileName, "r" };
			auto str = file.readWholeFile();
			std::string contents = std::string{ str->ptr, str->len };
			trim(contents);
			int currentCount = 0;
			if (!contents.empty()) {
				currentCount = std::stoi(contents);
			}
		} catch (std::system_error& e) {
			// ignore this - if file doesn't exist we will create it later.
		}

		if (currentCount >= desiredTestCount) {
			return false;
		}
		CFile file{ countFileName, "w" };
		fmt::print(file.file, "{}", currentCount);
		return true;
	}

	void run(bool useValgrind, int maxTries) {
		::run(inputDir / std::string{ BINARY }, inputDir / std::string{ PLUGIN }, inputDir / "tests",
		      outputDir / fmt::format("summary-{}.xml", getMachineName()), outputDir / "errors.txt", runDir,
		      oldBinaryDir, useValgrind, maxTries);
	}

	bool done() const { return testEnd.has_value() && Clock::now() > testEnd.value(); }

	void finalize() {
		auto toDelete = queueDirectory / fmt::format("{}.xml", label);
		fmt::print("Deleting: {0}", toDelete);
		std::filesystem::remove(toDelete);
	}
};

int autoRun(std::filesystem::path const& queueDirectory, std::filesystem::path const& runDir,
            std::filesystem::path const& shareDir, std::filesystem::path const& cacheDir, bool useValgrind,
            int maxTries) {
	// TODO: implement
	for (;;) {
		TestDescr test = TestDescr::getTest(queueDirectory, runDir, shareDir, cacheDir);
		fmt::print("Running test {}\n", test.label);

		test.run(useValgrind, maxTries);
	}
	return 0;
}

int extractErrors(std::filesystem::path const& summaryFileName, std::filesystem::path const& errorSummaryFileName) {
	fmt::print("Extracting from {}", summaryFileName);
	std::vector<XElement> xout;
	std::map<std::pair<std::string, int>, std::pair<int, int>> coverage;
	CFile file(summaryFileName, "r");
	auto events = identifyFailedTestPlans(XMLParser::parse(file));
	for (auto& ev : events) {
		auto t = std::dynamic_pointer_cast<Test>(ev);
		if (!t) {
			continue;
		}
		for (auto& tev : t->events) {
			if (tev->type == "CodeCoverage" || tev->type == "FaultInjected") {
				auto keyTuple = std::make_pair(tev->details.at("File"), std::stoi(tev->details.at("Line")));
				auto iter = coverage.find(keyTuple);
				if (iter != coverage.end()) {
					iter->second = std::make_pair(iter->second.first, iter->second.second + (t->ok ? 0 : 1));
				} else {
					coverage[keyTuple] = std::make_pair(1, t->ok ? 0 : 1);
				}
			}
		}
		if (!t->ok) {
			XElement el = XElement::fromTest(*t);
			std::vector<XNode*> oldChildren = std::move(el.children);
			std::vector<XNode*> toDelete;
			el.children = std::vector<XNode*>{};
			el.children.reserve(oldChildren.size());
			for (auto child : oldChildren) {
				if (child->type() == XType::Element) {
					auto element = dynamic_cast<XElement*>(child);
					if ((*element)["Type"] == "CodeCoverage" || (*element)["Type"] == "FaultInjected") {
						toDelete.push_back(child);
						continue;
					}
				}
				el.children.push_back(child);
			}
			for (auto p : toDelete) {
				delete p;
			}
			xout.push_back(el);
		}
	}
	return 0;
}

} // namespace

int main(int argc, char* argv[]) {
	g_rnd.seed(std::random_device{}());
	std::optional<std::string> valgrindFileName;
	std::string externalError = "";
	bool traceToStdout = false;
	if (argc < 2) {
		return usageMessage();
	}
	if (argv[1] == "summarize"sv) {
		if (argc < 4) {
			return usageMessage();
		}
		if (argc > 4) {
			valgrindFileName = std::string_view{ argv[4] };
		}
		if (argc > 5) {
			externalError = argv[5];
		}
		traceToStdout = argc == 7 && toLower(argv[6]) == "true";
		int unseed;
		bool retryableError;
		return summarize({ std::string{ argv[2] } }, argv[2], {}, {}, {}, {}, {}, {}, valgrindFileName, -1, unseed,
		                 retryableError, true, false, false, "", traceToStdout, externalError);
	} else if (argv[1] == "remote"sv) {
		if (argc < 7 || argc > 9) {
			return usageMessage();
		}
		return remote(argv[2], argv[3], std::stod(argv[4]), std::stoi(argv[5]), argv[6],
		              argc == 8 ? argv[7] : "default");
	} else if (argv[1] == "run"sv) {
		if (argc < 7) {
			return usageMessage();
		}
		std::optional<std::string> runDir;
		if (argv[2] != "temp"sv) {
			runDir = argv[2];
		}
		bool useValgrind = argc > 7 && toLower(argv[7]) == "true";
		int maxTries = argc > 8 ? std::stoi(argv[8]) : 3;
		return run(argv[3], argv[4], argv[5], argv[6], {}, runDir, {}, {}, useValgrind, maxTries);
	} else if (argv[1] == "replay"sv) {
		if (argc != 7) {
			return usageMessage();
		}
		std::optional<std::filesystem::path> runDir;
		if (argv[2] != "temp"sv) {
			runDir = std::string{ argv[2] };
		}
		return replay(argv[3], argv[4], argv[5], argv[6], runDir);
	} else if (argv[1] == "auto"sv) {
		if (argc < 5) {
			return usageMessage();
		}
		std::filesystem::path runDir = "runs", cacheDir = "test_harness_cache";
		if (argv[2] != "temp"sv) {
			std::filesystem::path base{ argv[2] };
			runDir = base / "runs";
			cacheDir = base / "test_harness_cache";
		}
		bool useValgrind = false;
		if (argc > 5) {
			std::string u{ argv[4] };
			std::transform(u.begin(), u.end(), u.begin(), [](auto c) { return std::tolower(c); });
			useValgrind = u == "true";
		}
		int maxTries = argc > 6 ? std::stoi(argv[6]) : 3;
		return autoRun(argv[3], runDir, argv[4], cacheDir, useValgrind, maxTries);
	} else if (argv[1] == "extract-errors"sv) {
		if (argc != 4) {
			return usageMessage();
		}
		return extractErrors(argv[2], argv[3]);
	} else if (argv[1] == "joshua-run"sv) {
		traceToStdout = true;
		auto oldBinaryFolder =
		    (argc > 2) ? argv[2] : std::filesystem::path{ "/opt" } / "joshua" / "global_data" / "oldBinaries";
		bool useValgrind = argc > 3 && toLower(argv[3]) == "true";
		int maxTries = (argc > 4) ? std::stoi(std::string{ argv[4] }) : 3;
		return run(std::filesystem::path("bin") / BINARY, "", "tests", "summary.xml", "error.xml", "tmp",
		           oldBinaryFolder, useValgrind, maxTries, true,
		           std::filesystem::path("/app") / "deploy" / "runtime" / ".tls_5_1" / PLUGIN);
	} else {
		fprintf(stderr, "Unknown Command %s\n", argv[1]);
		usageMessage();
		return 1;
	}
}
