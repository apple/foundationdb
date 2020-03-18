/*
 * Events.cpp
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
#include "rapidxml.hpp"
#include <cctype>
#include <memory>
#include <random>
#include <stack>
#include <stdexcept>
#include <string>
#include <fmt/core.h>
#include <string_view>
#include <vector>
#include <map>
#include <sstream>

using namespace std::literals;

namespace magnesium {

std::string toLower(std::string const& s) {
	std::string res = s;
	std::transform(res.begin(), res.end(), res.begin(), [](char c) { return std::tolower(c); });
	return res;
}

bool stringStartsWith(std::string_view const& str, std::string_view const& prefix) {
	if (str.length() < prefix.length()) return false;
	return str.substr(0, prefix.length()) == prefix;
}

bool stringEndsWith(std::string_view const& str, std::string_view const& postfix) {
	if (str.length() < postfix.length()) return false;
	return 0 == str.compare(str.length() - postfix.length(), postfix.length(), postfix);
}

std::vector<std::string> split(const std::string& s, char delim) {
	std::stringstream ss(s);
	std::string item;
	std::vector<std::string> elems;
	while (std::getline(ss, item, delim)) {
		elems.push_back(item);
		// elems.push_back(std::move(item)); // if C++11 (based on comment from @mchiasson)
	}
	return elems;
}

void ltrim(std::string& s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
}

// trim from end (in place)
void rtrim(std::string& s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(), s.end());
}

// trim from both ends (in place)
void trim(std::string& s) {
	ltrim(s);
	rtrim(s);
}

// trim from start (copying)
std::string ltrim_copy(std::string s) {
	ltrim(s);
	return s;
}

// trim from end (copying)
std::string rtrim_copy(std::string s) {
	rtrim(s);
	return s;
}

// trim from both ends (copying)
std::string trim_copy(std::string s) {
	trim(s);
	return s;
}

Severity stringToSeverity(std::string const& str) {
	return static_cast<Severity>(std::stoi(str));
}

Severity stringToSeverity(std::string_view str) {
	return stringToSeverity(std::string{ str });
}

std::string& Event::Details::operator[](std::string const& name) {
	for (auto& p : impl) {
		if (p.first == name) {
			return p.second;
		}
	}
	impl.push_back(std::make_pair(name, std::string{ "" }));
	return impl.back().second;
}

std::string const& Event::Details::at(const std::string& name) const {
	for (auto& p : impl) {
		if (p.first == name) {
			return p.second;
		}
	}
	throw std::out_of_range{ fmt::format("Key \"{}\" does not exist", name) };
}

std::string Event::formatTestError(bool includeDetails) {
	std::string s = type;
	if (type == "InternalError")
		s = fmt::format("{} {} {}", type.c_str(), details["File"], details["Line"]);
	else if (type == "TestFailure")
		s = fmt::format("{0} {1}", type, details["Reason"]);
	else if (type == "ValgrindError")
		s = fmt::format("{0} {1}", type, details["What"]);
	else if (type == "ExitCode")
		s = fmt::format("{0} 0x{1:x}", type, details.at("Code"));
	else if (type == "StdErrOutput")
		s = fmt::format("{0}: {1}", type, details["Output"]);
	else if (type == "BTreeIntegrityCheck")
		s = fmt::format("{0}: {1}", type, details["ErrorDetail"]);
	if (details.hasKey("Error")) s = fmt::format("{} {}", s, details.at("Error"));
	if (details.hasKey("WinErrorCode")) s = fmt::format("{} {}", s, details.at("WinErrorCode"));
	if (details.hasKey("LinuxErrorCode")) s = fmt::format("{} {}", s, details.at("LinuxErrorCode"));
	if (details.hasKey("Status")) s = fmt::format("{} Status={}", s, details.at("Status"));
	if (details.hasKey("In")) s = fmt::format("{} In {}", s, details.at("In"));
	if (details.hasKey("SQLiteError")) {
		auto& err = details.at("SQLiteError");
		s = fmt::format("{} SQLiteError={}({})", s, err, err);
	}
	if (details.hasKey("Details") && includeDetails) fmt::format("{} {}", s, details.at("Details"));

	return s;
}

std::mt19937_64 XMLParser::random = std::mt19937_64{ std::random_device{}() };

bool Event::extractAttributes(std::string const& key, std::string const& value) {
	if (key == "Type") {
		type = value;
	} else if (key == "Machine") {
		machine = value;
	} else if (key == "ID") {
		id = value;
	} else if (key == "Severity") {
		severity = stringToSeverity(std::string{ value });
	} else if (key == "Time") {
		time = std::stod(std::string{ value });
	} else {
		return false;
	}
	return true;
}

bool TestPlan::extractAttributes(std::string const& name, std::string const& value) {
	if (Event::extractAttributes(name, value)) {
	} else if (name == "BuggifyEnabled") {
		buggify = value != "0";
	} else if (name == "DeterminismCheck") {
		determinismCheck = value != "0";
	} else if (name == "OldBinary") {
		oldBinary = value;
	} else {
		return false;
	}
	return true;
}

bool Test::extractAttributes(std::string const& name, std::string const& value) {
	if (TestPlan::extractAttributes(name, value)) {
	} else if (name == "TestUID") {
		testUID = value;
	} else if (name == "TestFile") {
		testFile = value;
	} else if (name == "SourceVersion") {
		sourceVersion = value;
	} else if (name == "OK") {
		ok = value == "1" || toLower(value) == "true";
	} else if (name == "RandomSeed") {
		randomSeed = std::stoi(value);
	} else if (name == "RandomUnseed") {
		randomUnseed = std::stoi(value);
	} else if (name == "SimElapsedTime") {
		simElapsedTime = std::stod(value);
	} else if (name == "RealElapsedTime") {
		realElapsedTime = std::stod(value);
	} else if (name == "Passed") {
		passed = std::stoi(value);
	} else if (name == "Failed") {
		failed = std::stoi(value);
	} else if (name == "PeakMemory") {
		peakMemUsage = std::stol(value);
	} else {
		return false;
	}
	return true;
}

namespace {

std::shared_ptr<Event> parseEvent(rapidxml::xml_node<>* xEvent, std::filesystem::path const& file,
                                  bool keepOriginalElement, double startTime, double endTime, double samplingFactor) {
	if (samplingFactor != 1.0 && std::uniform_real_distribution<double>{ 0, 1 }(XMLParser::random) > samplingFactor) {
		return nullptr;
	}
	auto event = std::make_shared<Event>();
	event->traceFile = file;
	for (auto attrib = xEvent->first_attribute(); attrib; attrib = attrib->next_attribute()) {
		std::string name{ attrib->name() };
		std::string value{ attrib->value() };
		if (!event->extractAttributes(name, value)) {
			event->details[name] = value;
		}
	}
	if (event->time < startTime || event->time > endTime) {
		return nullptr;
	}
	if (keepOriginalElement) {
		event->original = xEvent;
	}
	return event;
}

std::shared_ptr<TestPlan> parseTestPlan(rapidxml::xml_node<>* xTest, std::filesystem::path const& file,
                                        bool keepOriginalElement) {
	auto test = std::make_shared<TestPlan>();
	for (auto attrib = xTest->first_attribute(); attrib; attrib = attrib->next_attribute()) {
		std::string name{ attrib->name() }, value{ attrib->value() };
		test->extractAttributes(name, value);
	}
	if (keepOriginalElement) {
		test->original = xTest;
	}
	return test;
}

std::shared_ptr<Test> parseTest(rapidxml::xml_node<>* xTest, std::filesystem::path const& file,
                                bool keepOriginalElement) {
	auto test = std::make_shared<Test>();
	test->traceFile = file;
	test->type = "Test";
	test->ok = false;
	for (auto attrib = xTest->first_attribute(); attrib; attrib = attrib->next_attribute()) {
		std::string name{ attrib->name() };
		std::string value{ attrib->value() };
		test->extractAttributes(name, value);
	}
	for (auto iter = xTest->first_node(); iter; iter->next_sibling()) {
		test->events.push_back(std::make_shared<Event>());
		auto& elem = test->events.back();
		for (auto attrib = iter->first_attribute(); attrib; attrib = attrib->next_attribute()) {
			std::string key{ attrib->name() };
			std::string value{ attrib->value() };
			if (!elem->extractAttributes(key, value)) {
				elem->details[key] = value;
			}
		}
	}
	if (keepOriginalElement) {
		test->original = xTest;
	}
	return test;
}

} // namespace

std::vector<std::shared_ptr<Event>> XMLParser::parse(CFile& file, bool keepOriginalElement, double startTime,
                                                     double endTime, double samplingFactor) {
	auto str = file.readWholeFile();
	rapidxml::xml_document<> doc;
	doc.parse<0>(str->ptr);
	std::vector<std::shared_ptr<Event>> result;
	ElementIterator end;
	ElementIterator iter{ &doc };
	for (; iter != end; ++iter) {
		if (iter->type() == rapidxml::node_element && iter->name() == "Trace"sv) {
			break;
		}
	}
	++iter;
	for (; iter != end; ++iter) {
		if (iter->type() != rapidxml::node_element) {
			continue;
		}
		std::string_view name{ iter->name() };
		std::shared_ptr<Event> ev;
		if (name == "Event") {
			ev = parseEvent(iter.ptr(), file.fileName, keepOriginalElement, startTime, endTime, samplingFactor);
		} else if (name == "Test") {
			ev = parseTest(iter.ptr(), file.fileName, keepOriginalElement);
		} else if (name == "TestPlan") {
			ev = parseTestPlan(iter.ptr(), file.fileName, keepOriginalElement);
		}
		if (ev) {
			result.push_back(std::move(ev));
		}
	}
	return result;
}

std::vector<std::shared_ptr<Event>> identifyFailedTestPlans(const std::vector<std::shared_ptr<Event>>& events) {
	std::vector<std::shared_ptr<Event>> result;
	std::map<std::string, std::shared_ptr<TestPlan>> failedPlans;
	for (auto& ev : events) {
		auto tp = std::dynamic_pointer_cast<TestPlan>(ev);
		if (!tp || tp->testUID == "") {
			result.push_back(tp);
			continue;
		}
		auto t = std::dynamic_pointer_cast<Test>(ev);
		if (t) {
			failedPlans.erase(tp->testUID + tp->traceFile);
			if (tp->traceFile != "" && stringEndsWith(tp->traceFile, "-2.txt")) {
				failedPlans.erase(split(tp->traceFile, '-')[0] + "-1.txt");
			}
			result.push_back(t);
		} else {
			if (failedPlans.find(tp->testUID + tp->traceFile) == failedPlans.end()) {
				failedPlans.emplace(tp->testUID + tp->traceFile, tp);
			}
		}
	}
	for (auto& fp : failedPlans) {
		auto res = std::make_shared<Test>();
		res->type = "FailedTestPlan";
		res->time = fp.second->time;
		res->machine = fp.second->machine;
		res->testUID = fp.second->testUID;
		res->testFile = fp.second->testFile;
		res->randomSeed = fp.second->randomSeed;
		res->buggify = fp.second->buggify;
		res->determinismCheck = fp.second->determinismCheck;
		res->oldBinary = fp.second->oldBinary;
		auto ev = std::make_shared<Event>();
		ev->severity = Severity::SevWarnAlways;
		ev->type = "TestNotSummarized";
		ev->time = fp.second->time;
		ev->machine = fp.second->machine;
		res->events.emplace_back(std::move(ev));
		result.emplace_back(std::move(res));
	}
	return result;
}

} // namespace magnesium
