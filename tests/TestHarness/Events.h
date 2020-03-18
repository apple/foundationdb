/*
 * Events.h
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

#pragma once
#include <limits>
#include <memory>
#include <random>
#include <string_view>
#include <string>
#include <utility>
#include <vector>
#include <filesystem>
#include <rapidxml.hpp>

namespace magnesium {

class ElementIterator {
	std::stack<rapidxml::xml_node<>*> stack;
	rapidxml::xml_node<>* current;

public:
	ElementIterator() : current(nullptr) {}
	ElementIterator(rapidxml::xml_node<>* begin) : current(begin) {}
	bool operator==(ElementIterator const& other) const { return current == other.current; }
	bool operator!=(ElementIterator const& other) const { return current == other.current; }
	ElementIterator& operator++() {
		auto next = current->first_node();
		if (next) {
			stack.push(current);
			current = next;
			return *this;
		}
		next = current->next_sibling();
		while (next == nullptr) {
			if (stack.empty()) {
				current = nullptr;
				return *this;
			}
			next = stack.top();
			stack.pop();
			next = next->next_sibling();
		}
		current = next;
		return *this;
	}

	rapidxml::xml_node<>* ptr() { return current; }

	rapidxml::xml_node<>& operator*() { return *current; }

	rapidxml::xml_node<>* operator->() { return current; }
};


std::string toLower(std::string const& s);
bool stringStartsWith(std::string_view const& str, std::string_view const& prefix);
bool stringEndsWith(std::string_view const& str, std::string_view const& postfix);
std::vector<std::string> split(const std::string& s, char delim);
void ltrim(std::string& s);
void rtrim(std::string& s);
void trim(std::string& s);
std::string ltrim_copy(std::string s);
std::string rtrim_copy(std::string s);
std::string trim_copy(std::string s);



enum class Severity : int { SevDebug = 5, SevInfo = 10, SevWarn = 20, SevWarnAlways = 30, SevError = 40 };
Severity stringToSeverity(std::string_view str);
Severity stringToSeverity(std::string const& str);

struct Event {
	class Details {
		std::vector<std::pair<std::string, std::string>> impl;

	public:
		using iterator = std::vector<std::pair<std::string, std::string>>::iterator;
		using const_iterator = std::vector<std::pair<std::string, std::string>>::const_iterator;

	public:
		std::string& operator[](std::string const& name);
		std::string const& at(std::string const&) const;

		template <class Str>
		bool hasKey(Str const& key) const {
			for (auto& p : impl) {
				if (p.first == key) {
					return true;
				}
			}
			return false;
		}
		iterator begin() { return impl.begin(); }
		const_iterator begin() const { return impl.begin(); }
		iterator end() { return impl.end(); }
		const_iterator end() const { return impl.end(); }
	};

	virtual bool extractAttributes(std::string const& key, std::string const& value);
	double time = 0.0;
	Severity severity = Severity::SevInfo;
	std::string type, machine, id, traceFile;
	Details details;
	rapidxml::xml_node<>* original = nullptr;

	std::string formatTestError(bool includeDetails);
};

struct TestPlan : Event {
	std::string testUID;
	std::string testFile;
	int randomSeed = 0;
	bool determinismCheck = true, buggify = true;
	std::string oldBinary;
	bool extractAttributes(std::string const& key, std::string const& value) override;
};

struct Test : TestPlan {
	std::string sourceVersion;
	double simElapsedTime = 0.0;
	double realElapsedTime = 0.0;
	bool ok = false;
	int passed = 0, failed = 0;
	int randomUnseed = 0;
	long peakMemUsage = 0;
	std::vector<std::shared_ptr<Event>> events; // Summarized events during the test

	bool extractAttributes(std::string const& key, std::string const& value) override;
};
//
//  we need some C wrappers as the C++ API doesn't support file locks
struct CFile {
	struct MutableString {
		char* ptr;
		const size_t len;
		MutableString(char* ptr, size_t len) : ptr(ptr), len(len) {}
		MutableString(MutableString const&) = delete;
		MutableString(MutableString&& other) : ptr(other.ptr), len(other.len) { other.ptr = nullptr; }
		~MutableString() {
			if (ptr) {
				delete[] ptr;
			}
		}
	};
	FILE* file;
	std::filesystem::path fileName;
	CFile(std::filesystem::path const& path, const char* mode) : file(fopen(path.c_str(), mode)), fileName(path) {
		if (file == nullptr) {
			throw std::system_error{ errno, std::generic_category() };
		}
	}
	~CFile() {
		if (file != nullptr) {
			fclose(file);
		}
	}
	std::shared_ptr<MutableString> wholeFile;
	std::shared_ptr<MutableString> readWholeFile() {
		if (wholeFile) {
			return wholeFile;
		}
		fseek(file, 0L, SEEK_END);
		size_t fSize = ftell(file);
		rewind(file);
		wholeFile = std::make_shared<MutableString>(new char[fSize + 1], fSize);
		fread(wholeFile->ptr, fSize, 1, file);
		return wholeFile;
	}

	int fd() { return fileno(file); }
};

class XMLParser {
public:
	static std::mt19937_64 random;
	static std::vector<std::shared_ptr<Event>> parse(CFile& file, bool keepOriginalElement = false,
	                                                 double startTime = -1,
	                                                 double endTime = std::numeric_limits<double>::max(),
	                                                 double samplingFactor = 1.0);
};

std::vector<std::shared_ptr<Event>> identifyFailedTestPlans(std::vector<std::shared_ptr<Event>> const& events);

} // namespace magnesium
