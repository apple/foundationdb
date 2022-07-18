/*
 * CodeProbe.cpp
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

#include "flow/CodeProbe.h"
#include "flow/CodeProbeUtils.h"
#include "flow/Arena.h"
#include "flow/network.h"

#include <map>
#include <fmt/format.h>
#include <boost/core/demangle.hpp>
#include <typeinfo>

namespace probe {

namespace {

std::vector<ExecutionContext> fromStrings(std::vector<std::string> const& ctxs) {
	std::vector<ExecutionContext> res;
	for (auto const& ctx : ctxs) {
		std::string c;
		c.reserve(ctx.size());
		std::transform(ctx.begin(), ctx.end(), std::back_inserter(c), [](char c) { return std::tolower(c); });
		if (c == "all") {
			res.push_back(ExecutionContext::Net2);
			res.push_back(ExecutionContext::Simulation);
		} else if (c == "simulation") {
			res.push_back(ExecutionContext::Simulation);
		} else if (c == "net2") {
			res.push_back(ExecutionContext::Net2);
		} else {
			throw invalid_option_value();
		}
	}
	std::sort(res.begin(), res.end());
	res.erase(std::unique(res.begin(), res.end()), res.end());
	return res;
}

StringRef normalizePath(const char* path) {
	auto srcBase = LiteralStringRef(FDB_SOURCE_DIR);
	auto binBase = LiteralStringRef(FDB_SOURCE_DIR);
	StringRef filename(reinterpret_cast<uint8_t const*>(path), strlen(path));
	if (filename.startsWith(srcBase)) {
		filename = filename.removePrefix(srcBase);
	} else if (filename.startsWith(binBase)) {
		filename = filename.removePrefix(binBase);
	}
	if (filename[0] == '/') {
		filename = filename.removePrefix("/"_sr);
	} else if (filename[0] == '\\') {
		filename = filename.removePrefix("/"_sr);
	}
	return filename;
}

struct CodeProbes {
	struct Location {
		StringRef file;
		unsigned line;
		Location(StringRef file, unsigned line) : file(file), line(line) {}
		bool operator==(Location const& rhs) const { return line == rhs.line && file == rhs.file; }
		bool operator!=(Location const& rhs) const { return line != rhs.line && file != rhs.file; }
		bool operator<(Location const& rhs) const {
			if (file < rhs.file) {
				return true;
			} else if (file == rhs.file) {
				return line < rhs.line;
			} else {
				return false;
			}
		}
		bool operator<=(Location const& rhs) const {
			if (file < rhs.file) {
				return true;
			} else if (file == rhs.file) {
				return line <= rhs.line;
			} else {
				return false;
			}
		}
		bool operator>(Location const& rhs) const { return rhs < *this; }
		bool operator>=(Location const& rhs) const { return rhs <= *this; }
	};

	std::multimap<Location, ICodeProbe const*> codeProbes;

	void traceMissedProbes(Optional<ExecutionContext> context) const {
		const ICodeProbe* prev = nullptr;
		for (auto probe : codeProbes) {
			if (!probe.second->wasHit() && (!context.present() || probe.second->expectInContext(context.get())) &&
			    *prev != *probe.second) {
				probe.second->trace(false);
			}
		}
	}

	void add(ICodeProbe const* probe) {
		const char* file = probe->filename();
		unsigned line = probe->line();
		Location loc(normalizePath(file), line);
		codeProbes.emplace(loc, probe);
	}

	static CodeProbes& instance() {
		static CodeProbes probes;
		return probes;
	}

	void verify() const {
		std::map<std::pair<StringRef, StringRef>, ICodeProbe const*> comments;
		for (auto probe : codeProbes) {
			auto file = probe.first.file;
			auto comment = probe.second->comment();
			auto commentEntry =
			    std::make_pair(file, StringRef(reinterpret_cast<uint8_t const*>(comment), strlen(comment)));
			ASSERT(file == normalizePath(probe.second->filename()));
			auto iter = comments.find(commentEntry);
			if (iter != comments.end() && probe.second->line() != iter->second->line()) {
				fmt::print("ERROR ({}:{}): {} isn't unique in file {}. Previously seen here: {}:{}\n",
				           probe.first.file,
				           probe.first.line,
				           iter->first.second,
				           probe.second->filename(),
				           iter->second->filename(),
				           iter->second->line());
				// ICodeProbe const& fst = *iter->second;
				// ICodeProbe const& snd = *probe.second;
				// fmt::print("\t1st Type: {}\n", boost::core::demangle(typeid(fst).name()));
				// fmt::print("\t2nd Type: {}\n", boost::core::demangle(typeid(snd).name()));
				// fmt::print("\n");
				// fmt::print("\t1st Comment: {}\n", fst.comment());
				// fmt::print("\t2nd Comment: {}\n", snd.comment());
				// fmt::print("\n");
				// fmt::print("\t1st CompUnit: {}\n", fst.compilationUnit());
				// fmt::print("\t2nd CompUnit: {}\n", snd.compilationUnit());
				// fmt::print("\n");
			} else {
				comments.emplace(commentEntry, probe.second);
			}
		}
	}

	void printXML() const {
		verify();
		fmt::print("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
		fmt::print("<CoverageTool>\n");
		if (codeProbes.empty()) {
			fmt::print("\t<CoverageCases/>\n");
			fmt::print("\t<Inputs/>\n");
		} else {
			std::vector<StringRef> files;
			fmt::print("\t<CoverageCases>\n");
			for (auto probe : codeProbes) {
				files.push_back(probe.first.file);
				fmt::print("\t\t<Case File=\"{}\" Line=\"{}\" Comment=\"{}\" Condition=\"{}\"/>\n",
				           probe.first.file,
				           probe.first.line,
				           probe.second->comment(),
				           probe.second->condition());
			}
			fmt::print("\t</CoverageCases>\n");
			fmt::print("\t<Inputs>\n");
			for (auto const& f : files) {
				fmt::print("\t\t<Input>{}</Input>\n", f);
			}
			fmt::print("\t</Inputs>\n");
		}
		fmt::print("</CoverageTool>\n");
	}

	void printJSON(std::vector<std::string> const& context = std::vector<std::string>()) const {
		verify();
		do {
			struct foo {};
			foo f;
			fmt::print("{}\n", boost::core::demangle(typeid(f).name()));
		} while (false);
		do {
			struct foo {};
			foo f;
			fmt::print("{}\n", boost::core::demangle(typeid(f).name()));
		} while (false);
		auto contexts = fromStrings(context);
		const ICodeProbe* prev = nullptr;
		for (auto probe : codeProbes) {
			auto p = probe.second;
			if (!contexts.empty()) {
				bool print = false;
				for (auto c : contexts) {
					print = print || p->expectInContext(c);
				}
				if (!print) {
					continue;
				}
			}
			if (prev == nullptr || *prev != *probe.second) {
				fmt::print(
				    "{{ \"File\": \"{}\", \"Line\": {}, \"Comment\": \"{}\", \"Condition\": \"{}\", \"Function\": "
				    "\"{}\" }}\n",
				    probe.first.file,
				    p->line(),
				    p->comment(),
				    p->condition(),
				    p->function());
			}
			prev = probe.second;
		}
	}
};

} // namespace

std::string functionNameFromInnerType(const char* name) {
	auto res = boost::core::demangle(name);
	auto pos = res.find_last_of(':');
	ASSERT(pos != res.npos);
	return res.substr(0, pos - 1);
}

void registerProbe(const ICodeProbe& probe) {
	CodeProbes::instance().add(&probe);
}

void traceMissedProbes(Optional<ExecutionContext> context) {
	CodeProbes::instance().traceMissedProbes(context);
}

ICodeProbe::~ICodeProbe() {}

bool ICodeProbe::operator==(const ICodeProbe& other) const {
	return strcmp(filename(), other.filename()) == 0 && line() == other.line();
}

bool ICodeProbe::operator!=(const ICodeProbe& other) const {
	return !(*this == other);
}

void ICodeProbe::printProbesXML() {
	CodeProbes::instance().printXML();
}

void ICodeProbe::printProbesJSON(std::vector<std::string> const& ctxs) {
	CodeProbes::instance().printJSON(ctxs);
}

// annotations
namespace assert {

bool NoSim::operator()(ICodeProbe* self) const {
	return !g_network->isSimulated();
}

bool SimOnly::operator()(ICodeProbe* self) const {
	return g_network->isSimulated();
}

} // namespace assert

} // namespace probe
