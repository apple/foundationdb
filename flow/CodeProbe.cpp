/*
 * CodeProbe.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include <boost/unordered_map.hpp>
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

std::string_view normalizePath(const char* path) {
	std::string_view srcBase(FDB_SOURCE_DIR);
	std::string_view binBase(FDB_SOURCE_DIR);
	std::string_view filename(path);
	if (srcBase.size() < filename.size() && filename.substr(0, srcBase.size()) == srcBase) {
		filename.remove_prefix(srcBase.size());
	} else if (binBase.size() < filename.size() && filename.substr(0, binBase.size()) == binBase) {
		filename.remove_prefix(binBase.size());
	}
	if (filename[0] == '/') {
		filename.remove_prefix(1);
	}
	return filename;
}

struct CodeProbes {
	struct Location {
		std::string_view file;
		unsigned line;
		Location(std::string_view file, unsigned line) : file(file), line(line) {}
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

	void traceMissedProbes(Optional<ExecutionContext> context) const;

	void add(ICodeProbe const* probe) {
		Location loc(probe->filename(), probe->line());
		codeProbes.emplace(loc, probe);
	}

	static CodeProbes& instance() {
		static CodeProbes probes;
		return probes;
	}

	void verify() const {
		std::map<std::pair<std::string_view, std::string_view>, ICodeProbe const*> comments;
		for (const auto& [probeLocation, probe] : codeProbes) {
			auto file = probeLocation.file;
			auto comment = probe->comment();
			auto commentEntry = std::make_pair(file, std::string_view(comment));
			ASSERT(file == probe->filename());
			auto iter = comments.find(commentEntry);
			if (iter != comments.end() && probe->line() != iter->second->line()) {
				fmt::print("ERROR ({}:{}): {} isn't unique in file {}. Previously seen here: {}:{}\n",
				           probeLocation.file,
				           probeLocation.line,
				           iter->first.second,
				           probe->filename(),
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
				comments.emplace(commentEntry, probe);
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
			std::vector<std::string_view> files;
			fmt::print("\t<CoverageCases>\n");
			for (const auto& [probeLocation, probe] : codeProbes) {
				files.push_back(probeLocation.file);
				fmt::print("\t\t<Case File=\"{}\" Line=\"{}\" Comment=\"{}\" Condition=\"{}\"/>\n",
				           probeLocation.file,
				           probeLocation.line,
				           probe->comment(),
				           probe->condition());
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
		for (const auto& [_probeLocation, probe] : codeProbes) {
			if (!contexts.empty()) {
				bool print = false;
				for (auto c : contexts) {
					print = print || probe->expectInContext(c);
				}
				if (!print) {
					continue;
				}
			}
			if (prev == nullptr || *prev != *probe) {
				fmt::print(
				    "{{ \"File\": \"{}\", \"Line\": {}, \"Comment\": \"{}\", \"Condition\": \"{}\", \"Function\": "
				    "\"{}\" }}\n",
				    probe->filename(),
				    probe->line(),
				    probe->comment(),
				    probe->condition(),
				    probe->function());
			}
			prev = probe;
		}
	}
};

size_t hash_value(CodeProbes::Location const& location) {
	size_t seed = 0;
	boost::hash_combine(seed, location.file);
	boost::hash_combine(seed, location.line);
	return seed;
}

void CodeProbes::traceMissedProbes(Optional<ExecutionContext> context) const {
	boost::unordered_map<Location, bool> locations;
	for (const auto& [probeLocation, probe] : codeProbes) {
		decltype(locations.begin()) iter;
		std::tie(iter, std::ignore) = locations.emplace(probeLocation, false);
		iter->second = iter->second || probe->wasHit();
	}
	for (const auto& [loc, probe] : codeProbes) {
		auto iter = locations.find(loc);
		ASSERT(iter != locations.end());
		if (!iter->second && probe->shouldTrace()) {
			iter->second = true;
			probe->trace(false);
		}
	}
}

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

ICodeProbe::~ICodeProbe() = default;

bool ICodeProbe::operator==(const ICodeProbe& other) const {
	return filename() == other.filename() && line() == other.line();
}

bool ICodeProbe::operator!=(const ICodeProbe& other) const {
	return !(*this == other);
}

std::string_view ICodeProbe::filename() const {
	return normalizePath(filePath());
}

void ICodeProbe::printProbesXML() {
	CodeProbes::instance().printXML();
}

void ICodeProbe::printProbesJSON(std::vector<std::string> const& ctxs) {
	CodeProbes::instance().printJSON(ctxs);
}

// annotations
namespace assert {

bool NoSim::operator()(ICodeProbe const* self) const {
	return !g_network->isSimulated();
}

bool SimOnly::operator()(ICodeProbe const* self) const {
	return g_network->isSimulated();
}

} // namespace assert

} // namespace probe
