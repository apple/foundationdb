#include "flow/TestProbe.h"
#include "flow/Arena.h"

#include <map>
#include <fmt/format.h>

namespace probe {

namespace {

struct CodeProbes {
	using LineMap = std::map<unsigned, ICodeProbe*>;
	std::map<StringRef, LineMap> codeProbes;

	void add(const char* file, unsigned line, ICodeProbe* probe) {
		auto srcBase = LiteralStringRef(FDB_SOURCE_DIR);
		auto binBase = LiteralStringRef(FDB_SOURCE_DIR);
		StringRef filename(reinterpret_cast<uint8_t const*>(file), strlen(file));
		StringRef f = filename;
		if (filename.startsWith(srcBase)) {
			f = filename.removePrefix(srcBase);
		} else if (filename.startsWith(binBase)) {
			f = filename.removePrefix(binBase);
		}
		if (f[0] == '/') {
			f = f.removePrefix("/"_sr);
		} else if (f[0] == '\\') {
			f = f.removePrefix("/"_sr);
		}
		codeProbes[f][line] = probe;
	}

	static CodeProbes& instance() {
		static CodeProbes probes;
		return probes;
	}

	void verify() const {
		std::map<StringRef, ICodeProbe*> comments;
		for (auto const& files : codeProbes) {
			for (auto const& probe : files.second) {
				auto cmt = probe.second->comment();
				auto res =
				    comments.emplace(StringRef(reinterpret_cast<uint8_t const*>(cmt), strlen(cmt)), probe.second);
				if (!res.second) {
					fmt::print(stderr,
					           "ERROR ({}:{}): Comment \"{}\" is a duplicate. First appeared here: {}:{}\n",
					           probe.second->filename(),
					           probe.second->line(),
					           probe.second->comment(),
					           res.first->second->filename(),
					           res.first->second->line());
				}
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
			for (auto const& fileMap : codeProbes) {
				files.push_back(fileMap.first);
				for (auto const& probe : fileMap.second) {
					fmt::print("\t\t<Case File=\"{}\" Line=\"{}\" Comment=\"{}\" Condition=\"{}\"/>\n",
					           fileMap.first,
					           probe.first,
					           probe.second->comment(),
					           probe.second->condition());
				}
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

	void printJSON() const {
		verify();
		fmt::print("{{\n");
		bool first = true;
		for (auto const& lineMap : codeProbes) {
			auto file = lineMap.first;
			for (auto const& probe : lineMap.second) {
				auto p = probe.second;
				if (!first) {
					fmt::print(",\n");
				}
				first = false;
				fmt::print("\t{{ File: \"{}\", Line: {}, Comment: \"{}\", Condition: \"{}\" }}",
				           file,
				           p->line(),
				           p->comment(),
				           p->condition());
			}
		}
		if (!first) {
			fmt::print("\n");
		}
		fmt::print("}}\n");
	}
};

} // namespace

ICodeProbe::~ICodeProbe() {}

ICodeProbe::ICodeProbe(const char* file, unsigned line) {
	CodeProbes::instance().add(file, line, this);
}

void ICodeProbe::printProbesXML() {
	CodeProbes::instance().printXML();
}

void ICodeProbe::printProbesJSON() {
	CodeProbes::instance().printJSON();
}
} // namespace probe
