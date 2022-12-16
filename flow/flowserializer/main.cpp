#include <string_view>
#include <cstdio>
#include <iostream>
#include <fmt/format.h>

#include "Compiler.h"

using namespace std::string_view_literals;
using namespace std::string_literals;

int main(int argc, const char* argv[]) {
	if (argc < 2) {
		return 1;
	}

	std::vector<std::string> includePaths;
	std::string sourceDir;
	std::string headerDir;

	int i = 1;
	auto expectValue = [argv, &i, argc]() {
		if (i >= argc) {
			fmt::print(stderr, "-I: expected path afterwards, found EOL\n");
			std::exit(1);
		} else if (argv[i][0] == '-') {
			fmt::print(stderr, "-I: expected path afterwards, got option\n");
			std::exit(1);
		}
	};
	for (; i < argc; ++i) {
		if (argv[i] == "-I"sv) {
			++i;
			expectValue();
			includePaths.emplace_back(argv[i]);
		} else if (argv[i] == "-s"sv) {
			++i;
			expectValue();
			sourceDir = argv[i];
		} else if (argv[i] == "-i"sv) {
			++i;
			expectValue();
			headerDir = argv[i];
		} else if (argv[i] == "-h"sv || argv[i] == "--help"sv) {
			fmt::print("Usage: {} [-I include-path]* [-s sourceDir] [-i headerDir] [-h] [--] (idl_file.fbs)+", argv[0]);
			return 0;
		} else if (argv[i] == "--"sv) {
			++i;
			break;
		} else if (argv[i][0] != '-') {
			break;
		} else {
			fmt::print(stderr, "Unexpected option: {}\n", argv[i]);
		}
	}

	try {
		flowserializer::Compiler compiler(includePaths);
		for (; i < argc; ++i) {
			compiler.compile(argv[i]);
		}
		compiler.generateCode(headerDir, sourceDir);
		compiler.describeTables();
	} catch (flowserializer::Error& e) {
		std::cerr << fmt::format("ERROR: {}", e.what());
	}

	//	for (int i = 1; i < argc; ++i) {
	//		fmt::print("Parsing file: {}\n", argv[i]);
	//		auto expr = flatbuffers::expression::ExpressionTree::fromFile(argv[i]);
	//		flatbuffers::emit(std::cout, expr);
	//	}
}