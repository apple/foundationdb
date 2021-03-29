#include "fdbcli/fdbcli.h"
#include "flow/Arena.h"

namespace fdb_cli {

bool tokencmp(StringRef token, const char* command) {
	if (token.size() != strlen(command))
		return false;

	return !memcmp(token.begin(), command, token.size());
}

void printUsage(StringRef command) {
	const auto& helpMap = CommandFactory::commands();
	auto i = helpMap.find(command.toString());
	if (i != helpMap.end())
		printf("Usage: %s\n", i->second.usage.c_str());
	else
		fprintf(stderr, "ERROR: Unknown command `%s'\n", command.toString().c_str());
}

}
