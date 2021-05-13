#define FDB_API_VERSION 710
#include <foundationdb/fdb_c.h>

int main(int argc, char* argv[]) {
	(void)argc;
	(void)argv;
	fdb_select_api_version(710);
	return 0;
}
