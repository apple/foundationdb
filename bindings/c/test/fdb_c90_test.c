#define FDB_API_VERSION 720
#include <foundationdb/fdb_c.h>

int main(int argc, char* argv[]) {
	(void)argc;
	(void)argv;
	fdb_select_api_version(720);
	return 0;
}
