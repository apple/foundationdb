#define FDB_API_VERSION 620
#include <foundationdb/fdb_c.h>

int main(int argc, char* argv[]) {
	fdb_select_api_version(620);
	return 0;
}
