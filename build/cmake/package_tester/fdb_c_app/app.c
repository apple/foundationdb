#define FDB_API_VERSION 700
#include <foundationdb/fdb_c.h>

int main(int argc, char* argv[]) {
	fdb_select_api_version(700);
	return 0;
}
