#define FDB_API_VERSION 610
#include <foundationdb/fdb_c.h>

int main(int argc, char* argv[]) {
	fdb_select_api_version(610);
	return 0;
}
