#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

int main(int argc, char* argv[]) {
	fdb_select_api_version(630);
	return 0;
}
