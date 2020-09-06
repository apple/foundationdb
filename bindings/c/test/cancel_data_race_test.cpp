#define FDB_API_VERSION 700
#include "foundationdb/fdb_c.h"

#include <thread>
#include <iostream>

using namespace std::chrono_literals;

void fdb_check(fdb_error_t e) {
	if (e) {
		std::cout << fdb_get_error(e) << std::endl;
		std::abort();
	}
}
int main(int argc, char** argv) {
	fdb_check(fdb_select_api_version(700));
	fdb_check(fdb_setup_network());
	std::thread network_thread{ &fdb_run_network };

	FDBDatabase* db;
	fdb_check(fdb_create_database(argv[1], &db));

	for (int i = 0; i < 1000; ++i) {
		std::cout << "Attempt " << i << "\n";
		FDBTransaction* tr;
		fdb_check(fdb_database_create_transaction(db, &tr));
		FDBFuture* watchFuture = fdb_transaction_watch(tr, (const uint8_t*)"foo", 3);
		fdb_transaction_set(tr, (const uint8_t*)"foo", 3, (const uint8_t*)"", 0);
		std::this_thread::sleep_for(2.5ms); // Tune this until you start seeing a mix of success and cancelled.
		fdb_future_cancel(watchFuture);
		fdb_check(fdb_future_block_until_ready(watchFuture));
		std::cout << fdb_get_error(fdb_future_get_error(watchFuture)) << std::endl;
		fdb_future_destroy(watchFuture);
		fdb_transaction_destroy(tr);
	}

	fdb_check(fdb_stop_network());
	network_thread.join();
}
