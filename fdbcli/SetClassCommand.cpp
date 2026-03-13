/*
 * SetClassCommand.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fmt/format.h"

#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace {

Future<Void> printProcessClass(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			// Hold the reference to the memory
			ThreadFuture<RangeResult> classTypeFuture =
			    tr->getRange(fdb_cli::processClassTypeSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			ThreadFuture<RangeResult> classSourceFuture =
			    tr->getRange(fdb_cli::processClassSourceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			co_await (success(safeThreadFutureToFuture(classSourceFuture)) &&
			          success(safeThreadFutureToFuture(classTypeFuture)));
			RangeResult processTypeList = classTypeFuture.get();
			RangeResult processSourceList = classSourceFuture.get();
			ASSERT(processSourceList.size() == processTypeList.size());
			if (!processTypeList.size())
				printf("No processes are registered in the database.\n");
			fmt::print("There are currently {} processes in the database:\n", processTypeList.size());
			for (int index = 0; index < processTypeList.size(); index++) {
				std::string address =
				    processTypeList[index].key.removePrefix(fdb_cli::processClassTypeSpecialKeyRange.begin).toString();
				// check the addresses are the same in each list
				std::string addressFromSourceList =
				    processSourceList[index]
				        .key.removePrefix(fdb_cli::processClassSourceSpecialKeyRange.begin)
				        .toString();
				ASSERT(address == addressFromSourceList);
				printf("  %s: %s (%s)\n",
				       address.c_str(),
				       processTypeList[index].value.toString().c_str(),
				       processSourceList[index].value.toString().c_str());
			}
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
};

Future<bool> setProcessClass(Reference<IDatabase> db, KeyRef network_address, KeyRef class_type) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		Error err;
		try {
			ThreadFuture<Optional<Value>> result =
			    tr->get(network_address.withPrefix(fdb_cli::processClassTypeSpecialKeyRange.begin));
			Optional<Value> val = co_await safeThreadFutureToFuture(result);
			if (!val.present()) {
				printf("No matching addresses found\n");
				co_return false;
			}
			tr->set(network_address.withPrefix(fdb_cli::processClassTypeSpecialKeyRange.begin), class_type);
			co_await safeThreadFutureToFuture(tr->commit());
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_special_keys_api_failure) {
			std::string errorMsgStr = co_await fdb_cli::getSpecialKeysFailureErrorMessage(tr);
			// error message already has \n at the end
			fprintf(stderr, "%s", errorMsgStr.c_str());
			co_return false;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

} // namespace

namespace fdb_cli {

const KeyRangeRef processClassSourceSpecialKeyRange =
    KeyRangeRef("\xff\xff/configuration/process/class_source/"_sr, "\xff\xff/configuration/process/class_source0"_sr);

const KeyRangeRef processClassTypeSpecialKeyRange =
    KeyRangeRef("\xff\xff/configuration/process/class_type/"_sr, "\xff\xff/configuration/process/class_type0"_sr);

Future<bool> setClassCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 3 && tokens.size() != 1) {
		printUsage(tokens[0]);
		co_return false;
	} else if (tokens.size() == 1) {
		co_await printProcessClass(db);
	} else {
		bool successful = co_await setProcessClass(db, tokens[1], tokens[2]);
		co_return successful;
	}
	co_return true;
}

CommandFactory setClassFactory(
    "setclass",
    CommandHelp("setclass [<ADDRESS> <CLASS>]",
                "change the class of a process",
                "If no address and class are specified, lists the classes of all servers.\n\nSetting the class to "
                "`default' resets the process class to the class specified on the command line. The available "
                "classes are `unset', `storage', `transaction', `resolution', `commit_proxy', `grv_proxy', "
                "`master', `test', "
                "`stateless', `log', `router', `cluster_controller', `fast_restore', `data_distributor', "
                "`coordinator', `ratekeeper', `backup', and `default'."));

} // namespace fdb_cli
