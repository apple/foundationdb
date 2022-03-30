/*
 * SetClassCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "contrib/fmt-8.1.1/include/fmt/format.h"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> printProcessClass(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// Hold the reference to the memory
			state ThreadFuture<RangeResult> classTypeFuture =
			    tr->getRange(fdb_cli::processClassTypeSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			state ThreadFuture<RangeResult> classSourceFuture =
			    tr->getRange(fdb_cli::processClassSourceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			wait(success(safeThreadFutureToFuture(classSourceFuture)) &&
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
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
};

ACTOR Future<bool> setProcessClass(Reference<IDatabase> db, KeyRef network_address, KeyRef class_type) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			state ThreadFuture<Optional<Value>> result =
			    tr->get(network_address.withPrefix(fdb_cli::processClassTypeSpecialKeyRange.begin));
			Optional<Value> val = wait(safeThreadFutureToFuture(result));
			if (!val.present()) {
				printf("No matching addresses found\n");
				return false;
			}
			tr->set(network_address.withPrefix(fdb_cli::processClassTypeSpecialKeyRange.begin), class_type);
			wait(safeThreadFutureToFuture(tr->commit()));
			return true;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_special_keys_api_failure) {
				std::string errorMsgStr = wait(fdb_cli::getSpecialKeysFailureErrorMessage(tr));
				// error message already has \n at the end
				fprintf(stderr, "%s", errorMsgStr.c_str());
				return false;
			}
			wait(safeThreadFutureToFuture(tr->onError(err)));
		}
	}
}

} // namespace

namespace fdb_cli {

const KeyRangeRef processClassSourceSpecialKeyRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/configuration/process/class_source/"),
                LiteralStringRef("\xff\xff/configuration/process/class_source0"));

const KeyRangeRef processClassTypeSpecialKeyRange =
    KeyRangeRef(LiteralStringRef("\xff\xff/configuration/process/class_type/"),
                LiteralStringRef("\xff\xff/configuration/process/class_type0"));

ACTOR Future<bool> setClassCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 3 && tokens.size() != 1) {
		printUsage(tokens[0]);
		return false;
	} else if (tokens.size() == 1) {
		wait(printProcessClass(db));
	} else {
		bool successful = wait(setProcessClass(db, tokens[1], tokens[2]));
		return successful;
	}
	return true;
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
                "`coordinator', `ratekeeper', `storage_cache', `backup', and `default'."));

} // namespace fdb_cli
