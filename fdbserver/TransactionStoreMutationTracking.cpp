/*
 * TxnMutationTracking.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/TransactionStoreMutationTracking.h"
#if defined(FDB_CLEAN_BUILD) && DEBUG_TRANSACTION_STATE_STORE_ENABLED
#error "You cannot use transaction store mutation tracking in a clean/release build."
#endif

// If DEBUG_TRANSACTION_STATE_STORE_ENABLED is set, tracking events will be logged for the
// keys in debugKeys and the ranges in debugRanges.
// Each entry is a pair of (label, keyOrRange) and the Label will be attached to the
// TransactionStoreMutationTracking TraceEvent for easier searching/recognition.

static std::vector<std::pair<const char*, KeyRef>> debugKeys = { { "SomeKey",
	                                                               "\xff/serverList/\x09I\x8c\xc7\xdd"_sr } };
static std::vector<std::pair<const char*, KeyRangeRef>> debugRanges = {
	{ "SomeRange", { "\xff/serverList/\x09I\x8c\xc7\xdd"_sr, "\xff/serverList/\x09I\x8c\xc7\xdd\xff"_sr } }
};

TraceEvent transactionStoreDebugMutationEnabled(const char* context,
                                                const std::string version,
                                                StringRef const& mutation,
                                                UID id) {
	const char* label = nullptr;

	for (auto& labelKey : debugKeys) {
		if (mutation == labelKey.second) {
			label = labelKey.first;
			break;
		}
	}

	for (auto& labelRange : debugRanges) {
		if (labelRange.second.contains(mutation)) {
			label = labelRange.first;
			break;
		}
	}

	if (label != nullptr) {
		TraceEvent event("TransactionStoreMutationTracking", id);
		event.detail("Label", label).detail("At", context).detail("Version", version).detail("Mutation", mutation);
		return event;
	}

	return TraceEvent();
}

#if DEBUG_TRANSACTION_STATE_STORE_ENABLED
TraceEvent transactionStoreDebugMutation(const char* context,
                                         const std::string version,
                                         StringRef const& mutation,
                                         UID id) {
	return transactionStoreDebugMutationEnabled(context, version, mutation, id);
}
#else
TraceEvent transactionStoreDebugMutation(const char* context, Version version, StringRef const& mutation, UID id) {
	return TraceEvent();
}
#endif
