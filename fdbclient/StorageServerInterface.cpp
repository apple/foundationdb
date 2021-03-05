/*
 * StorageServerInterface.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/StorageServerInterface.h"

// Includes template specializations for all tss comparisons on storage server types.
// New StorageServerInterface reply types must be added here or it won't compile.

template<>
bool TSSComparison::tssCompare(GetValueReply src, GetValueReply tss) {
	if (src.value.present() != tss.value.present() || (src.value.present() && src.value.get() != tss.value.get())) {
		// printf("GetValue mismatch for key %s: src=%s, tss=%s\n", req.key.toString().c_str(), src.get().present() ? src.get().value.toString().c_str() : "missing", tss.get.present() ? tss.get().value().toString().c_str() : "missing");
		printf("GetValue mismatch: src=%s, tss=%s\n", src.value.present() ? src.value.get().toString().c_str() : "missing", tss.value.present() ? tss.value.get().toString().c_str() : "missing");
		return false;
	}
    // printf("tss GetValueReply matched!\n");
	return true;
}

template<>
bool TSSComparison::tssCompare(GetKeyReply src, GetKeyReply tss) {
	if (src.sel != tss.sel) {
        // TODO print something useful
        printf("GetKey mismatch\n");
		return false;
	}
    // printf("tss GetKeyReply matched!\n");
	return true;
}

template<>
bool TSSComparison::tssCompare(GetKeyValuesReply src, GetKeyValuesReply tss) {
	if (src.more != tss.more || src.cached != tss.cached || src.data != tss.data) {
        // TODO print something useful
        printf("GetKeyValues mismatch\n");
		return false;
	}
    // printf("tss GetKeyValues matched!\n");
	return true;
}

template<>
bool TSSComparison::tssCompare(WatchValueReply src, WatchValueReply tss) {
    // TODO should this check that both returned the same version?
	return true;
}


// no-op template specializations for metrics replies
template<>
bool TSSComparison::tssCompare(StorageMetrics src, StorageMetrics tss) {
	return true;
}

template<>
bool TSSComparison::tssCompare(SplitMetricsReply src, SplitMetricsReply tss) {
	return true;
}

template<>
bool TSSComparison::tssCompare(ReadHotSubRangeReply src, ReadHotSubRangeReply tss) {
	return true;
}
