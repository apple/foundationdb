/*
 * AccumulativeChecksumUtil.cpp
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

#include "fdbserver/AccumulativeChecksumUtil.h"
#include "fdbserver/Knobs.h"

uint16_t getCommitProxyAccumulativeChecksumIndex(uint16_t commitProxyIndex) {
	// We leave flexibility in acs index generated from different components
	// Acs index ends with 1 indicates the mutation is from a commit proxy
	return commitProxyIndex * 10 + 1;
}

bool validateAccumulativeChecksumIndexAtStorageServer(MutationRef m) {
	if (m.checksum.present() && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM &&
	    (!m.accumulativeChecksumIndex.present())) {
		TraceEvent(SevError, "ACSIndexNotPresent").detail("Mutation", m);
		return false;
	} else if (m.checksum.present() && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM &&
	           m.accumulativeChecksumIndex.present() &&
	           m.accumulativeChecksumIndex.get() == invalidAccumulativeChecksumIndex) {
		TraceEvent(SevError, "ACSIndexNotSet").detail("Mutation", m);
		return false;
	}
	return true;
}
