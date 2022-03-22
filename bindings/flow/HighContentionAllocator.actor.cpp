/*
 * HighContentionAllocator.actor.cpp
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

#include "HighContentionAllocator.h"

namespace FDB {
ACTOR Future<Standalone<StringRef>> _allocate(Reference<Transaction> tr, Subspace counters, Subspace recent) {
	state int64_t start = 0;
	state int64_t window = 0;

	loop {
		FDBStandalone<RangeResultRef> range = wait(tr->getRange(counters.range(), 1, true, true));

		if (range.size() > 0) {
			start = counters.unpack(range[0].key).getInt(0);
		}

		state bool windowAdvanced = false;
		loop {
			// if thread safety is needed, this should be locked {
			if (windowAdvanced) {
				tr->clear(KeyRangeRef(counters.key(), counters.get(start).key()));
				tr->setOption(FDBTransactionOption::FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
				tr->clear(KeyRangeRef(recent.key(), recent.get(start).key()));
			}

			int64_t inc = 1;
			tr->atomicOp(counters.get(start).key(), StringRef((uint8_t*)&inc, 8), FDB_MUTATION_TYPE_ADD);
			Future<Optional<FDBStandalone<ValueRef>>> countFuture = tr->get(counters.get(start).key(), true);
			// }

			Optional<FDBStandalone<ValueRef>> countValue = wait(countFuture);

			int64_t count = 0;
			if (countValue.present()) {
				if (countValue.get().size() != 8) {
					throw invalid_directory_layer_metadata();
				}
				count = *(int64_t*)countValue.get().begin();
			}

			window = HighContentionAllocator::windowSize(start);
			if (count * 2 < window) {
				break;
			}

			start += window;
			windowAdvanced = true;
		}

		loop {
			state int64_t candidate = deterministicRandom()->randomInt(start, start + window);

			// if thread safety is needed, this should be locked {
			state Future<FDBStandalone<RangeResultRef>> latestCounter = tr->getRange(counters.range(), 1, true, true);
			state Future<Optional<FDBStandalone<ValueRef>>> candidateValue = tr->get(recent.get(candidate).key());
			tr->setOption(FDBTransactionOption::FDB_TR_OPTION_NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
			tr->set(recent.get(candidate).key(), ValueRef());
			// }

			wait(success(latestCounter) && success(candidateValue));
			int64_t currentWindowStart = 0;
			if (latestCounter.get().size() > 0) {
				currentWindowStart = counters.unpack(latestCounter.get()[0].key).getInt(0);
			}

			if (currentWindowStart > start) {
				break;
			}

			if (!candidateValue.get().present()) {
				tr->addWriteConflictKey(recent.get(candidate).key());
				return Tuple().append(candidate).pack();
			}
		}
	}
}

Future<Standalone<StringRef>> HighContentionAllocator::allocate(Reference<Transaction> const& tr) const {
	return _allocate(tr, counters, recent);
}

int64_t HighContentionAllocator::windowSize(int64_t start) {
	if (start < 255) {
		return 64;
	}
	if (start < 65535) {
		return 1024;
	}

	return 8192;
}
} // namespace FDB
