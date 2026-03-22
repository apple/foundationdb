/*
 * HighContentionPrefixAllocator.h
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

#pragma once

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Subspace.h"
#include "flow/UnitTest.h"

class HighContentionPrefixAllocator {
public:
	explicit HighContentionPrefixAllocator(Subspace subspace) : counters(subspace.get(0)), recent(subspace.get(1)) {}

	template <class TransactionT>
	Future<Standalone<StringRef>> allocate(Reference<TransactionT> tr) {
		return allocate(this, tr);
	}

	static int64_t windowSize(int64_t start) {
		if (start < 255) {
			return 64;
		}
		if (start < 65535) {
			return 1024;
		}

		return 8192;
	}

private:
	Subspace counters;
	Subspace recent;

	template <class TransactionT>
	Future<Standalone<StringRef>> allocate(HighContentionPrefixAllocator* self, Reference<TransactionT> tr) {
		int64_t start = 0;
		int64_t window = 0;

		while (true) {
			typename TransactionT::template FutureT<RangeResult> rangeFuture =
			    tr->getRange(self->counters.range(), 1, Snapshot::True, Reverse::True);
			RangeResult range = co_await safeThreadFutureToFuture(rangeFuture);

			if (range.size() > 0) {
				start = self->counters.unpack(range[0].key).getInt(0);
			}

			bool windowAdvanced = false;
			while (true) {
				// if thread safety is needed, this should be locked {
				if (windowAdvanced) {
					tr->clear(KeyRangeRef(self->counters.key(), self->counters.get(start).key()));
					tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
					tr->clear(KeyRangeRef(self->recent.key(), self->recent.get(start).key()));
				}

				int64_t inc = 1;
				tr->atomicOp(self->counters.get(start).key(), StringRef((uint8_t*)&inc, 8), MutationRef::AddValue);

				typename TransactionT::template FutureT<Optional<Value>> countFuture =
				    tr->get(self->counters.get(start).key(), Snapshot::True);
				// }

				Optional<Value> countValue = co_await safeThreadFutureToFuture(countFuture);

				int64_t count = 0;
				if (countValue.present()) {
					if (countValue.get().size() != 8) {
						throw invalid_directory_layer_metadata();
					}
					count = *(int64_t*)countValue.get().begin();
				}

				window = HighContentionPrefixAllocator::windowSize(start);
				if (count * 2 < window) {
					break;
				}

				start += window;
				windowAdvanced = true;
			}

			while (true) {
				int64_t candidate = deterministicRandom()->randomInt(start, start + window);

				// if thread safety is needed, this should be locked {
				typename TransactionT::template FutureT<RangeResult> latestCounterFuture =
				    tr->getRange(self->counters.range(), 1, Snapshot::True, Reverse::True);
				typename TransactionT::template FutureT<Optional<Value>> candidateValueFuture =
				    tr->get(self->recent.get(candidate).key());
				tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
				tr->set(self->recent.get(candidate).key(), ValueRef());
				// }

				co_await (success(safeThreadFutureToFuture(latestCounterFuture)) &&
				          success(safeThreadFutureToFuture(candidateValueFuture)));

				int64_t currentWindowStart = 0;
				if (latestCounterFuture.get().size() > 0) {
					currentWindowStart = self->counters.unpack(latestCounterFuture.get()[0].key).getInt(0);
				}

				if (currentWindowStart > start) {
					break;
				}

				if (!candidateValueFuture.get().present()) {
					tr->addWriteConflictRange(singleKeyRange(self->recent.get(candidate).key()));
					co_return Tuple::makeTuple(candidate).pack();
				}
			}
		}
	}
};
