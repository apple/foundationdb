/*
 * HighContentionPrefixAllocator.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_HIGHCONTENTIONPREFIXALLOCATOR_ACTOR_G_H)
#define FDBCLIENT_HIGHCONTENTIONPREFIXALLOCATOR_ACTOR_G_H
#include "fdbclient/HighContentionPrefixAllocator.actor.g.h"
#elif !defined(FDBCLIENT_HIGHCONTENTIONPREFIXALLOCATOR_ACTOR_H)
#define FDBCLIENT_HIGHCONTENTIONPREFIXALLOCATOR_ACTOR_H

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Subspace.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class HighContentionPrefixAllocator {
public:
	HighContentionPrefixAllocator(Subspace subspace) : counters(subspace.get(0)), recent(subspace.get(1)) {}

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

	ACTOR template <class TransactionT>
	Future<Standalone<StringRef>> allocate(HighContentionPrefixAllocator* self, Reference<TransactionT> tr) {
		state int64_t start = 0;
		state int64_t window = 0;

		loop {
			RangeResult range =
			    wait(safeThreadFutureToFuture(tr->getRange(self->counters.range(), 1, Snapshot::True, Reverse::True)));

			if (range.size() > 0) {
				start = self->counters.unpack(range[0].key).getInt(0);
			}

			state bool windowAdvanced = false;
			loop {
				// if thread safety is needed, this should be locked {
				if (windowAdvanced) {
					tr->clear(KeyRangeRef(self->counters.key(), self->counters.get(start).key()));
					tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
					tr->clear(KeyRangeRef(self->recent.key(), self->recent.get(start).key()));
				}

				int64_t inc = 1;
				tr->atomicOp(self->counters.get(start).key(), StringRef((uint8_t*)&inc, 8), MutationRef::AddValue);

				// }

				Optional<Value> countValue =
				    wait(safeThreadFutureToFuture(tr->get(self->counters.get(start).key(), Snapshot::True)));

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

			loop {
				state int64_t candidate = deterministicRandom()->randomInt(start, start + window);

				// if thread safety is needed, this should be locked {
				state typename TransactionT::template FutureT<RangeResult> latestCounterFuture =
				    tr->getRange(self->counters.range(), 1, Snapshot::True, Reverse::True);
				state typename TransactionT::template FutureT<Optional<Value>> candidateValueFuture =
				    tr->get(self->recent.get(candidate).key());
				tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
				tr->set(self->recent.get(candidate).key(), ValueRef());
				// }

				wait(success(safeThreadFutureToFuture(latestCounterFuture)) &&
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
					return Tuple::makeTuple(candidate).pack();
				}
			}
		}
	}
};

#include "flow/unactorcompiler.h"
#endif