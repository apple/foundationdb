/*
 * BuildIdempotencyIdMutations.h
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

#ifndef FDBCLIENT_BUILD_IDEMPOTENCY_ID_MUTATIONS_H
#define FDBCLIENT_BUILD_IDEMPOTENCY_ID_MUTATIONS_H

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/IdempotencyId.actor.h"

#pragma once

// Iterate through trs looking for idempotency ids for committed transactions. Call onKvReady for each constructed key
// value pair.
template <class OnKVReady>
void buildIdempotencyIdMutations(const std::vector<CommitTransactionRequest>& trs,
                                 IdempotencyIdKVBuilder& idempotencyKVBuilder,
                                 Version commitVersion,
                                 const std::vector<uint8_t>& committed,
                                 uint8_t committedValue,
                                 bool locked,
                                 const OnKVReady& onKvReady) {
	idempotencyKVBuilder.setCommitVersion(commitVersion);
	for (int h = 0; h < trs.size(); h += 256) {
		int end = std::min<int>(trs.size() - h, 256);
		for (int l = 0; l < end; ++l) {
			uint16_t batchIndex = h + l;
			if ((committed[batchIndex] == committedValue && (!locked || trs[batchIndex].isLockAware()))) {
				const auto& idempotency_id = trs[batchIndex].idempotencyId;
				if (idempotency_id.valid()) {
					idempotencyKVBuilder.add(idempotency_id, batchIndex);
				}
			}
		}
		Optional<KeyValue> kv = idempotencyKVBuilder.buildAndClear();
		if (kv.present()) {
			onKvReady(kv.get());
		}
	}
}

#endif