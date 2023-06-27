/*
 * TagThrottle.actor.cpp
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

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/TagThrottle.h"
#include "fdbclient/Tuple.h"

#include "flow/actorcompiler.h" // has to be last include

KeyRangeRef const tagQuotaKeys = KeyRangeRef("\xff/tagQuota/"_sr, "\xff/tagQuota0"_sr);
KeyRef const tagQuotaPrefix = tagQuotaKeys.begin;

Key ThrottleApi::getTagQuotaKey(TransactionTagRef tag) {
	return tag.withPrefix(tagQuotaPrefix);
}

bool ThrottleApi::ThroughputQuotaValue::isValid() const {
	return reservedQuota <= totalQuota && reservedQuota >= 0;
}

Tuple ThrottleApi::ThroughputQuotaValue::pack() const {
	return Tuple::makeTuple(reservedQuota, totalQuota);
}

ThrottleApi::ThroughputQuotaValue ThrottleApi::ThroughputQuotaValue::unpack(Tuple const& tuple) {
	if (tuple.size() != 2) {
		throw invalid_throttle_quota_value();
	}
	ThroughputQuotaValue result;
	try {
		result.reservedQuota = tuple.getInt(0);
		result.totalQuota = tuple.getInt(1);
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "ThroughputQuotaValueFailedToDeserialize").error(e);
		throw invalid_throttle_quota_value();
	}
	if (!result.isValid()) {
		TraceEvent(SevWarnAlways, "ThrougputQuotaValueInvalidQuotas")
		    .detail("ReservedQuota", result.reservedQuota)
		    .detail("TotalQuota", result.totalQuota);
		throw invalid_throttle_quota_value();
	}
	return result;
}

bool ThrottleApi::ThroughputQuotaValue::operator==(ThrottleApi::ThroughputQuotaValue const& rhs) const {
	return reservedQuota == rhs.reservedQuota && totalQuota == rhs.totalQuota;
}
