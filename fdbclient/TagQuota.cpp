/*
 * TagQuota.cpp
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

#include "fdbclient/TagQuota.h"
#include "fdbclient/Tuple.h"

Value TagQuotaValue::toValue() const {
	Tuple tuple;
	tuple.appendDouble(reservedReadQuota);
	tuple.appendDouble(totalReadQuota);
	tuple.appendDouble(reservedWriteQuota);
	tuple.appendDouble(totalWriteQuota);
	return tuple.pack();
}

TagQuotaValue TagQuotaValue::fromValue(ValueRef value) {
	auto tuple = Tuple::unpack(value);
	ASSERT_EQ(tuple.size(), 4);
	TagQuotaValue result;
	result.reservedReadQuota = tuple.getDouble(0);
	result.totalReadQuota = tuple.getDouble(1);
	result.reservedWriteQuota = tuple.getDouble(2);
	result.totalWriteQuota = tuple.getDouble(3);
	return result;
}
