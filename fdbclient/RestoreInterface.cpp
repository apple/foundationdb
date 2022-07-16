/*
 * RestoreInterface.h
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

#include "fdbclient/RestoreInterface.h"
#include "flow/serialize.h"

const KeyRef restoreRequestDoneKey = "\xff\x02/restoreRequestDone"_sr;
const KeyRef restoreRequestTriggerKey = "\xff\x02/restoreRequestTrigger"_sr;
const KeyRangeRef restoreRequestKeys("\xff\x02/restoreRequests/"_sr, "\xff\x02/restoreRequests0"_sr);

// Encode and decode restore request value
Value restoreRequestTriggerValue(UID randomID, int numRequests) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreRequestTriggerValue()));
	wr << numRequests;
	wr << randomID;
	return wr.toValue();
}

int decodeRestoreRequestTriggerValue(ValueRef const& value) {
	int s;
	UID randomID;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	reader >> randomID;
	return s;
}

Key restoreRequestKeyFor(int index) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreRequestKeys.begin);
	wr << index;
	return wr.toValue();
}

Value restoreRequestValue(RestoreRequest const& request) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreRequestValue()));
	wr << request;
	return wr.toValue();
}
