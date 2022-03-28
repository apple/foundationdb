/*
 * RestoreWorkerInterface.actor.cpp
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

#include "fdbserver/RestoreWorkerInterface.actor.h"
#include "flow/actorcompiler.h" // must be last include

const KeyRef restoreLeaderKey = "\xff\x02/restoreLeader"_sr;
const KeyRangeRef restoreWorkersKeys("\xff\x02/restoreWorkers/"_sr, "\xff\x02/restoreWorkers0"_sr);
const KeyRef restoreStatusKey = "\xff\x02/restoreStatus/"_sr;
const KeyRangeRef restoreApplierKeys("\xff\x02/restoreApplier/"_sr, "\xff\x02/restoreApplier0"_sr);
const KeyRef restoreApplierTxnValue = "1"_sr;

// restoreApplierKeys: track atomic transaction progress to ensure applying atomicOp exactly once
// Version and batchIndex are passed in as LittleEndian,
// they must be converted to BigEndian to maintain ordering in lexical order
const Key restoreApplierKeyFor(UID const& applierID, int64_t batchIndex, Version version) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreApplierKeys.begin);
	wr << applierID << bigEndian64(batchIndex) << bigEndian64(version);
	return wr.toValue();
}

std::tuple<UID, int64_t, Version> decodeRestoreApplierKey(ValueRef const& key) {
	BinaryReader rd(key, Unversioned());
	UID applierID;
	int64_t batchIndex;
	Version version;
	rd >> applierID >> batchIndex >> version;
	return std::make_tuple(applierID, bigEndian64(batchIndex), bigEndian64(version));
}

// Encode restore worker key for workerID
const Key restoreWorkerKeyFor(UID const& workerID) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreWorkersKeys.begin);
	wr << workerID;
	return wr.toValue();
}

// Encode restore agent value
const Value restoreWorkerInterfaceValue(RestoreWorkerInterface const& cmdInterf) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreWorkerInterfaceValue()));
	wr << cmdInterf;
	return wr.toValue();
}

RestoreWorkerInterface decodeRestoreWorkerInterfaceValue(ValueRef const& value) {
	RestoreWorkerInterface s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

Value restoreRequestDoneVersionValue(Version readVersion) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreRequestDoneVersionValue()));
	wr << readVersion;
	return wr.toValue();
}
Version decodeRestoreRequestDoneVersionValue(ValueRef const& value) {
	Version v;
	BinaryReader reader(value, IncludeVersion());
	reader >> v;
	return v;
}

RestoreRequest decodeRestoreRequestValue(ValueRef const& value) {
	RestoreRequest s;
	BinaryReader reader(value, IncludeVersion());
	reader >> s;
	return s;
}

// TODO: Register restore performance data to restoreStatus key
const Key restoreStatusKeyFor(StringRef statusType) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(restoreStatusKey);
	wr << statusType;
	return wr.toValue();
}

const Value restoreStatusValue(double val) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withRestoreStatusValue()));
	wr << StringRef(std::to_string(val));
	return wr.toValue();
}
