/*
 * ClientDBInfo.h
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

#ifndef FDBCLIENT_CLIENTDBINFO_H
#define FDBCLIENT_CLIENTDBINFO_H
#pragma once

#include "MasterProxyInterface.h"

// ClientDBInfo is all the information needed by a database client to access the database
// It is returned (and kept up to date) by the OpenDatabaseRequest interface of ClusterInterface
struct ClientDBInfo {
	UID id;  // Changes each time anything else changes
	vector< MasterProxyInterface > proxies;
	double clientTxnInfoSampleRate;
	int64_t clientTxnInfoSizeLimit;
	ClientDBInfo() : clientTxnInfoSampleRate(std::numeric_limits<double>::infinity()), clientTxnInfoSizeLimit(-1) {}

	bool operator == (ClientDBInfo const& r) const { return id == r.id; }
	bool operator != (ClientDBInfo const& r) const { return id != r.id; }

	template <class Archive>
	void serialize(Archive& ar) {
		ASSERT( ar.protocolVersion() >= 0x0FDB00A200040001LL );
		ar & proxies & id & clientTxnInfoSampleRate & clientTxnInfoSizeLimit;
	}
};

#endif