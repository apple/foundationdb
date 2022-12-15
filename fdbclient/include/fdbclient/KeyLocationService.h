/*
 * KeyLocationService.h
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
#ifndef FOUNDATIONDB_KEYLOCATIONSERVICE_H
#define FOUNDATIONDB_KEYLOCATIONSERVICE_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/DatabaseContext.h"

class IKeyLocationService {

	// If isBackward == true, returns the shard containing the key before 'key' (an infinitely long, inexpressible key).
	// Otherwise returns the shard containing key. It's possible the returned location is a failed interface.
	virtual Future<KeyRangeLocationInfo> getKeyLocation(TenantInfo tenant,
	                                                    Key key,
	                                                    SpanContext spanContext,
	                                                    Optional<UID> debugID,
	                                                    UseProvisionalProxies useProvisionalProxies,
	                                                    Reverse isBackward,
	                                                    Version version) = 0;

	virtual Future<std::vector<KeyRangeLocationInfo>> getKeyRangeLocations(TenantInfo tenant,
	                                                                       KeyRange keys,
	                                                                       int limit,
	                                                                       Reverse reverse,
	                                                                       SpanContext spanContext,
	                                                                       Optional<UID> debugID,
	                                                                       UseProvisionalProxies useProvisionalProxies,
	                                                                       Version version) = 0;
};

#endif // FOUNDATIONDB_KEYLOCATIONSERVICE_H
