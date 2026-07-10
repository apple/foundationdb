/*
 * Locality.cpp
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

#include "fdbrpc/Locality.h"

const UID LocalityData::UNSET_ID = UID(0x0ccb4e0feddb5583, 0x010f6b77d9d10ece);
alignas(8) const StringRef LocalityData::keyProcessId = "processid"_sr;
alignas(8) const StringRef LocalityData::keyZoneId = "zoneid"_sr;
alignas(8) const StringRef LocalityData::keyDcId = "dcid"_sr;
alignas(8) const StringRef LocalityData::keyMachineId = "machineid"_sr;
alignas(8) const StringRef LocalityData::keyDataHallId = "data_hall"_sr;
alignas(8) const StringRef LocalityData::ExcludeLocalityPrefix = "locality_"_sr;

LBDistance::Type loadBalanceDistance(LocalityData const& loc1, LocalityData const& loc2, NetworkAddress const& addr2) {
	if (FLOW_KNOBS->LOAD_BALANCE_ZONE_ID_LOCALITY_ENABLED && loc1.zoneId().present() &&
	    loc1.zoneId() == loc2.zoneId()) {
		return LBDistance::SAME_MACHINE;
	}
	// FIXME: add this back in when load balancing works with local requests
	// if ( g_network->isAddressOnThisHost( addr2 ) )
	//	return LBDistance::SAME_MACHINE;
	if (FLOW_KNOBS->LOAD_BALANCE_DC_ID_LOCALITY_ENABLED && loc1.dcId().present() && loc1.dcId() == loc2.dcId()) {
		return LBDistance::SAME_DC;
	}
	return LBDistance::DISTANT;
}
