/*
 * MetaclusterRegistration.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/MetaclusterRegistration.h"

std::string clusterTypeToString(const ClusterType& clusterType) {
	switch (clusterType) {
	case ClusterType::STANDALONE:
		return "standalone";
	case ClusterType::METACLUSTER_MANAGEMENT:
		return "metacluster_management";
	case ClusterType::METACLUSTER_DATA:
		return "metacluster_data";
	default:
		return "unknown";
	}
}

KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())>&
metacluster::metadata::metaclusterRegistration() {
	static KeyBackedObjectProperty<MetaclusterRegistrationEntry, decltype(IncludeVersion())> instance(
	    "\xff/metacluster/clusterRegistration"_sr, IncludeVersion());
	return instance;
}

KeyBackedObjectProperty<UnversionedMetaclusterRegistrationEntry, decltype(IncludeVersion())>&
metacluster::metadata::unversionedMetaclusterRegistration() {
	static KeyBackedObjectProperty<UnversionedMetaclusterRegistrationEntry, decltype(IncludeVersion())> instance(
	    "\xff/metacluster/clusterRegistration"_sr, IncludeVersion());
	return instance;
}
