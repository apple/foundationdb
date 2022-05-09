/*
 * MetaclusterInternalDataClusterSpecialKeys.actor.cpp
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

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MetaclusterManagement.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const KeyRangeRef MetaclusterInternalDataClusterImpl::submoduleRange =
    KeyRangeRef("data_cluster/"_sr, "data_cluster0"_sr);

MetaclusterInternalDataClusterImpl::MetaclusterInternalDataClusterImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> MetaclusterInternalDataClusterImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                 KeyRangeRef kr,
                                                                 GetRangeLimits limitsHint) const {
	return RangeResult();
}

Future<Void> MetaclusterInternalDataClusterImpl::processDataClusterCommand(ReadYourWritesTransaction* ryw) {
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(
	    KeyRangeRef("data_cluster/data_cluster/"_sr, "data_cluster/data_cluster0"_sr)
	        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METACLUSTER_INTERNAL).begin));

	std::vector<Future<Void>> clusterManagementFutures;

	for (auto itr : ranges) {
		if (!itr.value().first) {
			continue;
		}

		KeyRangeRef range =
		    itr.range()
		        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::METACLUSTER_INTERNAL).begin)
		        .removePrefix(MetaclusterInternalDataClusterImpl::submoduleRange.begin)
		        .removePrefix("data_cluster/"_sr);

		if (!itr.value().second.present()) {
			continue;
		}

		if (range.begin == "register"_sr) {
			clusterManagementFutures.push_back(
			    MetaclusterAPI::dataClusterRegister(&ryw->getTransaction(), itr.value().second.get()));
		} else if (range.begin == "remove"_sr) {
			clusterManagementFutures.push_back(MetaclusterAPI::dataClusterRemove(&ryw->getTransaction()));
		}
	}

	return waitForAll(clusterManagementFutures);
}

Future<Void> MetaclusterInternalDataClusterImpl::processTenantCommand(ReadYourWritesTransaction* ryw) {
	// TODO
	return Void();
}

Future<Optional<std::string>> MetaclusterInternalDataClusterImpl::commit(ReadYourWritesTransaction* ryw) {
	return tag(processDataClusterCommand(ryw) && processTenantCommand(ryw), Optional<std::string>());
}