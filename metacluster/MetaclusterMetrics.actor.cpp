/*
 * MetaclusterMetrics.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"

#include "metacluster/Metacluster.h"
#include "metacluster/MetaclusterMetrics.h"

#include "flow/actorcompiler.h" // has to be last include

namespace metacluster {
namespace internal {
ACTOR Future<MetaclusterMetrics> getMetaclusterMetricsImpl(Database db) {
	state Reference<ReadYourWritesTransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state std::map<ClusterName, DataClusterMetadata> clusters;
			state int64_t tenantCount;
			wait(store(clusters,
			           metacluster::listClustersTransaction(tr, ""_sr, "\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
			     store(tenantCount,
			           metacluster::metadata::management::tenantMetadata().tenantCount.getD(tr, Snapshot::False, 0)));

			state std::pair<ClusterUsage, ClusterUsage> capacityNumbers = util::metaclusterCapacity(clusters);

			MetaclusterMetrics metrics;
			metrics.numTenants = tenantCount;
			metrics.numDataClusters = clusters.size();
			metrics.tenantGroupCapacity = capacityNumbers.first.numTenantGroups;
			metrics.tenantGroupsAllocated = capacityNumbers.second.numTenantGroups;

			TraceEvent("MetaclusterCapacity")
			    .detail("TotalTenants", metrics.numTenants)
			    .detail("DataClusters", metrics.numDataClusters)
			    .detail("TenantGroupCapacity", metrics.tenantGroupCapacity)
			    .detail("TenantGroupsAllocated", metrics.tenantGroupsAllocated);

			CODE_PROBE(true, "Got metacluster metrics");

			return metrics;
		} catch (Error& e) {
			TraceEvent("MetaclusterUpdaterError").error(e);
			if (e.code() == error_code_unsupported_metacluster_version) {
				TraceEvent(SevWarnAlways, "MetaclusterMetricsFailure").error(e);
				MetaclusterMetrics metrics;
				metrics.error = e.what();
				return metrics;
			}

			wait(tr->onError(e));
		}
	}
}
} // namespace internal

Future<MetaclusterMetrics> MetaclusterMetrics::getMetaclusterMetrics(Database db) {
	return internal::getMetaclusterMetricsImpl(db);
}
} // namespace metacluster
