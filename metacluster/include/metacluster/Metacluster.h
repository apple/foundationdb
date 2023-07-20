/*
 * Metacluster.h
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

#ifndef METACLUSTER_METACLUSTER_H
#define METACLUSTER_METACLUSTER_H
#pragma once

// Including this file exposes most of the public Metacluster API. All of the included functionality should be part of
// the metacluster namespace. It should not be necessary to include any of the below included files directly in any
// external source file.
//
// There are a few headers that may be useful externally and are not exposed here:
//
// * MetaclusterUtil.actor.h - provides the metacluster::util namespace that includes some additional utility functions
// * MetaclusterMetrics.h - some functionality to get basic data about the metacluster for status, etc.
//
// A few more headers are useful for testing:
//
// * MetaclusterConsistency.actor.h - a metadata consistency check useful for testing of the metacluster
// * TenantConsistency.actor.h - a metadata consistency check useful for testing of tenants
// * MetaclusterData.actor.h - functionality to read all metacluster metadata into objects

#include "metacluster/ConfigureCluster.h"
#include "metacluster/ConfigureTenant.actor.h"
#include "metacluster/CreateMetacluster.actor.h"
#include "metacluster/CreateTenant.actor.h"
#include "metacluster/DecommissionMetacluster.actor.h"
#include "metacluster/DeleteTenant.actor.h"
#include "metacluster/GetCluster.actor.h"
#include "metacluster/GetTenant.actor.h"
#include "metacluster/GetTenantGroup.actor.h"
#include "metacluster/ListClusters.actor.h"
#include "metacluster/ListTenantGroups.actor.h"
#include "metacluster/ListTenants.actor.h"
#include "metacluster/MetaclusterTypes.h"
#include "metacluster/RegisterCluster.actor.h"
#include "metacluster/RemoveCluster.actor.h"
#include "metacluster/RenameTenant.actor.h"
#include "metacluster/RestoreCluster.actor.h"

#endif
