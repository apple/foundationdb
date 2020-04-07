/*
 * ClusterRecruitmentInterface.h
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

#ifndef FDBSERVER_CLUSTERRECRUITMENTINTERFACE_H
#define FDBSERVER_CLUSTERRECRUITMENTINTERFACE_H
#pragma once

#include <vector>

#include "fdbclient/ClusterInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/Knobs.h"



#include "fdbserver/ServerDBInfo.h" // include order hack

#endif
