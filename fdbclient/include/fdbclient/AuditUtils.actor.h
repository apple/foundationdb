/*
 * AuditUtils.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_AUDITUTILS_ACTOR_G_H)
#define FDBCLIENT_AUDITUTILS_ACTOR_G_H
#include "fdbclient/AuditUtils.actor.g.h"
#elif !defined(FDBCLIENT_AUDITUTILS_ACTOR_H)
#define FDBCLIENT_AUDITUTILS_ACTOR_H
#pragma once

#include "fdbclient/Audit.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/fdbrpc.h"

#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<UID> persistNewAuditState(Database cx, AuditStorageState auditState);
ACTOR Future<Void> persistAuditState(Database cx, AuditStorageState auditState);
ACTOR Future<AuditStorageState> getAuditState(Database cx, AuditType type, UID id);
ACTOR Future<std::vector<AuditStorageState>> getLatestAuditStates(Database cx, AuditType type, int num);

ACTOR Future<Void> persistAuditStateMap(Database cx, AuditStorageState auditState);
ACTOR Future<std::vector<AuditStorageState>> getAuditStateForRange(Database cx, UID id, KeyRange range);

ACTOR Future<Void> persistAuditMetadataState(Database cx, UID id, AuditStorageState auditState);
ACTOR Future<std::vector<AuditStorageState>> getAuditMetadataState(Database cx, UID id);
#include "flow/unactorcompiler.h"
#endif
