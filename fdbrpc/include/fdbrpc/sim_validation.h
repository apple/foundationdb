/*
 * sim_validation.h
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

#ifndef SIM_VALIDATION_H
#define SIM_VALIDATION_H
#pragma once

#include "flow/IRandom.h"
#include "flow/Trace.h"

// These functions track a value which is expected to be durable within a certain range of versions.
// They rely on the ability to magically store perfectly durable data in the simulator, so they have no
// effect outside simulation.
// Typical use is something like:
//   debug_advanceMaxCommittedVersion( self->id, version );
//   wait( commit(version) );
//   debug_advanceMinCommittedVersion( self->id, version );
// and then a call to debug_checkRestoredVersion() after some kind of reboot or recovery event

void debug_advanceCommittedVersions(UID id, int64_t minVersion, int64_t maxVersion);
void debug_advanceMaxCommittedVersion(UID id, int64_t version);
void debug_advanceMinCommittedVersion(UID id, int64_t version);

void debug_setVersionCheckEnabled(UID id, bool enabled);
void debug_removeVersions(UID id);
bool debug_versionsExist(UID id);
bool debug_checkRestoredVersion(UID id, int64_t version, std::string context, Severity sev = SevError);
bool debug_checkMinRestoredVersion(UID id, int64_t version, std::string context, Severity sev = SevError);
bool debug_checkMaxRestoredVersion(UID id, int64_t version, std::string context, Severity sev = SevError);

bool debug_isCheckRelocationDuration();
void debug_setCheckRelocationDuration(bool check);

void debug_advanceVersionTimestamp(int64_t version, double t);
bool debug_checkVersionTime(int64_t version, double t, std::string context, Severity sev = SevError);

#endif
