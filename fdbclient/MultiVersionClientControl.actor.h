/*
 * MultiVersionClientControl.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_G_H)
#define FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_G_H
#include "fdbclient/MultiVersionClientControl.actor.g.h"
#elif !defined(FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_H)
#define FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_H

#include <string>
#include "fdbclient/NativeAPI.actor.h"

#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<Void> uploadClientLibrary(Database db, StringRef metadataString, StringRef libFilePath);

#include "flow/unactorcompiler.h"
#endif