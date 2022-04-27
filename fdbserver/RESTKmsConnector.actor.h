/*
 * RESTKmsConnector.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTKMSCONNECTOR_ACTOR_G_H)
#define FDBSERVER_RESTKMSCONNECTOR_ACTOR_G_H
#include "fdbserver/RESTKmsConnector.actor.g.h"
#elif !defined(FDBSERVER_RESTKMSCONNECTOR_ACTOR_H)
#define FDBSERVER_RESTKMSCONNECTOR_ACTOR_H

#include "fdbserver/KmsConnector.h"

class RESTKmsConnector : public KmsConnector {
public:
	RESTKmsConnector() = default;
	Future<Void> connectorCore(KmsConnectorInterface interf);
};

#endif
