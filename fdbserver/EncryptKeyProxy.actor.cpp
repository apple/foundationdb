/*
 * EncryptKeyProxy.actor.cpp
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

#include "fdbserver/EncryptKeyProxyInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/EventTypes.actor.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct EncryptKeyProxyData : NonCopyable, ReferenceCounted<EncryptKeyProxyData> {
	UID myId;
	PromiseStream<Future<Void>> addActor;

	explicit EncryptKeyProxyData(UID id) : myId(id) {}
};

ACTOR Future<Void> encryptKeyProxyServer(EncryptKeyProxyInterface ekpInterface, Reference<AsyncVar<ServerDBInfo>> db) {
	state Reference<EncryptKeyProxyData> self(new EncryptKeyProxyData(ekpInterface.id()));
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(self->addActor.getFuture());
	self->addActor.send(traceRole(Role::ENCRYPT_KEY_PROXY, ekpInterface.id()));

	TraceEvent("EKP_Start", self->myId).log();

	// TODO(ahusain): skeleton implementation, more to come
	try {
		loop choose {
			when(HaltEncryptKeyProxyRequest req = waitNext(ekpInterface.haltEncryptKeyProxy.getFuture())) {
				req.reply.send(Void());
				TraceEvent("EKP_Halted", ekpInterface.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		TraceEvent("EKP_Terminated", ekpInterface.id()).errorUnsuppressed(e);
	}

	return Void();
}