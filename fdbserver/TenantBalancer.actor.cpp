/*
 * TenantBalancer.actor.cpp
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/TenantBalancerInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> extractClientInfo(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                            Reference<AsyncVar<ClientDBInfo>> info) {
	loop {
		ClientDBInfo clientInfo = dbInfo->get().client;
		info->set(clientInfo);
		wait(dbInfo->onChange());
	}
}

struct TenantBalancer {
	TenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : tbi(tbi), dbInfo(dbInfo), actors(false) {
		auto info = makeReference<AsyncVar<ClientDBInfo>>();
		db = DatabaseContext::create(info,
		                             extractClientInfo(dbInfo, info),
		                             LocalityData(),
		                             EnableLocalityLoadBalance::True,
		                             TaskPriority::DefaultEndpoint,
		                             LockAware::True); // TODO: Check these params

		agent = DatabaseBackupAgent(db);
	}

	TenantBalancerInterface tbi;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;

	Database db;

	ActorCollection actors;
	DatabaseBackupAgent agent;
};

ACTOR Future<Void> moveTenantToCluster(TenantBalancer* self, MoveTenantToClusterRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		MoveTenantToClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> receiveTenantFromCluster(TenantBalancer* self, ReceiveTenantFromClusterRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		ReceiveTenantFromClusterReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> getActiveMovements(TenantBalancer* self, GetActiveMovementsRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		GetActiveMovementsReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishSourceMovement(TenantBalancer* self, FinishSourceMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		FinishSourceMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> finishDestinationMovement(TenantBalancer* self, FinishDestinationMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		FinishDestinationMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> abortMovement(TenantBalancer* self, AbortMovementRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		AbortMovementReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> cleanupMovementSource(TenantBalancer* self, CleanupMovementSourceRequest req) {
	wait(delay(0)); // TODO: this is temporary; to be removed when we add code

	try {
		CleanupMovementSourceReply reply;
		req.reply.send(reply);
	} catch (Error& e) {
		req.reply.sendError(e);
	}

	return Void();
}

ACTOR Future<Void> tenantBalancerCore(TenantBalancer* self) {
	loop choose {
		when(MoveTenantToClusterRequest req = waitNext(self->tbi.moveTenantToCluster.getFuture())) {
			self->actors.add(moveTenantToCluster(self, req));
		}
		when(ReceiveTenantFromClusterRequest req = waitNext(self->tbi.receiveTenantFromCluster.getFuture())) {
			self->actors.add(receiveTenantFromCluster(self, req));
		}
		when(GetActiveMovementsRequest req = waitNext(self->tbi.getActiveMovements.getFuture())) {
			self->actors.add(getActiveMovements(self, req));
		}
		when(FinishSourceMovementRequest req = waitNext(self->tbi.finishSourceMovement.getFuture())) {
			self->actors.add(finishSourceMovement(self, req));
		}
		when(FinishDestinationMovementRequest req = waitNext(self->tbi.finishDestinationMovement.getFuture())) {
			self->actors.add(finishDestinationMovement(self, req));
		}
		when(AbortMovementRequest req = waitNext(self->tbi.abortMovement.getFuture())) {
			self->actors.add(abortMovement(self, req));
		}
		when(CleanupMovementSourceRequest req = waitNext(self->tbi.cleanupMovementSource.getFuture())) {
			self->actors.add(cleanupMovementSource(self, req));
		}
		when(wait(self->actors.getResult())) {}
	}
}

ACTOR Future<Void> tenantBalancer(TenantBalancerInterface tbi, Reference<AsyncVar<ServerDBInfo> const> db) {
	state TenantBalancer self(tbi, db);

	try {
		wait(tenantBalancerCore(&self));
		throw internal_error();
	} catch (Error& e) {
		TraceEvent("TenantBalancerTerminated").error(e);
		throw e;
	}
}