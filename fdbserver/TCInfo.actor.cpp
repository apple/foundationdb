/*
 * TCInfo.actor.cpp
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

#include "fdbclient/ServerKnobs.h"
#include "fdbserver/DDTeamCollection.h"
#include "fdbserver/TCInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class TCServerInfoImpl {
public:
	ACTOR static Future<Void> updateServerMetrics(TCServerInfo* server) {
		state StorageServerInterface ssi = server->lastKnownInterface;
		state Future<ErrorOr<GetStorageMetricsReply>> metricsRequest =
		    ssi.getStorageMetrics.tryGetReply(GetStorageMetricsRequest(), TaskPriority::DataDistributionLaunch);
		state Future<Void> resetRequest = Never();
		state Future<std::pair<StorageServerInterface, ProcessClass>> interfaceChanged(server->onInterfaceChanged);
		state Future<Void> serverRemoved(server->onRemoved);

		loop {
			choose {
				when(ErrorOr<GetStorageMetricsReply> rep = wait(metricsRequest)) {
					if (rep.present()) {
						server->serverMetrics = rep;
						if (server->updated.canBeSet()) {
							server->updated.send(Void());
						}
						break;
					}
					metricsRequest = Never();
					resetRequest = delay(SERVER_KNOBS->METRIC_DELAY, TaskPriority::DataDistributionLaunch);
				}
				when(std::pair<StorageServerInterface, ProcessClass> _ssi = wait(interfaceChanged)) {
					ssi = _ssi.first;
					interfaceChanged = server->onInterfaceChanged;
					resetRequest = Void();
				}
				when(wait(serverRemoved)) { return Void(); }
				when(wait(resetRequest)) { // To prevent a tight spin loop
					if (IFailureMonitor::failureMonitor().getState(ssi.getStorageMetrics.getEndpoint()).isFailed()) {
						resetRequest = IFailureMonitor::failureMonitor().onStateEqual(
						    ssi.getStorageMetrics.getEndpoint(), FailureStatus(false));
					} else {
						resetRequest = Never();
						metricsRequest = ssi.getStorageMetrics.tryGetReply(GetStorageMetricsRequest(),
						                                                   TaskPriority::DataDistributionLaunch);
					}
				}
			}
		}

		if (server->serverMetrics.get().lastUpdate < now() - SERVER_KNOBS->DD_SS_STUCK_TIME_LIMIT) {
			if (server->ssVersionTooFarBehind.get() == false) {
				TraceEvent("StorageServerStuck", server->collection->distributorId)
				    .detail("ServerId", server->id.toString())
				    .detail("LastUpdate", server->serverMetrics.get().lastUpdate);
				server->ssVersionTooFarBehind.set(true);
				server->collection->addLaggingStorageServer(server->lastKnownInterface.locality.zoneId().get());
			}
		} else if (server->serverMetrics.get().versionLag > SERVER_KNOBS->DD_SS_FAILURE_VERSIONLAG) {
			if (server->ssVersionTooFarBehind.get() == false) {
				TraceEvent(SevWarn, "SSVersionDiffLarge", server->collection->distributorId)
				    .detail("ServerId", server->id.toString())
				    .detail("VersionLag", server->serverMetrics.get().versionLag);
				server->ssVersionTooFarBehind.set(true);
				server->collection->addLaggingStorageServer(server->lastKnownInterface.locality.zoneId().get());
			}
		} else if (server->serverMetrics.get().versionLag < SERVER_KNOBS->DD_SS_ALLOWED_VERSIONLAG) {
			if (server->ssVersionTooFarBehind.get() == true) {
				TraceEvent("SSVersionDiffNormal", server->collection->distributorId)
				    .detail("ServerId", server->id.toString())
				    .detail("VersionLag", server->serverMetrics.get().versionLag);
				server->ssVersionTooFarBehind.set(false);
				server->collection->removeLaggingStorageServer(server->lastKnownInterface.locality.zoneId().get());
			}
		}
		return Void();
	}

	ACTOR static Future<Void> updateServerMetrics(Reference<TCServerInfo> server) {
		wait(updateServerMetrics(server.getPtr()));
		return Void();
	}

	ACTOR static Future<Void> serverMetricsPolling(TCServerInfo* server) {
		state double lastUpdate = now();
		loop {
			wait(server->updateServerMetrics());
			wait(delayUntil(lastUpdate + SERVER_KNOBS->STORAGE_METRICS_POLLING_DELAY +
			                    SERVER_KNOBS->STORAGE_METRICS_RANDOM_DELAY * deterministicRandom()->random01(),
			                TaskPriority::DataDistributionLaunch));
			lastUpdate = now();
		}
	}
};

class TCTeamInfoImpl {
public:
	ACTOR static Future<Void> updateStorageMetrics(TCTeamInfo* self) {
		std::vector<Future<Void>> updates;
		updates.reserve(self->servers.size());
		for (int i = 0; i < self->servers.size(); i++)
			updates.push_back(TCServerInfo::updateServerMetrics(self->servers[i]));
		wait(waitForAll(updates));
		return Void();
	}
};

Future<Void> TCServerInfo::updateServerMetrics() {
	return TCServerInfoImpl::updateServerMetrics(this);
}

Future<Void> TCServerInfo::updateServerMetrics(Reference<TCServerInfo> server) {
	return TCServerInfoImpl::updateServerMetrics(server);
}

Future<Void> TCServerInfo::serverMetricsPolling() {
	return TCServerInfoImpl::serverMetricsPolling(this);
}

TCServerInfo::~TCServerInfo() {
	if (collection && ssVersionTooFarBehind.get() && !lastKnownInterface.isTss()) {
		collection->removeLaggingStorageServer(lastKnownInterface.locality.zoneId().get());
	}
}

Future<Void> TCTeamInfo::updateStorageMetrics() {
	return TCTeamInfoImpl::updateStorageMetrics(this);
}
