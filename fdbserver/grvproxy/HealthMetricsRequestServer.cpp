/*
 * HealthMetricsRequestServer.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "HealthMetricsRequestServer.h"

HealthMetricsRequestServer::HealthMetricsRequestServer(GrvProxyInterface grvProxy) : grvProxy(grvProxy) {}

void HealthMetricsRequestServer::update(HealthMetrics const& healthMetrics, bool detailed) {
	healthMetricsReply.update(healthMetrics, detailed, true);
	if (detailed) {
		detailedHealthMetricsReply.update(healthMetrics, true, true);
	}
}

Future<Void> HealthMetricsRequestServer::run() {
	while (true) {
		GetHealthMetricsRequest req = co_await grvProxy.getHealthMetrics.getFuture();
		if (req.detailed) {
			req.reply.send(detailedHealthMetricsReply);
		} else {
			req.reply.send(healthMetricsReply);
		}
	}
}
