/*
 * HealthMetricsRequestServer.h
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

#pragma once

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GrvProxyInterface.h"
#include "flow/flow.h"

class HealthMetricsRequestServer {
	GrvProxyInterface grvProxy;
	GetHealthMetricsReply healthMetricsReply;
	GetHealthMetricsReply detailedHealthMetricsReply;

public:
	explicit HealthMetricsRequestServer(GrvProxyInterface grvProxy);

	void update(HealthMetrics const& healthMetrics, bool detailed);
	Future<Void> run();
};
