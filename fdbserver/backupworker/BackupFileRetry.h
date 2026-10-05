/*
 * BackupFileRetry.h
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

#include "flow/flow.h"

// A retry budget belongs to one immutable output segment. Each permitted attempt must recreate its files;
// failed append/finish operations can leave file offsets and publication state indeterminate.
class BackupFileRetry {
public:
	explicit BackupFileRetry(UID workerId);
	BackupFileRetry(UID workerId, int retryLimit, double initialDelay, double maxDelay);
	Future<Void> onError(Error error);

private:
	UID workerId;
	int retryLimit;
	int retries = 0;
	double nextDelay;
	double maxDelay;
	double startedAt;
};
